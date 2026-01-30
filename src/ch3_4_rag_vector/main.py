#!/usr/bin/env python3
"""
Chapter 3.4 — RAG-based SQL generation with Oracle AI Vector Search.

Instead of sending the entire schema to the LLM, this approach:
1. Embeds the user question using sentence-transformers
2. Finds the most relevant tables via VECTOR_DISTANCE in Oracle
3. Sends only those tables as context to the LLM (Ollama)

This scales to schemas with hundreds of tables where the full schema
would exceed the LLM context window.

Prerequisites:
  1. Run setup_vectors.py first to embed the schema
  2. Ollama must be running with a model pulled
"""

import json
import os
import re
from typing import Tuple

import httpx
import oracledb
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1:8b")
TOP_K_TABLES = int(os.getenv("RAG_TOP_K", "5"))

embedder = SentenceTransformer(EMBEDDING_MODEL)


def get_db_connection() -> oracledb.Connection:
    return oracledb.connect(
        user=os.getenv("ORA_USER"),
        password=os.getenv("ORA_PASSWORD"),
        dsn=os.getenv("ORA_DSN"),
    )


# ── RAG: retrieve relevant schema context ──────────────────

def retrieve_relevant_tables(question: str, conn: oracledb.Connection,
                             top_k: int = TOP_K_TABLES) -> list[dict]:
    """Embed the question and find most similar schema entries via Oracle VECTOR_DISTANCE."""
    q_emb = embedder.encode(question)
    vec_str = "[" + ",".join(str(float(x)) for x in q_emb) + "]"

    cur = conn.cursor()
    cur.execute("""
        SELECT table_name, description, metadata
        FROM schema_embeddings
        ORDER BY VECTOR_DISTANCE(embedding, TO_VECTOR(:1), COSINE)
        FETCH FIRST :2 ROWS ONLY
    """, [vec_str, top_k])

    results = []
    for table_name, description, metadata_clob in cur.fetchall():
        metadata_str = metadata_clob.read() if hasattr(metadata_clob, 'read') else metadata_clob
        results.append({
            "table_name": table_name,
            "description": description,
            "metadata": json.loads(metadata_str),
        })
    cur.close()
    return results


def build_schema_context(relevant_tables: list[dict]) -> str:
    """Build a schema context string from RAG-retrieved tables only."""
    parts = ["Database Schema (relevant tables retrieved via vector search):\n"]

    for entry in relevant_tables:
        meta = entry["metadata"]
        if entry["table_name"] == "_RELATIONSHIPS":
            parts.append("\nRelationships:")
            for rel in meta:
                parts.append(f"  - {rel['from']} -> {rel['to']} ({rel['type']})")
        else:
            parts.append(f"\nTable: {meta['name']}")
            parts.append(f"Description: {meta['description']}")
            parts.append("Columns:")
            for col in meta["columns"]:
                parts.append(f"  - {col['name']} ({col['type']}): {col['description']}")

    return "\n".join(parts)


# ── SQL generation (Ollama) ────────────────────────────────

SYSTEM_PROMPT = """\
You are an expert Oracle SQL generator.
Generate syntactically correct Oracle SQL based on the provided schema.
Rules:
- Use explicit column names, never SELECT *
- Include appropriate WHERE clauses for filtering
- Use FETCH FIRST N ROWS ONLY for limits, not ROWNUM
- Add helpful column aliases
- Use standard Oracle date functions
Return only the SQL query, no explanations or markdown."""


def generate_sql(question: str, schema_context: str) -> tuple[str, str]:
    prompt = f"{SYSTEM_PROMPT}\n\n{schema_context}\n\nQuestion: {question}\n\nSQL:"

    response = httpx.post(
        f"{OLLAMA_BASE_URL}/api/generate",
        json={
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.0, "num_predict": 500},
        },
        timeout=120.0,
    )
    response.raise_for_status()

    sql = response.json()["response"].strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()
    if "\n\n" in sql:
        sql = sql.split("\n\n")[0]
    sql = sql.rstrip(";")
    return sql, prompt


# ── validation & execution ─────────────────────────────────

def validate_sql(sql: str, conn: oracledb.Connection) -> Tuple[bool, str]:
    cur = conn.cursor()
    try:
        cur.execute(f"EXPLAIN PLAN FOR {sql}")
        cur.execute(
            "SELECT plan_table_output FROM TABLE(DBMS_XPLAN.DISPLAY('PLAN_TABLE', NULL, 'BASIC'))"
        )
        return True, "\n".join(row[0] for row in cur.fetchall())
    except oracledb.DatabaseError as exc:
        (error_obj,) = exc.args
        return False, f"Validation failed: {error_obj.message}"
    finally:
        cur.close()


def is_ddl(sql: str) -> bool:
    return any(
        re.search(p, sql, re.IGNORECASE)
        for p in [r"\bCREATE\b", r"\bALTER\b", r"\bDROP\b", r"\bTRUNCATE\b"]
    )


def execute_sql(sql: str, conn: oracledb.Connection, max_rows: int = 1000) -> list[dict]:
    if "FETCH FIRST" not in sql.upper():
        sql = f"{sql} FETCH FIRST {max_rows} ROWS ONLY"
    cur = conn.cursor()
    try:
        cur.execute(sql)
        columns = [d[0] for d in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        cur.close()


# ── main workflow ───────────────────────────────────────────

def process_question(question: str) -> dict:
    result = {
        "question": question,
        "retrieved_tables": None,
        "prompt": None,
        "generated_sql": None,
        "validation_status": None,
        "results": None,
        "error": None,
    }

    try:
        conn = get_db_connection()

        # RAG: retrieve relevant tables
        relevant = retrieve_relevant_tables(question, conn)
        result["retrieved_tables"] = [r["table_name"] for r in relevant]

        # Build context from retrieved tables only
        schema_context = build_schema_context(relevant)

        # Generate SQL
        sql, prompt = generate_sql(question, schema_context)
        result["generated_sql"] = sql
        result["prompt"] = prompt

        if is_ddl(sql):
            result["error"] = "DDL statements are not permitted."
            conn.close()
            return result

        # Validate & execute
        is_valid, msg = validate_sql(sql, conn)
        result["validation_status"] = "valid" if is_valid else "invalid"

        if not is_valid:
            result["error"] = msg
            conn.close()
            return result

        result["results"] = execute_sql(sql, conn)
        result["result_count"] = len(result["results"])
        conn.close()

    except Exception as exc:
        result["error"] = str(exc)

    return result


if __name__ == "__main__":
    question = input("Ask a question (or press Enter for default): ").strip()
    if not question:
        question = "Which products generated the most revenue"

    res = process_question(question)

    print(f"\nQuestion: {res['question']}")
    print(f"Retrieved tables (via vector search): {res['retrieved_tables']}")
    print(f"\n{'='*60}")
    print(f"LLM Prompt:\n{'='*60}\n{res['prompt']}\n{'='*60}")
    print(f"\nGenerated SQL:\n{res['generated_sql']}")
    print(f"Validation: {res['validation_status']}")

    if res["results"]:
        print(f"\nResults ({res['result_count']} rows):")
        for row in res["results"]:
            print(row)
    elif res["error"]:
        print(f"\nError: {res['error']}")
