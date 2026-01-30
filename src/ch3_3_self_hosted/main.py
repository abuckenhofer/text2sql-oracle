#!/usr/bin/env python3
"""
Chapter 3.3 — Python Middleware + Self-Hosted LLM (Ollama).

Same workflow as ch3_2 but calls a locally running Ollama instance
instead of a cloud API.  All data stays within your network.

Prerequisites:
  1. Install Ollama:  https://ollama.com
  2. Pull a model:    ollama pull llama3.1:8b
  3. Ollama must be running (default port 11434)
"""

import json
import os
import re
from typing import Tuple

import httpx
import oracledb
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "..", "schema.json")
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1:8b")


def get_db_connection() -> oracledb.Connection:
    return oracledb.connect(
        user=os.getenv("ORA_USER"),
        password=os.getenv("ORA_PASSWORD"),
        dsn=os.getenv("ORA_DSN"),
    )


# ── schema context ──────────────────────────────────────────

def load_schema_context(schema_file: str = SCHEMA_FILE) -> str:
    with open(schema_file) as f:
        schema = json.load(f)

    parts = [f"Database Schema: {schema['schema_name']}\n"]
    for table in schema["tables"]:
        parts.append(f"\nTable: {table['name']}")
        parts.append(f"Description: {table['description']}")
        parts.append("Columns:")
        for col in table["columns"]:
            parts.append(f"  - {col['name']} ({col['type']}): {col['description']}")

    if schema.get("relationships"):
        parts.append("\nRelationships:")
        for rel in schema["relationships"]:
            parts.append(f"  - {rel['from']} -> {rel['to']} ({rel['type']})")

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


def generate_sql_ollama(question: str, schema_context: str) -> tuple[str, str]:
    prompt = f"{SYSTEM_PROMPT}\n\n{schema_context}\n\nQuestion: {question}\n\nSQL:"

    response = httpx.post(
        f"{OLLAMA_BASE_URL}/api/generate",
        json={
            "model": OLLAMA_MODEL,
            "prompt": prompt,
            "stream": False,
            "options": {
                "temperature": 0.0,
                "num_predict": 500,
            },
        },
        timeout=120.0,
    )
    response.raise_for_status()

    sql = response.json()["response"].strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()
    # Trim trailing explanation if present
    if "\n\n" in sql:
        sql = sql.split("\n\n")[0]
    sql = sql.rstrip(";")
    return sql, prompt


def generate_sql_vllm(question: str, schema_context: str) -> tuple[str, str]:
    """Alternative: call a vLLM server exposing an OpenAI-compatible API."""
    vllm_url = os.getenv("VLLM_BASE_URL", "http://localhost:8000")
    vllm_model = os.getenv("VLLM_MODEL", "meta-llama/Llama-3.1-70B-Instruct")

    user_prompt = f"{schema_context}\n\nQuestion: {question}\n\nSQL:"
    full_prompt = f"{SYSTEM_PROMPT}\n\n{user_prompt}"

    response = httpx.post(
        f"{vllm_url}/v1/chat/completions",
        json={
            "model": vllm_model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            "temperature": 0.0,
            "max_tokens": 500,
        },
        timeout=120.0,
    )
    response.raise_for_status()

    sql = response.json()["choices"][0]["message"]["content"].strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()
    return sql, full_prompt


# ── validation & execution (same as ch3_2) ─────────────────

def validate_sql(sql: str, conn: oracledb.Connection) -> Tuple[bool, str]:
    cursor = conn.cursor()
    try:
        cursor.execute(f"EXPLAIN PLAN FOR {sql}")
        cursor.execute(
            "SELECT plan_table_output FROM TABLE(DBMS_XPLAN.DISPLAY('PLAN_TABLE', NULL, 'BASIC'))"
        )
        plan = "\n".join(row[0] for row in cursor.fetchall())
        return True, plan
    except oracledb.DatabaseError as exc:
        (error_obj,) = exc.args
        return False, f"Validation failed: {error_obj.message}"
    finally:
        cursor.close()


def is_ddl(sql: str) -> bool:
    ddl_keywords = [r"\bCREATE\b", r"\bALTER\b", r"\bDROP\b", r"\bTRUNCATE\b", r"\bRENAME\b"]
    return any(re.search(p, sql, re.IGNORECASE) for p in ddl_keywords)


def execute_sql(sql: str, conn: oracledb.Connection, max_rows: int = 1000) -> list[dict]:
    if "FETCH FIRST" not in sql.upper():
        sql = f"{sql} FETCH FIRST {max_rows} ROWS ONLY"

    cursor = conn.cursor()
    try:
        cursor.execute(sql)
        columns = [d[0] for d in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        cursor.close()


# ── main workflow ───────────────────────────────────────────

def process_question(question: str, use_vllm: bool = False) -> dict:
    result = {
        "question": question,
        "generated_sql": None,
        "validation_status": None,
        "results": None,
        "prompt": None,
        "error": None,
        "model_backend": "vllm" if use_vllm else "ollama",
    }

    try:
        schema_context = load_schema_context()

        if use_vllm:
            sql, prompt = generate_sql_vllm(question, schema_context)
        else:
            sql, prompt = generate_sql_ollama(question, schema_context)

        result["generated_sql"] = sql
        result["prompt"] = prompt

        if is_ddl(sql):
            result["error"] = "DDL statements are not permitted."
            return result

        conn = get_db_connection()
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
    use_vllm = os.getenv("USE_VLLM", "").lower() in ("1", "true", "yes")

    question = input("Ask a question (or press Enter for default): ").strip()
    if not question:
        question = "Show customers who placed orders in the last 30 days"

    res = process_question(question, use_vllm=use_vllm)

    print(f"\nBackend: {res['model_backend']}")
    print(f"Question: {res['question']}")
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
