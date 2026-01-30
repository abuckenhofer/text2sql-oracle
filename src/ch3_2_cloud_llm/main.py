#!/usr/bin/env python3
"""
Chapter 3.2 — Python Middleware + Cloud LLM (OpenAI).

Accepts natural-language questions, generates Oracle SQL via OpenAI,
validates with EXPLAIN PLAN, executes, and prints results.
"""

import json
import os
import re
from typing import Tuple

import oracledb
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "..", "schema.json")

openai_client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))


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


# ── SQL generation ──────────────────────────────────────────

SYSTEM_PROMPT = """\
You are an expert Oracle SQL generator.
Generate syntactically correct Oracle SQL based on the provided schema.
Rules:
- Use explicit column names, never SELECT *
- Include appropriate WHERE clauses for filtering
- Use FETCH FIRST N ROWS ONLY for limits, not ROWNUM
- Add helpful column aliases
- Use standard Oracle date functions
Return only the SQL query, no explanations."""


def build_prompt(question: str, schema_context: str) -> tuple[str, str]:
    user_prompt = f"{schema_context}\n\nQuestion: {question}\n\nSQL:"
    return SYSTEM_PROMPT, user_prompt


def generate_sql(question: str, schema_context: str) -> tuple[str, str]:
    system_prompt, user_prompt = build_prompt(question, schema_context)
    full_prompt = f"{system_prompt}\n\n{user_prompt}"

    response = openai_client.chat.completions.create(
        model="gpt-4",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.0,
        max_tokens=500,
    )
    sql = response.choices[0].message.content.strip()
    sql = sql.replace("```sql", "").replace("```", "").strip()
    sql = sql.rstrip(";")
    return sql, full_prompt


# ── validation & execution ─────────────────────────────────

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

def process_question(question: str) -> dict:
    result = {
        "question": question,
        "prompt": None,
        "generated_sql": None,
        "validation_status": None,
        "results": None,
        "error": None,
    }

    try:
        schema_context = load_schema_context()
        sql, prompt = generate_sql(question, schema_context)
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
    question = input("Ask a question (or press Enter for default): ").strip()
    if not question:
        question = "Show me the top 5 customers by order count in 2024"

    res = process_question(question)

    print(f"\nQuestion: {res['question']}")
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
