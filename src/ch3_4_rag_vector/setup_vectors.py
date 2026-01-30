#!/usr/bin/env python3
"""
Chapter 3.4 — RAG setup: embed schema descriptions and store as
Oracle VECTOR columns for similarity search.

Creates a SCHEMA_EMBEDDINGS table and populates it with vector
embeddings for each table and its columns from schema.json.

Requires: pip install sentence-transformers
"""

import json
import os

import oracledb
from dotenv import load_dotenv
from sentence_transformers import SentenceTransformer

load_dotenv(os.path.join(os.path.dirname(__file__), "..", "..", ".env"))

SCHEMA_FILE = os.path.join(os.path.dirname(__file__), "..", "schema.json")
EMBEDDING_MODEL = os.getenv("EMBEDDING_MODEL", "all-MiniLM-L6-v2")


def get_db_connection() -> oracledb.Connection:
    return oracledb.connect(
        user=os.getenv("ORA_USER"),
        password=os.getenv("ORA_PASSWORD"),
        dsn=os.getenv("ORA_DSN"),
    )


def build_table_descriptions(schema_file: str) -> list[dict]:
    """Build a rich text description per table for embedding."""
    with open(schema_file) as f:
        schema = json.load(f)

    descriptions = []
    for table in schema["tables"]:
        cols = ", ".join(
            f"{c['name']} ({c['type']}): {c['description']}"
            for c in table["columns"]
        )
        text = (
            f"Table {table['name']}: {table['description']}. "
            f"Columns: {cols}"
        )
        descriptions.append({
            "table_name": table["name"],
            "description": text,
            "full_metadata": json.dumps(table),
        })

    # Also embed relationships as a separate entry
    if schema.get("relationships"):
        rels = "; ".join(
            f"{r['from']} -> {r['to']} ({r['type']})"
            for r in schema["relationships"]
        )
        descriptions.append({
            "table_name": "_RELATIONSHIPS",
            "description": f"Foreign key relationships: {rels}",
            "full_metadata": json.dumps(schema["relationships"]),
        })

    return descriptions


def main():
    print(f"Loading embedding model: {EMBEDDING_MODEL}")
    model = SentenceTransformer(EMBEDDING_MODEL)
    dim = model.get_sentence_embedding_dimension()
    print(f"Embedding dimension: {dim}")

    descriptions = build_table_descriptions(SCHEMA_FILE)
    print(f"Built {len(descriptions)} descriptions to embed")

    # Generate embeddings
    texts = [d["description"] for d in descriptions]
    embeddings = model.encode(texts)

    # Store in Oracle
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("DROP TABLE schema_embeddings")
        print("Dropped existing schema_embeddings table")
    except oracledb.DatabaseError:
        pass

    cur.execute(f"""
        CREATE TABLE schema_embeddings (
            id          NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            table_name  VARCHAR2(128) NOT NULL,
            description CLOB NOT NULL,
            metadata    CLOB NOT NULL,
            embedding   VECTOR({dim}, FLOAT32)
        )
    """)
    print("Created schema_embeddings table")

    for desc, emb in zip(descriptions, embeddings):
        vec_str = "[" + ",".join(str(float(x)) for x in emb) + "]"
        cur.execute(
            """INSERT INTO schema_embeddings
               (table_name, description, metadata, embedding)
               VALUES (:1, :2, :3, TO_VECTOR(:4))""",
            [desc["table_name"], desc["description"],
             desc["full_metadata"], vec_str],
        )
        print(f"  Embedded: {desc['table_name']}")

    conn.commit()
    print(f"\nStored {len(descriptions)} embeddings. Done.")
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
