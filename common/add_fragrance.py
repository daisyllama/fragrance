"""
SQL-connector pipeline for adding one fragrance end-to-end: MERGE a scraped
row into frag_raw, then run the same regex extraction 01_clean_from_raw.py
uses (via common.cleaning.EXTRACTED_FIELDS_SQL) and MERGE the result into
fragrance_cleaned.

Each function takes a plain DB-API cursor (e.g. from databricks-sql-connector),
so this is usable by any DB-API-compliant connection — not Streamlit-specific
— even though streamlit_app/app.py is currently the only caller.

All scraped/user text is passed as bound query parameters, never
string-interpolated: descriptions and accords text can contain quotes that
would otherwise break or inject into the SQL.
"""

from common.cleaning import EXTRACTED_FIELDS_SQL

CATALOG_SCHEMA = "fragrance_db.default"


def merge_frag_raw(cursor, scraped: dict) -> None:
    """MERGE one scraped row into frag_raw, keyed by url. Mirrors the MERGE
    in scraping/notebooks/fragrantica_scraper.ipynb's 'Add the scraped
    fragrance to frag_raw' cell, run over SQL instead of DeltaTable.merge."""
    cursor.execute(
        f"""
        MERGE INTO {CATALOG_SCHEMA}.frag_raw AS target
        USING (
            SELECT
                :url AS url,
                :name AS name,
                :gender AS gender,
                :rating AS rating,
                :rating_count AS rating_count,
                :main_accords AS main_accords,
                :perfumers AS perfumers,
                :description AS description
        ) AS source
        ON target.url = source.url
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """,
        {
            "url": scraped["url"],
            "name": str(scraped.get("name")),
            "gender": str(scraped.get("gender")),
            "rating": str(scraped.get("rating")),
            "rating_count": str(scraped.get("rating_count")),
            "main_accords": str(scraped.get("main_accords", [])),
            "perfumers": str(scraped.get("perfumers", [])),
            "description": str(scraped.get("description")),
        },
    )


def extract_cleaned_row(cursor, scraped: dict) -> dict:
    """Run the same regex extraction 01_clean_from_raw.py uses on this one
    row (read-only — no write). Returns the transformed row (dict),
    including its derived `id` and `perfume_string`."""
    params = {
        "url": scraped["url"],
        "main_accords": str(scraped.get("main_accords", [])),
        "description": str(scraped.get("description")),
        "gender": str(scraped.get("gender")),
    }

    cursor.execute(
        f"""
        WITH source_row AS (
            SELECT :url AS url, :main_accords AS main_accords, :description AS description, :gender AS gender
        ),
        extracted AS (
            SELECT {EXTRACTED_FIELDS_SQL}, gender, url, description
            FROM source_row
        )
        SELECT *, {CATALOG_SCHEMA}.generate_perfume_string(accords, top_notes, mid_notes, base_notes) AS perfume_string
        FROM extracted
        """,
        params,
    )
    columns = [c[0] for c in cursor.description]
    return dict(zip(columns, cursor.fetchone()))


def check_existing_state(cursor, fragrance_id: str) -> dict:
    """Look up whether this id already exists — in fragrance_cleaned, and
    (separately) what perfume_string it was last embedded with, if any.
    Call this BEFORE merge_fragrance_cleaned, since that call overwrites
    the fragrance_cleaned row this checks. Returns
    {"in_cleaned": bool, "embedded_perfume_string": str | None}."""
    cursor.execute(
        f"SELECT 1 FROM {CATALOG_SCHEMA}.fragrance_cleaned WHERE id = :id",
        {"id": fragrance_id},
    )
    in_cleaned = cursor.fetchone() is not None

    cursor.execute(
        f"SELECT perfume_string FROM {CATALOG_SCHEMA}.fragrance_embeddings WHERE id = :id",
        {"id": fragrance_id},
    )
    embedding_row = cursor.fetchone()
    embedded_perfume_string = embedding_row[0] if embedding_row else None

    return {"in_cleaned": in_cleaned, "embedded_perfume_string": embedded_perfume_string}


def merge_fragrance_cleaned(cursor, row: dict) -> None:
    """MERGE an already-extracted row (from extract_cleaned_row) into
    fragrance_cleaned, keyed by id."""
    cursor.execute(
        f"""
        MERGE INTO {CATALOG_SCHEMA}.fragrance_cleaned AS target
        USING (
            SELECT
                :id AS id, :name AS name, :brand AS brand, :release_year AS release_year,
                :gender AS gender, :accords AS accords, :top_notes AS top_notes,
                :mid_notes AS mid_notes, :base_notes AS base_notes, :url AS url,
                :description AS description, :perfume_string AS perfume_string
        ) AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """,
        {
            "id": row["id"],
            "name": row["name"],
            "brand": row["brand"],
            "release_year": row["release_year"],
            "gender": row["gender"],
            "accords": row["accords"],
            "top_notes": row["top_notes"],
            "mid_notes": row["mid_notes"],
            "base_notes": row["base_notes"],
            "url": row["url"],
            "description": row["description"],
            "perfume_string": row["perfume_string"],
        },
    )
