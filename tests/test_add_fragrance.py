import pytest

from common.add_fragrance import (
    check_existing_state,
    extract_cleaned_row,
    merge_fragrance_cleaned,
    merge_frag_raw,
)


class FakeCursor:
    """Minimal DB-API-shaped cursor stub. Scripted per test to return
    whatever the next .execute() call should produce, and records every
    query+params pair so tests can assert on exactly what was sent —
    this is the boundary these functions actually depend on, so it's the
    right thing to fake rather than a real Databricks connection."""

    def __init__(self, extraction_row=None, fetchone_queue=None):
        self.description = None
        self._extraction_row = extraction_row
        self._fetchone_queue = list(fetchone_queue or [])
        self.queries = []  # list of (sql, params)

    def execute(self, sql, params=None):
        self.queries.append((sql, params))
        if self._extraction_row is not None and "generate_perfume_string" in sql:
            self.description = [(k,) for k in self._extraction_row]
            self._next_fetchone = tuple(self._extraction_row.values())
        elif self._fetchone_queue:
            self._next_fetchone = self._fetchone_queue.pop(0)
        else:
            self._next_fetchone = None
        return self

    def fetchone(self):
        return self._next_fetchone


SCRAPED = {
    "url": "https://www.fragrantica.com/perfume/Fragrance-World/Velvet-Rouge-104781.html",
    "name": "Velvet Rouge",
    "gender": "for women and men",
    "rating": 4.2,
    "rating_count": 70,
    "main_accords": ["rose", "woody"],
    "perfumers": [],
    "description": "a rose scent",
}

EXTRACTED_ROW = {
    "id": "104781",
    "name": "velvet rouge",
    "brand": "fragrance world",
    "release_year": "2023",
    "accords": "rose,woody",
    "top_notes": "white tea",
    "mid_notes": "rose",
    "base_notes": "musk",
    "gender": "for women and men",
    "url": SCRAPED["url"],
    "description": SCRAPED["description"],
    "perfume_string": "accords_rose accords_woody",
}


class TestMergeFragRaw:
    def test_sends_a_merge_statement_keyed_on_url(self):
        cur = FakeCursor()
        merge_frag_raw(cur, SCRAPED)
        sql, params = cur.queries[0]
        assert "MERGE INTO" in sql
        assert "frag_raw" in sql
        assert "target.url = source.url" in sql

    def test_binds_every_scraped_field_as_a_query_parameter(self):
        cur = FakeCursor()
        merge_frag_raw(cur, SCRAPED)
        _, params = cur.queries[0]
        assert params["url"] == SCRAPED["url"]
        assert params["name"] == "Velvet Rouge"
        assert params["main_accords"] == "['rose', 'woody']"

    def test_does_not_string_interpolate_untrusted_text_into_the_sql(self):
        # A quote or SQL-meaningful character in scraped text must never
        # land in the query text itself — only in the bound parameter dict.
        scraped = dict(SCRAPED, description="a scent with a ' quote and -- comment")
        cur = FakeCursor()
        merge_frag_raw(cur, scraped)
        sql, params = cur.queries[0]
        assert "' quote" not in sql
        assert params["description"] == "a scent with a ' quote and -- comment"

    def test_missing_optional_fields_are_stringified_not_left_as_none(self):
        scraped = {"url": "https://x.com/y.html"}  # everything else missing
        cur = FakeCursor()
        merge_frag_raw(cur, scraped)
        _, params = cur.queries[0]
        assert params["main_accords"] == "[]"
        assert params["perfumers"] == "[]"
        assert params["name"] == "None"  # str(None) — matches existing frag_raw schema (string columns)


class TestExtractCleanedRow:
    def test_returns_dict_built_from_cursor_description_and_fetchone(self):
        cur = FakeCursor(extraction_row=EXTRACTED_ROW)
        row = extract_cleaned_row(cur, SCRAPED)
        assert row == EXTRACTED_ROW

    def test_does_not_write_anything(self):
        cur = FakeCursor(extraction_row=EXTRACTED_ROW)
        extract_cleaned_row(cur, SCRAPED)
        sql, _ = cur.queries[0]
        assert "MERGE" not in sql
        assert "SELECT" in sql

    def test_passes_url_main_accords_description_gender_as_params(self):
        cur = FakeCursor(extraction_row=EXTRACTED_ROW)
        extract_cleaned_row(cur, SCRAPED)
        _, params = cur.queries[0]
        assert params["url"] == SCRAPED["url"]
        assert params["gender"] == SCRAPED["gender"]


class TestCheckExistingState:
    def test_brand_new_fragrance_reports_not_in_cleaned_and_no_embedding(self):
        cur = FakeCursor(fetchone_queue=[None, None])
        result = check_existing_state(cur, "104781")
        assert result == {"in_cleaned": False, "embedded_perfume_string": None}

    def test_exists_in_cleaned_but_never_embedded(self):
        cur = FakeCursor(fetchone_queue=[(1,), None])
        result = check_existing_state(cur, "104781")
        assert result == {"in_cleaned": True, "embedded_perfume_string": None}

    def test_exists_and_already_embedded_returns_its_stored_perfume_string(self):
        cur = FakeCursor(fetchone_queue=[(1,), ("accords_rose",)])
        result = check_existing_state(cur, "104781")
        assert result == {"in_cleaned": True, "embedded_perfume_string": "accords_rose"}

    def test_queries_both_tables_filtered_by_the_given_id(self):
        cur = FakeCursor(fetchone_queue=[None, None])
        check_existing_state(cur, "104781")
        assert len(cur.queries) == 2
        cleaned_sql, cleaned_params = cur.queries[0]
        embeddings_sql, embeddings_params = cur.queries[1]
        assert "fragrance_cleaned" in cleaned_sql
        assert "fragrance_embeddings" in embeddings_sql
        assert cleaned_params["id"] == "104781"
        assert embeddings_params["id"] == "104781"


class TestMergeFragranceCleaned:
    def test_sends_a_merge_statement_keyed_on_id(self):
        cur = FakeCursor()
        merge_fragrance_cleaned(cur, EXTRACTED_ROW)
        sql, _ = cur.queries[0]
        assert "MERGE INTO" in sql
        assert "fragrance_cleaned" in sql
        assert "target.id = source.id" in sql

    def test_binds_every_row_field_as_a_query_parameter(self):
        cur = FakeCursor()
        merge_fragrance_cleaned(cur, EXTRACTED_ROW)
        _, params = cur.queries[0]
        for key in ("id", "name", "brand", "perfume_string"):
            assert params[key] == EXTRACTED_ROW[key]

    def test_does_not_string_interpolate_row_text_into_the_sql(self):
        row = dict(EXTRACTED_ROW, description="notes with a ' quote")
        cur = FakeCursor()
        merge_fragrance_cleaned(cur, row)
        sql, params = cur.queries[0]
        assert "' quote" not in sql
        assert params["description"] == "notes with a ' quote"
