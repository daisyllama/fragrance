from common.cleaning import EXTRACTED_FIELDS_SQL


class TestExtractedFieldsSql:
    def test_produces_every_expected_column_alias(self):
        # These are the columns notebooks/01_clean_from_raw.py,
        # notebooks/04_add_update_fragrance.py, and common/add_fragrance.py
        # all rely on being present in the result of this fragment.
        for alias in ("id", "name", "brand", "release_year", "accords", "top_notes", "mid_notes", "base_notes"):
            assert f"as {alias}" in EXTRACTED_FIELDS_SQL

    def test_does_not_define_gender(self):
        # gender is deliberately excluded — callers derive/supply it
        # themselves (see the module docstring). Guards against someone
        # accidentally folding gender derivation back into the shared
        # fragment, which would silently override callers that already
        # pass their own gender.
        assert "as gender" not in EXTRACTED_FIELDS_SQL

    def test_is_a_bare_column_list_not_a_full_query(self):
        # Callers interpolate this directly after a SELECT keyword, so it
        # must not itself start with SELECT/WITH or end with a semicolon.
        stripped = EXTRACTED_FIELDS_SQL.strip()
        assert not stripped.upper().startswith("SELECT")
        assert not stripped.upper().startswith("WITH")
        assert not stripped.endswith(";")

    def test_survives_nested_fstring_embedding_unchanged(self):
        # Callers (common/add_fragrance.py, notebooks/01_clean_from_raw.py)
        # embed this fragment inside an f-string via {EXTRACTED_FIELDS_SQL}.
        # It contains a literal regex quantifier, `[0-9]{4}` (exactly 4
        # digits, for release_year) — since that's inside the *value* being
        # substituted rather than the outer f-string's own source text,
        # Python only evaluates {EXTRACTED_FIELDS_SQL} itself and leaves
        # {4} alone. Confirms that still holds and the text passes through
        # byte-for-byte.
        embedded = f"WITH a AS (SELECT {EXTRACTED_FIELDS_SQL} FROM t) SELECT * FROM a"
        assert EXTRACTED_FIELDS_SQL in embedded
        assert "[0-9]{4}" in embedded

    def test_references_expected_source_columns(self):
        # The fragment reads from these raw columns (present in both
        # frag_raw and the single-row `temp_input`/`source_row` shapes
        # callers build) — a rename in one without the other breaks this
        # silently at query time.
        for source_col in ("url", "description", "main_accords"):
            assert source_col in EXTRACTED_FIELDS_SQL

    def test_interpolates_cleanly_into_an_fstring_select(self):
        # Exercises the exact usage pattern both callers use.
        query = f"SELECT {EXTRACTED_FIELDS_SQL} FROM some_table"
        assert query.count("SELECT") == 1
        assert "as perfume_string" not in query  # that's added by callers, not this fragment
