"""
Shared SQL for extracting structured fragrance fields out of a raw
url/description pair (id, name, brand, release_year, accords, notes).

Used by notebooks/01_clean_from_raw.py (bulk transform of the whole frag_raw
table), notebooks/04_add_update_fragrance.py (single-row transform of one
pasted-in entry), and common/add_fragrance.py (single-row transform of one
scraped entry, called from streamlit_app/app.py), so this regex extraction
logic exists in exactly one place instead of being copy-pasted between them.

`gender` is deliberately NOT included here — 01 derives it from the
url-derived name via pattern matching, while 04 takes it directly from a
user-selected dropdown (there's no reliable pattern to derive it from a
single pasted-in entry), so each caller handles it on its own.
"""

EXTRACTED_FIELDS_SQL = """
    REGEXP_EXTRACT(url, '([a-zA-Z0-9]+)\\.html$', 1) as id,
    replace(regexp_extract(url, '/perfume/[^/]+/([^/]+)-[0-9]+\\.html', 1), '-', ' ') as name,
    replace(regexp_extract(url, '/perfume/([^/]+)/', 1), '-', ' ') as brand,
    REGEXP_EXTRACT(description, 'was launched in ([0-9]{4})', 1) as release_year,
    replace(replace(replace(main_accords, '[', ''), ']', ''),"'", "") as accords,
    lower(replace(regexp_extract(description, '(?i)top note[s]? (is|are) ([^.;]*)', 2), ' and', ',')) as top_notes,
    lower(replace(regexp_extract(description, '(?i)middle note[s]? (is|are) ([^.;]*)', 2), ' and', ',')) as mid_notes,
    lower(replace(regexp_extract(description, '(?i)base note[s]? (is|are) ([^.;]*)', 2), ' and', ',')) as base_notes
""".strip()
