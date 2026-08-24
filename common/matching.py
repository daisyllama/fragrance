"""
Shared fuzzy-name-matching logic for perfume search.

Pure Python — no Spark/Databricks/Streamlit dependencies — so it can be
imported unchanged by both notebooks/03_perfume_search.py (running
inside Databricks, wrapped in a pandas_udf) and streamlit_app/app.py
(running locally, called directly). Each caller keeps only the thin wrapper
for its own runtime; the matching logic itself lives here once.
"""

import re

import pandas as pd
from rapidfuzz import fuzz

# Perfume names store the concentration inconsistently ("edp" vs "eau de
# parfum", etc.) — collapse both spellings to the same abbreviation before
# scoring so either form matches regardless of which one the name uses.
CONCENTRATION_ALIASES = {
    r"\beau de parfum\b": "edp",
    r"\beau de toilette\b": "edt",
    r"\beau de cologne\b": "edc",
    r"\bextrait de parfum\b": "extrait",
    r"\bparfum extrait\b": "extrait",
}


def normalize_concentration(text: str) -> str:
    for pattern, replacement in CONCENTRATION_ALIASES.items():
        text = re.sub(pattern, replacement, text)
    return text


def fuzzy_score(name, term_norm: str) -> float:
    """Rank by match type first (exact > whole-word > substring > fuzzy),
    edit-distance only as a tiebreaker within the weakest tier. Prevents a
    close typo neighbor (e.g. "heroes", "herbae") from scoring near an
    actual match (e.g. "herbes") just because their edit distance is small
    — those signals are too correlated to separate cleanly by blending."""
    if pd.isna(name):
        return 0.0
    name_norm = normalize_concentration(str(name).lower())

    if name_norm == term_norm:
        return 100.0
    if term_norm in re.findall(r"\w+", name_norm):  # exact whole-word match
        return 95.0
    if name_norm.startswith(term_norm):
        return 90.0
    if term_norm in name_norm:  # substring match
        return 85.0

    ratio = fuzz.ratio(name_norm, term_norm)
    token_set = fuzz.token_set_ratio(name_norm, term_norm)
    token_sort = fuzz.token_sort_ratio(name_norm, term_norm)
    fuzzy = ratio * 0.4 + token_set * 0.4 + token_sort * 0.2
    return min(fuzzy, 80.0)  # capped so it can never outrank a real match above
