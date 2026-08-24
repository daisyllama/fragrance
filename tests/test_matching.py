import math

from common.matching import CONCENTRATION_ALIASES, fuzzy_score, normalize_concentration


class TestNormalizeConcentration:
    def test_collapses_eau_de_parfum_to_edp(self):
        assert normalize_concentration("miss dior eau de parfum") == "miss dior edp"

    def test_collapses_eau_de_toilette_to_edt(self):
        assert normalize_concentration("acqua di gio eau de toilette") == "acqua di gio edt"

    def test_collapses_eau_de_cologne_to_edc(self):
        assert normalize_concentration("4711 eau de cologne") == "4711 edc"

    def test_collapses_both_extrait_spellings_to_same_token(self):
        assert normalize_concentration("x extrait de parfum") == "x extrait"
        assert normalize_concentration("x parfum extrait") == "x extrait"

    def test_leaves_text_with_no_concentration_phrase_unchanged(self):
        assert normalize_concentration("velvet rouge") == "velvet rouge"

    def test_does_not_touch_substrings_outside_word_boundaries(self):
        # "eau de parfumerie" should NOT collapse to "edperie" — the regex
        # is word-bounded on "parfum", not a bare substring match.
        text = "eau de parfumerie shop"
        assert normalize_concentration(text) == text

    def test_every_alias_pattern_is_a_valid_regex(self):
        import re
        for pattern in CONCENTRATION_ALIASES:
            re.compile(pattern)  # raises re.error if invalid


class TestFuzzyScore:
    def test_exact_match_scores_100(self):
        assert fuzzy_score("Herbes", "herbes") == 100.0

    def test_exact_match_is_case_insensitive(self):
        assert fuzzy_score("HERBES", "herbes") == 100.0

    def test_whole_word_match_within_longer_name_scores_95(self):
        assert fuzzy_score("Terre d'Herbes", "herbes") == 95.0

    def test_whole_word_match_scores_95_regardless_of_word_position(self):
        assert fuzzy_score("Herbes de Provence", "herbes") == 95.0

    def test_prefix_match_scores_90(self):
        # "herbesde" has no word boundary at "herbes" (single token), so it
        # falls through the whole-word tier to the prefix tier.
        assert fuzzy_score("herbesde provence", "herbes") == 90.0

    def test_substring_match_scores_85(self):
        # "xherbesx" contains "herbes" but isn't a whole word or a prefix.
        assert fuzzy_score("xherbesx fragrance", "herbes") == 85.0

    def test_typo_neighbor_never_reaches_the_real_match_tiers(self):
        # These made the case for the tiered rewrite: close edit-distance
        # neighbors must never score >= 85 (the substring-match floor),
        # since they are not an actual match by type.
        assert fuzzy_score("Heroes", "herbes") < 85.0
        assert fuzzy_score("Herbae", "herbes") < 85.0

    def test_fuzzy_tier_is_capped_at_80(self):
        # Even a near-perfect edit-distance match that isn't a real
        # substring/word match must stay capped below the real-match tiers.
        score = fuzzy_score("Herbae", "herbes")
        assert score <= 80.0

    def test_completely_unrelated_names_score_low(self):
        assert fuzzy_score("Chanel No 5", "herbes") < 50.0

    def test_nan_name_scores_zero(self):
        assert fuzzy_score(float("nan"), "herbes") == 0.0

    def test_none_name_scores_zero(self):
        assert fuzzy_score(None, "herbes") == 0.0

    def test_concentration_alias_makes_edp_and_eau_de_parfum_score_equally(self):
        # This is what normalize_concentration is for: the caller is
        # expected to normalize both sides before calling fuzzy_score, so
        # verify that once normalized, the two spellings are indistinguishable.
        term_norm = normalize_concentration("miss dior eau de parfum")
        assert fuzzy_score("Miss Dior EDP", term_norm) == 100.0
        assert fuzzy_score("Miss Dior Eau de Parfum", term_norm) == 100.0

    def test_score_is_never_nan(self):
        score = fuzzy_score("Something Unrelated", "xyz123")
        assert not math.isnan(score)
