from unittest.mock import MagicMock, patch

import requests

from common.scraping import scrape_fragrantica

# Minimal fixture reproducing the real Fragrantica page structure this
# scraper's selectors are written against (confirmed by fetching live pages
# during development — see common/scraping.py's inline comments for where
# each marker comes from).
FULL_PAGE_HTML = """
<html><body>
<h1 class="text-2xl md:text-[2.5rem] font-light tracking-tight text-center md:text-left">
    Sauvage Dior for men
</h1>
<p class="text-xs" itemprop="aggregateRating" itemtype="http://schema.org/AggregateRating">
    Perfume rating&nbsp;<span itemprop="ratingValue">3.85</span>&nbsp;out of&nbsp;<span itemprop="bestRating">5</span>
    &nbsp;with&nbsp;<span itemprop="ratingCount" content="33812">33,812</span>&nbsp;votes
</p>
<h6>main accords</h6>
<div class="flex flex-col">
    <div><span class="truncate">fresh spicy</span></div>
    <div><span class="truncate">amber</span></div>
    <div><span class="truncate">citrus</span></div>
</div>
<div id="perfume-description-content" itemprop="description">
    <p><b>Sauvage</b> by <b>Dior</b> is a fragrance for men. Sauvage was launched in 2015.
    Top notes are Bergamot and Pepper; middle notes are Lavender and Vetiver; base notes are Ambroxan and Cedar.</p>
    <div class="fragrantica-blockquote"><p>Marketing copy that should NOT be picked up.</p></div>
</div>
<h3>Perfumer</h3>
<div>
    <a href="/noses/Francois_Demachy.html"><span>Francois Demachy</span></a>
</div>
<a href="/noses/">Perfumers</a>
</body></html>
"""

NO_PERFUMER_HTML = """
<html><body>
<h1 class="text-center">Velvet Rouge for women and men</h1>
<a href="/noses/">Perfumers</a>
</body></html>
"""


def _mock_response(html: str, status_code: int = 200):
    response = MagicMock()
    response.status_code = status_code
    response.content = html.encode("utf-8")
    response.raise_for_status = MagicMock()
    return response


class TestScrapeFragrantica:
    def test_extracts_name_and_gender_for_men(self):
        with patch("common.scraping.requests.get", return_value=_mock_response(FULL_PAGE_HTML)):
            data = scrape_fragrantica("https://example.com/x.html")
        assert data["name"] == "Sauvage Dior"
        assert data["gender"] == "for men"

    def test_extracts_rating_and_rating_count_from_schema_org_markup(self):
        with patch("common.scraping.requests.get", return_value=_mock_response(FULL_PAGE_HTML)):
            data = scrape_fragrantica("https://example.com/x.html")
        assert data["rating"] == 3.85
        assert data["rating_count"] == 33812  # from the `content` attribute, not the "33,812" display text

    def test_extracts_main_accords_in_order(self):
        with patch("common.scraping.requests.get", return_value=_mock_response(FULL_PAGE_HTML)):
            data = scrape_fragrantica("https://example.com/x.html")
        assert data["main_accords"] == ["fresh spicy", "amber", "citrus"]

    def test_extracts_perfumer_name(self):
        with patch("common.scraping.requests.get", return_value=_mock_response(FULL_PAGE_HTML)):
            data = scrape_fragrantica("https://example.com/x.html")
        assert data["perfumers"] == ["Francois Demachy"]

    def test_extracts_description_first_paragraph_only(self):
        with patch("common.scraping.requests.get", return_value=_mock_response(FULL_PAGE_HTML)):
            data = scrape_fragrantica("https://example.com/x.html")
        assert "Top notes are Bergamot and Pepper" in data["description"]
        assert "Marketing copy" not in data["description"]

    def test_description_has_no_smashed_together_words_around_bold_tags(self):
        # Regression guard: get_text(separator=' ') fix for <b>Sauvage</b>
        # being concatenated directly onto the next word with no space.
        with patch("common.scraping.requests.get", return_value=_mock_response(FULL_PAGE_HTML)):
            data = scrape_fragrantica("https://example.com/x.html")
        assert "Sauvagewas" not in data["description"]
        assert "Sauvage by Dior" in data["description"]

    def test_returns_url_unchanged_in_result(self):
        url = "https://www.fragrantica.com/perfume/Dior/Sauvage-31861.html"
        with patch("common.scraping.requests.get", return_value=_mock_response(FULL_PAGE_HTML)):
            data = scrape_fragrantica(url)
        assert data["url"] == url

    def test_missing_perfumer_section_returns_empty_list_not_error(self):
        with patch("common.scraping.requests.get", return_value=_mock_response(NO_PERFUMER_HTML)):
            data = scrape_fragrantica("https://example.com/x.html")
        assert data["perfumers"] == []

    def test_missing_optional_fields_default_to_na_sentinels(self):
        with patch("common.scraping.requests.get", return_value=_mock_response(NO_PERFUMER_HTML)):
            data = scrape_fragrantica("https://example.com/x.html")
        assert data["rating"] == "N/A"
        assert data["rating_count"] == "N/A"
        assert data["main_accords"] == []
        assert data["description"] == "N/A"

    def test_network_error_returns_error_dict_instead_of_raising(self):
        with patch(
            "common.scraping.requests.get",
            side_effect=requests.exceptions.ConnectionError("refused"),
        ):
            data = scrape_fragrantica("https://example.com/x.html")
        assert "error" in data
        assert "refused" in data["error"]

    def test_http_error_status_returns_error_dict(self):
        response = _mock_response("<html></html>", status_code=403)
        response.raise_for_status.side_effect = requests.exceptions.HTTPError("403 Forbidden")
        with patch("common.scraping.requests.get", return_value=response):
            data = scrape_fragrantica("https://example.com/x.html")
        assert "error" in data

    def test_no_network_call_made_when_mocked(self):
        # Sanity check on the test setup itself: confirms these tests never
        # hit the real network.
        with patch("common.scraping.requests.get", return_value=_mock_response(FULL_PAGE_HTML)) as mock_get:
            scrape_fragrantica("https://example.com/x.html")
        mock_get.assert_called_once()
