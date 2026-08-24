"""
Scrape a single Fragrantica perfume page.

Pure Python (requests + BeautifulSoup) — no Spark/Streamlit dependencies —
so it's importable unchanged by both scraping/notebooks/fragrantica_scraper.ipynb
(running inside Databricks) and streamlit_app/app.py (running locally).
"""

import re

import requests
from bs4 import BeautifulSoup


def scrape_fragrantica(url: str) -> dict:
    """
    Scrapes a Fragrantica perfume page for specific details.

    Args:
        url (str): The URL of the Fragrantica perfume page.

    Returns:
        dict: A dictionary containing the extracted perfume data, or
        {"error": ...} if the page couldn't be fetched.
    """
    # Using a complete set of headers to mimic a real browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.google.com/',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }

    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching URL {url}: {e}")
        return {"error": str(e)}

    soup = BeautifulSoup(response.content, 'html.parser')
    data = {"url": url}

    # 1. Name and Gender
    data['name'] = 'N/A'
    data['gender'] = 'N/A'
    try:
        # The main title is usually in an <h1> tag with class 'text-center'
        title_tag = soup.find('h1', class_='text-center')
        if title_tag:
            full_title = title_tag.text.strip()

            # Extract gender from the end of the title
            gender_match = re.search(r'for (women and men|men|women)$', full_title, re.IGNORECASE)
            if gender_match:
                data['gender'] = gender_match.group(0).lower()
                # Remove the gender part to get the clean name
                data['name'] = full_title[:gender_match.start()].strip()
            else:
                data['name'] = full_title

    except Exception as e:
        print(f"Error extracting name/gender: {e}")

    # 2. Rating and Rating Count
    # As of the current site redesign, the rating lives in a
    # schema.org AggregateRating block: a <span itemprop="ratingValue"> for
    # the score, and a <span itemprop="ratingCount" content="33812"> for the
    # vote count (the `content` attribute holds the clean unformatted
    # number; the visible text is comma-formatted, e.g. "33,812").
    data['rating'] = 'N/A'
    data['rating_count'] = 'N/A'
    try:
        rating_tag = soup.find(attrs={'itemprop': 'ratingValue'})
        if rating_tag:
            data['rating'] = float(rating_tag.get_text(strip=True))

        count_tag = soup.find(attrs={'itemprop': 'ratingCount'})
        if count_tag:
            count_value = count_tag.get('content') or count_tag.get_text(strip=True)
            data['rating_count'] = int(re.sub(r'[^\d]', '', count_value))

    except Exception as e:
        print(f"Error extracting rating/count: {e}")

    # 3. Main Accords
    # Now under an <h6>main accords</h6> heading, followed by a sibling div
    # containing one <span class="truncate">accord name</span> per accord
    # (no more accord-box/accord-bar classes).
    data['main_accords'] = []
    try:
        accords_heading = soup.find('h6', string=lambda s: s and 'main accords' in s.lower())
        if accords_heading:
            accords_container = accords_heading.find_next_sibling('div')
            if accords_container:
                for span in accords_container.find_all('span', class_='truncate'):
                    data['main_accords'].append(span.get_text(strip=True).lower())

    except Exception as e:
        print(f"Error extracting main accords: {e}")

    # 4. Perfumers
    # Links to /noses/<name>.html (the site's URL path for perfumer pages),
    # excluding the generic nav link to /noses/ itself. Name text is in a
    # <span> inside the link.
    data['perfumers'] = []
    try:
        for link in soup.find_all('a', href=re.compile(r'^/noses/.+')):
            name_tag = link.find('span')
            name = (name_tag or link).get_text(strip=True)
            if name and name not in data['perfumers']:
                data['perfumers'].append(name)

    except Exception as e:
        print(f"Error extracting perfumers: {e}")

    # 5. Description
    # Now in a <div itemprop="description">, whose first <p> holds the
    # "X by Y is a fragrance for Z. Top notes are ...; middle notes are
    # ...; base notes are ..." text that downstream note-extraction regexes
    # (see common/cleaning.py) depend on.
    data['description'] = 'N/A'
    try:
        description_container = soup.find(attrs={'itemprop': 'description'})
        if description_container:
            description_p = description_container.find('p')
            if description_p:
                text = description_p.get_text(separator=' ', strip=True)
                data['description'] = re.sub(r'\s+', ' ', text)

    except Exception as e:
        print(f"Error extracting description: {e}")

    return data
