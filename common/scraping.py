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

    # 2. Rating and Rating Count (REVISED - using more general text search)
    data['rating'] = 'N/A'
    data['rating_count'] = 'N/A'
    try:
        # Search for the text pattern "Perfume rating X.XX out of 5 with Y votes" anywhere in the text
        rating_text_tag = soup.find(string=re.compile(r'Perfume rating [\d\.]+ out of 5 with [\d,]+ votes'))
        if rating_text_tag:
            rating_text = rating_text_tag.strip()

            rating_match = re.search(r'rating ([\d\.]+) out of 5', rating_text)
            count_match = re.search(r'with ([\d,]+) votes', rating_text)

            data['rating'] = float(rating_match.group(1)) if rating_match else 'N/A'
            data['rating_count'] = int(count_match.group(1).replace(',', '')) if count_match else 'N/A'

        # Fallback: Look for the specific div that contains the rating stars and text
        if data['rating'] == 'N/A':
            rating_div = soup.find('div', class_='rating-stars')
            if rating_div:
                rating_value_tag = rating_div.find('span', itemprop='ratingValue')
                review_count_tag = rating_div.find('span', itemprop='reviewCount')

                if rating_value_tag:
                    data['rating'] = float(rating_value_tag.text.strip())
                if review_count_tag:
                    data['rating_count'] = int(review_count_tag.text.strip().replace(',', ''))

    except Exception as e:
        print(f"Error extracting rating/count: {e}")

    # 3. Main Accords
    data['main_accords'] = []
    try:
        # Main accords are usually in a div with class 'accord-box'
        accord_box = soup.find('div', class_='accord-box')
        if accord_box:
            accords = accord_box.find_all('div', class_='accord-bar')
            for accord in accords:
                accord_name_tag = accord.find('span')
                if accord_name_tag:
                    data['main_accords'].append(accord_name_tag.text.strip().lower())
                else:
                    data['main_accords'].append(accord.text.strip().lower())

    except Exception as e:
        print(f"Error extracting main accords: {e}")

    # 4. Perfumers
    data['perfumers'] = []
    try:
        # Perfumer information is often a link with a href containing '/perfumer/'
        perfumer_tag = soup.find('a', href=re.compile(r'/perfumer/'))
        if perfumer_tag:
            data['perfumers'].append(perfumer_tag.text.strip())

    except Exception as e:
        print(f"Error extracting perfumers: {e}")

    # 5. Description (REVISED - looking for the first <p> tag after the main title block)
    data['description'] = 'N/A'
    try:
        # Find the main title <h1> tag
        title_tag = soup.find('h1', class_='text-center')
        if title_tag:
            # Find the next sibling that is a <p> tag, which is often the main description
            description_p = title_tag.find_next_sibling('p')
            if description_p:
                data['description'] = description_p.text.strip()
            else:
                # Fallback to the original selector if the first one fails
                description_div = soup.find('div', class_='text-content')
                if description_div:
                    data['description'] = description_div.text.strip()

    except Exception as e:
        print(f"Error extracting description: {e}")

    return data
