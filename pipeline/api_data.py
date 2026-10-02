# pipeline/api_data.py
import os
import requests
import logging

logger = logging.getLogger(__name__)

def get_data():
    app_id = os.getenv("APP_ID")
    app_key = os.getenv("API_KEY")

    jobs = []
    max_pages = 20

    for page in range(1, max_pages + 1):
        url = f"https://api.adzuna.com/v1/api/jobs/gb/search/{page}"
        params = {
            "app_id": app_id,
            "app_key": app_key,
            "results_per_page": 25,
            "what": "data engineer Analyst",
        }

        try:
            response = requests.get(url, params=params, timeout=10)
            if response.status_code != 200:
                logger.warning(f"Adzuna API returned {response.status_code} on page {page}. Stopping fetch.")
                break

            all_data = response.json()
            results = all_data.get('results', [])
            if not results:
                break

            for k in results:
                area = k.get('location', {}).get('area', [])
                jobs.append({
                    "id": k.get('id'),
                    "title": k.get('title'),
                    "Company": k.get('company', {}).get('display_name'),
                    "location": area[-1] if area else 'Unknown',
                    "Salary_min": k.get('salary_min'),
                    "Salary_max": k.get('salary_max'),
                    "job_descr": k.get('description'),
                    "Date_posted": k.get('created')
                })

        except Exception as e:
            logger.error(f"Failed to process page {page}: {e}")
            break

    return jobs