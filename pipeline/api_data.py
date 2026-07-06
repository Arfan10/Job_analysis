import os
import requests
import logging
from tenacity import retry, wait_exponential, stop_after_attempt
import datetime as dt
from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    filename='job_data_fetch.log',
    filemode='w',
    format='%(asctime)s - %(levelname)s - %(message)s'
)

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=4, max=10)
)
def get_data():

    jobs = []

    for i in range(1, 51):  # Fetching first 50 pages (25 results each)

        try:
            response = requests.get(
                f"https://api.adzuna.com/v1/api/jobs/gb/search/{i}",
                params={
                    "app_id": os.getenv("APP_ID"),
                    "app_key": os.getenv("API_KEY"),
                    "results_per_page": 25,
                    "what": "data engineer",
                    "what_and": "Analyst",
                }
            )

            response.raise_for_status()

            print(f"Fetching data from page {i}...")

            all_data = response.json()

            for k in all_data['results']:

                job = {
                    "id": k.get('id'),
                    "title": k.get('title'),
                    "Company": k.get('company', {}).get('display_name'),
                    "location": k.get('location', {}).get('area', ['Unknown'])[-1],
                    "Salary_min": k.get('salary_min'),
                    "Salary_max": k.get('salary_max'),
                    "job_descr": k.get('description'),
                    "Date_posted": k.get('created')
                }

                jobs.append(job)

        except Exception as e:
            logging.error(f"Error occurred while fetching data from page {i}: {e}")
            print(e)

    return jobs