import requests

def get_data():
    response = requests.get("https://api.adzuna.com/v1/api/jobs/gb/search/1?app_id=0bf53f05&app_key=c68222254b7c9bc784bfda17f9c1bf20")
    return response.json()