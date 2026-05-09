import requests

def get_data():
    title = []
    Results = []
    location = []
    Salary = []
    Company = []

    for i in range(1, 6):
        print(f"Fetching data from page {i}...")
        response = requests.get(f"https://api.adzuna.com/v1/api/jobs/gb/search/{i}?app_id=0bf53f05&app_key=c68222254b7c9bc784bfda17f9c1bf20&results_per_page=25&what=software%20engineer%20&what_and=analyst&location0=UK")
        all_data = response.json()
        for item in all_data['results']:
            title.append(item.get('title', 'N/A'))
            area = item['location'].get('area', [])
            location.append(area[-1] if area else 'Unknown')
            Salary.append(item.get('salary_max', 0))
            Company.append(item.get('company', {}).get('display_name', 'Unknown'))

    return title, location, Salary, Company

title, location, Salary, Company = get_data()
print(f"Title: {title}\nLocation: {location}\nSalary: {Salary}\nCompany: {Company}")