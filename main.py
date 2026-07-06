import json, os, datetime as dt
from pipeline.api_data import get_data
from pipeline.loader import connect_db, load_json_to_db

data = get_data()
conn = connect_db()
print("Database connected")

# 2. Save JSON
os.makedirs("data/raw", exist_ok=True)
filename = f"job_data_{dt.datetime.now().strftime('%Y-%m-%d')}.json"
path = os.path.join("data/raw", filename)

with open(path, "w", encoding="utf-8") as f:
    json.dump(data, f, indent=2, ensure_ascii=False)

print(f"JSON saved to {path}")

# 3. Load into DB
load_json_to_db(path)


while True:
    pass