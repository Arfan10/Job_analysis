from api_data import get_data
from db import connect_db

data = get_data()
print(data[:2])

conn = connect_db()
print("Database connected")