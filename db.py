import psycopg

con = psycopg.connect(
    host="host.docker.internal",
    dbname="postgres",  
    user="postgres",
    password="Arfanshaikh@10",
    port=5432
)


# Cursor

cur = con.cursor()

cur.execute("SELECT version();")

rows = cur.fetchall()



con.close()