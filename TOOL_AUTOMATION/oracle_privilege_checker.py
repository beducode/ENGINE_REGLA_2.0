import json
import oracledb

with open("config.json", "r") as f:
    cfg = json.load(f)

conn = oracledb.connect(
    user=cfg["user"],
    password=cfg["password"],
    dsn=cfg["dsn"]
)

cur = conn.cursor()

print("=" * 80)
print("SESSION PRIVILEGES")
print("=" * 80)

cur.execute("SELECT * FROM SESSION_PRIVS ORDER BY PRIVILEGE")
for row in cur.fetchall():
    print(row[0])

print("\n" + "=" * 80)
print("USER ROLE PRIVS")
print("=" * 80)

cur.execute("SELECT * FROM USER_ROLE_PRIVS ORDER BY GRANTED_ROLE")
for row in cur.fetchall():
    print(row[0])

cur.close()
conn.close()