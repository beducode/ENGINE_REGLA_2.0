from pathlib import Path

base = Path("ddl")

folders = [
    "tables",
    "sequences",
    "views",
    "functions",
    "procedures",
    "packages",
    "package_bodies",
    "triggers",
    "indexes",
    "synonyms"
]

for folder in folders:
    (base / folder).mkdir(parents=True, exist_ok=True)

Path("logs").mkdir(exist_ok=True)
Path("reports").mkdir(exist_ok=True)

print("✅ DDL folder structure created successfully.")