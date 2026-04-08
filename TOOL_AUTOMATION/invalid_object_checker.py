import json
from pathlib import Path
from datetime import datetime

import oracledb
from openpyxl import Workbook
from openpyxl.styles import Font

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
OUTPUT_DIR = BASE_DIR / "reports"


def load_config():
    with open(CONFIG_PATH, "r") as f:
        return json.load(f)


def connect(cfg):
    return oracledb.connect(
        user=cfg["user"],
        password=cfg["password"],
        dsn=cfg["dsn"]
    )


def get_invalid_objects(cursor):
    cursor.execute("""
        SELECT OBJECT_NAME, OBJECT_TYPE
        FROM USER_OBJECTS
        WHERE STATUS = 'INVALID'
        ORDER BY OBJECT_TYPE, OBJECT_NAME
    """)
    return cursor.fetchall()


def get_errors(cursor, obj_name, obj_type):
    cursor.execute("""
        SELECT LINE, POSITION, TEXT
        FROM USER_ERRORS
        WHERE NAME = :name
          AND TYPE = :type
        ORDER BY SEQUENCE
    """, name=obj_name, type=obj_type)
    return cursor.fetchall()


def export_excel(results, schema):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    file_path = OUTPUT_DIR / f"invalid_objects_{schema}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    wb = Workbook()
    ws = wb.active
    ws.title = "INVALID_OBJECTS"

    headers = ["OBJECT_NAME", "OBJECT_TYPE", "LINE", "POSITION", "ERROR_TEXT"]
    ws.append(headers)

    for cell in ws[1]:
        cell.font = Font(bold=True)

    for row in results:
        ws.append(row)

    wb.save(file_path)
    return file_path


def main():
    cfg = load_config()
    conn = connect(cfg)
    cur = conn.cursor()

    schema = cfg["user"].upper()
    invalids = get_invalid_objects(cur)

    if not invalids:
        print("✅ Tidak ada invalid object.")
        return

    results = []

    for obj_name, obj_type in invalids:
        print(f"❌ {obj_type}: {obj_name}")
        errors = get_errors(cur, obj_name, obj_type)

        if not errors:
            results.append([obj_name, obj_type, None, None, "NO ERROR DETAILS"])
        else:
            for line, pos, text in errors:
                print(f"   LINE {line}, COL {pos} -> {text}")
                results.append([obj_name, obj_type, line, pos, text])

    excel = export_excel(results, schema)
    print(f"\n📄 Excel report: {excel}")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()