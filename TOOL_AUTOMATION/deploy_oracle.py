import json
import traceback
from pathlib import Path
from datetime import datetime

import oracledb
from openpyxl import Workbook
from openpyxl.styles import Font

from schema_rewriter import (
    rewrite_schema_references,
    normalize_procedure_header,
    normalize_function_header,
    normalize_package_header,
    normalize_trigger_header
)

from table_migrator import (
    extract_table_name_from_ddl,
    get_table_row_count,
    backup_existing_table,
    drop_table,
    migrate_data_from_backup
)

# =========================================================
# PATHS
# =========================================================
BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
DDL_DIR = BASE_DIR / "ddl"
LOG_DIR = BASE_DIR / "logs"
REPORT_DIR = BASE_DIR / "reports"

# =========================================================
# CONFIG
# =========================================================
def load_config():
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return json.load(f)

# =========================================================
# HELPERS
# =========================================================
def now_str():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def connect_oracle(cfg):
    return oracledb.connect(
        user=cfg["user"],
        password=cfg["password"],
        dsn=cfg["dsn"]
    )

def object_exists(cursor, object_name: str, object_type: str):
    cursor.execute("""
        SELECT COUNT(*)
        FROM USER_OBJECTS
        WHERE OBJECT_NAME = :obj_name
          AND OBJECT_TYPE = :obj_type
    """, obj_name=object_name.upper(), obj_type=object_type.upper())
    return cursor.fetchone()[0] > 0

def get_object_status(cursor, object_name: str, object_type: str):
    cursor.execute("""
        SELECT STATUS
        FROM USER_OBJECTS
        WHERE OBJECT_NAME = :obj_name
          AND OBJECT_TYPE = :obj_type
    """, obj_name=object_name.upper(), obj_type=object_type.upper())
    row = cursor.fetchone()
    return row[0] if row else None

def get_compile_errors(cursor, object_name: str, object_type: str):
    cursor.execute("""
        SELECT LINE, POSITION, TEXT
        FROM USER_ERRORS
        WHERE NAME = :obj_name
          AND TYPE = :obj_type
        ORDER BY SEQUENCE
    """, obj_name=object_name.upper(), obj_type=object_type.upper())
    return cursor.fetchall()

def execute_sql(cursor, sql_text: str):
    sql_text = sql_text.strip()

    if sql_text.endswith(";"):
        sql_text = sql_text[:-1].rstrip()

    while sql_text.endswith("/"):
        sql_text = sql_text[:-1].rstrip()

    cursor.execute(sql_text)

def save_log(lines):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    path = LOG_DIR / f"deploy_log_{now_str()}.log"
    path.write_text("\n".join(lines), encoding="utf-8")
    return path

def save_excel_report(rows):
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    path = REPORT_DIR / f"deploy_report_{now_str()}.xlsx"

    wb = Workbook()
    ws = wb.active
    ws.title = "DEPLOY_REPORT"

    headers = [
        "OBJECT_TYPE",
        "OBJECT_NAME",
        "DEPLOY_STATUS",
        "OBJECT_STATUS",
        "ROW_COUNT",
        "ERROR_DETAIL"
    ]
    ws.append(headers)

    for c in ws[1]:
        c.font = Font(bold=True)

    for r in rows:
        ws.append(r)

    wb.save(path)
    return path

# =========================================================
# SQL PREPARE
# =========================================================
def prepare_sql(sql_text: str, folder_name: str, object_name: str, source_schema: str, target_schema: str):
    sql_text = rewrite_schema_references(sql_text, source_schema, target_schema)

    if folder_name == "procedures":
        sql_text = normalize_procedure_header(sql_text, target_schema, object_name)
    elif folder_name == "functions":
        sql_text = normalize_function_header(sql_text, target_schema, object_name)
    elif folder_name in ("packages", "package_bodies"):
        sql_text = normalize_package_header(sql_text, target_schema, object_name)
    elif folder_name == "triggers":
        sql_text = normalize_trigger_header(sql_text, target_schema, object_name)

    return sql_text

# =========================================================
# TABLE DEPLOY
# =========================================================
def deploy_table(cursor, ddl_sql: str, table_name: str, preserve_backup_tables=True):
    if not object_exists(cursor, table_name, "TABLE"):
        execute_sql(cursor, ddl_sql)
        return None

    row_count = get_table_row_count(cursor, table_name)

    if row_count == 0:
        drop_table(cursor, table_name)
        execute_sql(cursor, ddl_sql)
        return 0

    backup_table = backup_existing_table(cursor, table_name)
    drop_table(cursor, table_name)
    execute_sql(cursor, ddl_sql)
    migrate_data_from_backup(cursor, backup_table, table_name)

    if not preserve_backup_tables:
        drop_table(cursor, backup_table)

    return row_count

# =========================================================
# INVALID OBJECT COMPILER
# =========================================================
def compile_invalid_objects(cursor):
    cursor.execute("""
        SELECT OBJECT_NAME, OBJECT_TYPE
        FROM USER_OBJECTS
        WHERE STATUS = 'INVALID'
        ORDER BY OBJECT_TYPE, OBJECT_NAME
    """)
    rows = cursor.fetchall()

    for obj_name, obj_type in rows:
        try:
            if obj_type == "PROCEDURE":
                cursor.execute(f'ALTER PROCEDURE "{obj_name}" COMPILE')
            elif obj_type == "FUNCTION":
                cursor.execute(f'ALTER FUNCTION "{obj_name}" COMPILE')
            elif obj_type == "PACKAGE":
                cursor.execute(f'ALTER PACKAGE "{obj_name}" COMPILE')
            elif obj_type == "PACKAGE BODY":
                cursor.execute(f'ALTER PACKAGE "{obj_name}" COMPILE BODY')
            elif obj_type == "VIEW":
                cursor.execute(f'ALTER VIEW "{obj_name}" COMPILE')
            elif obj_type == "TRIGGER":
                cursor.execute(f'ALTER TRIGGER "{obj_name}" COMPILE')
        except:
            pass

# =========================================================
# MAIN DEPLOY
# =========================================================
def main():
    cfg = load_config()

    user = cfg["user"].upper()
    source_schema = cfg.get("source_schema", user).upper()
    preserve_backup_tables = cfg.get("preserve_backup_tables", True)
    stop_on_error = cfg.get("stop_on_error", False)

    conn = connect_oracle(cfg)
    cur = conn.cursor()

    logs = []
    report_rows = []

    deploy_order = [
        ("tables", "TABLE"),
        ("sequences", "SEQUENCE"),
        ("views", "VIEW"),
        ("functions", "FUNCTION"),
        ("packages", "PACKAGE"),
        ("package_bodies", "PACKAGE BODY"),
        ("procedures", "PROCEDURE"),
        ("triggers", "TRIGGER"),
        ("indexes", "INDEX"),
        ("synonyms", "SYNONYM"),
    ]

    print("=" * 100)
    print("ORACLE ENTERPRISE DEPLOYMENT TOOL")
    print("=" * 100)
    print(f"TARGET USER   : {user}")
    print(f"SOURCE SCHEMA : {source_schema}")
    print("=" * 100)

    for folder_name, object_type in deploy_order:
        folder = DDL_DIR / folder_name
        if not folder.exists():
            continue

        sql_files = sorted(folder.glob("*.sql"))
        if not sql_files:
            continue

        print(f"\\n📁 DEPLOYING {folder_name.upper()} ({len(sql_files)} files)")
        print("-" * 100)

        for file_path in sql_files:
            obj_name = file_path.stem.upper()
            row_count = None

            try:
                print(f"\\n🚀 {object_type}: {file_path.name}")

                ddl_sql = file_path.read_text(encoding="utf-8", errors="ignore")
                ddl_sql = prepare_sql(ddl_sql, folder_name, obj_name, source_schema, user)

                if folder_name == "tables":
                    table_name = extract_table_name_from_ddl(ddl_sql)
                    if not table_name:
                        raise Exception(f"Tidak bisa parse nama table dari file: {file_path.name}")

                    row_count = deploy_table(cur, ddl_sql, table_name, preserve_backup_tables)
                    obj_status = get_object_status(cur, table_name, "TABLE")
                    actual_name = table_name
                else:
                    execute_sql(cur, ddl_sql)
                    obj_status = get_object_status(cur, obj_name, object_type)
                    actual_name = obj_name

                logs.append(f"[SUCCESS] {object_type} {actual_name}")
                report_rows.append([
                    object_type,
                    actual_name,
                    "SUCCESS",
                    obj_status,
                    row_count,
                    ""
                ])

            except Exception as e:
                err = str(e)
                detail = traceback.format_exc()

                print(f"❌ FAILED: {obj_name} -> {err}")
                logs.append(f"[FAILED] {object_type} {obj_name} -> {err}")
                logs.append(detail)

                report_rows.append([
                    object_type,
                    obj_name,
                    "FAILED",
                    "",
                    row_count,
                    err
                ])

                if stop_on_error:
                    conn.rollback()
                    log_file = save_log(logs)
                    excel_file = save_excel_report(report_rows)
                    print(f"\\n📝 Log: {log_file}")
                    print(f"📊 Report: {excel_file}")
                    raise

    print("\\n🔄 Compiling invalid objects...")
    compile_invalid_objects(cur)

    print("\\n🔍 Checking invalid objects...")
    cur.execute("""
        SELECT OBJECT_NAME, OBJECT_TYPE
        FROM USER_OBJECTS
        WHERE STATUS = 'INVALID'
        ORDER BY OBJECT_TYPE, OBJECT_NAME
    """)
    invalids = cur.fetchall()

    for obj_name, obj_type in invalids:
        errors = get_compile_errors(cur, obj_name, obj_type)
        err_text = " | ".join([f"LINE {l}, COL {p}: {t}" for l, p, t in errors]) if errors else "NO ERROR DETAILS"

        report_rows.append([
            obj_type,
            obj_name,
            "POST-COMPILE INVALID",
            "INVALID",
            "",
            err_text
        ])

        print(f"❌ INVALID {obj_type}: {obj_name}")
        for l, p, t in errors:
            print(f"   LINE {l}, COL {p} -> {t}")

    conn.commit()

    log_file = save_log(logs)
    excel_file = save_excel_report(report_rows)

    print("\\n" + "=" * 100)
    print("DEPLOYMENT FINISHED")
    print("=" * 100)
    print(f"📝 Log file   : {log_file}")
    print(f"📊 Excel file : {excel_file}")
    print("=" * 100)

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()