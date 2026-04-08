import json
import traceback
import re
from pathlib import Path
from datetime import datetime

import oracledb

# =========================================================
# PATHS
# =========================================================
BASE_DIR = Path(__file__).resolve().parent
SOURCE_CONFIG_PATH = BASE_DIR / "config_source.json"
TARGET_CONFIG_PATH = BASE_DIR / "config_target.json"
OUTPUT_DIR = BASE_DIR / "ddl" / "procedures"
LOG_DIR = BASE_DIR / "logs"

# =========================================================
# CONFIG
# =========================================================
def load_json(path: Path):
    if not path.exists():
        raise FileNotFoundError(f"Config tidak ditemukan: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def normalize_config(raw_cfg: dict) -> dict:
    cfg = dict(raw_cfg)

    user = cfg.get("user") or cfg.get("username") or cfg.get("db_user")
    password = cfg.get("password") or cfg.get("db_password")
    dsn = cfg.get("dsn") or cfg.get("db_dsn")

    if not dsn:
        host = cfg.get("host")
        port = cfg.get("port", 1521)
        service_name = cfg.get("service_name") or cfg.get("sid")
        if host and service_name:
            dsn = f"{host}:{port}/{service_name}"

    final_cfg = {
        "user": user,
        "password": password,
        "dsn": dsn
    }

    validate_config(final_cfg)
    return final_cfg

def validate_config(cfg: dict):
    missing = []
    if not cfg.get("user"):
        missing.append("user")
    if not cfg.get("password"):
        missing.append("password")
    if not cfg.get("dsn"):
        missing.append("dsn")

    if missing:
        raise ValueError("Config tidak lengkap: " + ", ".join(missing))

def load_source_config():
    return normalize_config(load_json(SOURCE_CONFIG_PATH))

def load_target_config():
    return normalize_config(load_json(TARGET_CONFIG_PATH))

# =========================================================
# DB
# =========================================================
def connect_oracle(cfg: dict):
    return oracledb.connect(
        user=cfg["user"],
        password=cfg["password"],
        dsn=cfg["dsn"]
    )

# =========================================================
# HELPERS
# =========================================================
def now_str():
    return datetime.now().strftime("%Y%m%d_%H%M%S")

def log(msg: str):
    print(msg)

def write_log(lines):
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_file = LOG_DIR / f"redeploy_procedures_{now_str()}.log"
    with open(log_file, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return log_file

# =========================================================
# SQL CLEANER / REWRITER
# =========================================================
def clean_metadata_sql(sql_text: str) -> str:
    """
    Rapihkan hasil DBMS_METADATA agar aman dieksekusi ulang.
    """
    if not sql_text:
        return sql_text

    sql_text = sql_text.replace("\r\n", "\n").replace("\r", "\n").strip()

    # Hapus slash berlebih di akhir kalau ada
    while sql_text.endswith("/"):
        sql_text = sql_text[:-1].rstrip()

    return sql_text

def rewrite_schema_references(sql_text: str, source_schema: str, target_schema: str) -> str:
    """
    Rewrite semua referensi schema di body SQL.
    """
    if not sql_text:
        return sql_text

    source_schema = source_schema.upper()
    target_schema = target_schema.upper()

    replacements = [
        (f'"{source_schema}"."', f'{target_schema}.'),
        (f'"{source_schema}".', f'{target_schema}.'),
        (f'{source_schema}.', f'{target_schema}.'),
    ]

    for old, new in replacements:
        sql_text = sql_text.replace(old, new)

    return sql_text

def normalize_procedure_header(sql_text: str, target_schema: str, procedure_name: str) -> str:
    """
    Bangun ulang header procedure Oracle agar selalu valid.
    Apa pun bentuk header dari DBMS_METADATA, hasil final akan jadi:

    CREATE OR REPLACE [EDITIONABLE/NONEDITIONABLE] PROCEDURE TARGET_SCHEMA.PROC_NAME
    """
    if not sql_text:
        return sql_text

    target_schema = target_schema.upper()
    procedure_name = procedure_name.upper()

    lines = sql_text.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    if not lines:
        return sql_text

    # Cari baris header CREATE OR REPLACE ... PROCEDURE
    header_idx = None
    header_line = None

    for i, line in enumerate(lines):
        stripped = line.strip()
        if re.match(
            r'^CREATE\s+OR\s+REPLACE\s+(?:NONEDITIONABLE\s+|EDITIONABLE\s+)?PROCEDURE\b',
            stripped,
            re.IGNORECASE
        ):
            header_idx = i
            header_line = stripped
            break

    if header_idx is None:
        return sql_text

    # Ambil prefix sampai kata PROCEDURE saja
    m = re.match(
        r'^(CREATE\s+OR\s+REPLACE\s+(?:NONEDITIONABLE\s+|EDITIONABLE\s+)?PROCEDURE)\b',
        header_line,
        re.IGNORECASE
    )

    if not m:
        return sql_text

    prefix = m.group(1).upper()

    # Bangun ulang header secara paksa
    clean_header = f"{prefix} {target_schema}.{procedure_name}"

    lines[header_idx] = clean_header

    return "\n".join(lines)

def rewrite_procedure_ddl(sql_text: str, source_schema: str, target_schema: str, procedure_name: str) -> str:
    sql_text = clean_metadata_sql(sql_text)
    sql_text = normalize_procedure_header(sql_text, target_schema, procedure_name)
    sql_text = rewrite_schema_references(sql_text, source_schema, target_schema)
    return sql_text

# =========================================================
# EXTRACT PROCEDURE LIST
# =========================================================
def get_all_procedures(cursor):
    cursor.execute("""
        SELECT OBJECT_NAME
        FROM USER_OBJECTS
        WHERE OBJECT_TYPE = 'PROCEDURE'
        ORDER BY OBJECT_NAME
    """)
    return [row[0] for row in cursor.fetchall()]

# =========================================================
# GET DDL
# =========================================================
def configure_metadata_session(cursor):
    cursor.execute("""
        BEGIN
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SQLTERMINATOR', TRUE);
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'PRETTY', TRUE);
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'STORAGE', FALSE);
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'TABLESPACE', FALSE);
            DBMS_METADATA.SET_TRANSFORM_PARAM(DBMS_METADATA.SESSION_TRANSFORM, 'SEGMENT_ATTRIBUTES', FALSE);
        END;
    """)

def get_procedure_ddl(cursor, procedure_name: str):
    sql = """
        SELECT DBMS_METADATA.GET_DDL('PROCEDURE', :obj_name, USER)
        FROM DUAL
    """
    cursor.execute(sql, obj_name=procedure_name)
    row = cursor.fetchone()
    if not row or not row[0]:
        return None

    ddl = row[0].read() if hasattr(row[0], "read") else str(row[0])
    return ddl

# =========================================================
# FILE EXPORT
# =========================================================
def save_procedure_sql(procedure_name: str, ddl: str):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    file_path = OUTPUT_DIR / f"{procedure_name}.sql"
    file_path.write_text(ddl, encoding="utf-8")
    return file_path

# =========================================================
# EXECUTE TARGET
# =========================================================
def execute_ddl(cursor, ddl: str, object_name: str):
    ddl = ddl.strip()
    if ddl.endswith(";"):
        ddl = ddl[:-1].rstrip()

    if not ddl:
        return

    print("\n" + "=" * 100)
    print(f"🚀 EXECUTING PROCEDURE: {object_name}")
    print("=" * 100)
    print(ddl[:4000])
    print("=" * 100)

    cursor.execute(ddl)

# =========================================================
# MAIN PROCESS
# =========================================================
def main():
    source_cfg = load_source_config()
    target_cfg = load_target_config()

    source_schema = source_cfg["user"].upper()
    target_schema = target_cfg["user"].upper()

    print("=" * 100)
    print("ORACLE PROCEDURE EXTRACT + REDEPLOY TOOL")
    print("=" * 100)
    print(f"SOURCE : {source_schema} @ {source_cfg['dsn']}")
    print(f"TARGET : {target_schema} @ {target_cfg['dsn']}")
    print("=" * 100)

    source_conn = connect_oracle(source_cfg)
    target_conn = connect_oracle(target_cfg)

    source_cur = source_conn.cursor()
    target_cur = target_conn.cursor()

    logs = []
    success = []
    failed = []

    try:
        configure_metadata_session(source_cur)

        procedures = get_all_procedures(source_cur)
        print(f"\n📦 Total Procedures ditemukan: {len(procedures)}")

        for i, proc_name in enumerate(procedures, start=1):
            try:
                print(f"\n[{i}/{len(procedures)}] Processing: {proc_name}")

                ddl = get_procedure_ddl(source_cur, proc_name)
                if not ddl:
                    raise Exception("DDL procedure kosong / tidak ditemukan")

                # ddl = rewrite_procedure_ddl(ddl, source_schema, target_schema)
                ddl = rewrite_procedure_ddl(ddl, source_schema, target_schema, proc_name)

                file_path = save_procedure_sql(proc_name, ddl)
                print(f"💾 Saved: {file_path}")

                execute_ddl(target_cur, ddl, proc_name)

                success.append(proc_name)
                logs.append(f"[SUCCESS] {proc_name}")

            except Exception as e:
                failed.append(proc_name)
                err = f"[FAILED] {proc_name} -> {str(e)}"
                logs.append(err)
                logs.append(traceback.format_exc())
                print(f"❌ {err}")

        target_conn.commit()

        print("\n" + "=" * 100)
        print("SUMMARY")
        print("=" * 100)
        print(f"SUCCESS : {len(success)}")
        print(f"FAILED  : {len(failed)}")

        if success:
            print("\n✔ SUCCESS LIST")
            for x in success:
                print(f"  - {x}")

        if failed:
            print("\n✖ FAILED LIST")
            for x in failed:
                print(f"  - {x}")

        log_file = write_log(logs)
        print(f"\n📝 Log file: {log_file}")

    except Exception as e:
        print("\n🔥 FATAL ERROR")
        print(str(e))
        print(traceback.format_exc())
        target_conn.rollback()

    finally:
        source_cur.close()
        target_cur.close()
        source_conn.close()
        target_conn.close()
        print("\n🔒 Connections closed.")

if __name__ == "__main__":
    main()