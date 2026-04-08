import os
import re
from pathlib import Path

# =========================================
# CONFIG
# =========================================
INPUT_FILE = "DDL View.txt"   # ganti sesuai nama file Anda
OUTPUT_DIR = "output_db_objects"

# =========================================
# HELPERS
# =========================================
def normalize_text(text: str) -> str:
    return text.replace("\r\n", "\n").replace("\r", "\n")

def sanitize_filename(name: str) -> str:
    name = name.replace('"', '').strip()
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    return name

def extract_simple_object_name(full_name: str) -> str:
    """
    Ambil nama object terakhir dari:
    PSAK413.NITIP_ADAM -> NITIP_ADAM
    "PSAK413"."NITIP_ADAM" -> NITIP_ADAM
    "NITIP_ADAM" -> NITIP_ADAM
    """
    full_name = full_name.strip()
    full_name = full_name.replace('"', '')
    parts = full_name.split('.')
    return sanitize_filename(parts[-1])

def ensure_oracle_slash(obj_type: str, sql_text: str) -> str:
    """
    Tambahkan '/' di akhir untuk object PL/SQL jika belum ada.
    """
    plsql_types = {
        "procedures",
        "functions",
        "packages",
        "package_bodies",
        "triggers",
        "views",  # optional, aman untuk Oracle script style
    }

    sql_text = sql_text.rstrip()

    if obj_type in plsql_types:
        if not re.search(r'\n/\s*$', sql_text):
            sql_text += "\n/"
    else:
        if not sql_text.endswith(";"):
            sql_text += ";"

    return sql_text + "\n"

def detect_object(line: str):
    """
    Return tuple:
    (object_folder, object_name, object_kind)
    atau None
    """
    line_clean = line.strip()

    patterns = [
        # PACKAGE BODY
        (
            r'(?i)^CREATE\s+OR\s+REPLACE\s+PACKAGE\s+BODY\s+((?:"?[\w$#]+"?\.)?"?[\w$#]+"?)',
            "package_bodies"
        ),
        # PACKAGE
        (
            r'(?i)^CREATE\s+OR\s+REPLACE\s+PACKAGE\s+((?:"?[\w$#]+"?\.)?"?[\w$#]+"?)',
            "packages"
        ),
        # PROCEDURE
        (
            r'(?i)^CREATE\s+OR\s+REPLACE\s+PROCEDURE\s+((?:"?[\w$#]+"?\.)?"?[\w$#]+"?)',
            "procedures"
        ),
        # FUNCTION
        (
            r'(?i)^CREATE\s+OR\s+REPLACE\s+FUNCTION\s+((?:"?[\w$#]+"?\.)?"?[\w$#]+"?)',
            "functions"
        ),
        # TRIGGER
        (
            r'(?i)^CREATE\s+OR\s+REPLACE\s+TRIGGER\s+((?:"?[\w$#]+"?\.)?"?[\w$#]+"?)',
            "triggers"
        ),
        # VIEW
        (
            r'(?i)^CREATE\s+(?:OR\s+REPLACE\s+)?VIEW\s+((?:"?[\w$#]+"?\.)?"?[\w$#]+"?)',
            "views"
        ),
        # TABLE
        (
            r'(?i)^CREATE\s+TABLE\s+((?:"?[\w$#]+"?\.)?"?[\w$#]+"?)',
            "tables"
        ),
        # SEQUENCE
        (
            r'(?i)^CREATE\s+SEQUENCE\s+((?:"?[\w$#]+"?\.)?"?[\w$#]+"?)',
            "sequences"
        ),
        # SYNONYM
        (
            r'(?i)^CREATE\s+(?:OR\s+REPLACE\s+)?SYNONYM\s+((?:"?[\w$#]+"?\.)?"?[\w$#]+"?)',
            "synonyms"
        ),
        # INDEX
        (
            r'(?i)^CREATE\s+(?:UNIQUE\s+)?INDEX\s+((?:"?[\w$#]+"?\.)?"?[\w$#]+"?)',
            "indexes"
        ),
    ]

    for pattern, folder in patterns:
        m = re.match(pattern, line_clean)
        if m:
            full_name = m.group(1)
            obj_name = extract_simple_object_name(full_name)
            return folder, obj_name, full_name

    return None

def is_start_of_object(line: str) -> bool:
    return detect_object(line) is not None

# =========================================
# MAIN
# =========================================
def split_ddl_file(input_file: str, output_dir: str):
    with open(input_file, "r", encoding="utf-8", errors="ignore") as f:
        content = normalize_text(f.read())

    lines = content.split("\n")

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    objects = []
    current_lines = []
    current_meta = None
    unknown_counter = 1

    i = 0
    while i < len(lines):
        line = lines[i]

        # Deteksi object baru
        obj = detect_object(line)

        if obj:
            # Simpan object sebelumnya kalau ada
            if current_lines and current_meta:
                objects.append((current_meta, "\n".join(current_lines).strip()))

            current_meta = obj
            current_lines = [line]
            i += 1
            continue

        # Kalau belum ada object aktif, skip
        if current_meta is None:
            i += 1
            continue

        current_lines.append(line)

        # RULE ENDING:
        # 1. PL/SQL block biasanya ditutup dengan baris "/" sendiri
        # 2. TABLE / INDEX / SEQUENCE / SYNONYM biasanya cukup ";"
        folder = current_meta[0]
        stripped = line.strip()

        if folder in {"procedures", "functions", "packages", "package_bodies", "triggers"}:
            if stripped == "/":
                objects.append((current_meta, "\n".join(current_lines).strip()))
                current_lines = []
                current_meta = None

        elif folder in {"views"}:
            # view kadang pakai ";" saja, kadang "/"
            if stripped == "/" or stripped.endswith(";"):
                objects.append((current_meta, "\n".join(current_lines).strip()))
                current_lines = []
                current_meta = None

        elif folder in {"tables", "sequences", "synonyms", "indexes"}:
            if stripped.endswith(";"):
                objects.append((current_meta, "\n".join(current_lines).strip()))
                current_lines = []
                current_meta = None

        i += 1

    # Simpan object terakhir kalau masih ada
    if current_lines and current_meta:
        objects.append((current_meta, "\n".join(current_lines).strip()))

    # =========================================
    # WRITE FILES
    # =========================================
    stats = {}

    for meta, sql_text in objects:
        folder, obj_name, full_name = meta

        if not obj_name:
            obj_name = f"UNKNOWN_{unknown_counter:03d}"
            unknown_counter += 1
            folder = "unknown"

        target_dir = Path(output_dir) / folder
        target_dir.mkdir(parents=True, exist_ok=True)

        file_name = f"{sanitize_filename(obj_name)}.sql"
        file_path = target_dir / file_name

        final_sql = ensure_oracle_slash(folder, sql_text)

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(final_sql)

        stats[folder] = stats.get(folder, 0) + 1
        print(f"Saved: {file_path}")

    # =========================================
    # SUMMARY
    # =========================================
    print("\n==============================")
    print("SELESAI SPLIT DDL")
    print("==============================")
    total = 0
    for k in sorted(stats.keys()):
        print(f"{k:15} : {stats[k]}")
        total += stats[k]
    print("------------------------------")
    print(f"TOTAL OBJECTS   : {total}")
    print(f"OUTPUT FOLDER   : {output_dir}")

# =========================================
# RUN
# =========================================
if __name__ == "__main__":
    split_ddl_file(INPUT_FILE, OUTPUT_DIR)