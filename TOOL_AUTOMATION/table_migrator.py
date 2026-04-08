import re
from datetime import datetime


def extract_table_name_from_ddl(sql_text: str):
    sql_text = sql_text.replace("\r\n", "\n").replace("\r", "\n")

    patterns = [
        r'CREATE\s+TABLE\s+"[^"]+"\."([^"]+)"',
        r'CREATE\s+TABLE\s+([A-Z0-9_#$]+)\.([A-Z0-9_#$]+)',
        r'CREATE\s+TABLE\s+"([^"]+)"',
        r'CREATE\s+TABLE\s+([A-Z0-9_#$]+)'
    ]

    for p in patterns:
        m = re.search(p, sql_text, re.IGNORECASE)
        if m:
            if len(m.groups()) == 2:
                return m.group(2).upper()
            return m.group(1).upper()

    return None


def get_table_columns(cursor, table_name: str):
    cursor.execute("""
        SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, COLUMN_ID
        FROM USER_TAB_COLUMNS
        WHERE TABLE_NAME = :tbl
        ORDER BY COLUMN_ID
    """, tbl=table_name.upper())

    rows = cursor.fetchall()

    result = {}
    for col_name, data_type, nullable, column_id in rows:
        result[col_name.upper()] = {
            "data_type": data_type.upper(),
            "nullable": nullable.upper(),
            "column_id": column_id
        }
    return result


def get_table_row_count(cursor, table_name: str):
    cursor.execute(f'SELECT COUNT(*) FROM "{table_name.upper()}"')
    return cursor.fetchone()[0]


def get_default_value_for_type(data_type: str):
    dt = data_type.upper()

    if dt in ("VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "CLOB", "NCLOB"):
        return "'-'"
    elif dt in ("NUMBER", "FLOAT", "INTEGER", "BINARY_FLOAT", "BINARY_DOUBLE"):
        return "0"
    elif dt == "DATE":
        return "SYSDATE"
    elif "TIMESTAMP" in dt:
        return "SYSTIMESTAMP"
    else:
        return "NULL"


def backup_existing_table(cursor, table_name: str):
    suffix = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"{table_name.upper()}_BAK_{suffix}"

    sql = f'CREATE TABLE "{backup_name}" AS SELECT * FROM "{table_name.upper()}"'
    cursor.execute(sql)

    return backup_name


def drop_table(cursor, table_name: str):
    sql = f'DROP TABLE "{table_name.upper()}" CASCADE CONSTRAINTS PURGE'
    cursor.execute(sql)


def migrate_data_from_backup(cursor, backup_table: str, target_table: str):
    backup_cols = get_table_columns(cursor, backup_table)
    target_cols = get_table_columns(cursor, target_table)

    insert_columns = []
    select_expressions = []

    for col_name, meta in target_cols.items():
        insert_columns.append(f'"{col_name}"')

        if col_name in backup_cols:
            select_expressions.append(f'"{col_name}"')
        else:
            if meta["nullable"] == "N":
                default_val = get_default_value_for_type(meta["data_type"])
                select_expressions.append(f'{default_val} AS "{col_name}"')
            else:
                select_expressions.append(f'NULL AS "{col_name}"')

    insert_cols_sql = ", ".join(insert_columns)
    select_sql = ", ".join(select_expressions)

    sql = f'''
        INSERT INTO "{target_table.upper()}" ({insert_cols_sql})
        SELECT {select_sql}
        FROM "{backup_table.upper()}"
    '''
    cursor.execute(sql)