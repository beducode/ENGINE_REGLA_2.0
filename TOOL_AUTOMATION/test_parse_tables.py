from table_migrator import default_val

print(default_val("VARCHAR2"))
print(default_val("NUMBER"))
print(default_val("DATE"))
from table_migrator import extract_table_name_from_ddl

samples = [
    'CREATE TABLE "PSAK413"."2019_COMMITMENT" (ID NUMBER)',
    'CREATE TABLE PSAK413.TEST_TABLE (ID NUMBER)',
    'CREATE TABLE "MY_TABLE" (ID NUMBER)',
    'CREATE TABLE SIMPLE_TABLE (ID NUMBER)'
]

for s in samples:
    print(s)
    print("=>", extract_table_name_from_ddl(s))
    print("-" * 80)