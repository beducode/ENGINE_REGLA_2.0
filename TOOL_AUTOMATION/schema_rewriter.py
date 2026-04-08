import re

def rewrite_schema_references(sql_text: str, source_schema: str, target_schema: str) -> str:
    if not sql_text:
        return sql_text

    source_schema = source_schema.upper()
    target_schema = target_schema.upper()

    replacements = [
        (f'"{source_schema}"."', f'"{target_schema}".'),
        (f'"{source_schema}".', f'"{target_schema}".'),
        (f'{source_schema}.', f'{target_schema}.'),
        (f'"{source_schema}"', f'"{target_schema}"')
    ]

    for old, new in replacements:
        sql_text = sql_text.replace(old, new)

    return sql_text


def normalize_procedure_header(sql_text: str, target_schema: str, procedure_name: str) -> str:
    if not sql_text:
        return sql_text

    target_schema = target_schema.upper()
    procedure_name = procedure_name.upper()

    sql_text = sql_text.replace("\r\n", "\n").replace("\r", "\n")

    pattern = re.compile(
        r'^(CREATE\s+OR\s+REPLACE\s+(?:NONEDITIONABLE\s+|EDITIONABLE\s+)?PROCEDURE)\s+.*?$',
        re.IGNORECASE | re.MULTILINE
    )

    match = pattern.search(sql_text)
    if not match:
        return sql_text

    original_header = match.group(0)
    prefix = match.group(1).upper()

    new_header = f"{prefix} {target_schema}.{procedure_name}"
    sql_text = sql_text.replace(original_header, new_header, 1)

    return sql_text


def normalize_function_header(sql_text: str, target_schema: str, function_name: str) -> str:
    if not sql_text:
        return sql_text

    target_schema = target_schema.upper()
    function_name = function_name.upper()

    pattern = re.compile(
        r'^(CREATE\s+OR\s+REPLACE\s+(?:NONEDITIONABLE\s+|EDITIONABLE\s+)?FUNCTION)\s+.*?$',
        re.IGNORECASE | re.MULTILINE
    )

    match = pattern.search(sql_text)
    if not match:
        return sql_text

    original_header = match.group(0)
    prefix = match.group(1).upper()

    new_header = f"{prefix} {target_schema}.{function_name}"
    sql_text = sql_text.replace(original_header, new_header, 1)

    return sql_text


def normalize_package_header(sql_text: str, target_schema: str, package_name: str) -> str:
    if not sql_text:
        return sql_text

    target_schema = target_schema.upper()
    package_name = package_name.upper()

    pattern = re.compile(
        r'^(CREATE\s+OR\s+REPLACE\s+(?:NONEDITIONABLE\s+|EDITIONABLE\s+)?PACKAGE(?:\s+BODY)?)\s+.*?$',
        re.IGNORECASE | re.MULTILINE
    )

    match = pattern.search(sql_text)
    if not match:
        return sql_text

    original_header = match.group(0)
    prefix = match.group(1).upper()

    new_header = f"{prefix} {target_schema}.{package_name}"
    sql_text = sql_text.replace(original_header, new_header, 1)

    return sql_text


def normalize_trigger_header(sql_text: str, target_schema: str, trigger_name: str) -> str:
    if not sql_text:
        return sql_text

    target_schema = target_schema.upper()
    trigger_name = trigger_name.upper()

    pattern = re.compile(
        r'^(CREATE\s+OR\s+REPLACE\s+(?:NONEDITIONABLE\s+|EDITIONABLE\s+)?TRIGGER)\s+.*?$',
        re.IGNORECASE | re.MULTILINE
    )

    match = pattern.search(sql_text)
    if not match:
        return sql_text

    original_header = match.group(0)
    prefix = match.group(1).upper()

    new_header = f"{prefix} {target_schema}.{trigger_name}"
    sql_text = sql_text.replace(original_header, new_header, 1)

    return sql_text