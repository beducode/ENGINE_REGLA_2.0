CREATE OR REPLACE FUNCTION FN_GENERATE_RULE(v_RULE_ID NUMBER DEFAULT 0, v_DETAIL_TYPE VARCHAR2 DEFAULT ' ')
return VARCHAR
AS
    v_Script1 varchar2(4000);
BEGIN

    v_Script1 := ' ';

    for seg_rule in
    (
    SELECT
      --add double quote to column_name
      'A."' || column_name || '"' v_column_name ,
      data_type v_data_type,
      operator v_operator,
      value1 v_value1,
      value2 v_value2,
      QUERY_GROUPING v_QG,
      AND_OR_CONDITION v_AOC,
      LAG(QUERY_GROUPING, 1, MIN_QG) OVER (PARTITION BY rule_id ORDER BY QUERY_GROUPING, SEQUENCE) v_PREV_QG,
      LEAD(QUERY_GROUPING, 1, MAX_QG) OVER (PARTITION BY rule_id ORDER BY QUERY_GROUPING, SEQUENCE) v_NEXT_QG,
      jml v_jml,
      rn v_rn,
      PKID v_PKID
    FROM (SELECT
      MIN(QUERY_GROUPING) OVER (PARTITION BY rule_id) MIN_QG,
      MAX(QUERY_GROUPING) OVER (PARTITION BY rule_id) MAX_QG,
      ROW_NUMBER() OVER (PARTITION BY rule_id ORDER BY QUERY_GROUPING, sequence) rn,
      COUNT(0) OVER (PARTITION BY rule_id) jml,
      column_name,
      data_type,
      operator,
      value1,
      value2,
      QUERY_GROUPING,
      SEQUENCE,
      rule_id,
      AND_OR_CONDITION,
      PKID
    FROM IFRS_SCENARIO_RULES_DETAIL
    WHERE rule_id = v_RULE_ID
    and (detail_type = v_DETAIL_TYPE or NVL(v_DETAIL_TYPE,' ') = ' ')
    ) A)
    loop

      v_Script1 :=
      NVL(V_Script1, ' ') || ' ' || seg_rule.V_AOC || ' ' || CASE
        WHEN seg_rule.v_QG <> seg_rule.v_PREV_QG THEN '('
        ELSE ' '
      END
      || NVL(CASE
        WHEN RTRIM(LTRIM(seg_rule.v_data_type)) IN ('NUMBER', 'DECIMAL', 'NUMERIC', 'FLOAT', 'INT') THEN CASE
            WHEN seg_rule.v_operator IN ('=', '<>', '>', '<', '>=', '<=') THEN NVL('NVL(' || seg_rule.v_column_name || ',0)', '')
              || ' '
              || NVL(seg_rule.v_operator, '')
              || ' '
              || NVL(seg_rule.v_value1, '')
            WHEN LOWER(seg_rule.v_operator) = 'between' THEN NVL('NVL(' || seg_rule.v_column_name || ',0)', '')
              || ' '
              || NVL(seg_rule.v_operator, '')
              || ' '
              || NVL(seg_rule.v_value1, '')
              || ' and '
              || NVL(seg_rule.v_value2, '')
            WHEN LOWER(seg_rule.v_operator) = 'in' THEN NVL('NVL(' || seg_rule.v_column_name || ',0)', '')
              || ' '
              || NVL(seg_rule.v_operator, '')
              || ' '
              || '('
              || NVL(seg_rule.v_value1, '')
              || ')'
            ELSE 'xxx'
          END
        WHEN RTRIM(LTRIM(seg_rule.v_data_type)) = 'BOOLEAN' THEN
            NVL('NVL(' || seg_rule.v_column_name || ',0)', '')
              || ' '
              || NVL(seg_rule.v_operator, '')
              || ' '
              || CASE WHEN NVL(seg_rule.v_value1, 'FALSE') = 'FALSE' THEN '0' ELSE '1' END
        WHEN RTRIM(LTRIM(seg_rule.v_data_type)) = 'DATE' THEN CASE
            WHEN seg_rule.v_operator IN ('=', '<>', '>', '<', '>=', '<=') THEN NVL(seg_rule.v_column_name, '')
              || ' '
              || NVL(seg_rule.v_operator, '')
              || '  to_date('''
              || NVL(seg_rule.v_value1, '')
              || ''',''MM/DD/YYYY'')'
            WHEN LOWER(seg_rule.v_operator) = 'between' THEN NVL(seg_rule.v_column_name, '')
              || ' '
              || NVL(seg_rule.v_operator, '')
              || ' '
              || '   CONVERT(DATE,'''
              || NVL(seg_rule.v_value1, '')
              || ''',110)'
              || ' and '
              || '  CONVERT(DATE,'''
              || NVL(seg_rule.v_value2, '')
              || ''',110)'
            WHEN LOWER(seg_rule.v_operator) IN ('=', '<>', '>', '<', '>=', '<=') THEN NVL(seg_rule.v_column_name, '')
              || ' '
              || NVL(seg_rule.v_operator, '')
              || ' '
              || '('
              || '  to_date('''
              || NVL(seg_rule.v_value1, '')
              || ''',''MM/DD/YYYY'')'
              || ')'
            ELSE 'xXx'
          END
        WHEN UPPER(RTRIM(LTRIM(seg_rule.v_data_type))) IN ('CHAR', 'VARCHAR', 'VARCHAR2') THEN CASE
            WHEN RTRIM(LTRIM(seg_rule.v_operator)) = '=' THEN NVL('NVL(' || seg_rule.v_column_name || ', '' '')', ' ')
              || ' '
              || NVL(seg_rule.v_operator, ' ')
              || ''''
              || NVL(seg_rule.v_value1, ' ')
              || ''''
            WHEN RTRIM(LTRIM(LOWER(seg_rule.v_operator))) = 'between' THEN NVL('NVL(' || seg_rule.v_column_name || ', '' '')', ' ')
              || ' '
              || NVL(seg_rule.v_operator, '')
              || '  '
              || NVL(seg_rule.v_value1, '')
              || ' and '
              || NVL(seg_rule.v_value2, '')
            WHEN RTRIM(LTRIM(LOWER(seg_rule.v_operator))) IN ('in', 'not in', 'like', 'not like') THEN NVL('NVL(' || seg_rule.v_column_name || ', '' '')', ' ')
              || ' '
              || NVL(seg_rule.v_operator, '')
              || '  '
              || '('''
              || NVL(REPLACE(seg_rule.v_value1, ',', ''','''), '')
              || ''')'
            ELSE 'XXX'
          END
        ELSE 'XxX'
      END, ' ') || CASE
        WHEN seg_rule.v_QG <> seg_rule.v_NEXT_QG OR
          seg_rule.v_rn = seg_rule.v_jml THEN ')'
        ELSE ' '
      END;
    end loop;

    v_Script1 := '(' || LTRIM(SUBSTR(v_Script1, 6, LENGTH(RTRIM(v_Script1))));

    RETURN v_Script1;

END;