CREATE OR REPLACE PROCEDURE USPS_GETDATASELISTBOX(
	v_TableName VARCHAR2,
	v_ColumnName VARCHAR2,
	v_CurrValues VARCHAR2,
	Cur_Out OUT SYS_REFCURSOR
)
AS
	v_Query VARCHAR2(4000);
	v_TableName2 VARCHAR2(500);
	v_ColumnName2 VARCHAR2(500);
	v_CurrValues2 VARCHAR2(500);
BEGIN
    IF UPPER(v_ColumnName) = 'PRODUCT_TYPE'
        THEN
        v_ColumnName2 := 'PRD_TYPE';
        v_TableName2 := 'IFRS_MASTER_PRODUCT_PARAM';
    ELSIF UPPER(v_ColumnName) = 'PRODUCT_CODE'
        THEN
        v_ColumnName2 := 'PRD_CODE';
        v_TableName2 := 'IFRS_MASTER_PRODUCT_PARAM';
    ELSE
         v_ColumnName2 := v_ColumnName;
        v_TableName2 := v_TableName;
    END IF;

	IF v_CurrValues IS NOT NULL
		THEN v_CurrValues2 :=
			'AND ' || v_ColumnName2 || ' NOT IN (
			SELECT REGEXP_SUBSTR('''||v_CurrValues||''', ''[^,]+'', 1, LEVEL) AS ' || v_ColumnName2 || '
			FROM DUAL
			CONNECT BY REGEXP_SUBSTR('''||v_CurrValues||''', ''[^,]+'', 1, LEVEL) IS NOT NULL)';
	ELSE
		v_CurrValues2 := ' ';
	END IF;

	v_Query :=
		'SELECT DISTINCT ' || v_ColumnName2 || ' AS Code
		FROM ' || v_TableName2 || '
		WHERE ' || v_ColumnName2 || ' IS NOT NULL
		AND TRIM(' || v_ColumnName2 || ') <> '' ''
		'|| v_CurrValues2 ||'
		ORDER BY ' || v_ColumnName2 || ' ASC';

	OPEN Cur_Out FOR v_Query;
END;