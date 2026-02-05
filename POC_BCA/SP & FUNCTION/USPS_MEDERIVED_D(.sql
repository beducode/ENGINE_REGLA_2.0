CREATE OR REPLACE PROCEDURE  USPS_MEDERIVED_D(
    v_MODEL_ID number,
    v_ME_CODE varchar2,
    Cur_out OUT SYS_REFCURSOR
)
AS
    v_ColumnName varchar2(30000);
    v_ColumnNameAlias varchar2(32767);
    v_Query varchar2(30000);
BEGIN
    SELECT LISTAGG('''' || ME_CODE || ''' "' || ME_CODE || '"', ',')
    WITHIN GROUP (ORDER BY ME_CODE)
    INTO v_ColumnName
    FROM
    (
        SELECT ME_CODE
        FROM IFRS_ME_DERIVED_VARIABLE_PEN
        WHERE MODEL_ID = v_MODEL_ID AND ME_CODE LIKE v_ME_CODE || '%'
        GROUP BY ME_CODE
    )B;

    SELECT LISTAGG('"' || ME_CODE || '"', ',')
    WITHIN GROUP (ORDER BY ME_CODE)
    INTO v_ColumnNameAlias
    FROM
    (
        SELECT ME_CODE
        FROM IFRS_ME_DERIVED_VARIABLE_PEN
        WHERE MODEL_ID = v_MODEL_ID AND ME_CODE LIKE v_ME_CODE || '%'
        GROUP BY ME_CODE
    )B;

    v_Query :=
        'SELECT
            PERIOD
            ,' || v_ColumnNameAlias || '
        FROM(
            SELECT
                ME_CODE,
                ME_PERIOD AS PERIOD,
                CAST(ME_VALUE AS VARCHAR2(250)) AS ME_VALUE
            FROM IFRS_ME_DERIVED_VARIABLE_PEN
            WHERE MODEL_ID = ' || TO_CHAR(v_MODEL_ID) || '
                AND ME_CODE LIKE ''' || v_ME_CODE || '%''
            )TMP
            PIVOT(MAX(ME_VALUE)
                    FOR ME_CODE IN (' || v_ColumnName || ')
            )PVT
        ORDER BY PERIOD';

    OPEN Cur_out FOR v_Query;

END;