CREATE OR REPLACE EDITIONABLE PROCEDURE PSAK413.SP_IFRS_EIL_UPDATE_PORTFOLIO
    p_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
	p_DOWNLOAD_DATE IN DATE,
    p_PRC           IN CHAR DEFAULT 'M'
)
AS
    v_RULE_ID        VARCHAR2(250);
    v_TABLE_NAME     VARCHAR2(100);
    v_STR_SQL        CLOB;
    v_GROUP_SEGMENT  VARCHAR2(250);
    v_SEGMENT        VARCHAR2(250);
    v_SUB_SEGMENT    VARCHAR2(250);
    v_CONDITION      VARCHAR2(4000);
    v_CURRDATE       DATE;
    v_EXCEPT_ID      VARCHAR2(50);
    v_ERROR_FLAG     NUMBER(1) := 0;
    v_TABLENAME      VARCHAR2(100);
BEGIN
    ----------------------------------------------------------------
    -- INIT
    ----------------------------------------------------------------
    IF p_DOWNLOAD_DATE IS NULL THEN
        SELECT CURRDATE INTO v_CURRDATE FROM IFRS_PRC_DATE;
    ELSE
        v_CURRDATE := p_DOWNLOAD_DATE;
    END IF;


    v_TABLENAME :=
        CASE
            WHEN p_PRC = 'M' THEN 'IFRS_MASTER_ACCOUNT_MONTHLY'
            ELSE 'IFRS_MASTER_ACCOUNT_MONTHLY_' || p_RUNID ||''
        END;

    ----------------------------------------------------------------
    -- RESET SEGMENT DATA (DYNAMIC UPDATE)
    ----------------------------------------------------------------
    v_STR_SQL :=
        'UPDATE ' || v_TABLENAME || ' A ' ||
        'SET A.SUB_SEGMENT = NULL, ' ||
        '    A.SEGMENT = NULL, ' ||
        '    A.GROUP_SEGMENT = NULL, ' ||
        '    A.SEGMENTATION_ID = NULL ' ||
        'WHERE A.DOWNLOAD_DATE = DATE ''' ||
        TO_CHAR(v_CURRDATE, 'YYYY-MM-DD') || '''';

    EXECUTE IMMEDIATE v_STR_SQL;
   
   
   v_STR_SQL :=
    ' DELETE PSAK413.IFRS_EXCEPTION_ACCOUNT WHERE EXCEPTION_ID = 1 AND DOWNLOAD_DATE = DATE ''' ||
            TO_CHAR(v_CURRDATE, 'YYYY-MM-DD') || ''' ';
       
EXECUTE IMMEDIATE v_STR_SQL;

    ----------------------------------------------------------------
    -- CURSOR LOOP
    ----------------------------------------------------------------
    FOR rec IN (
        SELECT PKID AS RULE_ID,
               v_TABLENAME AS TABLE_NAME,
               SEGMENT_NAME_LV1 AS GROUP_SEGMENT,
               SEGMENT_NAME_LV2 AS SEGMENT,
               SEGMENT_NAME_LV3 AS SUB_SEGMENT,
               MERGE_SQL_CONDITIONS AS CONDITION
        FROM IFRS_SEGMENTATION_MAPPING
        WHERE SEGMENT_TYPE = 'PORTFOLIO'
    )
    LOOP
        v_RULE_ID       := rec.RULE_ID;
        v_TABLE_NAME    := rec.TABLE_NAME;
        v_GROUP_SEGMENT := rec.GROUP_SEGMENT;
        v_SEGMENT       := rec.SEGMENT;
        v_SUB_SEGMENT   := rec.SUB_SEGMENT;
        v_CONDITION     := rec.CONDITION;
       
      
      v_STR_SQL :=
    ' INSERT INTO PSAK413.IFRS_EXCEPTION_ACCOUNT 
	(DOWNLOAD_DATE, DATA_SOURCE, EXCEPTION_ID, MASTERID, ACCOUNT_NUMBER, CUSTOMER_NAME,
    PRODUCT_GROUP, TABLE_NAME, FIELD_NAME, VALUE, CREATEDBY, CREATEDDATE, CREATEDHOST,
    FLAG)
	SELECT DOWNLOAD_DATE , DATA_SOURCE ,  1 , MASTERID ,
	ACCOUNT_NUMBER , CUSTOMER_NAME, PRODUCT_GROUP , ''' || V_TABLE_NAME || ''', ''SEGMENTATION_ID'',
	''MULTIPLE_SEGMENT-'' || SEGMENTATION_ID || ''-' || V_RULE_ID || ''' , ''SP_IFRS_EIL_UPDATE_PORTFOLIO'', SYSDATE , ''HOST'', ''0'' 
	 FROM ' || V_TABLE_NAME || ' ifrs_master_account 
	 WHERE (' || v_CONDITION || ') ' ||
            'AND ifrs_master_account.DOWNLOAD_DATE = DATE ''' ||
            TO_CHAR(v_CURRDATE, 'YYYY-MM-DD') || '''
	AND SEGMENTATION_ID IS NOT NULL ';

 DBMS_OUTPUT.PUT_LINE(SUBSTR(v_STR_SQL, 1, 30000));

        v_STR_SQL :=
            'UPDATE ' || v_TABLE_NAME || ' ifrs_master_account ' ||
            'SET ifrs_master_account.SUB_SEGMENT = ''' || v_SUB_SEGMENT || ''', ' ||
            '    ifrs_master_account.SEGMENT = ''' || v_SEGMENT || ''', ' ||
            '    ifrs_master_account.GROUP_SEGMENT = ''' || v_GROUP_SEGMENT || ''', ' ||
            '    ifrs_master_account.SEGMENTATION_ID = ''' || v_RULE_ID || ''' ' ||
            'WHERE (' || v_CONDITION || ') ' ||
            'AND ifrs_master_account.DOWNLOAD_DATE = DATE ''' ||
            TO_CHAR(v_CURRDATE, 'YYYY-MM-DD') || '''';

        EXECUTE IMMEDIATE v_STR_SQL;
    END LOOP;

    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        -- ROLLBACK TO SAFE STATE AND RE-RAISE
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR IN SP_IFRS_EIL_UPDATE_PORTFOLIO: ' || SQLERRM);
        RAISE;
END