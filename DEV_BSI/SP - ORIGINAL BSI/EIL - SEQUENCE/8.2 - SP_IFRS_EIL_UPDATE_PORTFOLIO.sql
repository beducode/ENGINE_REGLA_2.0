CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_EIL_UPDATE_PORTFOLIO_DEV (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
	P_DOWNLOAD_DATE IN DATE,
    P_PRC           IN CHAR DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    V_RULE_ID        VARCHAR2(250);
    V_TABLE_NAME     VARCHAR2(100);
    V_STR_SQL        CLOB;
    V_GROUP_SEGMENT  VARCHAR2(250);
    V_SEGMENT        VARCHAR2(250);
    V_SUB_SEGMENT    VARCHAR2(250);
    V_CONDITION      VARCHAR2(4000);
    V_CURRDATE       DATE;
    V_EXCEPT_ID      VARCHAR2(50);
    V_ERROR_FLAG     NUMBER(1) := 0;
    V_TABLENAME      VARCHAR2(100);
    V_SP_NAME       VARCHAR2(100) := 'SP_IFRS_EIL_UPDATE_PORTFOLIO_DEV';
BEGIN
    --------------------------------------------------------------
    ------ INIT
    --------------------------------------------------------------
    IF P_DOWNLOAD_DATE IS NULL THEN
        SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;
    ELSE
        V_CURRDATE := P_DOWNLOAD_DATE;
    END IF;

    IF P_PRC = 'S' THEN
        V_TABLENAME  := 'IFRS_MASTER_ACCOUNT_' || P_RUNID;
    ELSE
        V_TABLENAME  := 'IFRS_MASTER_ACCOUNT';
    END IF;

    --------------------------------------------------------------
    ---- RESET SEGMENT DATA (DYNAMIC UPDATE)
    ------------------------------------------------------------
    V_STR_SQL :=
        'UPDATE ' || V_TABLENAME || ' A ' ||
        'SET A.SUB_SEGMENT = NULL, ' ||
        '    A.SEGMENT = NULL, ' ||
        '    A.GROUP_SEGMENT = NULL, ' ||
        '    A.SEGMENTATION_ID = NULL ' ||
        'WHERE A.DOWNLOAD_DATE = DATE ''' ||
        TO_CHAR(V_CURRDATE, 'YYYY-MM-DD') || '''';

    DBMS_OUTPUT.PUT_LINE(SUBSTR(V_STR_SQL, 1, 30000));
    EXECUTE IMMEDIATE V_STR_SQL;
   
    V_STR_SQL := 'DELETE FROM PSAK413.IFRS_EXCEPTION_ACCOUNT WHERE EXCEPTION_ID = 1 AND DOWNLOAD_DATE = DATE ''' || TO_CHAR(V_CURRDATE, 'YYYY-MM-DD') || ''' ';   
    EXECUTE IMMEDIATE V_STR_SQL;

    ------------------------------------------------------------
    ---- CURSOR LOOP
    ------------------------------------------------------------
    FOR rec IN (
        SELECT PKID AS RULE_ID,
               V_TABLENAME AS TABLE_NAME,
               SEGMENT_NAME_LV1 AS GROUP_SEGMENT,
               SEGMENT_NAME_LV2 AS SEGMENT,
               SEGMENT_NAME_LV3 AS SUB_SEGMENT,
               MERGE_SQL_CONDITIONS AS CONDITION
        FROM IFRS_SEGMENTATION_MAPPING
        WHERE SEGMENT_TYPE = 'PORTFOLIO'
    )
    LOOP
        V_RULE_ID       := rec.RULE_ID;
        V_TABLE_NAME    := rec.TABLE_NAME;
        V_GROUP_SEGMENT := rec.GROUP_SEGMENT;
        V_SEGMENT       := rec.SEGMENT;
        V_SUB_SEGMENT   := rec.SUB_SEGMENT;
        V_CONDITION     := rec.CONDITION;
      
        V_STR_SQL :=' INSERT INTO PSAK413.IFRS_EXCEPTION_ACCOUNT 
        (DOWNLOAD_DATE, DATA_SOURCE, EXCEPTION_ID, MASTERID, ACCOUNT_NUMBER, CUSTOMER_NAME,
        PRODUCT_GROUP, TABLE_NAME, FIELD_NAME, VALUE, CREATEDBY, CREATEDDATE, CREATEDHOST,
        FLAG)
        SELECT DOWNLOAD_DATE , DATA_SOURCE ,  1 , MASTERID ,
        ACCOUNT_NUMBER , CUSTOMER_NAME, PRODUCT_GROUP , ''' || V_TABLE_NAME || ''', ''SEGMENTATION_ID'',
        ''MULTIPLE_SEGMENT-'' || SEGMENTATION_ID || ''-' || V_RULE_ID || ''' , ''SP_IFRS_EIL_UPDATE_PORTFOLIO'', SYSDATE , ''HOST'', ''0'' 
        FROM ' || V_TABLE_NAME || ' ifrs_master_account 
        WHERE (' || V_CONDITION || ') ' ||
            'AND ifrs_master_account.DOWNLOAD_DATE = DATE ''' ||
            TO_CHAR(V_CURRDATE, 'YYYY-MM-DD') || '''
        AND SEGMENTATION_ID IS NOT NULL ';

        DBMS_OUTPUT.PUT_LINE(SUBSTR(V_STR_SQL, 1, 30000));
        EXECUTE IMMEDIATE V_STR_SQL;

        V_STR_SQL :=
            'UPDATE ' || V_TABLE_NAME || ' ifrs_master_account ' ||
            'SET ifrs_master_account.SUB_SEGMENT = ''' || V_SUB_SEGMENT || ''', ' ||
            '    ifrs_master_account.SEGMENT = ''' || V_SEGMENT || ''', ' ||
            '    ifrs_master_account.GROUP_SEGMENT = ''' || V_GROUP_SEGMENT || ''', ' ||
            '    ifrs_master_account.SEGMENTATION_ID = ''' || V_RULE_ID || ''' ' ||
            'WHERE (' || V_CONDITION || ') ' ||
            'AND ifrs_master_account.DOWNLOAD_DATE = DATE ''' ||
            TO_CHAR(V_CURRDATE, 'YYYY-MM-DD') || '''';

        EXECUTE IMMEDIATE V_STR_SQL;
    END LOOP;

    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        -- ROLLBACK TO SAFE STATE AND RE-RAISE
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR IN ' || V_SP_NAME || ': ' || SQLERRM);
        RAISE;
END;