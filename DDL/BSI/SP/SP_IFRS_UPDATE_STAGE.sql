CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_UPDATE_STAGE (
	p_DOWNLOAD_DATE IN DATE
)
AS
    v_RULE_ID        VARCHAR2(250);
    v_STAGE_DETAIL     VARCHAR2(100);
    v_STR_SQL        VARCHAR2(4000);
    v_IS_SICR  		 NUMBER;
    v_CONDITION      VARCHAR2(4000);
    v_CURRDATE       DATE;
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

    v_TABLENAME := 'IFRS_MASTER_ACCOUNT_MONTHLY';
  
    ----------------------------------------------------------------
    -- RESET STAGE (DYNAMIC UPDATE)
    ----------------------------------------------------------------
    v_STR_SQL :=
        'UPDATE ' || v_TABLENAME || ' A ' ||
        'SET A.STAGE = NULL ' ||
        'WHERE A.ACCOUNT_STATUS = ''A'' AND A.DOWNLOAD_DATE = DATE ''' || TO_CHAR(v_CURRDATE, 'YYYY-MM-DD') || '''';

    EXECUTE IMMEDIATE v_STR_SQL;
    COMMIT;
    ----------------------------------------------------------------
    -- CURSOR LOOP
    ----------------------------------------------------------------
    FOR rec IN (
        SELECT PKID , STAGE_DETAIL ,IS_SICR  , SQL_CONDITIONS 
        FROM IFRS_STAGE_CONFIG
        WHERE IS_DELETE = 0 AND IS_SICR = 0
    )
    LOOP
        v_RULE_ID        := rec.PKID;
        v_STAGE_DETAIL   := rec.STAGE_DETAIL;
        v_IS_SICR		 := rec.IS_SICR;
        v_CONDITION      := rec.SQL_CONDITIONS;

        v_STR_SQL :=
            'UPDATE ' || v_TABLENAME || ' ifrs_master_account ' ||
            ' SET ifrs_master_account.STAGE = CASE WHEN ''' || v_STAGE_DETAIL || ''' = ''Stage 1'' THEN 1 ' ||
            ' WHEN ''' || v_STAGE_DETAIL || ''' = ''Stage 2'' THEN 2 END, ' ||
            ' SICR_FLAG = 0 ' ||
            ' WHERE (' || v_CONDITION || ') ' ||
            ' AND ifrs_master_account.ACCOUNT_STATUS = ''A'' AND ifrs_master_account.DOWNLOAD_DATE = DATE ''' || TO_CHAR(v_CURRDATE, 'YYYY-MM-DD') || '''';

        EXECUTE IMMEDIATE v_STR_SQL;
    END LOOP;

    COMMIT;
   
   --------------------------------------------------------------------------------------
   	FOR rec IN (
        SELECT PKID , STAGE_DETAIL ,IS_SICR  , SQL_CONDITIONS 
        FROM IFRS_STAGE_CONFIG
        WHERE IS_DELETE = 0 AND IS_SICR = 1
    	)
    LOOP
   		v_RULE_ID        := rec.PKID;
        v_STAGE_DETAIL   := rec.STAGE_DETAIL;
        v_IS_SICR		 := rec.IS_SICR;
        v_CONDITION      := rec.SQL_CONDITIONS;

	   	v_STR_SQL :=
	            'UPDATE ' || v_TABLENAME || ' ifrs_master_account ' ||
	            ' SET ifrs_master_account.STAGE = 2 , SICR_FLAG = 1 ' ||
	            ' WHERE (' || v_CONDITION || ') ' ||
	            ' AND ifrs_master_account.ACCOUNT_STATUS = ''A'' AND ifrs_master_account.STAGE = 1 AND ifrs_master_account.DOWNLOAD_DATE = DATE ''' || TO_CHAR(v_CURRDATE, 'YYYY-MM-DD') || '''';
	
	    EXECUTE IMMEDIATE v_STR_SQL;
    END LOOP;
   
    COMMIT;
   
   	C_SP_IFRS_STAGE_BENCANA(v_CURRDATE);
   
EXCEPTION
    WHEN OTHERS THEN
        -- ROLLBACK TO SAFE STATE AND RE-RAISE
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR IN SP_IFRS_UPDATE_STAGE: ' || SQLERRM);
        RAISE;
END;
/
