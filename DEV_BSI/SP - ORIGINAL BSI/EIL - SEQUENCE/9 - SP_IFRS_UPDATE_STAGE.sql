CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_UPDATE_STAGE_DEV (
    P_RUNID		     IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE  IN DATE, 
    P_SYSCODE        IN VARCHAR2 DEFAULT '0', 
    P_PRC			 IN VARCHAR2 DEFAULT 'S'
) 
AUTHID CURRENT_USER
AS
	---- DATE   
    V_CURRDATE DATE; 
	V_EOMDATE DATE ;  

	---- PROCESS
	V_COUNT NUMBER;
	V_RULE_ID VARCHAR2(10);
	V_ID NUMBER DEFAULT 0; 
	V_MAX_ID NUMBER DEFAULT 0; 
	V_DATADATE VARCHAR(20); 
    ---- QUERY  
    V_STR_QUERY CLOB;
	V_STR_SQL CLOB;
    V_SQL_CONDITIONS CLOB;
    V_SCRIPT1 CLOB;

	---- TABLE LIST 
	V_TAB_OWNER CONSTANT VARCHAR2(30) := 'PSAK413';
    V_TABLEINSERT1 VARCHAR2(100);
    V_TABLEINSERT2 VARCHAR2(100);
    V_TABLESELECT1 VARCHAR2(100);
    V_TABLELGDCONFIG VARCHAR2(100);
	V_TABLE_NAME VARCHAR2(100);
    V_STAGE_DETAIL VARCHAR(100);
    V_IS_SICR NUMBER;
    V_CONDITION VARCHAR2(4000);

    ---- CONDITION
    V_RETURNROWS NUMBER;
    V_RETURNROWS2 NUMBER;
    V_TABLEDEST VARCHAR2(100);
    V_COLUMNDEST VARCHAR2(100);
    V_SP_NAME VARCHAR2(100) := 'SP_IFRS_UPDATE_STAGE_DEV';
    V_OPERATION VARCHAR2(100);

    V_SYSCODE VARCHAR(500);
 
BEGIN
    ----------------------------------------------------------------
    -- INIT
    ----------------------------------------------------------------
    IF P_DOWNLOAD_DATE IS NULL THEN
        SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;
    ELSE
        V_CURRDATE := P_DOWNLOAD_DATE;
    END IF;

    ----- TAMBAHAN JIKA P_SYSCODE NYA DI DAPAT NULL
    IF COALESCE(P_SYSCODE, NULL) IS NULL THEN
        V_SYSCODE := '0';
	ELSE
		V_SYSCODE := P_SYSCODE;
    END IF;

    IF P_PRC = 'S' THEN 
        V_TABLE_NAME := 'IFRS_MASTER_ACCOUNT_' || P_RUNID || ''; 
    ELSE 
        V_TABLE_NAME := 'IFRS_MASTER_ACCOUNT'; 
    END IF;

    IF P_PRC = 'S' THEN
        PSAK413.SP_IFRS_CREATE_TABLE_SIMULATE('IFRS_MASTER_ACCOUNT', V_TABLE_NAME); 
    END IF;

    -------- RECORD RUN_ID --------
    PSAK413.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
	COMMIT;
    -------- RECORD RUN_ID --------
  
    ----------------------------------------------------------------
    -- RESET STAGE (DYNAMIC UPDATE)
    ----------------------------------------------------------------
    V_STR_SQL := 'UPDATE ' || V_TABLE_NAME || ' A ' ||
        'SET A.STAGE = NULL ' ||
        'WHERE A.ACCOUNT_STATUS = ''A'' AND A.DOWNLOAD_DATE = DATE ''' || TO_CHAR(V_CURRDATE, 'YYYY-MM-DD') || '''';

    EXECUTE IMMEDIATE V_STR_SQL;
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
        V_RULE_ID        := rec.PKID;
        V_STAGE_DETAIL   := rec.STAGE_DETAIL;
        V_IS_SICR		 := rec.IS_SICR;
        V_CONDITION      := rec.SQL_CONDITIONS;

        V_STR_SQL :=
            'UPDATE ' || V_TABLE_NAME || ' ifrs_master_account ' ||
            ' SET ifrs_master_account.STAGE = CASE WHEN ''' || V_STAGE_DETAIL || ''' = ''Stage 1'' THEN 1 ' ||
            ' WHEN ''' || V_STAGE_DETAIL || ''' = ''Stage 2'' THEN 2 END, ' ||
            ' SICR_FLAG = 0 ' ||
            ' WHERE (' || V_CONDITION || ') ' ||
            ' AND ifrs_master_account.ACCOUNT_STATUS = ''A'' AND ifrs_master_account.DOWNLOAD_DATE = DATE ''' || TO_CHAR(V_CURRDATE, 'YYYY-MM-DD') || '''';

        DBMS_OUTPUT.PUT_LINE(V_STR_SQL);
        EXECUTE IMMEDIATE V_STR_SQL;

    END LOOP;
    COMMIT;
   
   	FOR rec IN (
        SELECT PKID , STAGE_DETAIL ,IS_SICR  , SQL_CONDITIONS 
        FROM IFRS_STAGE_CONFIG
        WHERE IS_DELETE = 0 AND IS_SICR = 1
        AND SQL_CONDITIONS IS NOT NULL
    	)
    LOOP
   		V_RULE_ID        := rec.PKID;
        V_STAGE_DETAIL   := rec.STAGE_DETAIL;
        V_IS_SICR		 := rec.IS_SICR;
        V_CONDITION      := rec.SQL_CONDITIONS;

	   	V_STR_SQL := 'UPDATE ' || V_TABLE_NAME || ' ifrs_master_account ' ||
	            ' SET ifrs_master_account.STAGE = 2 , SICR_FLAG = 1 ' ||
	            ' WHERE (' || V_CONDITION || ') ' ||
	            ' AND ifrs_master_account.ACCOUNT_STATUS = ''A'' AND ifrs_master_account.STAGE = 1 AND ifrs_master_account.DOWNLOAD_DATE = DATE ''' || TO_CHAR(V_CURRDATE, 'YYYY-MM-DD') || '''';
        
        DBMS_OUTPUT.PUT_LINE(V_STR_QUERY);
	    EXECUTE IMMEDIATE V_STR_SQL;

    END LOOP;
    COMMIT;
   
   	PSAK413.C_SP_IFRS_STAGE_BENCANA_DEV(V_CURRDATE);

    -------- ====== LOG ======
    V_TABLEDEST := V_TAB_OWNER || '.' || V_TABLE_NAME;
    V_COLUMNDEST := '-';
    V_OPERATION := 'INSERT';
    
    PSAK413.SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SP_NAME, V_OPERATION, NVL(V_RETURNROWS2,0), P_RUNID);
	COMMIT;
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_STR_QUERY := 'SELECT * FROM ' || V_TAB_OWNER || '.' || V_TABLE_NAME || ' WHERE DOWNLOAD_DATE = DATE ''' || TO_CHAR(V_CURRDATE, 'YYYY-MM-DD') || '''';
    PSAK413.SP_IFRS_RESULT_PREV(V_CURRDATE, V_STR_QUERY, V_SP_NAME, NVL(V_RETURNROWS2,0), P_RUNID);
	COMMIT;
    -------- ====== RESULT ======

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;

        RAISE_APPLICATION_ERROR(-20001,'ERROR IN ' || V_SP_NAME || ' : ' || SQLERRM);
END;