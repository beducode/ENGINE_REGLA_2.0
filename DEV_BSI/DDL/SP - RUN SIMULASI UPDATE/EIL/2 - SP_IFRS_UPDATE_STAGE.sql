CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_UPDATE_STAGE (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE,
    P_SYSCODE       IN VARCHAR2 DEFAULT '0',
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    V_RULE_ID        VARCHAR2(250);
    V_STAGE_DETAIL   VARCHAR2(100);
    
    V_STR_QUERY     CLOB;
    V_QUERYS        CLOB;
    V_TAB_OWNER     CONSTANT VARCHAR2(30) := 'PSAK413';


    V_TABLEDEST     VARCHAR2(100);
    V_COLUMNDEST    VARCHAR2(100);
    V_SP_NAME       VARCHAR2(100) := 'SP_IFRS_UPDATE_STAGE';
    V_OPERATION     VARCHAR2(100);
    V_RETURNROWS2   NUMBER;

    V_IS_SICR  		 NUMBER;
    V_CONDITION      VARCHAR2(4000);
    V_CURRDATE       DATE;
    V_IFRS_MASTER_ACCOUNT_MONTHLY VARCHAR2(100);
BEGIN
    ----------------------------------------------------------------
    -- INIT
    ----------------------------------------------------------------
    IF P_DOWNLOAD_DATE IS NULL THEN
        SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;
    ELSE
        V_CURRDATE := P_DOWNLOAD_DATE;
    END IF;

    IF P_PRC = 'S' THEN 
        V_IFRS_MASTER_ACCOUNT_MONTHLY   := 'IFRS_MASTER_ACCOUNT_MONTHLY_' || P_RUNID;
    ELSE
        V_IFRS_MASTER_ACCOUNT_MONTHLY   := 'IFRS_MASTER_ACCOUNT_MONTHLY';
    END IF;

    IF P_PRC = 'S' THEN
        PSAK413.SP_IFRS_CREATE_TABLE_SIMULATE('IFRS_MASTER_ACCOUNT_MONTHLY', V_IFRS_MASTER_ACCOUNT_MONTHLY, V_CURRDATE);
    END IF;

    -------- RECORD RUN_ID --------
    PSAK413.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, 0, CURRENT_DATE); 
	COMMIT;
  
    ----------------------------------------------------------------
    -- RESET STAGE (DYNAMIC UPDATE)
    ----------------------------------------------------------------
    V_STR_QUERY := 'UPDATE ' || V_TAB_OWNER || '.' || V_IFRS_MASTER_ACCOUNT_MONTHLY || ' A ' ||
                   'SET A.STAGE = NULL ' ||
                   'WHERE A.ACCOUNT_STATUS = ''A'' AND A.DOWNLOAD_DATE = DATE ''' || TO_CHAR(V_CURRDATE, 'YYYY-MM-DD') || '''';

    EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;
    ----------------------------------------------------------------
    ---- UPDATE STAGE NON-SICR
    ----------------------------------------------------------------
    FOR REC IN (
        SELECT PKID , STAGE_DETAIL ,IS_SICR  , SQL_CONDITIONS 
        FROM IFRS_STAGE_CONFIG
        WHERE IS_DELETE = 0 AND IS_SICR = 0
    )
    LOOP
        V_RULE_ID        := REC.PKID;
        V_STAGE_DETAIL   := REC.STAGE_DETAIL;
        V_IS_SICR		 := REC.IS_SICR;
        V_CONDITION      := PSAK413.FN_NORMALIZE_CONDITION(REC.SQL_CONDITIONS);

        V_STR_QUERY :=
            'UPDATE ' || V_TAB_OWNER || '.' || V_IFRS_MASTER_ACCOUNT_MONTHLY || ' A ' ||
            ' SET A.STAGE = CASE WHEN ''' || V_STAGE_DETAIL || ''' = ''Stage 1'' THEN 1 ' ||
            ' WHEN ''' || V_STAGE_DETAIL || ''' = ''Stage 2'' THEN 2 END, ' ||
            ' A.SICR_FLAG = 0 ' ||
            ' WHERE (' || V_CONDITION || ') ' ||
            ' AND A.ACCOUNT_STATUS = ''A'' AND A.DOWNLOAD_DATE = DATE ''' || TO_CHAR(V_CURRDATE, 'YYYY-MM-DD') || '''';

        DBMS_OUTPUT.PUT_LINE('EXECUTING NON-SICR STAGE UPDATE: ' || V_STR_QUERY);   
        EXECUTE IMMEDIATE V_STR_QUERY;
    END LOOP;

    COMMIT;
   
    ----------------------------------------------------------------
    ---- UPDATE STAGE SICR
    ----------------------------------------------------------------
   	FOR REC IN (
        SELECT PKID , STAGE_DETAIL ,IS_SICR  , SQL_CONDITIONS 
        FROM IFRS_STAGE_CONFIG
        WHERE IS_DELETE = 0 AND IS_SICR = 1
    	)
    LOOP
   		V_RULE_ID        := REC.PKID;
        V_STAGE_DETAIL   := REC.STAGE_DETAIL;
        V_IS_SICR		 := REC.IS_SICR;
        V_CONDITION      := PSAK413.FN_NORMALIZE_CONDITION(REC.SQL_CONDITIONS);

	   	V_STR_QUERY :=
	            'UPDATE ' || V_TAB_OWNER || '.' || V_IFRS_MASTER_ACCOUNT_MONTHLY || ' A ' ||
	            ' SET A.STAGE = 2 , A.SICR_FLAG = 1 ' ||
	            ' WHERE (' || V_CONDITION || ') ' ||
	            ' AND A.ACCOUNT_STATUS = ''A'' AND A.STAGE = 1 AND A.DOWNLOAD_DATE = DATE ''' || TO_CHAR(V_CURRDATE, 'YYYY-MM-DD') || '''';

        DBMS_OUTPUT.PUT_LINE('EXECUTING SICR STAGE UPDATE: ' || V_STR_QUERY);
	    EXECUTE IMMEDIATE V_STR_QUERY;
    END LOOP;
   
    COMMIT;
   
   	PSAK413.C_SP_IFRS_STAGE_BENCANA(P_RUNID, V_CURRDATE, P_PRC);
   
    -----------------------------
    -- LOG & INSERT FINAL DATA
    -----------------------------
    V_TABLEDEST := V_TAB_OWNER || '.' || V_IFRS_MASTER_ACCOUNT_MONTHLY;
    V_COLUMNDEST := '-';
    V_OPERATION := 'INSERT';
 
    PSAK413.SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SP_NAME, V_OPERATION, NVL(V_RETURNROWS2,0), P_RUNID);
    COMMIT;  
   
	V_QUERYS := 'SELECT * FROM ' || V_TAB_OWNER || '.' || V_IFRS_MASTER_ACCOUNT_MONTHLY;

    PSAK413.SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SP_NAME, NVL(V_RETURNROWS2,0), P_RUNID);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;

        RAISE_APPLICATION_ERROR(-20001,'ERROR IN ' || V_SP_NAME || ' : ' || SQLERRM);
END;