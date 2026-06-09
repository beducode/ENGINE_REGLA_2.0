CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_UPDATE_IMA_RESULT_DEV (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE,
    P_SYSCODE       IN VARCHAR2 DEFAULT '0',
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    V_CURRDATE       DATE;
    V_DATADATE       VARCHAR2(20);
    V_TABLE_NAME     VARCHAR2(50);
    V_COUNT          NUMBER;
	V_MODEL_ID       NUMBER;

    -- DYNAMIC NAMES / QUERY
    V_STR_QUERY      CLOB;

    -- TABLE NAMES
    V_TAB_OWNER     CONSTANT 		VARCHAR2(30) := 'PSAK413';
    V_TABLENAME					VARCHAR2(100);

    -- LOG / RESULT
    V_TABLEDEST     VARCHAR2(100);
    V_COLUMNDEST    VARCHAR2(100);
    V_SP_NAME       VARCHAR2(100) := 'SP_IFRS_UPDATE_IMA_RESULT_DEV';
    V_OPERATION     VARCHAR2(100);
    V_QUERYS        VARCHAR2(8000);
    V_RETURNROWS2   NUMBER;
    
    V_STR_SQL        CLOB;
    V_STR_SQL_RULE   CLOB;
    V_RULE_ID        VARCHAR2(250);
    V_ID             NUMBER := 0;
    V_MAX_ID         NUMBER := 0;
    V_SEQUENCE       VARCHAR2(50);
BEGIN
    ----------------------------------------------------------------
    -- INIT
    ----------------------------------------------------------------
    IF P_DOWNLOAD_DATE IS NULL THEN
        SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;
    ELSE
        V_CURRDATE := P_DOWNLOAD_DATE;
    END IF;

	-- CHOOSE TABLE NAMES BASED ON MODE
    IF P_PRC = 'S' THEN
        V_TABLENAME		:= 'IFRS_MASTER_ACCOUNT_' || P_RUNID;
    ELSE
    	V_TABLENAME   	:= 'IFRS_MASTER_ACCOUNT';
    END IF;

    -------- RECORD RUN_ID --------
    PSAK413.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
	COMMIT;
    -------- RECORD RUN_ID --------

    ----------------------------------------------------------------
    -- RESET SEGMENT DATA (DYNAMIC UPDATE)
    ----------------------------------------------------------------
    V_STR_SQL :=
        'UPDATE ' || V_TABLENAME || ' A                
			 SET A.EIL_AMOUNT = 0
			  ,A.BUCKET_ID = 0                
			  ,A.LIFETIME = 0
			  ,A.LT_RULE_ID = 0               
			  ,A.LGD_RULE_ID = 0          
			  ,A.PD_RULE_ID = 0          
			  ,A.PD_ME_MODEL_ID = 0              
			  ,A.EAD_RULE_ID = 0           
			  ,A.EIL_MODEL_ID = 0
			  ,A.EIL_MARGIN = 0
			  ,A.EIL_POKOK = 0
			  ,A.EAD_AMOUNT = 0
			  ,A.PD_RATE = 0
			  ,A.LGD_RATE = 0               
			 WHERE A.ACCOUNT_STATUS = ''A''                  
			  AND A.IS_IMPAIRED = 1
			  AND A.IMPAIRED_FLAG = ''C''                  
			  AND A.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_currdate,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') '; 
			 
	EXECUTE IMMEDIATE V_STR_SQL;
    COMMIT;

    V_STR_SQL := 'MERGE INTO ' || V_TABLENAME || ' t
				USING (
				  SELECT DOWNLOAD_DATE, MASTERID, EIL_AMOUNT, BUCKET_ID, LT_RULE_ID, LIFETIME,
				         LGD_RULE_ID,PD_ME_MODEL_ID, PD_RULE_ID, EAD_RULE_ID, EIL_MODEL_ID, EIL_MARGIN, EIL_PRINCIPAL,
				         PD, LGD, EAD_AMOUNT
				  FROM IFRS_EIL_RESULT_HEADER s
				  WHERE s.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(V_currdate,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
				) s
				ON (s.MASTERID = t.MASTERID AND s.DOWNLOAD_DATE = t.DOWNLOAD_DATE)
				WHEN MATCHED THEN UPDATE SET
				   t.EIL_AMOUNT     = s.EIL_AMOUNT,
				   t.BUCKET_ID      = s.BUCKET_ID,
				   t.LT_RULE_ID     = s.LT_RULE_ID,
				   t.LIFETIME       = s.LIFETIME,
				   t.LGD_RULE_ID    = s.LGD_RULE_ID,
				   t.PD_RULE_ID     = s.PD_RULE_ID,
				   t.PD_ME_MODEL_ID = s.PD_ME_MODEL_ID,
				   t.EAD_RULE_ID    = s.EAD_RULE_ID,
				   t.EIL_MODEL_ID   = s.EIL_MODEL_ID,
				   t.EIL_MARGIN     = s.EIL_MARGIN,
				   t.EIL_POKOK      = s.EIL_PRINCIPAL,
				   t.PD_RATE        = s.PD,
				   t.LGD_RATE       = s.LGD,
				   t.EAD_AMOUNT     = s.EAD_AMOUNT';
   
	DBMS_OUTPUT.PUT_LINE(V_STR_SQL);
	EXECUTE IMMEDIATE V_STR_SQL;
	COMMIT;


    -------- ====== LOG ======
    V_TABLEDEST := V_TAB_OWNER || '.' || V_TABLENAME;
    V_COLUMNDEST := '-';
    V_OPERATION := 'INSERT';
    
    PSAK413.SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SP_NAME, V_OPERATION, NVL(V_RETURNROWS2,0), P_RUNID);
	COMMIT;
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_STR_QUERY := 'SELECT * FROM ' || V_TAB_OWNER || '.' || V_TABLENAME || ' WHERE DOWNLOAD_DATE = DATE ''' || TO_CHAR(V_CURRDATE, 'YYYY-MM-DD') || '''';
    PSAK413.SP_IFRS_RESULT_PREV(V_CURRDATE, V_STR_QUERY, V_SP_NAME, NVL(V_RETURNROWS2,0), P_RUNID);
	COMMIT;
    -------- ====== RESULT ======

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;

        RAISE_APPLICATION_ERROR(-20001,'ERROR IN ' || V_SP_NAME || ' : ' || SQLERRM);
END;