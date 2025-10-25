CREATE OR REPLACE PROCEDURESP_IFRS_ACCT_EIR_ECF_ALIGN4(IN P_RUNID CHARACTER VARYING DEFAULT 'S_00000_0000'::CHARACTER VARYING, IN P_DOWNLOAD_DATE DATE DEFAULT NULL::DATE, IN P_PRC CHARACTER VARYING DEFAULT 'S'::CHARACTER VARYING)
 LANGUAGE PLPGSQL
AS $$
DECLARE
    ---- DATE
    V_PREVDATE DATE;
    V_CURRDATE DATE;

    ---- QUERY   
    V_STR_QUERY TEXT;

    ---- TABLE LIST       
    V_TABLEINSERT1 VARCHAR(100);

    ---- VARIABLE PROCESS
    V_ROUND INT;
    V_FUNCROUND INT;
    
    ---- CONDITION
    V_RETURNROWS INT;
    V_RETURNROWS2 INT;
    V_TABLEDEST VARCHAR(100);
    V_COLUMNDEST VARCHAR(100);
    V_SPNAME VARCHAR(100);
    V_OPERATION VARCHAR(100);

    ---- RESULT
    V_QUERYS TEXT;

    --- VARIABLE
    V_SP_NAME VARCHAR(100);
    STACK TEXT; 
    FCESIG TEXT;
BEGIN 
    -------- ====== VARIABLE ======
	GET DIAGNOSTICS STACK = PG_CONTEXT;
	FCESIG := substring(STACK from 'function (.*?) line');
	V_SP_NAME := UPPER(LEFT(fcesig::regprocedure::text, POSITION('(' in fcesig::regprocedure::text)-1));

    IF COALESCE(P_PRC, NULL) IS NULL THEN
        P_PRC := 'S';
    END IF;

    IF COALESCE(P_RUNID, NULL) IS NULL THEN
        P_RUNID := 'S_00000_0000';
    END IF;

    IF P_PRC = 'S' THEN 
        V_TABLEINSERT1 := 'IFRS_ACCT_EIR_ECF_NOCF_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_ACCT_EIR_ECF_NOCF';
    END IF;
    
    IF P_DOWNLOAD_DATE IS NULL 
    THEN
        SELECT
            CURRDATE, PREVDATE INTO V_CURRDATE, V_PREVDATE
        FROM
            IFRS_PRC_DATE;
    ELSE        
        V_CURRDATE := P_DOWNLOAD_DATE;
        V_PREVDATE := V_CURRDATE - INTERVAL '1 DAY';
    END IF;
    
    SELECT CAST(VALUE1 AS INT), CAST(VALUE2 AS INT) INTO V_ROUND, V_FUNCROUND
    FROM TBLM_COMMONCODEDETAIL
    WHERE COMMONCODE = 'SCM003';
    
    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- RECORD RUN_ID --------
    CALL SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, PG_BACKEND_PID(), CURRENT_DATE);
    -------- RECORD RUN_ID --------

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT1 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM IFRS_ACCT_EIR_ECF_NOCF WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_ACCT_EIR_ECF_ALIGN4', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T8' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T8' || ' 
        (
            MASTERID 
            ,DTMAX 
            ,CURRDATE 
        ) SELECT 
            MASTERID 
            ,MAX(PMT_DATE) AS DTMAX 
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        FROM ' || V_TABLEINSERT1 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        GROUP BY MASTERID ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_ACCT_EIR_ECF_ALIGN4', 'TMP T8 PREPARED');
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET 
            N_UNAMORT_AMT = 0 
            ,N_COST_UNAMORT_AMT = 0 
            ,N_FEE_UNAMORT_AMT = 0 
            ,N_AMORT_AMT = - 1 * N_UNAMORT_AMT_PREV 
            ,N_COST_AMORT_AMT = - 1 * N_COST_UNAMORT_AMT_PREV 
            ,N_FEE_AMORT_AMT = - 1 * N_FEE_UNAMORT_AMT_PREV 
            ,N_FAIRVALUE = 0 
        FROM ' || 'TMP_T8' || ' B 
        WHERE A.MASTERID = B.MASTERID 
        AND B.DTMAX = A.PMT_DATE 
        AND A.DOWNLOAD_DATE = B.CURRDATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET 
            N_DAILY_AMORT_COST = CASE 
                WHEN I_DAYS2 = 0 
                THEN 0 
                ELSE CASE
                    WHEN ' || V_FUNCROUND || ' = 0 
                    THEN ROUND(N_COST_AMORT_AMT / CAST(I_DAYS2 AS NUMERIC(32, 6)), ' || V_ROUND || ') 
                    ELSE TRUNC(N_COST_AMORT_AMT / CAST(I_DAYS2 AS NUMERIC(32, 6)), ' || V_ROUND || ') 
                END 
            END 
            ,N_DAILY_AMORT_FEE = CASE 
                WHEN I_DAYS2 = 0 
                THEN 0 
                ELSE CASE
                    WHEN ' || V_FUNCROUND || ' = 0 
                    THEN ROUND(N_FEE_AMORT_AMT / CAST(I_DAYS2 AS NUMERIC(32, 6)), ' || V_ROUND || ') 
                    ELSE TRUNC(N_FEE_AMORT_AMT / CAST(I_DAYS2 AS NUMERIC(32, 6)), ' || V_ROUND || ') 
                END 
            END 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_ACCT_EIR_ECF_ALIGN4', 'DAILY AMORT UPDATED');
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET ENDAMORTDATE = B.DTMAX 
        FROM ' || 'TMP_T8' || ' B 
        WHERE A.MASTERID = B.MASTERID 
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_ACCT_EIR_ECF_ALIGN4', '');
        
    ---------- ====== BODY ======

END;

$$
