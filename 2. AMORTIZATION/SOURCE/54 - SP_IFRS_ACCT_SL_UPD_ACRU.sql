---- DROP PROCEDURE SP_IFRS_ACCT_SL_UPD_ACRU;

CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_SL_UPD_ACRU(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000',
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S')
LANGUAGE PLPGSQL AS $$
DECLARE
    ---- DATE
    V_PREVDATE DATE;
    V_CURRDATE DATE;

    ---- QUERY   
    V_STR_QUERY TEXT;

    ---- TABLE LIST       
    V_TABLEINSERT1 VARCHAR(100);
    V_TABLEINSERT2 VARCHAR(100);
    V_TABLEINSERT3 VARCHAR(100);
    V_TABLEINSERT4 VARCHAR(100);
    V_TABLEINSERT5 VARCHAR(100);
    V_TABLEINSERT6 VARCHAR(100);

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
        V_TABLEINSERT1 := 'IFRS_ACCT_SL_ACCRU_PREV_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_ACCT_SL_ACF_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_ACCT_SL_ACCRU_PREV';
        V_TABLEINSERT2 := 'IFRS_ACCT_SL_ACF';
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

    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_ACCT_SL_UPD_ACRU', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_AP' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_AP' || ' 
        (
            MASTERID 
            ,FLAG_CF 
            ,AMOUNT 
        ) SELECT 
            MASTERID 
            ,FLAG_CF 
            ,SUM(CASE 
                WHEN FLAG_REVERSE = ''Y'' 
                THEN -1 * AMOUNT 
                ELSE AMOUNT 
            END) AS AMOUNT 
        FROM ' || V_TABLEINSERT1 || ' 
        WHERE STATUS = ''ACT'' 
        GROUP BY MASTERID, FLAG_CF';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' A 
        SET N_ACCRU_PREV_COST = B.AMOUNT
        FROM (
            SELECT 
                X.*
                ,Y.* 
            FROM ' || 'TMP_AP' || ' X 
            CROSS JOIN 
            IFRS_PRC_DATE_AMORT Y
        ) B
        WHERE B.MASTERID = A.MASTERID
            AND A.DOWNLOAD_DATE = B.CURRDATE
            AND B.FLAG_CF=''C'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' A 
        SET N_ACCRU_PREV_FEE=B.AMOUNT
        FROM (
            SELECT 
                X.*
                ,Y.* 
            FROM ' || 'TMP_AP' || ' X 
            CROSS JOIN IFRS_PRC_DATE_AMORT Y
        ) B 
        WHERE B.MASTERID = A.MASTERID
            AND A.DOWNLOAD_DATE = B.CURRDATE
            AND B.FLAG_CF=''F'' ';
    EXECUTE (V_STR_QUERY);

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_ACCT_SL_UPD_ACRU', '');

    ---------- ====== BODY ======

END;

$$;