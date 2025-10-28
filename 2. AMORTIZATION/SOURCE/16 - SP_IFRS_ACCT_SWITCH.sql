CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_SWITCH(IN P_RUNID CHARACTER VARYING DEFAULT 'S_00000_0000'::CHARACTER VARYING, IN P_DOWNLOAD_DATE DATE DEFAULT NULL::DATE, IN P_PRC CHARACTER VARYING DEFAULT 'S'::CHARACTER VARYING)
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
    V_TABLEINSERT2 VARCHAR(100);
    V_TABLEINSERT3 VARCHAR(100);
    V_TABLEINSERT4 VARCHAR(100);
    V_TABLEINSERT5 VARCHAR(100);
    V_TABLEINSERT6 VARCHAR(100);
    V_TABLEINSERT7 VARCHAR(100);
    V_TABLEINSERT8 VARCHAR(100);
    V_TABLEINSERT9 VARCHAR(100);
    V_TABLEINSERT10 VARCHAR(100);
    
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
        V_TABLEINSERT1 := 'IFRS_ACCT_SWITCH_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_LBM_ACCT_SWITCH_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_ACCT_SL_ECF_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_ACCT_EIR_ECF_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_ACCT_COST_FEE_SUMM_' || P_RUNID || '';
        V_TABLEINSERT6 := 'IFRS_IMA_AMORT_CURR_' || P_RUNID || '';
        V_TABLEINSERT7 := 'IFRS_IMA_AMORT_PREV_' || P_RUNID || '';
        V_TABLEINSERT8 := 'IFRS_PRODUCT_PARAM_' || P_RUNID || '';
        V_TABLEINSERT9 := 'IFRS_ACCT_CLOSED_' || P_RUNID || '';
        V_TABLEINSERT10 := 'IFRS_LBM_ACCT_EIR_ECF_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_ACCT_SWITCH';
        V_TABLEINSERT2 := 'IFRS_LBM_ACCT_SWITCH';
        V_TABLEINSERT3 := 'IFRS_ACCT_SL_ECF';
        V_TABLEINSERT4 := 'IFRS_ACCT_EIR_ECF';
        V_TABLEINSERT5 := 'IFRS_ACCT_COST_FEE_SUMM';
        V_TABLEINSERT6 := 'IFRS_IMA_AMORT_CURR';
        V_TABLEINSERT7 := 'IFRS_IMA_AMORT_PREV';
        V_TABLEINSERT8 := 'IFRS_PRODUCT_PARAM';
        V_TABLEINSERT9 := 'IFRS_ACCT_CLOSED';
        V_TABLEINSERT10 := 'IFRS_LBM_ACCT_EIR_ECF';
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
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM IFRS_ACCT_SWITCH WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT2 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT2 || ' AS SELECT * FROM IFRS_LBM_ACCT_SWITCH WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT10 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT10 || ' AS SELECT * FROM IFRS_LBM_ACCT_EIR_ECF WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_ACCT_SWITCH', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
        WHERE DOWNLOAD_DATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        ( 
            DOWNLOAD_DATE 
            ,DATASOURCE 
            ,MASTERID 
            ,FACNO 
            ,CIFNO 
            ,ACCTNO 
            ,PREV_ACCTNO 
            ,PREV_MASTERID 
            ,CREATEDBY 
            ,CREATEDDATE 
            ,PREV_SL_ECF 
            ,PREV_EIR_ECF 
            ,PRDCODE 
            ,BRCODE 
            ,PRDTYPE 
            ,PREV_DATASOURCE 
            ,PREV_FACNO 
            ,PREV_CIFNO 
            ,PREV_BRCODE 
            ,PREV_PRDTYPE 
            ,PREV_PRDCODE 
            ,CCY 
            ,LOAN_AMT 
            ,PLAFOND 
            ,REMARKS 
        ) SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE 
            ,A.DATA_SOURCE 
            ,A.MASTERID 
            ,A.FACILITY_NUMBER 
            ,A.CUSTOMER_NUMBER 
            ,A.ACCOUNT_NUMBER 
            ,B.ACCOUNT_NUMBER 
            ,B.MASTERID 
            ,''SP_IFRS_ACCT_SWITCH'' 
            ,CURRENT_TIMESTAMP 
            ,''N'' 
            ,''N'' 
            ,A.PRODUCT_CODE 
            ,A.BRANCH_CODE 
            ,A.PRODUCT_TYPE 
            ,B.DATA_SOURCE 
            ,B.FACILITY_NUMBER 
            ,B.CUSTOMER_NUMBER 
            ,B.BRANCH_CODE 
            ,B.PRODUCT_TYPE 
            ,B.PRODUCT_CODE 
            ,A.CURRENCY 
            ,A.LOAN_AMT 
            ,A.PLAFOND 
            ,''CHANGE_BRANCH'' AS REMARKS 
        FROM ' || V_TABLEINSERT6 || ' A 
        JOIN ' || V_TABLEINSERT7 || ' B 
        ON A.MASTERID = B.MASTERID 
        AND A.CURRENCY = B.CURRENCY 
        WHERE A.BRANCH_CODE <> B.BRANCH_CODE 
        AND B.MASTERID NOT IN ( 
            SELECT DISTINCT MASTERID 
            FROM ' || V_TABLEINSERT1 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        ) AND A.MASTERID NOT IN (
            SELECT MASTERID 
            FROM ' || V_TABLEINSERT9 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        ) ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET METHOD = B.AMORT_TYPE 
        FROM ( 
            SELECT X.*, Y.* 
            FROM ' || V_TABLEINSERT8 || ' X 
            CROSS JOIN IFRS_PRC_DATE_AMORT Y 
        ) B 
        WHERE B.PRD_CODE = A.PRDCODE 
        AND B.DATA_SOURCE = A.DATASOURCE 
        AND B.PRD_TYPE = A.PRDTYPE 
        AND (A.CCY = B.CCY OR B.CCY = ''ALL'') 
        AND A.DOWNLOAD_DATE = B.CURRDATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT2 || ' 
        WHERE DOWNLOAD_DATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
        ( 
            ID 
            ,DOWNLOAD_DATE 
            ,FACNO 
            ,CIFNO 
            ,ACCTNO 
            ,BRCODE 
            ,PRDTYPE 
            ,PRDCODE 
            ,DATASOURCE 
            ,MASTERID 
            ,PREV_ACCTNO 
            ,PREV_FACNO 
            ,PREV_CIFNO 
            ,PREV_BRCODE 
            ,PREV_PRDTYPE 
            ,PREV_PRDCODE 
            ,PREV_DATASOURCE 
            ,PREV_MASTERID 
            ,PREV_SL_ECF 
            ,PREV_EIR_ECF 
            ,METHOD 
            ,CCY 
            ,LOAN_AMT 
            ,PLAFOND 
            ,SW_ADJ_FEE 
            ,SW_ADJ_COST 
            ,CREATEDDATE 
            ,CREATEDBY 
        ) SELECT 
            ID 
            ,DOWNLOAD_DATE 
            ,FACNO 
            ,CIFNO 
            ,ACCTNO 
            ,BRCODE 
            ,PRDTYPE 
            ,PRDCODE 
            ,DATASOURCE 
            ,MASTERID 
            ,PREV_ACCTNO 
            ,PREV_FACNO 
            ,PREV_CIFNO 
            ,PREV_BRCODE 
            ,PREV_PRDTYPE 
            ,PREV_PRDCODE 
            ,PREV_DATASOURCE 
            ,PREV_MASTERID 
            ,PREV_SL_ECF 
            ,PREV_EIR_ECF 
            ,METHOD 
            ,CCY 
            ,LOAN_AMT 
            ,PLAFOND 
            ,SW_ADJ_FEE 
            ,SW_ADJ_COST 
            ,CREATEDDATE 
            ,CREATEDBY 
        FROM ' || V_TABLEINSERT1 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET PREV_SL_ECF = ''Y'' 
        FROM ( 
            SELECT B.MASTERID, C.CURRDATE 
            FROM ' || V_TABLEINSERT3 || ' B 
            CROSS JOIN IFRS_PRC_DATE_AMORT C 
            WHERE COALESCE(B.AMORTSTOPDATE::VARCHAR(10), '''') = '''' 
            AND B.PREVDATE = B.PMTDATE 
        ) B 
        WHERE B.MASTERID = A.MASTERID 
        AND A.DOWNLOAD_DATE = B.CURRDATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET PREV_EIR_ECF = ''Y'' 
        FROM ( 
            SELECT B.MASTERID, C.CURRDATE 
            FROM ' || V_TABLEINSERT4 || ' B 
            CROSS JOIN IFRS_PRC_DATE_AMORT C 
            WHERE COALESCE(B.AMORTSTOPDATE::VARCHAR(10), '''') = '''' 
            AND B.PREV_PMT_DATE = B.PMT_DATE 
        ) B 
        WHERE B.MASTERID = A.MASTERID 
        AND A.DOWNLOAD_DATE = B.CURRDATE 
        AND REMARKS = ''CHANGE_BRANCH'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' A 
        SET PREV_EIR_ECF = ''Y'' 
        FROM ( 
            SELECT B.MASTERID, C.CURRDATE 
            FROM ' || V_TABLEINSERT10 || ' B 
            CROSS JOIN IFRS_PRC_DATE_AMORT C 
            WHERE COALESCE(B.AMORTSTOPDATE::VARCHAR(10), '''') = '''' 
            AND B.PREV_PMT_DATE = B.PMT_DATE 
        ) B 
        WHERE B.MASTERID = A.PREV_MASTERID 
        AND A.DOWNLOAD_DATE = B.CURRDATE ';
    EXECUTE (V_STR_QUERY);
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET 
            PREV_SL_ECF = ''X'' 
            ,PREV_EIR_ECF = ''X'' 
            ,CREATEDBY = ''DIFF_METHOD'' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND ( 
            (
                PREV_SL_ECF = ''Y'' 
                AND METHOD = ''EIR''
            ) OR (
                PREV_EIR_ECF = ''Y'' 
                AND METHOD = ''SL''
            )
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' A 
        SET 
            PREV_SL_ECF = ''X'' 
            ,PREV_EIR_ECF = ''X'' 
            ,CREATEDBY = ''DIFF_METHOD'' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND ( 
            (
                PREV_SL_ECF = ''Y'' 
                AND METHOD = ''EIR''
            ) OR (
                PREV_EIR_ECF = ''Y'' 
                AND METHOD = ''SL''
            )
        ) ';
    EXECUTE (V_STR_QUERY);
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET 
            PREV_SL_ECF = ''X'' 
            ,PREV_EIR_ECF = ''X'' 
            ,CREATEDBY = ''SL_ECF_EXIST'' 
        FROM ( 
            SELECT B.MASTERID, C.CURRDATE 
            FROM ' || V_TABLEINSERT3 || ' B 
            CROSS JOIN IFRS_PRC_DATE_AMORT C 
            WHERE COALESCE(B.AMORTSTOPDATE::VARCHAR(10), '''') = '''' 
            AND B.PREVDATE = B.PMTDATE 
        ) B 
        WHERE B.MASTERID = A.MASTERID 
        AND A.DOWNLOAD_DATE = B.CURRDATE 
        AND A.MASTERID <> A.PREV_MASTERID ';
    EXECUTE (V_STR_QUERY);
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET 
            PREV_SL_ECF = ''X'' 
            ,PREV_EIR_ECF = ''X'' 
            ,CREATEDBY = ''EIR_ECF_EXIST'' 
        FROM ( 
            SELECT B.MASTERID, C.CURRDATE 
            FROM ' || V_TABLEINSERT3 || ' B 
            CROSS JOIN IFRS_PRC_DATE_AMORT C 
            WHERE COALESCE(B.AMORTSTOPDATE::VARCHAR(10), '''') = '''' 
            AND B.PREVDATE = B.PMTDATE 
        ) B 
        WHERE B.MASTERID = A.MASTERID 
        AND A.DOWNLOAD_DATE = B.CURRDATE 
        AND A.MASTERID <> A.PREV_MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT5 || ' A 
        SET 
            SW_MASTERID = B.MASTERID         
            ,BRCODE = B.BRCODE 
            ,CIFNO = B.CIFNO 
            ,FACNO = B.FACNO 
            ,ACCTNO = B.ACCTNO 
            ,DATASOURCE = B.DATASOURCE 
        FROM ( 
            SELECT B.* 
            FROM ' || V_TABLEINSERT1 || ' B 
            JOIN IFRS_PRC_DATE_AMORT C 
            ON C.CURRDATE = B.DOWNLOAD_DATE 
            WHERE B.PREV_SL_ECF = ''Y'' 
            OR B.PREV_EIR_ECF = ''Y'' 
        ) B 
        WHERE A.MASTERID = B.MASTERID 
        AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT5 || ' A 
        SET MASTERID = SW_MASTERID 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND SW_MASTERID IN ( 
            SELECT MASTERID 
            FROM ' || V_TABLEINSERT1 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        ) ';
    EXECUTE (V_STR_QUERY);

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_ACCT_SWITCH', '');

    RAISE NOTICE 'SP_IFRS_ACCT_SWITCH | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT1;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_ACCT_SWITCH';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT1 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$
