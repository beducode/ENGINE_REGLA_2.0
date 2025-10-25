CREATE OR REPLACE PROCEDURESP_IFRS_ACCT_SL_SWITCH(IN P_RUNID CHARACTER VARYING DEFAULT 'S_00000_0000'::CHARACTER VARYING, IN P_DOWNLOAD_DATE DATE DEFAULT NULL::DATE, IN P_PRC CHARACTER VARYING DEFAULT 'S'::CHARACTER VARYING)
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

    ---- VARIABLE PROCESS
    V_NUM INT;
    
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
        V_TABLEINSERT1 := 'IFRS_ACCT_SL_COST_FEE_PREV_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_ACCT_SL_ECF_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_ACCT_SWITCH_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_ACCT_SL_COST_FEE_ECF_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_ACCT_SL_ACF_' || P_RUNID || '';
        V_TABLEINSERT6 := 'IFRS_ACCT_SL_ACCRU_PREV_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_ACCT_SL_COST_FEE_PREV';
        V_TABLEINSERT2 := 'IFRS_ACCT_SL_ECF';
        V_TABLEINSERT3 := 'IFRS_ACCT_SWITCH';
        V_TABLEINSERT4 := 'IFRS_ACCT_SL_COST_FEE_ECF';
        V_TABLEINSERT5 := 'IFRS_ACCT_SL_ACF';
        V_TABLEINSERT6 := 'IFRS_ACCT_SL_ACCRU_PREV';
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
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE IF NOT EXISTS VW_LAST_SL_CF_PREV (
            MASTERID VARCHAR(20) NOT NULL
            ,DOWNLOAD_DATE DATE NOT NULL
            ,SEQ VARCHAR(10) NOT NULL
        )';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_ACCT_SL_SWITCH', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET STATUS = ''ACT'' 
        WHERE STATUS = ''REV'' 
        AND CREATEDBY = ''SL_SWITCH''
        AND DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET STATUS = ''ACT'' 
        WHERE STATUS = ''REV2'' 
        AND CREATEDBY = ''SL_SWITCH''
        AND DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT COUNT(*) FROM ' || V_TABLEINSERT3 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND PREV_SL_ECF = ''Y'' ';
    EXECUTE (V_STR_QUERY) INTO V_NUM;

    --? COMMENT FOR TESTING
    IF V_NUM <= 0 THEN 
        CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_ACCT_SL_SWITCH', '');
        RETURN;
    END IF;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
        ( 
            DOWNLOAD_DATE 
		    ,MASTERID 
		    ,FACNO 
		    ,CIFNO 
		    ,ACCTNO 
		    ,DATASOURCE 
		    ,PREVDATE 
		    ,PMTDATE 
		    ,I_DAYSCNT 
		    ,N_UNAMORT_COST 
		    ,N_UNAMORT_FEE 
		    ,N_AMORT_COST 
		    ,N_AMORT_FEE 
		    ,CREATEDDATE 
		    ,CREATEDBY 
		    ,STATUS 
		    ,AMORTSTOPDATE 
		    ,AMORTSTOPREASON 
		    ,N_DAILY_AMORT_COST 
		    ,N_DAILY_AMORT_FEE 
		    ,AMORTENDDATE 
		    ,PERIOD 
		    ,UNAMORT_COST_PREV 
		    ,UNAMORT_FEE_PREV 
		    -- SWITCH ADJUST CARRY FORWARD 
		    ,SW_ADJ_COST 
		    ,SW_ADJ_FEE 
        ) SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE
            ,A.MASTERID 
		    ,A.FACNO 
		    ,A.CIFNO 
		    ,A.ACCTNO 
		    ,A.DATASOURCE 
		    ,B.PREVDATE 
		    ,B.PMTDATE 
		    ,B.I_DAYSCNT 
		    ,B.N_UNAMORT_COST 
		    ,B.N_UNAMORT_FEE 
		    ,B.N_AMORT_COST 
		    ,B.N_AMORT_FEE 
		    ,CURRENT_TIMESTAMP 
		    ,''SL_SWITCH'' 
		    ,B.STATUS 
		    ,B.AMORTSTOPDATE 
		    ,B.AMORTSTOPREASON 
		    ,B.N_DAILY_AMORT_COST 
		    ,B.N_DAILY_AMORT_FEE 
		    ,B.AMORTENDDATE 
		    ,B.PERIOD 
		    ,B.UNAMORT_COST_PREV 
		    ,B.UNAMORT_FEE_PREV 
		    -- SWITCH ADJUST CARRY FORWARD 
		    ,B.SW_ADJ_COST 
		    ,B.SW_ADJ_FEE 
        FROM ' || V_TABLEINSERT3 || ' A 
        JOIN ' || V_TABLEINSERT2 || ' B 
        ON B.AMORTSTOPDATE IS NULL 
        AND B.MASTERID = A.PREV_MASTERID 
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND A.PREV_SL_ECF = ''Y'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || ' 
        (
            DOWNLOAD_DATE 
		    ,ECFDATE 
		    ,MASTERID 
		    ,BRCODE 
		    ,CIFNO 
		    ,FACNO 
		    ,ACCTNO 
		    ,DATASOURCE 
		    ,CCY 
		    ,PRDCODE 
		    ,TRXCODE 
		    ,FLAG_CF 
		    ,FLAG_REVERSE 
		    ,METHOD 
		    ,STATUS 
		    ,SRCPROCESS 
		    ,AMOUNT 
		    ,CREATEDDATE 
		    ,CREATEDBY 
		    ,SEQ 
		    ,AMOUNT_ORG 
		    ,ORG_CCY 
		    ,ORG_CCY_EXRATE 
		    ,PRDTYPE 
		    ,CF_ID 
        ) SELECT 
            C.DOWNLOAD_DATE 
		    ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS ECFDATE 
		    ,A.MASTERID 
		    ,A.BRCODE 
		    ,A.CIFNO 
		    ,A.FACNO 
		    ,A.ACCTNO 
		    ,A.DATASOURCE 
		    ,C.CCY 
		    ,A.PRDCODE 
		    ,C.TRXCODE 
		    ,C.FLAG_CF 
		    ,C.FLAG_REVERSE 
		    ,C.METHOD 
		    ,C.STATUS 
		    ,C.SRCPROCESS 
		    ,C.AMOUNT 
		    ,CURRENT_TIMESTAMP 
		    ,''SL_SWITCH'' 
		    ,C.SEQ 
		    ,C.AMOUNT_ORG 
		    ,C.ORG_CCY 
		    ,C.ORG_CCY_EXRATE 
		    ,A.PRDTYPE 
		    ,C.CF_ID 
        FROM ' || V_TABLEINSERT3 || ' A
        JOIN ' || V_TABLEINSERT2 || ' B 
            ON B.AMORTSTOPDATE IS NULL 
            AND B.MASTERID = A.PREV_MASTERID 
            AND B.PREVDATE = B.PMTDATE 
            AND B.DOWNLOAD_DATE < ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        JOIN ' || V_TABLEINSERT4 || ' C 
            ON C.ECFDATE = B.DOWNLOAD_DATE 
            AND C.MASTERID = A.PREV_MASTERID 
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND  A.PREV_SL_ECF = ''Y'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET 
            STATUS = CASE 
                WHEN A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                THEN ''REV''
                ELSE ''REV2''
            END 
            ,CREATEDBY = ''SL_SWITCH'' 
        FROM (
            SELECT 
                C.MASTERID 
                ,C.DOWNLOAD_DATE 
                ,C.SEQ 
            FROM ' || V_TABLEINSERT3 || ' A 
            JOIN IFRS_PRC_DATE_AMORT P 
            ON P.CURRDATE = A.DOWNLOAD_DATE 
            JOIN VW_LAST_SL_CF_PREV C 
            ON C.MASTERID = A.PREV_MASTERID 
            WHERE A.PREV_SL_ECF = ''Y'' 
        ) C 
        WHERE A.DOWNLOAD_DATE = C.DOWNLOAD_DATE 
        AND A.MASTERID = C.MASTERID 
        AND A.SEQ = C.SEQ ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (
            DOWNLOAD_DATE 
		    ,ECFDATE 
		    ,MASTERID 
		    ,BRCODE 
		    ,CIFNO 
		    ,FACNO 
		    ,ACCTNO 
		    ,DATASOURCE 
		    ,CCY 
		    ,PRDCODE 
		    ,TRXCODE 
		    ,FLAG_CF 
		    ,FLAG_REVERSE 
		    ,METHOD 
		    ,STATUS 
		    ,SRCPROCESS 
		    ,AMOUNT 
		    ,CREATEDDATE 
		    ,CREATEDBY 
		    ,ISUSED 
		    ,SEQ 
		    ,AMOUNT_ORG 
		    ,ORG_CCY 
		    ,ORG_CCY_EXRATE 
		    ,PRDTYPE 
		    ,CF_ID 
        ) SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE 
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS ECFDATE 
            ,A.MASTERID 
		    ,A.BRCODE 
		    ,A.CIFNO 
		    ,A.FACNO 
		    ,A.ACCTNO 
		    ,A.DATASOURCE 
		    ,D.CCY 
		    ,A.PRDCODE 
		    ,D.TRXCODE 
		    ,D.FLAG_CF 
		    ,D.FLAG_REVERSE 
		    ,D.METHOD 
		    ,''ACT'' STATUS 
		    ,D.SRCPROCESS 
		    ,D.AMOUNT 
		    ,CURRENT_TIMESTAMP 
		    ,''SL_SWITCH'' 
		    ,D.ISUSED 
		    ,''0'' SEQ_NEW 
		    ,D.AMOUNT_ORG 
		    ,D.ORG_CCY 
		    ,D.ORG_CCY_EXRATE 
		    ,A.PRDTYPE 
		    ,D.CF_ID 
        FROM ' || V_TABLEINSERT3 || ' A 
        JOIN VW_LAST_SL_CF_PREV C 
            ON C.MASTERID = A.PREV_MASTERID 
        JOIN ' || V_TABLEINSERT1 || ' D
            ON D.DOWNLOAD_DATE = C.DOWNLOAD_DATE
            AND D.MASTERID = A.PREV_MASTERID
            AND D.SEQ = C.SEQ
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND A.PREV_SL_ECF = ''Y'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' A 
        SET 
            AMORTSTOPDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ,AMORTSTOPREASON = ''SL_SWITCH'' 
        WHERE AMORTSTOPDATE IS NULL 
        AND DOWNLOAD_DATE < ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND MASTERID IN (
            SELECT PREV_MASTERID 
            FROM ' || V_TABLEINSERT3 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND PREV_SL_ECF = ''Y'' 
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || 'TMP_T1' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T1' || ' (MASTERID)
        SELECT PREV_MASTERID AS MASTERID 
        FROM ' || V_TABLEINSERT3 || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND PREV_SL_ECF = ''Y'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || 'TMP_T2' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T2' || ' (MASTERID)
        SELECT DISTINCT MASTERID 
        FROM ' || V_TABLEINSERT5 || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE 
        AND DO_AMORT = ''Y'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || 'TMP_T3' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T3' || ' (MASTERID)
        SELECT A.MASTERID 
        FROM ' || 'TMP_T1' || ' A ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || 'TMP_P1' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_P1' || ' (ID) 
        SELECT MAX(ID) AS ID 
        FROM ' || V_TABLEINSERT5 || ' 
        WHERE MASTERID IN (
            SELECT MASTERID 
            FROM ' || 'TMP_T3' || '
        ) AND DO_AMORT = ''N'' 
        GROUP BY MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT3 || ' A 
        SET 
            SW_ADJ_COST = B.N_ACCRU_COST 
            ,SW_ADJ_FEE = B.N_ACCRU_FEE 
        FROM (
            SELECT 
                B.MASTERID 
			    ,B.N_ACCRU_COST 
			    ,B.N_ACCRU_FEE 
			    ,C.CURRDATE 
            FROM ' || V_TABLEINSERT5 || ' B 
            CROSS JOIN IFRS_PRC_DATE_AMORT C 
            WHERE B.ID IN (SELECT ID FROM ' || 'TMP_P1' || ')
        ) B 
        WHERE A.DOWNLOAD_DATE = B.CURRDATE 
        AND A.PREV_SL_ECF = ''Y'' 
        AND A.PREV_MASTERID = B.MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_TF' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_TF' || ' 
        (
            SUM_AMT 
            ,DOWNLOAD_DATE 
            ,MASTERID 
        ) SELECT 
            SUM(A.N_AMOUNT) AS SUM_AMT 
            ,A.DOWNLOAD_DATE 
            ,A.MASTERID
        FROM (
            SELECT 
                CASE 
                    WHEN A.FLAG_REVERSE = ''Y'' 
                    THEN -1 * A.AMOUNT
                    ELSE A.AMOUNT
                END AS N_AMOUNT
                ,A.ECFDATE AS DOWNLOAD_DATE 
                ,A.MASTERID 
            FROM ' || V_TABLEINSERT4 || ' A 
            WHERE A.MASTERID IN (SELECT MASTERID FROM ' || 'TMP_T3' || ') 
            AND A.STATUS = ''ACT'' 
            AND A.FLAG_CF = ''F'' 
            AND A.METHOD = ''SL'' 
        ) A 
        GROUP BY A.DOWNLOAD_DATE, A.MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || 'TMP_TC' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_TC' || ' 
        (
            SUM_AMT 
            ,DOWNLOAD_DATE 
            ,MASTERID
        ) SELECT 
            SUM(A.N_AMOUNT) AS SUM_AMT 
            ,A.DOWNLOAD_DATE 
            ,A.MASTERID 
        FROM ( 
            SELECT 
                CASE 
                    WHEN A.FLAG_REVERSE = ''Y''
                    THEN -1 * A.AMOUNT
                    ELSE A.AMOUNT
                END AS N_AMOUNT
                ,A.ECFDATE AS DOWNLOAD_DATE
                ,A.MASTERID
            FROM ' || V_TABLEINSERT4 || ' A
            WHERE A.MASTERID IN (SELECT MASTERID FROM ' || 'TMP_T3' || ')
            AND A.STATUS = ''ACT''
            AND A.FLAG_CF = ''C''
            AND A.METHOD = ''SL''
        ) A
        GROUP BY A.DOWNLOAD_DATE, A.MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT6 || ' 
        (
            FACNO 
		    ,CIFNO 
		    ,DOWNLOAD_DATE 
		    ,ECFDATE 
		    ,DATASOURCE 
		    ,PRDCODE 
		    ,TRXCODE 
		    ,CCY 
		    ,AMOUNT 
		    ,STATUS 
		    ,CREATEDDATE 
		    ,ACCTNO 
		    ,MASTERID 
		    ,FLAG_CF 
		    ,FLAG_REVERSE 
		    ,AMORTDATE 
		    ,SRCPROCESS 
		    ,ORG_CCY 
		    ,ORG_CCY_EXRATE 
		    ,PRDTYPE 
		    ,CF_ID 
        ) SELECT 
            A.FACNO 
            ,A.CIFNO 
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE 
            ,A.ECFDATE
		    ,A.DATASOURCE 
		    ,B.PRDCODE 
		    ,B.TRXCODE 
		    ,B.CCY 
            ,CAST(
                CAST(
                    CASE 
                        WHEN B.FLAG_REVERSE = ''Y'' 
                        THEN -1 * B.AMOUNT 
                        ELSE B.AMOUNT 
                    END AS FLOAT
                ) / CAST(C.SUM_AMT AS FLOAT) 
                AS NUMERIC(32, 20)
            ) * A.N_ACCRU_FEE AS N_AMOUNT 
            ,B.STATUS 
            ,CURRENT_TIMESTAMP 
            ,A.ACCTNO 
		    ,A.MASTERID 
		    ,B.FLAG_CF 
		    ,''N'' 
		    ,NULL AS AMORTDATE 
		    ,''SW'' 
		    ,B.ORG_CCY 
		    ,B.ORG_CCY_EXRATE 
		    ,B.PRDTYPE 
		    ,B.CF_ID 
        FROM ' || V_TABLEINSERT5 || ' A 
        JOIN ' || V_TABLEINSERT4 || ' B 
            ON B.ECFDATE = A.ECFDATE 
            AND A.MASTERID = B.MASTERID 
            AND B.FLAG_CF = ''F'' 
        JOIN ' || 'TMP_TF' || ' C
            ON C.DOWNLOAD_DATE = A.ECFDATE
            AND C.MASTERID = A.MASTERID
        WHERE A.ID IN (SELECT ID FROM ' || 'TMP_P1' || ') ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT6 || ' 
        (
            FACNO 
		    ,CIFNO 
		    ,DOWNLOAD_DATE 
		    ,ECFDATE 
		    ,DATASOURCE 
		    ,PRDCODE 
		    ,TRXCODE 
		    ,CCY 
		    ,AMOUNT 
		    ,STATUS 
		    ,CREATEDDATE 
		    ,ACCTNO 
		    ,MASTERID 
		    ,FLAG_CF 
		    ,FLAG_REVERSE 
		    ,AMORTDATE 
		    ,SRCPROCESS 
		    ,ORG_CCY 
		    ,ORG_CCY_EXRATE 
		    ,PRDTYPE 
		    ,CF_ID 
        ) SELECT 
            A.FACNO 
            ,A.CIFNO 
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE 
            ,A.ECFDATE 
		    ,A.DATASOURCE 
		    ,B.PRDCODE 
		    ,B.TRXCODE 
		    ,B.CCY 
            ,CAST(
                CAST(
                    CASE 
                        WHEN B.FLAG_REVERSE = ''Y''
                        THEN -1 * B.AMOUNT
                        ELSE B.AMOUNT
                    END AS FLOAT 
                ) / CAST(C.SUM_AMT AS FLOAT) 
                AS NUMERIC(32, 20) 
            ) * A.N_ACCRU_COST AS N_AMOUNT 
            ,B.STATUS 
		    ,CURRENT_TIMESTAMP 
		    ,A.ACCTNO 
		    ,A.MASTERID 
		    ,B.FLAG_CF 
		    ,''N'' 
		    ,NULL AS AMORTDATE 
		    ,''SW'' 
		    ,B.ORG_CCY 
		    ,B.ORG_CCY_EXRATE 
		    ,B.PRDTYPE 
		    ,B.CF_ID 
        FROM ' || V_TABLEINSERT5 || ' A
        JOIN ' || V_TABLEINSERT4 || ' B
            ON B.ECFDATE = A.ECFDATE
            AND A.MASTERID = B.MASTERID
            AND B.FLAG_CF = ''C''
        JOIN ' || 'TMP_TC' || ' C
            ON C.DOWNLOAD_DATE = A.ECFDATE
            AND C.MASTERID = A.MASTERID
        WHERE A.ID IN (SELECT ID FROM ' || 'TMP_P1' || ') ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' A 
        SET STATUS = TO_CHAR(''' || V_CURRDATE || '''::DATE, ''YYYYMMDD'') 
        WHERE STATUS = ''ACT'' 
        AND MASTERID IN (
            SELECT PREV_MASTERID 
            FROM ' || V_TABLEINSERT3 || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND PREV_SL_ECF = ''Y''
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || 'TMP_SW1' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_SW1' || ' (MASTERID, PMTDATE) 
        SELECT 
            A.MASTERID 
            ,MIN(A.PMTDATE) AS PMTDATE 
        FROM ' || V_TABLEINSERT2 || ' A 
        JOIN ' || V_TABLEINSERT3 || ' B
            ON A.MASTERID = B.PREV_MASTERID
            AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND B.PREV_SL_ECF = ''Y'' 
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND A.AMORTSTOPDATE IS NULL 
            AND A.PMTDATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND A.PMTDATE <> A.PREVDATE 
        GROUP BY A.MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' A 
        SET 
            SW_ADJ_COST = COALESCE(A.SW_ADJ_COST, 0) + X.SW_ADJ_COST
		    ,SW_ADJ_FEE = COALESCE(A.SW_ADJ_FEE, 0) + X.SW_ADJ_FEE 
        FROM (
            SELECT 
                A.*
                ,B.SW_ADJ_COST
                ,B.SW_ADJ_FEE
                ,C.CURRDATE
            FROM ' || 'TMP_SW1' || ' A
            JOIN ' || V_TABLEINSERT3 || ' B ON B.MASTERID = A.MASTERID
                AND B.DOWNLOAD_DATE =  ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
                AND B.PREV_SL_ECF = ''Y''
            CROSS JOIN IFRS_PRC_DATE_AMORT C
            ) X
        WHERE A.DOWNLOAD_DATE = X.CURRDATE
        AND A.MASTERID = X.MASTERID
        AND A.PMTDATE = X.PMTDATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || 'IFRS_ACCT_COST_FEE_SUMM' || ' A 
        SET 
            AMOUNT_FEE = COALESCE(AMOUNT_FEE, 0) + COALESCE(AMORT_FEE, 0)
		    ,AMOUNT_COST = COALESCE(AMOUNT_COST, 0) + COALESCE(AMORT_COST, 0)
		    ,CREATEDBY = ''SL_SWITCH''
	    FROM (
		    SELECT 
                B.DOWNLOAD_DATE
			    ,B.MASTERID
		    FROM ' || V_TABLEINSERT3 || ' B
		    WHERE B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND B.MASTERID IN (
                SELECT MASTERID
                FROM ' || V_TABLEINSERT3 || '
                WHERE DOWNLOAD_DATE =  ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                AND PREV_SL_ECF = ''Y''
            )
        ) X
	    WHERE A.DOWNLOAD_DATE = X.DOWNLOAD_DATE
        AND A.MASTERID = X.MASTERID ';
    EXECUTE (V_STR_QUERY);
    
    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_ACCT_SL_SWITCH', '');

    RAISE NOTICE 'SP_IFRS_ACCT_SL_SWITCH | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT6;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_ACCT_SL_SWITCH';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT6 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$