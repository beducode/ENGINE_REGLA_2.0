---- DROP PROCEDURE SP_IFRS_ACCT_EIR_ECF_MERGE;

CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_EIR_ECF_MERGE(
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
        V_TABLEINSERT2 := 'IFRS_ACCT_EIR_ECF_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_ACCT_NOCF_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_ACCT_EIR_CF_ECF_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_ACCT_EIR_ECF_NOCF';
        V_TABLEINSERT2 := 'IFRS_ACCT_EIR_ECF';
        V_TABLEINSERT3 := 'IFRS_ACCT_NOCF';
        V_TABLEINSERT4 := 'IFRS_ACCT_EIR_CF_ECF';
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

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT3 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT3 || ' AS SELECT * FROM IFRS_ACCT_NOCF WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT4 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT4 || ' AS SELECT * FROM IFRS_ACCT_EIR_CF_ECF WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_ACCT_EIR_ECF_MERGE', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_NOCF' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T1' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T1' || ' (MASTERID)
        SELECT DISTINCT MASTERID 
        FROM ' || V_TABLEINSERT1 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_NOCF' || ' 
        (
            MASTERID 
            ,DOWNLOAD_DATE 
        ) SELECT 
            A.MASTERID 
            ,A.DOWNLOAD_DATE 
        FROM ' || V_TABLEINSERT2 || ' A 
        JOIN ' || 'TMP_T1' || ' 
        B ON A.MASTERID = B.MASTERID 
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        GROUP BY A.MASTERID, A.DOWNLOAD_DATE 
        HAVING MIN(A.N_AMORT_AMT) < 0 
        AND MAX(A.N_AMORT_AMT) > 0 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT3 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT3 || ' 
        (
            MASTERID 
            ,DOWNLOAD_DATE 
        ) SELECT 
            MASTERID 
            ,DOWNLOAD_DATE 
        FROM ' || 'TMP_NOCF' || '';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' A 
        SET 
            NOCF_OSPRN = B.N_FAIRVALUE 
		    ,NOCF_OSPRN_PREV = B.N_FAIRVALUE_PREV 
		    ,NOCF_INT_RATE = B.N_EFF_INT_RATE 
		    ,NOCF_PRN_PAYMENT = B.N_INSTALLMENT 
		    ,NOCF_EFF_INT_AMT = B.N_EFF_INT_AMT 
		    ,NOCF_UNAMORT_AMT = B.N_UNAMORT_AMT 
		    ,NOCF_AMORT_AMT = B.N_AMORT_AMT 
		    ,NOCF_UNAMORT_AMT_PREV = B.N_UNAMORT_AMT_PREV 
        FROM (
            SELECT E.* 
            FROM ' || V_TABLEINSERT1 || ' E 
            JOIN ' || 'TMP_NOCF' || ' F 
            ON F.DOWNLOAD_DATE = E.DOWNLOAD_DATE 
            AND F.MASTERID = E.MASTERID 
        ) B 
        WHERE A.MASTERID = B.MASTERID 
        AND B.PMT_DATE = A.PMT_DATE 
        AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' A 
        SET 
            N_UNAMORT_AMT = N_UNAMORT_AMT - COALESCE(NOCF_UNAMORT_AMT, 0) 
            ,N_AMORT_AMT = N_AMORT_AMT - COALESCE(NOCF_AMORT_AMT, 0) 
            ,N_UNAMORT_AMT_PREV = N_UNAMORT_AMT_PREV - COALESCE(NOCF_UNAMORT_AMT_PREV, 0) 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND MASTERID IN (
            SELECT MASTERID 
            FROM ' || 'TMP_NOCF' || ' 
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' A 
        SET 
            N_FEE_UNAMORT_AMT = N_UNAMORT_AMT * (
                B.FEE_AMT + CASE 
                    WHEN B.BENEFIT < 0 
                    THEN B.BENEFIT 
                    ELSE 0 
                END 
            ) / (B.FEE_AMT + B.COST_AMT + B.BENEFIT) 
            ,N_FEE_AMORT_AMT = N_AMORT_AMT * (
                B.FEE_AMT + CASE 
                    WHEN B.BENEFIT < 0 
                    THEN B.BENEFIT 
                    ELSE 0 
                END 
            ) / (B.FEE_AMT + B.COST_AMT + B.BENEFIT)
            ,N_FEE_UNAMORT_AMT_PREV = N_UNAMORT_AMT_PREV * (
                B.FEE_AMT + CASE 
                    WHEN B.BENEFIT < 0 
                    THEN B.BENEFIT 
                    ELSE 0 
                END 
            ) / (B.FEE_AMT + B.COST_AMT + B.BENEFIT) 
            ,N_COST_UNAMORT_AMT = N_UNAMORT_AMT * (
                B.COST_AMT + CASE 
                    WHEN B.BENEFIT > 0 
                    THEN B.BENEFIT 
                    ELSE 0 
                END 
            ) / (B.FEE_AMT + B.COST_AMT + B.BENEFIT)
            ,N_COST_AMORT_AMT = N_AMORT_AMT * (
                B.COST_AMT + CASE 
                    WHEN B.BENEFIT > 0 
                    THEN B.BENEFIT 
                    ELSE 0 
                END
            ) / (B.FEE_AMT + B.COST_AMT + B.BENEFIT) 
            ,N_COST_UNAMORT_AMT_PREV = N_UNAMORT_AMT_PREV * (
                B.COST_AMT + CASE 
                    WHEN B.BENEFIT > 0 
                    THEN B.BENEFIT 
                    ELSE 0 
                END
            ) / (B.FEE_AMT + B.COST_AMT + B.BENEFIT) 
        FROM (
            SELECT 
                E.DOWNLOAD_DATE 
			    ,E.MASTERID 
			    ,E.PMT_DATE 
			    ,G.FEE_AMT 
			    ,G.COST_AMT 
			    ,COALESCE(G.BENEFIT, 0) AS BENEFIT 
            FROM ' || V_TABLEINSERT2 || ' E 
            JOIN ' || 'TMP_NOCF' || ' F 
            ON F.DOWNLOAD_DATE = E.DOWNLOAD_DATE 
            AND F.MASTERID = E.MASTERID 
            JOIN ' || V_TABLEINSERT4 || ' G 
            ON G.MASTERID = E.MASTERID 
        ) B 
        WHERE A.MASTERID = B.MASTERID 
        AND A.PMT_DATE = B.PMT_DATE 
        AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE 
        AND (B.FEE_AMT + B.COST_AMT + B.BENEFIT) <> 0 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' A 
        SET 
            N_FAIRVALUE_PREV = NOCF_OSPRN_PREV + N_UNAMORT_AMT_PREV 
            ,N_FAIRVALUE = NOCF_OSPRN + N_UNAMORT_AMT 
            ,N_DAILY_AMORT_COST = CASE 
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
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND MASTERID IN (
            SELECT MASTERID 
            FROM ' || 'TMP_NOCF' || '
        ) ';
    EXECUTE (V_STR_QUERY);

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_ACCT_EIR_ECF_MERGE', '');

    RAISE NOTICE 'SP_IFRS_ACCT_EIR_ECF_MERGE | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT3;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_ACCT_EIR_ECF_MERGE';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT3 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;