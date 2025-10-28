CREATE OR REPLACE PROCEDURE SP_IFRS_STAFF_BENEFIT_SUMM(IN P_RUNID CHARACTER VARYING DEFAULT 'S_00000_0000'::CHARACTER VARYING, IN P_DOWNLOAD_DATE DATE DEFAULT NULL::DATE, IN P_PRC CHARACTER VARYING DEFAULT 'S'::CHARACTER VARYING)
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
        V_TABLEINSERT1 := 'IFRS_STAFF_BENEFIT_SUMM_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_ACCT_EIR_COST_FEE_ECF_' || P_RUNID || '';
    ELSE 
        
        V_TABLEINSERT1 := 'IFRS_STAFF_BENEFIT_SUMM';
        V_TABLEINSERT2 := 'IFRS_ACCT_EIR_COST_FEE_ECF';
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
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM IFRS_STAFF_BENEFIT_SUMM WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_STAFF_BENEFIT_SUMM', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
        WHERE DOWNLOAD_DATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (
            DOWNLOAD_DATE 
	        ,MASTERID 
	        ,BRCODE 
	        ,CIFNO 
	        ,FACNO 
	        ,ACCTNO 
	        ,DATASOURCE 
	        ,CCY 
	        ,AMOUNT_FEE 
	        ,AMOUNT_COST 
	        ,CREATEDDATE 
	        ,CREATEDBY 
	        ,AMORT_FEE 
	        ,AMORT_COST 
        ) SELECT 
            DOWNLOAD_DATE 
	        ,MASTERID 
	        ,BRCODE 
	        ,CIFNO 
	        ,FACNO 
	        ,ACCTNO 
	        ,DATASOURCE 
	        ,CCY 
	        ,AMOUNT_FEE 
	        ,AMOUNT_COST 
	        ,CREATEDDATE 
	        ,CREATEDBY 
	        ,AMORT_FEE 
	        ,AMORT_COST 
        FROM (
            SELECT 
                ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE 
                ,A.MASTERID 
		        ,A.BRCODE 
		        ,A.CIFNO 
		        ,A.FACNO 
		        ,A.ACCTNO 
		        ,A.DATASOURCE 
		        ,A.CCY 
		        ,SUM(COALESCE(A.AMOUNT_FEE, 0)) AS AMOUNT_FEE 
		        ,SUM(COALESCE(A.AMOUNT_COST, 0)) AS AMOUNT_COST 
		        ,CURRENT_TIMESTAMP AS CREATEDDATE 
		        ,''CF_SUMM'' AS CREATEDBY 
		        ,0 AS AMORT_FEE 
		        ,0 AS AMORT_COST 
            FROM (
                SELECT 
                    MASTERID 
			        ,BRCODE 
			        ,CIFNO 
			        ,FACNO 
			        ,ACCTNO 
			        ,DATASOURCE 
			        ,CCY 
			        ,AMOUNT_FEE 
			        ,AMOUNT_COST 
                FROM ' || V_TABLEINSERT1 || ' 
                WHERE DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE 
                UNION ALL 
                SELECT 
                    MASTERID 
			        ,BRCODE 
			        ,CIFNO 
			        ,FACNO 
			        ,ACCTNO 
			        ,DATASOURCE 
			        ,CCY 
                    ,SUM(CASE 
                        WHEN FLAG_CF = ''F'' 
                        THEN CASE 
                            WHEN FLAG_REVERSE = ''Y'' 
                            THEN -1 * AMOUNT 
                            ELSE AMOUNT 
                        END 
                        ELSE 0 
                    END) AS AMOUNT_FEE 
                    ,SUM(CASE 
                        WHEN FLAG_CF = ''C'' 
                        THEN CASE 
                            WHEN FLAG_REVERSE = ''Y'' 
                            THEN -1 * AMOUNT 
                            ELSE AMOUNT 
                        END 
                        ELSE 0 
                    END) AS AMOUNT_COST 
                FROM ' || V_TABLEINSERT2 || ' 
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
                AND STATUS IN (''ACT'', ''PNL'') 
                AND SRCPROCESS NOT IN (''SL_TO_EIR'') 
                AND TRXCODE = ''BENEFIT'' 
                GROUP BY 
                    MASTERID 
			        ,BRCODE 
			        ,CIFNO 
			        ,FACNO 
			        ,ACCTNO 
			        ,DATASOURCE 
			        ,CCY 
            ) A 
            GROUP BY 
                A.MASTERID 
		        ,A.BRCODE 
		        ,A.CIFNO 
		        ,A.FACNO 
		        ,A.ACCTNO 
		        ,A.DATASOURCE 
		        ,A.CCY 
        ) Z ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET 
            AMORT_FEE = B.AMORT_FEE 
            ,AMORT_COST = B.AMORT_COST 
        FROM (
            SELECT X.*, Y.* 
            FROM ' || V_TABLEINSERT1 || ' X 
            JOIN ' || 'IFRS_PRC_DATE_AMORT' || ' Y 
            ON Y.PREVDATE = X.DOWNLOAD_DATE 
        ) B 
        WHERE A.DOWNLOAD_DATE = B.CURRDATE 
        AND A.MASTERID = B.MASTERID ';
    EXECUTE (V_STR_QUERY);

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_STAFF_BENEFIT_SUMM', '');

    RAISE NOTICE 'SP_IFRS_STAFF_BENEFIT_SUMM | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT1;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_STAFF_BENEFIT_SUMM';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT1 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$
