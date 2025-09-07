---- DROP PROCEDURE SP_IFRS_PROCESS_TRAN_DAILY;

CREATE OR REPLACE PROCEDURE SP_IFRS_PROCESS_TRAN_DAILY(
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
        V_TABLEINSERT1 := 'IFRS_IMA_AMORT_CURR_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_ACCT_AMORT_RESTRU_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_ACCT_COST_FEE_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_PRODUCT_PARAM_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_IMA_AMORT_CURR';
        V_TABLEINSERT2 := 'IFRS_ACCT_AMORT_RESTRU';
        V_TABLEINSERT3 := 'IFRS_ACCT_COST_FEE';
        V_TABLEINSERT4 := 'IFRS_PRODUCT_PARAM';
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
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT3 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT3 || ' AS SELECT * FROM IFRS_ACCT_COST_FEE WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_PROCESS_TRAN_DAILY', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT3 || ' WHERE DOWNLOAD_DATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    ---- INSERT FOR RESTRUCTURE ACCOUNT AS TRANSACTION

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT3 || ' 
        (
            DOWNLOAD_DATE 
            ,MASTERID 
            ,BRCODE 
            ,CIFNO 
            ,FACNO 
            ,ACCTNO 
            ,DATASOURCE 
            ,PRD_TYPE 
            ,PRD_CODE 
            ,TRX_CODE 
            ,CCY 
            ,FLAG_CF 
            ,FLAG_REVERSE 
            ,METHOD 
            ,STATUS 
            ,SRCPROCESS 
            ,AMOUNT 
            ,CREATEDDATE 
            ,CREATEDBY 
            ,TRX_REFF_NUMBER 
            ,SOURCE_TABLE 
            ,TRX_LEVEL 
        ) SELECT 
            A.DOWNLOAD_DATE 
            ,A.MASTERID 
            ,A.BRANCH_CODE 
            ,A.CUSTOMER_NUMBER 
            ,A.FACILITY_NUMBER 
            ,A.ACCOUNT_NUMBER 
            ,A.DATA_SOURCE 
            ,A.PRODUCT_TYPE 
            ,A.PRODUCT_CODE 
            ,''RESTRU'' AS TRX_CODE 
            ,A.CURRENCY 
            ,''F'' AS FLAG_CF 
            ,''N'' AS FLAG_REVERSE 
            ,A.AMORT_TYPE 
            ,''ACT'' AS STATUS 
            ,''RESTRU'' AS SRCPROCESS 
            ,B.PRORATE_UNAMORT_FEE 
            ,CURRENT_TIMESTAMP AS CREATEDDATE 
            ,''SP_IFRS_TRAN_DAILY'' AS CREATEDBY 
            ,''RESTRU - '' || B.PREV_MASTERID AS TRX_REFF_NUMBER 
            ,''' || V_TABLEINSERT2 || ''' AS SOURCE_TABLE 
            ,NULL AS TRX_LEVEL 
        FROM ' || V_TABLEINSERT1 || ' A 
        JOIN ' || V_TABLEINSERT2 || ' B 
        ON A.MASTERID = B.MASTERID 
        AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 

        UNION ALL 

        SELECT 
            A.DOWNLOAD_DATE 
            ,A.MASTERID 
            ,A.BRANCH_CODE 
            ,A.CUSTOMER_NUMBER 
            ,A.FACILITY_NUMBER 
            ,A.ACCOUNT_NUMBER 
            ,A.DATA_SOURCE 
            ,A.PRODUCT_TYPE 
            ,A.PRODUCT_CODE 
            ,''RESTRU'' AS TRX_CODE 
            ,A.CURRENCY 
            ,''C'' AS FLAG_CF 
            ,''N'' AS FLAG_REVERSE 
            ,A.AMORT_TYPE 
            ,''ACT'' AS STATUS 
            ,''RESTRU'' AS SRCPROCESS 
            ,B.PRORATE_UNAMORT_COST 
            ,CURRENT_TIMESTAMP AS CREATEDDATE 
            ,''SP_IFRS_TRAN_DAILY'' AS CREATEDBY 
            ,''RESTRU - '' || B.PREV_MASTERID AS TRX_REFF_NUMBER 
            ,''' || V_TABLEINSERT2 || ''' AS SOURCE_TABLE 
            ,NULL AS TRX_LEVEL 
        FROM ' || V_TABLEINSERT1 || ' A 
        JOIN ' || V_TABLEINSERT2 || ' B 
        ON A.MASTERID = B.MASTERID 
        AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;


    ---- END RESTRUCTURE ACCOUNT 

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT3 || ' 
        (
            DOWNLOAD_DATE 
            ,MASTERID 
            ,BRCODE 
            ,CIFNO 
            ,FACNO 
            ,ACCTNO 
            ,DATASOURCE 
            ,PRD_TYPE 
            ,PRD_CODE 
            ,TRX_CODE 
            ,CCY 
            ,FLAG_CF 
            ,FLAG_REVERSE 
            ,METHOD 
            ,STATUS 
            ,SRCPROCESS 
            ,AMOUNT 
            ,CREATEDDATE 
            ,CREATEDBY 
            ,TRX_REFF_NUMBER 
            ,SOURCE_TABLE 
            ,TRX_LEVEL 
        ) SELECT 
            A.DOWNLOAD_DATE 
            ,A.MASTERID 
            ,A.BRANCH_CODE 
            ,NULL AS CIFNO 
            ,A.FACILITY_NUMBER 
            ,A.ACCOUNT_NUMBER 
            ,A.DATA_SOURCE 
            ,A.PRD_TYPE 
            ,A.PRD_CODE 
            ,A.TRX_CODE 
            ,A.CCY 
            ,LEFT(COALESCE(B.IFRS_TXN_CLASS, ''F''), 1) AS FLAG_CF 
            ,LEFT(COALESCE(A.DEBET_CREDIT_FLAG, ''X''), 1) AS FLAG_REVERSE 
            ,''X'' AS METHOD 
            ,''ACT'' AS STATUS 
            ,''TRAN_DAILY'' AS SRCPROCESS 
            ,A.ORG_CCY_AMT 
            ,CURRENT_TIMESTAMP AS CREATEDDATE 
            ,''SP_IFRS_TRAN_DAILY'' AS CREATEDBY 
            ,TRX_REFERENCE_NUMBER 
            ,SOURCE_TABLE 
            ,TRX_LEVEL 
        FROM ' || 'IFRS_TRANSACTION_DAILY' || ' A 
        JOIN (
            SELECT DISTINCT 
                DATA_SOURCE 
                ,PRD_TYPE 
                ,PRD_CODE 
                ,TRX_CODE 
                ,CCY 
                ,IFRS_TXN_CLASS 
            FROM ' || 'IFRS_TRANSACTION_PARAM' || ' 
            WHERE IFRS_TXN_CLASS IN (''FEE'', ''COST'') 
            AND AMORTIZATION_FLAG = ''Y'' 
        ) B 
        ON ( 
            B.DATA_SOURCE = A.DATA_SOURCE 
            OR COALESCE(B.DATA_SOURCE, ''ALL'') = ''ALL'' 
        ) AND ( 
            B.PRD_TYPE = A.PRD_TYPE 
            OR COALESCE(B.PRD_TYPE, ''ALL'') = ''ALL'' 
        ) AND (
            B.PRD_CODE = A.PRD_CODE 
            OR COALESCE(B.PRD_CODE, ''ALL'') = ''ALL'' 
        ) AND B.TRX_CODE = A.TRX_CODE 
        AND ( 
            B.CCY = A.CCY 
            OR B.CCY = ''ALL'' 
        ) 
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT3 || ' 
        ( 
            DOWNLOAD_DATE 
            ,MASTERID 
            ,BRCODE 
            ,CIFNO 
            ,FACNO 
            ,ACCTNO 
            ,DATASOURCE 
            ,PRD_TYPE 
            ,PRD_CODE 
            ,TRX_CODE 
            ,CCY 
            ,FLAG_CF 
            ,FLAG_REVERSE 
            ,METHOD 
            ,STATUS 
            ,SRCPROCESS 
            ,AMOUNT 
            ,CREATEDDATE 
            ,CREATEDBY 
            ,TRX_REFF_NUMBER 
            ,SOURCE_TABLE 
            ,TRX_LEVEL 
        ) SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ,MASTERID 
            ,BRCODE 
            ,CIFNO 
            ,FACNO 
            ,ACCTNO 
            ,DATASOURCE 
            ,PRD_TYPE 
            ,PRD_CODE 
            ,TRX_CODE 
            ,CCY 
            ,FLAG_CF 
            ,CASE 
                WHEN FLAG_AL IN (''A'', ''O'') 
                THEN CASE 
                    WHEN FLAG_CF = ''F'' 
                    THEN CASE 
                        WHEN FLAG_REVERSE = ''N'' 
                        THEN ''C'' 
                        ELSE ''D'' 
                    END 
                    ELSE CASE 
                        WHEN FLAG_REVERSE = ''N'' 
                        THEN ''D'' 
                        ELSE ''C'' 
                    END 
                END 
                ELSE CASE 
                    WHEN FLAG_CF = ''F'' 
                    THEN CASE 
                        WHEN FLAG_REVERSE = ''N'' 
                        THEN ''C'' 
                        ELSE ''D'' 
                    END 
                    ELSE CASE 
                        WHEN FLAG_REVERSE = ''N'' 
                        THEN ''D'' 
                        ELSE ''C'' 
                    END 
                END 
            END AS FLAG_REVERSE 
            ,METHOD 
            ,''ACT'' 
            ,SRCPROCESS 
            ,AMOUNT 
            ,CREATEDDATE 
            ,CREATEDBY 
            ,TRX_REFF_NUMBER 
            ,SOURCE_TABLE 
            ,TRX_LEVEL 
        FROM ' || V_TABLEINSERT3 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND STATUS = ''NPRCD'' ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_PROCESS_TRAN_DAILY', 'INSERTED');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT3 || ' A
        SET 
            CIFNO = B.CUSTOMER_NUMBER 
            ,PRD_CODE = B.PRODUCT_CODE 
            ,PRD_TYPE = B.PRODUCT_TYPE 
            ,DATASOURCE = B.DATA_SOURCE 
            ,BRCODE = B.BRANCH_CODE 
            ,FACNO = B.FACILITY_NUMBER 
        FROM ' || V_TABLEINSERT1 || ' B 
        WHERE B.MASTERID = A.MASTERID 
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_PROCESS_TRAN_DAILY', 'UPD FROM IMA');
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_PROCESS_TRAN_DAILY', 'UPD FROM TRAN PARAM');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT3 || ' A 
        SET FLAG_AL = B.FLAG_AL 
        FROM ' || V_TABLEINSERT4 || ' B 
        WHERE ( 
            B.DATA_SOURCE = A.DATASOURCE 
            OR COALESCE(B.DATA_SOURCE, ''ALL'') = ''ALL'' 
        ) AND ( 
            B.PRD_TYPE = A.PRD_TYPE 
            OR COALESCE(B.PRD_TYPE, ''ALL'') = ''ALL'' 
        ) AND ( 
            B.PRD_CODE = A.PRD_CODE 
            OR COALESCE(B.PRD_CODE, ''ALL'') = ''ALL'' 
        ) AND ( 
            B.CCY = A.CCY 
            OR COALESCE(B.CCY, ''ALL'') = ''ALL''
        ) 
        AND A.SRCPROCESS = ''TRAN_DAILY'' 
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_PROCESS_TRAN_DAILY', 'UPD FROM PROD PARAM');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT3 || ' A 
        SET 
        AMOUNT = CASE 
            WHEN FLAG_AL IN (''A'', ''O'') 
            THEN CASE 
                WHEN FLAG_CF = ''F'' 
                THEN -1 * AMOUNT 
                ELSE AMOUNT 
            END 
            ELSE CASE 
                WHEN FLAG_CF = ''C'' 
                THEN -1 * AMOUNT 
                ELSE AMOUNT 
            END 
        END 
        ,FLAG_REVERSE = CASE 
            WHEN FLAG_AL IN (''A'', ''O'') 
            THEN CASE 
                WHEN FLAG_CF = ''F'' 
                THEN CASE 
                    WHEN FLAG_REVERSE = ''C'' 
                    THEN ''N'' 
                    ELSE ''Y'' 
                END 
                ELSE CASE 
                    WHEN FLAG_REVERSE = ''D'' 
                    THEN ''N'' 
                    ELSE ''Y'' 
                END 
            END 
            ELSE CASE 
                WHEN FLAG_CF = ''F'' 
                THEN CASE 
                    WHEN FLAG_REVERSE = ''C'' 
                    THEN ''N'' 
                    ELSE ''Y'' 
                END 
                ELSE CASE 
                    WHEN FLAG_REVERSE = ''D'' 
                    THEN ''N'' 
                    ELSE ''Y'' 
                END 
            END 
        END 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND STATUS = ''ACT'' 
        AND SRCPROCESS = ''TRAN_DAILY'' ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_PROCESS_TRAN_DAILY', 'UPD AMT REV');

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_PROCESS_TRAN_DAILY', '');

    RAISE NOTICE 'SP_IFRS_PROCESS_TRAN_DAILY | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT3;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_PROCESS_TRAN_DAILY';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT3 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;