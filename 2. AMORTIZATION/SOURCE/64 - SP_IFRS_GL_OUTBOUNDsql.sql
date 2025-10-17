CREATE OR REPLACE SP_IFRS_GL_OUTBOUND(IN P_RUNID CHARACTER VARYING DEFAULT 'S_00000_0000'::CHARACTER VARYING, IN P_DOWNLOAD_DATE DATE DEFAULT NULL::DATE, IN P_PRC CHARACTER VARYING DEFAULT 'S'::CHARACTER VARYING, IN P_ROUND INTEGER DEFAULT 2, IN P_FUNCROUND INTEGER DEFAULT 0)
 LANGUAGE PLPGSQL
AS $$
DECLARE
    ---- DATE
    V_PREVDATE DATE;
    V_CURRDATE DATE;

    ---- QUERY   
    V_STR_QUERY TEXT;

    ---- TABLE LIST       
    V_TABLENAME VARCHAR(100);
    V_TABLEINSERT1 VARCHAR(100);
    V_TABLEINSERT2 VARCHAR(100);
    V_TABLEINSERT3 VARCHAR(100);
    V_TABLEINSERT4 VARCHAR(100);
    V_TABLEINSERT5 VARCHAR(100);

    ---- VARIABLE PROCESS
    V_SANDI_DATE DATE;
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
	FCESIG := SUBSTRING(STACK FROM 'FUNCTION (.*?) LINE');
	V_SP_NAME := UPPER(LEFT(FCESIG::REG::TEXT, POSITION('(' IN FCESIG::REG::TEXT)-1));

    IF COALESCE(P_PRC, NULL) IS NULL THEN
        P_PRC := 'S';
    END IF;

    IF COALESCE(P_RUNID, NULL) IS NULL THEN
        P_RUNID := 'S_00000_0000';
    END IF;

    IF P_PRC = 'S' THEN 
        V_TABLENAME := 'TMP_IMA_' || P_RUNID || '';
        V_TABLEINSERT1 := 'IFRS_ACCT_JOURNAL_DATA_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_BTPN_MAPPING_SANDI_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_GL_OUTBOUND_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_IMA_AMORT_CURR_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_IMA_AMORT_PREV_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_ACCT_JOURNAL_DATA';
        V_TABLEINSERT2 := 'IFRS_BTPN_MAPPING_SANDI';
        V_TABLEINSERT3 := 'IFRS_GL_OUTBOUND';
        V_TABLEINSERT4 := 'IFRS_IMA_AMORT_CURR';
        V_TABLEINSERT5 := 'IFRS_IMA_AMORT_PREV';
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
    
    IF P_ROUND IS NULL THEN
        V_ROUND := 2;
    ELSE
        V_ROUND := P_ROUND;
    END IF;

    IF P_FUNCROUND IS NULL THEN
        V_FUNCROUND := 0;
    ELSE
        V_FUNCROUND := P_FUNCROUND;
    END IF;

    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- RECORD RUN_ID --------
    CALL SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, PG_BACKEND_PID(), CURRENT_DATE);
    -------- RECORD RUN_ID --------

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT2 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT2 || ' AS SELECT * FROM IFRS_BTPN_MAPPING_SANDI WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT3 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT3 || ' AS SELECT * FROM IFRS_GL_OUTBOUND WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_GL_OUTBOUND', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT MAX(BUSS_DATE) FROM ' || V_TABLEINSERT2 || ' ';
    EXECUTE (V_STR_QUERY) INTO V_SANDI_DATE;

    V_SANDI_DATE := COALESCE(V_SANDI_DATE, V_CURRDATE);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT3 || ' 
        WHERE BUSS_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND CLASS = ''A'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT3 || ' 
        (    
            BUSS_DATE  
            ,BRANCH                    
            ,ACCOUNT_NO  
            ,DESCRIPTION  
            ,CCY  
            ,ACCOUNT_TYPE  
            ,VALUE_DATE                 
            ,SIGN  
            ,AMOUNT  
            ,NARRATIVE1  
            ,GROUP_OR_USER_ID  
            ,TIME_STAMP  
            ,PRODUCT_CODE                    
            ,CUSTOMER_TYPE  
            ,TRANSACTION_RATE  
            ,AMOUNT_LEV  
            ,JURNAL_NUMBER  
            ,SOURCE_DATA  
            ,EVENT_TYPE  
            ,CLASS                        
        ) SELECT    
            BUSS_DATE  
            ,BRANCH  
            ,ACCOUNT_NO  
            ,MAX(LEFT(DESCRIPTION,50))        
            ,CCY  
            ,ACCOUNT_TYPE  
            ,VALUE_DATE                  
            ,SIGN  
            ,SUM(AMOUNT) AS AMOUNT  
            ,NARRATIVE1    
            ,GROUP_OR_USER_ID  
            ,MAX(TIME_STAMP) TIME_STAMP              
            ,PRODUCT_CODE                
            ,CUSTOMER_TYPE  
            ,TRANSACTION_RATE  
            ,SUM(AMOUNT_LEV) AS AMOUNT_LEV    
            ,JURNAL_NUMBER  
            ,SOURCE_DATA  
            ,EVENT_TYPE  
            ,''A'' AS CLASS     
        FROM (
            SELECT   
                A.DOWNLOAD_DATE AS BUSS_DATE  
                ,A.BRANCH AS BRANCH  
                ,A.GLNO AS ACCOUNT_NO  
                ,LEFT(B.DESCRIPTION,50) AS DESCRIPTION  
                ,A.CCY AS CCY  
                ,COALESCE(C.ACCOUNT_TYPE, E.ACCOUNT_TYPE,A.ACCOUNT_TYPE) AS ACCOUNT_TYPE  
                ,A.DOWNLOAD_DATE AS VALUE_DATE  
                ,A.DRCR AS SIGN  
                ,ROUND(CAST(COALESCE(A.N_AMOUNT, 0) AS NUMERIC(38, 2)), ' || V_ROUND || ') AS AMOUNT  
                ,A.REVERSE AS NARRATIVE1  
                ,''REGLA'' AS GROUP_OR_USER_ID  
                ,TO_CHAR(CURRENT_TIMESTAMP, ''YYYY-MM-DD HH24:MI:SS.MS'') AS TIME_STAMP
                ,LEFT(PRDCODE, 6) AS PRODUCT_CODE  
                ,COALESCE(C.CUSTOMER_TYPE, E.CUSTOMER_TYPE,A.CUSTOMER_TYPE) AS CUSTOMER_TYPE                         
                ,CAST(COALESCE(D.RATE_AMOUNT, 1) AS NUMERIC(38, 2)) AS TRANSACTION_RATE  
                ,(ROUND(CAST(COALESCE(A.N_AMOUNT, 0) AS NUMERIC(38, 2)), ' || V_ROUND || ') * CAST(COALESCE(D.RATE_AMOUNT, 1) AS NUMERIC(38, 2))) AS AMOUNT_LEV  
                ,CONCAT(TO_CHAR(A.DOWNLOAD_DATE, ''YYYYMMDD''), CASE 
                    WHEN A.JOURNALCODE = ''AMORT'' 
                    THEN A.JOURNALCODE 
                    ELSE A.JOURNALCODE2 
                END, A.BRANCH, A.CCY, LEFT(PRDCODE, 6)) AS JURNAL_NUMBER  
                ,''PSAK71'' AS SOURCE_DATA  
                ,CASE 
                    WHEN A.JOURNALCODE = ''AMORT'' 
                    THEN A.JOURNALCODE 
                    ELSE A.JOURNALCODE2 
                END AS EVENT_TYPE                        
            FROM ' || V_TABLEINSERT1 || ' A  
            LEFT JOIN (
                SELECT 
                    ACCOUNT
                    ,MAX(DESCRIPTION) AS DESCRIPTION  
                FROM (     
                    SELECT 
                        ACCOUNT
                        ,SANDI_LBU
                        ,LEFT(DESCRIPTION,50) AS DESCRIPTION     
                    FROM ' || V_TABLEINSERT2 || '
                    WHERE SANDI_LBU = ''175'' 
                        AND LEFT(ACCOUNT, 2) = ''10''                    
                        AND BUSS_DATE = ''' || CAST(V_SANDI_DATE AS VARCHAR(10)) || '''::DATE 
                    UNION ALL    
                    SELECT 
                        ACCOUNT
                        ,SANDI_LBU
                        ,LEFT(DESCRIPTION,50)   AS DESCRIPTION    
                    FROM ' || V_TABLEINSERT2 || ' 
                    WHERE SANDI_LBU <> ''175''                    
                    AND BUSS_DATE = ''' || CAST(V_SANDI_DATE AS VARCHAR(10)) || '''::DATE 
                ) A                   
                GROUP BY ACCOUNT 
            ) B 
                ON A.GLNO = B.ACCOUNT                       
            LEFT JOIN ' || V_TABLEINSERT4 || ' C 
                ON A.MASTERID = C.MASTERID                     
            LEFT JOIN ' || 'IFRS_MASTER_EXCHANGE_RATE' || ' D 
                ON A.DOWNLOAD_DATE = D.DOWNLOAD_DATE 
                AND A.CCY = D.CURRENCY  
            LEFT JOIN ' || V_TABLEINSERT5 || ' E 
                ON A.MASTERID = E.MASTERID  
            WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
        ) A
        GROUP BY   
            BUSS_DATE  
            ,BRANCH  
            ,ACCOUNT_NO                         
            ,CCY  
            ,ACCOUNT_TYPE  
            ,VALUE_DATE  
            ,SIGN  
            ,NARRATIVE1  
            ,GROUP_OR_USER_ID                      
            ,PRODUCT_CODE  
            ,CUSTOMER_TYPE  
            ,TRANSACTION_RATE                         
            ,JURNAL_NUMBER  
            ,SOURCE_DATA  
            ,EVENT_TYPE     
        ORDER BY  
            BUSS_DATE ASC  
            ,BRANCH ASC  
            ,CCY ASC  
            ,AMOUNT ASC  
            ,SIGN DESC  
            ,ACCOUNT_NO ASC ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_GL_OUTBOUND', '');

    RAISE NOTICE 'SP_IFRS_GL_OUTBOUND | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT3;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_GL_OUTBOUND';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT3 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$
