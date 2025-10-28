CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_JRNL_DATA_MTM(IN P_RUNID CHARACTER VARYING DEFAULT 'S_00000_0000'::CHARACTER VARYING, IN P_DOWNLOAD_DATE DATE DEFAULT NULL::DATE, IN P_PRC CHARACTER VARYING DEFAULT 'S'::CHARACTER VARYING)
 LANGUAGE PLPGSQL
AS $$
DECLARE
    ---- DATE
    V_PREVDATE DATE;
    V_CURRDATE DATE;
    V_LASTYEAR DATE;
    V_PREVMONTH DATE;
    V_CURRMONTH DATE;
    V_LASTYEARNEXTMONTH DATE;

    ---- QUERY   
    V_STR_QUERY TEXT;

    ---- TABLE LIST       
    V_TABLENAME VARCHAR(100);
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
        V_TABLENAME := 'TMP_IMA_' || P_RUNID || '';
        V_TABLEINSERT1 := 'IFRS_ACCT_JOURNAL_DATA_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_EIR_ADJUSTMENT_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_IMA_AMORT_CURR_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_JOURNAL_PARAM_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_ACCT_JOURNAL_DATA';
        V_TABLEINSERT2 := 'IFRS_EIR_ADJUSTMENT';
        V_TABLEINSERT3 := 'IFRS_IMA_AMORT_CURR';
        V_TABLEINSERT4 := 'IFRS_JOURNAL_PARAM';
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
    
    V_PREVMONTH := F_EOMONTH(V_PREVDATE, 1, 'M', 'PREV');
    V_CURRMONTH := F_EOMONTH(V_CURRDATE, 0, 'M', 'NEXT');
    V_LASTYEAR := F_EOMONTH(V_CURRDATE, 1, 'Y', 'PREV');
    V_LASTYEARNEXTMONTH := F_EOMONTH(V_LASTYEAR, 1, 'M', 'NEXT');
    
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
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT2 || ' AS SELECT * FROM IFRS_EIR_ADJUSTMENT WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_ACCT_JRNL_DATA_MTM', '');

    IF V_CURRDATE = V_CURRMONTH 
    THEN 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
            (
                DOWNLOAD_DATE
                ,MASTERID
                ,FACNO
                ,CIFNO
                ,ACCTNO
                ,DATASOURCE
                ,PRDTYPE
                ,PRDCODE
                ,TRXCODE
                ,CCY
                ,JOURNALCODE
                ,STATUS
                ,REVERSE
                ,FLAG_CF
                ,DRCR
                ,GLNO
                ,N_AMOUNT
                ,N_AMOUNT_IDR
                ,SOURCEPROCESS
                ,INTMID
                ,CREATEDDATE
                ,CREATEDBY
                ,BRANCH
                ,JOURNALCODE2
                ,JOURNAL_DESC
                ,NOREF
                ,VALCTR_CODE
                ,GL_INTERNAL_CODE
                ,METHOD
                ,ACCOUNT_TYPE  
                ,CUSTOMER_TYPE   
            ) SELECT 
                A.DOWNLOAD_DATE
                ,A.MASTERID
                ,IMC.FACILITY_NUMBER
                ,IMC.CUSTOMER_NUMBER
                ,A.ACCOUNT_NUMBER
                ,IMC.DATA_SOURCE
                ,IMC.PRODUCT_TYPE
                ,IMC.PRODUCT_CODE
                ,B.TRX_CODE
                ,IMC.CURRENCY
                ,B.JOURNALCODE
                ,''ACT'' STATUS
                ,''N'' REVERSE
                ,B.FLAG_CF
                ,CASE 
                    WHEN A.TOT_ADJUST >= 0  
                    THEN B.DRCR    
                    ELSE CASE WHEN B.DRCR = ''D'' THEN ''C'' ELSE ''D'' END  
                END
                ,B.GLNO
                ,ABS(A.TOT_ADJUST)
                ,ABS(A.TOT_ADJUST * COALESCE(IMC.EXCHANGE_RATE, 1))
                ,''PLMTM'' AS SOURCEPROCESS
                ,NULL
                ,CURRENT_TIMESTAMP
                ,''SP_JOURNAL_DATA2''
                ,IMC.BRANCH_CODE
                ,NULL JOURNALCODE2
                ,B.JOURNAL_DESC
                ,B.JOURNALCODE
                ,B.COSTCENTER || ''-'' || COALESCE(IMC.APPLICATION_NO, '''')
                ,B.GL_INTERNAL_CODE
                ,NULL METHOD
                ,IMC.ACCOUNT_TYPE AS ACCOUNT_TYPE  
                ,IMC.CUSTOMER_TYPE AS CUSTOMER_TYPE 
            FROM ' || V_TABLEINSERT2 || ' A
            JOIN ' || V_TABLEINSERT3 || ' IMC 
                ON A.MASTERID = IMC.MASTERID 
                AND A.DOWNLOAD_DATE = IMC.DOWNLOAD_DATE
            JOIN ' || V_TABLEINSERT4 || ' B
                ON B.JOURNALCODE = ''PLMTM''
                AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME, '''')
            WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRMONTH AS VARCHAR(10)) || '''::DATE  
                AND IMC.IFRS9_CLASS = ''FVTPL'' ';
        EXECUTE (V_STR_QUERY);

        GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
        V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
        V_RETURNROWS := 0;

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
            (
                DOWNLOAD_DATE
                ,MASTERID
                ,FACNO
                ,CIFNO
                ,ACCTNO
                ,DATASOURCE
                ,PRDTYPE
                ,PRDCODE
                ,TRXCODE
                ,CCY
                ,JOURNALCODE
                ,STATUS
                ,REVERSE
                ,FLAG_CF
                ,DRCR
                ,GLNO
                ,N_AMOUNT
                ,N_AMOUNT_IDR
                ,SOURCEPROCESS
                ,INTMID
                ,CREATEDDATE
                ,CREATEDBY
                ,BRANCH
                ,JOURNALCODE2
                ,JOURNAL_DESC
                ,NOREF
                ,VALCTR_CODE
                ,GL_INTERNAL_CODE
                ,METHOD
                ,ACCOUNT_TYPE  
                ,CUSTOMER_TYPE   
            ) SELECT 
                IMC.DOWNLOAD_DATE
                ,A.MASTERID
                ,A.FACNO
                ,A.CIFNO
                ,A.ACCTNO
                ,A.DATASOURCE
                ,A.PRDTYPE
                ,A.PRDCODE
                ,A.TRXCODE
                ,A.CCY
                ,A.JOURNALCODE
                ,A.STATUS
                ,''Y'' REVERSE
                ,A.FLAG_CF
                ,CASE 
                    WHEN A.DRCR = ''D'' 
                    THEN ''C''
                    ELSE ''D''
                END AS DRCR
                ,A.GLNO
                ,A.N_AMOUNT
                ,A.N_AMOUNT_IDR
                ,A.SOURCEPROCESS
                ,A.INTMID
                ,CURRENT_TIMESTAMP
                ,''SP_JOURNAL_DATA_REV''
                ,A.BRANCH
                ,A.JOURNALCODE2
                ,A.JOURNAL_DESC
                ,A.NOREF
                ,A.VALCTR_CODE
                ,A.GL_INTERNAL_CODE
                ,A.METHOD
                ,IMC.ACCOUNT_TYPE AS ACCOUNT_TYPE  
                ,IMC.CUSTOMER_TYPE AS CUSTOMER_TYPE 
            FROM ' || V_TABLEINSERT1 || ' A
            JOIN ' || V_TABLEINSERT3 || ' IMC 
                ON A.MASTERID = IMC.MASTERID
            WHERE IMC.DOWNLOAD_DATE = ''' || CAST(V_CURRMONTH AS VARCHAR(10)) || '''::DATE  
                AND IMC.IFRS9_CLASS = ''FVTPL''
                AND A.DOWNLOAD_DATE = ''' || CAST(V_PREVMONTH AS VARCHAR(10)) || '''::DATE  
                AND A.REVERSE = ''N''  
                AND  JOURNALCODE = ''PLMTM'' ';
        EXECUTE (V_STR_QUERY);

        GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
        V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
        V_RETURNROWS := 0;

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
            WHERE JOURNALCODE = ''OCIMTM''
            AND DOWNLOAD_DATE = ''' || CAST(V_CURRMONTH AS VARCHAR(10)) || '''::DATE 
        ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
            (
                DOWNLOAD_DATE
                ,MASTERID
                ,FACNO
                ,CIFNO
                ,ACCTNO
                ,DATASOURCE
                ,PRDTYPE
                ,PRDCODE
                ,TRXCODE
                ,CCY
                ,JOURNALCODE
                ,STATUS
                ,REVERSE
                ,FLAG_CF
                ,DRCR
                ,GLNO
                ,N_AMOUNT
                ,N_AMOUNT_IDR
                ,SOURCEPROCESS
                ,INTMID
                ,CREATEDDATE
                ,CREATEDBY
                ,BRANCH
                ,JOURNALCODE2
                ,JOURNAL_DESC
                ,NOREF
                ,VALCTR_CODE
                ,GL_INTERNAL_CODE
                ,METHOD
                ,ACCOUNT_TYPE  
                ,CUSTOMER_TYPE   
            ) SELECT 
                A.DOWNLOAD_DATE
                ,A.MASTERID
                ,IMC.FACILITY_NUMBER
                ,IMC.CUSTOMER_NUMBER
                ,A.ACCOUNT_NUMBER
                ,IMC.DATA_SOURCE
                ,IMC.PRODUCT_TYPE
                ,IMC.PRODUCT_CODE
                ,B.TRX_CODE
                ,IMC.CURRENCY
                ,B.JOURNALCODE
                ,''ACT'' STATUS
                ,''N'' REVERSE
                ,B.FLAG_CF
                ,CASE 
                    WHEN A.TOT_ADJUST >= 0  
                    THEN B.DRCR    
                    ELSE CASE WHEN B.DRCR = ''D'' THEN ''C'' ELSE ''D'' END  
                END
                ,B.GLNO
                ,ABS(A.TOT_ADJUST)
                ,ABS(A.TOT_ADJUST * COALESCE(IMC.EXCHANGE_RATE, 1))
                ,''OCIMTM'' AS SOURCEPROCESS
                ,NULL
                ,CURRENT_TIMESTAMP
                ,''SP_JOURNAL_DATA2''
                ,IMC.BRANCH_CODE
                ,NULL JOURNALCODE2
                ,B.JOURNAL_DESC
                ,B.JOURNALCODE
                ,B.COSTCENTER || ''-'' || COALESCE(IMC.APPLICATION_NO, '''')
                ,B.GL_INTERNAL_CODE
                ,NULL METHOD
                ,IMC.ACCOUNT_TYPE AS ACCOUNT_TYPE  
                ,IMC.CUSTOMER_TYPE AS CUSTOMER_TYPE 
            FROM ' || V_TABLEINSERT2 || ' A
            JOIN ' || V_TABLEINSERT3 || ' IMC 
                ON A.MASTERID = IMC.MASTERID 
                AND A.DOWNLOAD_DATE = IMC.DOWNLOAD_DATE
            JOIN ' || V_TABLEINSERT4 || ' B
                ON B.JOURNALCODE = ''OCIMTM''
                AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME, '''')
            WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRMONTH AS VARCHAR(10)) || '''::DATE  
                AND IMC.IFRS9_CLASS = ''FVOCI'' ';
        EXECUTE (V_STR_QUERY);

        GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
        V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
        V_RETURNROWS := 0;

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
            (
                DOWNLOAD_DATE
                ,MASTERID
                ,FACNO
                ,CIFNO
                ,ACCTNO
                ,DATASOURCE
                ,PRDTYPE
                ,PRDCODE
                ,TRXCODE
                ,CCY
                ,JOURNALCODE
                ,STATUS
                ,REVERSE
                ,FLAG_CF
                ,DRCR
                ,GLNO
                ,N_AMOUNT
                ,N_AMOUNT_IDR
                ,SOURCEPROCESS
                ,INTMID
                ,CREATEDDATE
                ,CREATEDBY
                ,BRANCH
                ,JOURNALCODE2
                ,JOURNAL_DESC
                ,NOREF
                ,VALCTR_CODE
                ,GL_INTERNAL_CODE
                ,METHOD
                ,ACCOUNT_TYPE  
                ,CUSTOMER_TYPE   
            ) SELECT 
                IMC.DOWNLOAD_DATE
                ,A.MASTERID
                ,A.FACNO
                ,A.CIFNO
                ,A.ACCTNO
                ,A.DATASOURCE
                ,A.PRDTYPE
                ,A.PRDCODE
                ,A.TRXCODE
                ,A.CCY
                ,A.JOURNALCODE
                ,A.STATUS
                ,''Y'' REVERSE
                ,A.FLAG_CF
                ,CASE 
                    WHEN A.DRCR = ''D'' 
                    THEN ''C''
                    ELSE ''D''
                END AS DRCR
                ,A.GLNO
                ,A.N_AMOUNT
                ,A.N_AMOUNT_IDR
                ,A.SOURCEPROCESS
                ,A.INTMID
                ,CURRENT_TIMESTAMP
                ,''SP_JOURNAL_DATA_REV''
                ,A.BRANCH
                ,A.JOURNALCODE2
                ,A.JOURNAL_DESC
                ,A.NOREF
                ,A.VALCTR_CODE
                ,A.GL_INTERNAL_CODE
                ,A.METHOD
                ,IMC.ACCOUNT_TYPE AS ACCOUNT_TYPE  
                ,IMC.CUSTOMER_TYPE AS CUSTOMER_TYPE 
            FROM ' || V_TABLEINSERT1 || ' A
            JOIN ' || V_TABLEINSERT3 || ' IMC 
                ON A.MASTERID = IMC.MASTERID
            WHERE IMC.DOWNLOAD_DATE = ''' || CAST(V_CURRMONTH AS VARCHAR(10)) || '''::DATE  
                AND IMC.IFRS9_CLASS = ''FVOCI''
                AND A.DOWNLOAD_DATE = ''' || CAST(V_PREVMONTH AS VARCHAR(10)) || '''::DATE  
                AND A.REVERSE = ''N''  
                AND  JOURNALCODE = ''FVOCI'' ';
        EXECUTE (V_STR_QUERY);

        GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
        V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
        V_RETURNROWS := 0;
    END IF;

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_ACCT_JRNL_DATA_MTM', '');

    RAISE NOTICE 'SP_IFRS_ACCT_JRNL_DATA_MTM | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT1;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_ACCT_JRNL_DATA_MTM';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT1 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$
