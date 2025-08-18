---- DROP PROCEDURE SP_IFRS_LBM_ACCT_EIR_ECF_MAIN;

CREATE OR REPLACE PROCEDURE SP_IFRS_LBM_ACCT_EIR_ECF_MAIN(
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
    V_TABLEINSERT7 VARCHAR(100);
    V_TABLEINSERT8 VARCHAR(100);
    V_TABLEINSERT9 VARCHAR(100);
    V_TABLEINSERT10 VARCHAR(100);
    V_TABLEINSERT11 VARCHAR(100);
    V_TABLEINSERT12 VARCHAR(100);
    V_TABLEINSERT13 VARCHAR(100);
    V_TABLEINSERT14 VARCHAR(100);
    V_TABLEINSERT15 VARCHAR(100);
    V_TABLEINSERT16 VARCHAR(100);
    V_TABLEINSERT17 VARCHAR(100);
    V_TABLEINSERT18 VARCHAR(100);
    V_TABLEINSERT19 VARCHAR(100);
    V_TABLEINSERT20 VARCHAR(100);
    V_TABLEINSERT21 VARCHAR(100);
    V_TABLEINSERT22 VARCHAR(100);
    V_TABLEINSERT23 VARCHAR(100);
    V_TABLEINSERT24 VARCHAR(100);
    V_TABLEINSERT25 VARCHAR(100);
    V_TABLEINSERT26 VARCHAR(100);
    V_TABLEINSERT27 VARCHAR(100);

    ---- VARIABLE PROCESS
    V_ROUND INT;
    V_FUNCROUND INT;
    V_MIN_ID INT;
    V_MAX_ID INT;
    V_ID2 INT;
    V_X INT;
    V_X_INC INT;
    V_PARAM_DISABLE_ACCRU_PREV INT;
    
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
        V_TABLEINSERT1 := 'IFRS_ACCT_COST_FEE_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_LBM_ACCT_EIR_ACCRU_PREV_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_LBM_ACCT_EIR_ACF_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_LBM_ACCT_EIR_CF_ECF_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_LBM_ACCT_EIR_CF_ECF1_' || P_RUNID || '';
        V_TABLEINSERT6 := 'IFRS_LBM_ACCT_EIR_COST_FEE_ECF_' || P_RUNID || '';
        V_TABLEINSERT7 := 'IFRS_LBM_ACCT_EIR_COST_FEE_PREV_' || P_RUNID || '';
        V_TABLEINSERT8 := 'IFRS_LBM_ACCT_EIR_ECF_' || P_RUNID || '';
        V_TABLEINSERT9 := 'IFRS_LBM_ACCT_EIR_ECF_NOCF_' || P_RUNID || '';
        V_TABLEINSERT10 := 'IFRS_LBM_ACCT_EIR_FAILED_GS_' || P_RUNID || '';
        V_TABLEINSERT11 := 'IFRS_LBM_ACCT_EIR_FAILED_GS3_' || P_RUNID || '';
        V_TABLEINSERT12 := 'IFRS_LBM_ACCT_EIR_FAILED_GS4_' || P_RUNID || '';
        V_TABLEINSERT13 := 'IFRS_LBM_ACCT_EIR_GAIN_LOSS_' || P_RUNID || '';
        V_TABLEINSERT14 := 'IFRS_LBM_ACCT_EIR_GS_RESULT_' || P_RUNID || '';
        V_TABLEINSERT15 := 'IFRS_LBM_ACCT_EIR_GS_RESULT3_' || P_RUNID || '';
        V_TABLEINSERT16 := 'IFRS_LBM_ACCT_EIR_GS_RESULT4_' || P_RUNID || '';
        V_TABLEINSERT17 := 'IFRS_LBM_ACCT_EIR_PAYM_' || P_RUNID || '';
        V_TABLEINSERT18 := 'IFRS_LBM_ACCT_EIR_STOP_REV_' || P_RUNID || '';
        V_TABLEINSERT19 := 'IFRS_ACCT_JOURNAL_INTM_' || P_RUNID || '';
        V_TABLEINSERT20 := 'IFRS_ACCT_SWITCH_' || P_RUNID || '';
        V_TABLEINSERT21 := 'IFRS_LBM_EVENT_CHANGES_' || P_RUNID || '';
        V_TABLEINSERT22 := 'IFRS_LBM_GS_MASTERID_' || P_RUNID || '';
        V_TABLEINSERT23 := 'IFRS_IMA_AMORT_CURR_' || P_RUNID || '';
        V_TABLEINSERT24 := 'IFRS_LBM_PAYM_CORE_' || P_RUNID || '';
        V_TABLEINSERT25 := 'IFRS_LBM_PAYM_CORE_SRC_' || P_RUNID || '';
        V_TABLEINSERT26 := 'IFRS_PRODUCT_PARAM_' || P_RUNID || '';
        V_TABLEINSERT27 := 'IFRS_TRANSACTION_DAILY_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_ACCT_COST_FEE';
        V_TABLEINSERT2 := 'IFRS_LBM_ACCT_EIR_ACCRU_PREV';
        V_TABLEINSERT3 := 'IFRS_LBM_ACCT_EIR_ACF';
        V_TABLEINSERT4 := 'IFRS_LBM_ACCT_EIR_CF_ECF';
        V_TABLEINSERT5 := 'IFRS_LBM_ACCT_EIR_CF_ECF1';
        V_TABLEINSERT6 := 'IFRS_LBM_ACCT_EIR_COST_FEE_ECF';
        V_TABLEINSERT7 := 'IFRS_LBM_ACCT_EIR_COST_FEE_PREV';
        V_TABLEINSERT8 := 'IFRS_LBM_ACCT_EIR_ECF';
        V_TABLEINSERT9 := 'IFRS_LBM_ACCT_EIR_ECF_NOCF';
        V_TABLEINSERT10 := 'IFRS_LBM_ACCT_EIR_FAILED_GS';
        V_TABLEINSERT11 := 'IFRS_LBM_ACCT_EIR_FAILED_GS3';
        V_TABLEINSERT12 := 'IFRS_LBM_ACCT_EIR_FAILED_GS4';
        V_TABLEINSERT13 := 'IFRS_LBM_ACCT_EIR_GAIN_LOSS';
        V_TABLEINSERT14 := 'IFRS_LBM_ACCT_EIR_GS_RESULT';
        V_TABLEINSERT15 := 'IFRS_LBM_ACCT_EIR_GS_RESULT3';
        V_TABLEINSERT16 := 'IFRS_LBM_ACCT_EIR_GS_RESULT4';
        V_TABLEINSERT17 := 'IFRS_LBM_ACCT_EIR_PAYM';
        V_TABLEINSERT18 := 'IFRS_LBM_ACCT_EIR_STOP_REV';
        V_TABLEINSERT19 := 'IFRS_ACCT_JOURNAL_INTM';
        V_TABLEINSERT20 := 'IFRS_ACCT_SWITCH';
        V_TABLEINSERT21 := 'IFRS_LBM_EVENT_CHANGES';
        V_TABLEINSERT22 := 'IFRS_LBM_GS_MASTERID';
        V_TABLEINSERT23 := 'IFRS_IMA_AMORT_CURR';
        V_TABLEINSERT24 := 'IFRS_LBM_PAYM_CORE';
        V_TABLEINSERT25 := 'IFRS_LBM_PAYM_CORE_SRC';
        V_TABLEINSERT26 := 'IFRS_PRODUCT_PARAM';
        V_TABLEINSERT27 := 'IFRS_TRANSACTION_DAILY';
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

    V_MIN_ID := 0;
    V_MAX_ID := 0;
    V_ID2 := 0;
    V_X := 0;
    V_X_INC := 0;
    V_PARAM_DISABLE_ACCRU_PREV := 0;

    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT4 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT4 || ' AS SELECT * FROM IFRS_LBM_ACCT_EIR_CF_ECF WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT5 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT5 || ' AS SELECT * FROM IFRS_LBM_ACCT_EIR_CF_ECF1 WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT9 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT9 || ' AS SELECT * FROM IFRS_LBM_ACCT_EIR_ECF_NOCF WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT10 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT10 || ' AS SELECT * FROM IFRS_LBM_ACCT_EIR_FAILED_GS WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT11 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT11 || ' AS SELECT * FROM IFRS_LBM_ACCT_EIR_FAILED_GS3 WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT12 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT12 || ' AS SELECT * FROM IFRS_LBM_ACCT_EIR_FAILED_GS4 WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT13 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT13 || ' AS SELECT * FROM IFRS_LBM_ACCT_EIR_GAIN_LOSS WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT14 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT14 || ' AS SELECT * FROM IFRS_LBM_ACCT_EIR_GS_RESULT WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT15 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT15 || ' AS SELECT * FROM IFRS_LBM_ACCT_EIR_GS_RESULT3 WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT16 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT16 || ' AS SELECT * FROM IFRS_LBM_ACCT_EIR_GS_RESULT4 WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT17 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT17 || ' AS SELECT * FROM IFRS_LBM_ACCT_EIR_PAYM WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT18 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT18 || ' AS SELECT * FROM IFRS_LBM_ACCT_EIR_STOP_REV WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT21 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT21 || ' AS SELECT * FROM IFRS_LBM_EVENT_CHANGES WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT22 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT22 || ' AS SELECT * FROM IFRS_LBM_GS_MASTERID WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT23 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT23 || ' AS SELECT * FROM IFRS_IMA_AMORT_CURR WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT24 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT24 || ' AS SELECT * FROM IFRS_LBM_PAYM_CORE WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_LBM_ACCT_EIR_ECF_MAIN', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT2 || ' 
        WHERE DOWNLOAD_DATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
        AND SRCPROCESS = ''ECF'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET STATUS = ''ACT'' 
        WHERE STATUS = ''PNL'' 
        AND CREATEDBY = ''EIRECF1'' 
        AND DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT7 || ' A 
        SET STATUS = ''ACT'' 
        WHERE STATUS = ''PNL'' 
        AND CREATEDBY = ''EIRECF2'' 
        AND DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT7 || ' A 
        SET STATUS = ''ACT'' 
        WHERE STATUS = ''PNL'' 
        AND CREATEDBY = ''EIRECF1'' 
        AND DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T7' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T7' || ' 
        (                
            MID                
            ,STAFFLOAN                
            ,PKID                
            ,NPVRATE                
        ) SELECT 
            A.MASTERID 
            ,CASE                 
                WHEN COALESCE(STAFF_LOAN_FLAG, ''N'') IN (''N'', '''')                
                THEN 0
                ELSE 1                
            END                
            ,A.ID                
            ,CASE                 
                WHEN STAFF_LOAN_FLAG = ''Y''                
                THEN COALESCE(P.MARKET_RATE, 0)                
                ELSE 0                
            END MARKET_RATE                
        FROM ' || V_TABLEINSERT23 || ' A 
        LEFT JOIN ' || V_TABLEINSERT26 ||' P 
            ON P.DATA_SOURCE = A.DATA_SOURCE                
            AND P.PRD_TYPE = A.PRODUCT_TYPE                
            AND P.PRD_CODE = A.PRODUCT_CODE                
            AND (                
                P.CCY = A.CURRENCY                
                OR COALESCE(P.CCY, ''ALL'') = ''ALL''                
            ) 
        WHERE A.EIR_STATUS = ''Y''                
            AND A.AMORT_TYPE <> ''SL'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT4 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMP_TODAYREV ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_TODAYREV' || ' AS 
        SELECT DISTINCT MASTERID 
        FROM ' || V_TABLEINSERT1 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND FLAG_REVERSE = ''Y'' 
        AND CF_ID_REV IS NOT NULL ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || ' 
        (                
            MASTERID                
            ,FEE_AMT                
            ,COST_AMT                
            ,FEE_AMT_ACRU                
            ,COST_AMT_ACRU                
            ,STAFFLOAN                
            ,PKID                
            ,NPV_RATE                
            ,GAIN_LOSS_CALC --20180226 SET N                        
        ) SELECT 
            A.MID                
            ,SUM(COALESCE(CASE                 
                WHEN C.FLAG_CF = ''F''                
                THEN CASE                 
                    WHEN C.FLAG_REVERSE = ''Y''                
                    THEN - 1 * C.AMOUNT                
                    ELSE C.AMOUNT              
                END                
                ELSE 0                
            END, 0))                
            ,SUM(COALESCE(CASE                 
                WHEN C.FLAG_CF = ''C''                
                THEN CASE                 
                    WHEN C.FLAG_REVERSE = ''Y''                
                    THEN - 1 * C.AMOUNT                
                    ELSE C.AMOUNT                
                END            
                ELSE 0                
            END, 0))                
            ,0                
            ,0                
            ,A.STAFFLOAN                
            ,A.PKID                
            ,A.NPVRATE                
            ,''N'' --20180226                        
        FROM ' || 'TMP_T7' || ' A                
        LEFT JOIN ' || V_TABLEINSERT1 || ' C 
            ON C.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
            AND C.MASTERID = A.MID                
            AND C.STATUS = ''ACT''              
            AND C.METHOD = ''EIR''                
            --20180108 EXCLUDE CF REVERSAL AND ITS PAIR                          
            AND C.CF_ID NOT IN (                
                SELECT CF_ID                
                FROM ' || V_TABLEINSERT1 || ' 
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
                AND FLAG_REVERSE = ''Y''                
                AND CF_ID_REV IS NOT NULL                
                            
                UNION ALL                
                            
                SELECT CF_ID_REV                
                FROM ' || V_TABLEINSERT1 || ' 
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
                AND FLAG_REVERSE = ''Y''                
                AND CF_ID_REV IS NOT NULL                
            )                
            --WHERE C.METHOD = ''EIR''                         
        GROUP BY 
            A.MID                
            ,A.STAFFLOAN                
            ,A.PKID                
            ,A.NPVRATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT4 || ' A 
        SET 
            NEW_FEE_AMT = COALESCE(FEE_AMT, 0) 
            ,NEW_COST_AMT = COALESCE(COST_AMT, 0) 
            ,NEW_TOTAL_AMT = COALESCE(NEW_FEE_AMT, 0) + COALESCE(NEW_COST_AMT, 0) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T10' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T10' || ' 
        (                
            MASTERID                
            ,FEE_AMT                
            ,COST_AMT                
        ) SELECT 
            B.MASTERID                
            ,SUM(COALESCE(CASE                 
                WHEN B.FLAG_CF = ''F''                
                THEN CASE                 
                    WHEN B.FLAG_REVERSE = ''Y''                
                    THEN - 1 * CASE                 
                        WHEN CFREV.MASTERID IS NULL                
                        THEN B.AMOUNT                
                        ELSE B.AMOUNT                
                    END                
                    ELSE CASE                 
                        WHEN CFREV.MASTERID IS NULL                
                        THEN B.AMOUNT                
                        ELSE B.AMOUNT                
                    END                
                END                
                ELSE 0                
            END, 0)) AS FEE_AMT                
            ,SUM(COALESCE(CASE                 
                WHEN B.FLAG_CF = ''C''                
                THEN CASE                 
                    WHEN B.FLAG_REVERSE = ''Y''                
                    THEN - 1 * CASE                 
                        WHEN CFREV.MASTERID IS NULL                
                        THEN B.AMOUNT                
                        ELSE B.AMOUNT                
                    END                
                    ELSE CASE                 
                        WHEN CFREV.MASTERID IS NULL                
                        THEN B.AMOUNT                
                        ELSE B.AMOUNT                
                    END                
                END                
                ELSE 0                
            END, 0)) AS COST_AMT                
        FROM ' || V_TABLEINSERT7 || ' B 
        JOIN ' || 'VW_LBM_LAST_EIR_CF_PREV' || ' X 
            ON X.MASTERID = B.MASTERID                
            AND X.DOWNLOAD_DATE = B.DOWNLOAD_DATE                
            AND B.SEQ = X.SEQ                
            --20160407 EIR STOP REV                          
        LEFT JOIN (                
            SELECT DISTINCT MASTERID                
            FROM ' || V_TABLEINSERT18 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
        ) A 
            ON A.MASTERID = B.MASTERID                
            --20180116 RESONA REQ                          
        LEFT JOIN ' || 'TMP_TODAYREV' || ' CFREV 
            ON CFREV.MASTERID = B.MASTERID                
        WHERE B.DOWNLOAD_DATE IN (                
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
            ,''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE                 
        )                
            AND B.STATUS = ''ACT''                
            AND A.MASTERID IS NULL                
            --20180116 EXCLUDE CF REVERSAL AND ITS PAIR                          
            AND B.CF_ID NOT IN (                
                SELECT CF_ID                
                FROM ' || V_TABLEINSERT1 || ' 
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
                AND FLAG_REVERSE = ''Y''                
                AND CF_ID_REV IS NOT NULL                
                                
                UNION ALL                
                                
                SELECT CF_ID_REV                
                FROM ' || V_TABLEINSERT1 || ' 
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
                AND FLAG_REVERSE = ''Y''                
                AND CF_ID_REV IS NOT NULL                
            )                
            --20180426 EXCLUDE PREVDATE IF NOT FROM SP ACF ACCRU                  
            AND CASE                 
                WHEN B.DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE                 
                    AND B.SEQ <> ''2''                
                THEN 0                
                ELSE 1                
            END = 1                
        GROUP BY B.MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT4 || ' A 
        SET 
            FEE_AMT = A.FEE_AMT + B.FEE_AMT                
            ,COST_AMT = A.COST_AMT + B.COST_AMT                
        FROM ' || 'TMP_T10' || ' B                
        WHERE B.MASTERID = A.MASTERID ';
    EXECUTE (V_STR_QUERY);

    IF V_PARAM_DISABLE_ACCRU_PREV != 0 
    THEN 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T1' || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T1' || ' 
            (                
                MASTERID                
                ,ACCTNO                
            ) SELECT DISTINCT 
                MASTERID                
                ,ACCTNO                
            FROM ' || V_TABLEINSERT3 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                
            AND DO_AMORT = ''Y'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T3' || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T3' || ' (MASTERID) 
            SELECT MASTERID                
            FROM ' || V_TABLEINSERT4 || ' 
            WHERE MASTERID NOT IN (                
                SELECT MASTERID                
                FROM ' || 'TMP_T1' || ' 
            ) ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_P1' || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_P1' || ' (ID) 
            SELECT MAX(ID) AS ID                
            FROM ' || V_TABLEINSERT3 || ' 
            WHERE MASTERID IN (                
                SELECT MASTERID                
                FROM ' || 'TMP_T3' || ' 
            )                
            AND DO_AMORT = ''N''                
            AND DOWNLOAD_DATE < ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND DOWNLOAD_DATE >= ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE 
            GROUP BY MASTERID ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT4 || ' A 
            SET 
                FEE_AMT = FEE_AMT - B.N_ACCRU_FEE                
                ,COST_AMT = COST_AMT - B.N_ACCRU_COST                
            FROM (                
                SELECT *                
                FROM ' || V_TABLEINSERT3 || ' 
                WHERE ID IN (                
                    SELECT ID                
                    FROM ' || 'TMP_P1' || ' 
                )                
            ) B                
            WHERE (B.MASTERID = A.MASTERID)                
            --20160407 EIR STOP REV                          
            AND A.MASTERID NOT IN (                
                SELECT MASTERID                
                FROM ' || V_TABLEINSERT18 || ' 
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            )           
            ----ADD 20180924              
            AND A.MASTERID NOT IN (          
                SELECT DISTINCT MASTERID 
                FROM ' || V_TABLEINSERT20 || ' 
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ) ';
        EXECUTE (V_STR_QUERY);

        /* 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT4 || ' A 
            SET FEE_AMT = FEE_AMT + B.N_AMOUNT                
            FROM (                
                SELECT MASTERID, SUM(N_AMOUNT)  N_AMOUNT                 
                FROM ' || V_TABLEINSERT19 || ' 
                WHERE CF_ID IN (                
                    SELECT CF_ID_REV                
                    FROM IFRS_ACCT_COST_FEE                
                    WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
                    AND FLAG_REVERSE = ''Y''                
                    AND CF_ID_REV IS NOT NULL                
                )                
                    AND DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE                 
                    AND [REVERSE] = ''N''                
                    AND JOURNALCODE = ''ACCRU''           
                    AND FLAG_CF = ''F''          
                GROUP BY MASTERID                
            ) B                
            WHERE (B.MASTERID = A.MASTERID)                
            --20180404 ADD FILTER                      
            AND A.MASTERID IN (                
                SELECT MASTERID                
                FROM ' || 'TMP_T3' || '
            )                
            --20160407 SL STOP REV                          
            AND A.MASTERID NOT IN (                
                SELECT MASTERID                
                FROM ' || V_TABLEINSERT18 || ' 
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
            ) ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT4 || ' A 
            SET COST_AMT = COST_AMT + B.N_AMOUNT                
            FROM (                
                SELECT MASTERID, SUM(N_AMOUNT)  N_AMOUNT                 
                FROM ' || V_TABLEINSERT19 || '
                WHERE CF_ID IN (                
                    SELECT CF_ID_REV                
                    FROM ' || V_TABLEINSERT1 || ' 
                    WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
                    AND FLAG_REVERSE = ''Y''                
                    AND CF_ID_REV IS NOT NULL                
                )                
                    AND DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE        
                    AND REVERSE = ''N''                
                    AND JOURNALCODE = ''ACCRU''                
                    AND FLAG_CF = ''C''           
                GROUP BY MASTERID               
            ) B                
            WHERE (B.MASTERID = IFRS_LBM_ACCT_EIR_CF_ECF.MASTERID)                
            --20180404 ADD FILTER                      
            AND IFRS_LBM_ACCT_EIR_CF_ECF.MASTERID IN (                
                SELECT MASTERID                
                FROM ' || 'TMP_T3' || ' 
            )                
            --20160407 SL STOP REV                          
            AND IFRS_LBM_ACCT_EIR_CF_ECF.MASTERID NOT IN (                
                SELECT MASTERID                
                FROM ' || V_TABLEINSERT18 || ' 
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
            ) ';
        EXECUTE (V_STR_QUERY);
        */
    END IF;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T10' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T10' || ' 
        (                
            MASTERID                
            ,FEE_AMT                
            ,COST_AMT                
        ) SELECT 
            B.MASTERID                
            ,SUM(COALESCE(CASE                 
                WHEN B.FLAG_CF = ''F''                
                THEN CASE                 
                    WHEN B.FLAG_REVERSE = ''Y''                
                    THEN - 1 * B.AMOUNT                
                    ELSE B.AMOUNT                
                END                
                ELSE 0                
            END, 0)) AS FEE_AMT                
            ,SUM(COALESCE(CASE                 
                WHEN B.FLAG_CF = ''C''                
                THEN CASE                 
                    WHEN B.FLAG_REVERSE = ''Y''                
                    THEN - 1 * B.AMOUNT                
                    ELSE B.AMOUNT                
                END                
                ELSE 0                
            END, 0)) AS COST_AMT                
        FROM ' || V_TABLEINSERT2 || ' B 
        WHERE B.STATUS = ''ACT''                
        --20180116 EXCLUDE CF REV AND ITS PAIR                          
        AND B.CF_ID NOT IN (                
            SELECT CF_ID                
            FROM ' || V_TABLEINSERT1 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND FLAG_REVERSE = ''Y''            
            AND CF_ID_REV IS NOT NULL                
                            
            UNION ALL                
                            
            SELECT CF_ID_REV                
            FROM ' || V_TABLEINSERT1 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND FLAG_REVERSE = ''Y''                
            AND CF_ID_REV IS NOT NULL                
        )                
        GROUP BY B.MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT4 || ' A 
        SET 
            FEE_AMT_ACRU = B.FEE_AMT                
            ,COST_AMT_ACRU = B.COST_AMT                
        FROM ' || 'TMP_T10' || ' B                
        WHERE (B.MASTERID = A.MASTERID) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT4 || ' A 
        SET 
            TOTAL_AMT = ROUND(FEE_AMT + COST_AMT, 0)                
            ,TOTAL_AMT_ACRU = ROUND(FEE_AMT + COST_AMT + FEE_AMT_ACRU + COST_AMT_ACRU, 0) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T13' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T13' || ' 
        (                
            MASTERID                
            ,N_EFF_INT_RATE                
            ,ENDAMORTDATE                
        ) SELECT 
            B.MASTERID                
            ,B.N_EFF_INT_RATE                
            ,B.ENDAMORTDATE                
        FROM ' || V_TABLEINSERT8 || ' B                
        WHERE B.AMORTSTOPDATE IS NULL  
        AND B.PMT_DATE = B.PREV_PMT_DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT4 || ' A 
        SET 
            PREV_EIR = N_EFF_INT_RATE                
            ,PREV_ENDAMORTDATE = B.ENDAMORTDATE                
        FROM ' || 'TMP_T13' || ' B                 
        WHERE (B.MASTERID = A.MASTERID) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT4 || ' A 
        SET GAIN_LOSS_CALC = ''Y''
        WHERE MASTERID IN (                
            SELECT MASTERID                
            FROM ' || V_TABLEINSERT21 || '
            WHERE EVENT_ID = 6                
            AND EFFECTIVE_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        )                
        AND MASTERID NOT IN (                
        SELECT MASTERID                
        FROM ' || V_TABLEINSERT21 || '
        WHERE EVENT_ID IN (0,1,2,3)             
            AND EFFECTIVE_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT4 || ' A 
        SET GAIN_LOSS_CALC = ''N''                
        WHERE PREV_EIR IS NULL                
        AND GAIN_LOSS_CALC = ''Y'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET 
            STATUS = ''PNL''                
            ,CREATEDBY = ''EIRECF1''                
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND MASTERID IN (                
            SELECT MASTERID                
            FROM ' || V_TABLEINSERT4 || ' 
            WHERE TOTAL_AMT = 0                
            OR TOTAL_AMT_ACRU = 0                
        )                
        AND STATUS = ''ACT''                
        --20180116 EXCLUDE CF REV AND ITS PAIR, WILL BE HANDLED BY ACF_ABN                          
        AND CF_ID NOT IN (                
            SELECT CF_ID                
            FROM ' || V_TABLEINSERT1 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND FLAG_REVERSE = ''Y''                
            AND CF_ID_REV IS NOT NULL                
                            
            UNION ALL                
                            
            SELECT CF_ID_REV                
            FROM ' || V_TABLEINSERT1 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND FLAG_REVERSE = ''Y''                
            AND CF_ID_REV IS NOT NULL                
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T11' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T11' || ' 
        (                
            MASTERID                
            ,DOWNLOAD_DATE                
            ,SEQ                
            ,CURRDATE            
        ) SELECT 
            B.MASTERID                
            ,B.DOWNLOAD_DATE                
            ,B.SEQ                
            ,P.CURRDATE                
        FROM ' || 'VW_LBM_LAST_EIR_CF_PREV' || ' B 
        CROSS JOIN IFRS_PRC_DATE_AMORT P 
        WHERE B.MASTERID IN (                
            SELECT MASTERID                
            FROM ' || V_TABLEINSERT4 || ' 
            WHERE TOTAL_AMT = 0                
            OR TOTAL_AMT_ACRU = 0                
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT7 || ' A 
        SET 
            STATUS = CASE                 
                WHEN STATUS = ''ACT''                
                THEN ''PNL''                
                ELSE STATUS                
            END                
            ,CREATEDBY = ''EIRECF2''                
        FROM ' || 'TMP_T11' || ' B 
        WHERE A.DOWNLOAD_DATE = B.CURRDATE                
        AND A.MASTERID = B.MASTERID                
        AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE                
        AND A.SEQ = B.SEQ                
        --20180116 EXCLUDE CF REV AND ITS PAIR, WILL BE HANDLED BY SP ACF ABN                          
        AND A.CF_ID NOT IN (                
            SELECT CF_ID                
            FROM ' || V_TABLEINSERT1 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE              
            AND FLAG_REVERSE = ''Y''                
            AND CF_ID_REV IS NOT NULL                
                            
            UNION ALL                
                            
            SELECT CF_ID_REV                
            FROM ' || V_TABLEINSERT1 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE              
            AND FLAG_REVERSE = ''Y''                
            AND CF_ID_REV IS NOT NULL                
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T12' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T12' || ' 
        (                
            MASTERID                
            ,DOWNLOAD_DATE                
            ,SEQ                
            ,PREVDATE                
        ) SELECT 
            B.MASTERID                
            ,B.DOWNLOAD_DATE                
            ,B.SEQ                
            ,P.PREVDATE                
        FROM ' || 'VW_LBM_LAST_EIR_CF_PREV' || ' B 
        CROSS JOIN IFRS_PRC_DATE_AMORT P 
        WHERE B.MASTERID IN (                
            SELECT MASTERID                
            FROM ' || V_TABLEINSERT4 || '
            WHERE TOTAL_AMT = 0                
            OR TOTAL_AMT_ACRU = 0                
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT7 || ' A 
        SET 
            STATUS = CASE                 
                WHEN STATUS = ''ACT''                
                THEN ''PNL2''                
                ELSE STATUS                
            END                
            ,CREATEDBY = ''EIRECF2''                
        FROM ' || 'TMP_T12' || ' B 
        WHERE A.DOWNLOAD_DATE = B.PREVDATE                
        AND A.MASTERID = B.MASTERID                
        AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE                
        AND A.SEQ = B.SEQ                
        --20180116 EXCLUDE CF REV AND ITS PAIR, WILL BE HANDLED BY SP ACF ABN                          
        AND A.CF_ID NOT IN (                
            SELECT CF_ID                
            FROM ' || V_TABLEINSERT1 || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE          
            AND FLAG_REVERSE = ''Y''                
            AND CF_ID_REV IS NOT NULL                
                            
            UNION ALL                
                            
            SELECT CF_ID_REV                
            FROM ' || V_TABLEINSERT1 || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE          
            AND FLAG_REVERSE = ''Y''                
            AND CF_ID_REV IS NOT NULL                
        )                
        --20180426 EXCLUDE PREVDATE IF NOT FROM SP ACF ACCRU                  
        AND CASE                 
            WHEN A.DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE                 
                AND A.SEQ <> ''2''                
            THEN 0                
            ELSE 1                
        END = 1 ';
    EXECUTE (V_STR_QUERY);

    IF V_PARAM_DISABLE_ACCRU_PREV != 0 
    THEN 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_P1' || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_P1' || ' (ID)
            SELECT MAX(ID) AS ID                
            FROM ' || V_TABLEINSERT3 || '
            WHERE MASTERID IN (                
                SELECT MASTERID                
                FROM ' || 'TMP_T3' || ' 
            )                
            AND DO_AMORT = ''N'' 
            AND DOWNLOAD_DATE < ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
            AND DOWNLOAD_DATE >= ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE                 
            -- ADD FILTER PNL ED ACCTNO                          
            AND MASTERID IN (                
                SELECT MASTERID                
                FROM ' || V_TABLEINSERT4 || '
                WHERE TOTAL_AMT = 0                
                OR TOTAL_AMT_ACRU = 0                
            )                
            GROUP BY MASTERID ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_TF' || '';
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
                        THEN - 1 * A.AMOUNT                
                        ELSE A.AMOUNT                
                    END AS N_AMOUNT                
                    ,A.ECFDATE DOWNLOAD_DATE              
                    ,A.MASTERID                
                FROM ' || V_TABLEINSERT6 || ' A 
                WHERE A.MASTERID IN (                
                    SELECT MASTERID                
                    FROM ' || 'TMP_T3' || ' 
                )                
                    AND A.STATUS = ''ACT'' 
                    AND A.FLAG_CF = ''F'' 
                    AND A.METHOD = ''EIR'' 
            ) A                
            GROUP BY 
                A.DOWNLOAD_DATE                
                ,A.MASTERID ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_TC' || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_TC' || ' 
            (                
                SUM_AMT                
                ,DOWNLOAD_DATE                
                ,MASTERID                
            )           
            SELECT 
                SUM(A.N_AMOUNT) AS SUM_AMT                
                ,A.DOWNLOAD_DATE                
                ,A.MASTERID                
            FROM (                
                SELECT 
                    CASE                 
                        WHEN A.FLAG_REVERSE = ''Y''
                        THEN - 1 * A.AMOUNT                
                        ELSE A.AMOUNT                
                    END AS N_AMOUNT                
                    ,A.ECFDATE DOWNLOAD_DATE                
                    ,A.MASTERID                
                FROM ' || V_TABLEINSERT6 || ' A 
                WHERE A.MASTERID IN (                
                    SELECT MASTERID                
                    FROM ' || 'TMP_T3' || ' 
                )                
                    AND A.STATUS = ''ACT'' 
                    AND A.FLAG_CF = ''C'' 
                    AND A.METHOD = ''EIR'' 
            ) A                
            GROUP BY 
                A.DOWNLOAD_DATE                
                ,A.MASTERID ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
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
                ,METHOD                
            ) SELECT 
                A.FACNO                
                ,A.CIFNO                
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
                ,A.ECFDATE                
                ,A.DATASOURCE                
                ,B.PRDCODE                
                ,B.TRXCODE                
                ,B.CCY                
                ,ROUND(CAST(CAST(CAST(CASE                 
                    WHEN B.FLAG_REVERSE = ''Y''                
                    THEN - 1 * B.AMOUNT                
                    ELSE B.AMOUNT                
                END AS FLOAT) / NULLIF(CAST(C.SUM_AMT AS FLOAT), 0) AS NUMERIC(32, 20)) * A.N_ACCRU_FEE AS NUMERIC), ' || V_ROUND || ') AS N_AMOUNT                
                ,B.STATUS                
                ,CURRENT_TIMESTAMP                
                ,A.ACCTNO                
                ,A.MASTERID                
                ,B.FLAG_CF                
                ,''N''                
                ,NULL AS AMORTDATE                
                ,''ECF''                
                ,B.ORG_CCY                
                ,B.ORG_CCY_EXRATE                
                ,B.PRDTYPE                
                ,B.CF_ID                
                ,B.METHOD                
            FROM ' || V_TABLEINSERT3 || ' A 
            JOIN ' || V_TABLEINSERT6 || ' B 
                ON B.ECFDATE = A.ECFDATE                
                AND A.MASTERID = B.MASTERID                
                AND B.FLAG_CF = ''F'' AND B.STATUS = ''ACT''              
            JOIN ' || 'TMP_TF' || ' C 
                ON C.DOWNLOAD_DATE = A.ECFDATE                
                AND C.MASTERID = A.MASTERID                
            WHERE A.ID IN (                
                SELECT ID                
                FROM ' || 'TMP_P1' || '
            )                
            --20180108 EXCLUDE CF REV AND ITS PAIR                          
            AND B.CF_ID NOT IN (                
                SELECT CF_ID                
                FROM ' || V_TABLEINSERT1 || '
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
                AND FLAG_REVERSE = ''Y''                
                AND CF_ID_REV IS NOT NULL                
                                
                UNION ALL                
                                
                SELECT CF_ID_REV                
                FROM ' || V_TABLEINSERT1 || '
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
                AND FLAG_REVERSE = ''Y''                
                AND CF_ID_REV IS NOT NULL                
            ) ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
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
                ,METHOD                
            ) SELECT 
                A.FACNO                
                ,A.CIFNO                
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
                ,A.ECFDATE                
                ,A.DATASOURCE                
                ,B.PRDCODE                
                ,B.TRXCODE                
                ,B.CCY                
                ,ROUND(CAST(CAST(CAST(CASE                 
                    WHEN B.FLAG_REVERSE = ''Y''                
                    THEN - 1 * B.AMOUNT                
                    ELSE B.AMOUNT                
                END AS FLOAT) / NULLIF(CAST(C.SUM_AMT AS FLOAT), 0) AS NUMERIC(32, 20)) * A.N_ACCRU_COST AS NUMERIC), ' || V_ROUND || ') AS N_AMOUNT                
                ,B.STATUS                
                ,CURRENT_TIMESTAMP                
                ,A.ACCTNO                
                ,A.MASTERID                
                ,B.FLAG_CF                
                ,''N''                
                ,NULL AS AMORTDATE                
                ,''ECF''                
                ,B.ORG_CCY                
                ,B.ORG_CCY_EXRATE                
                ,B.PRDTYPE                
                ,B.CF_ID                
                ,B.METHOD                
            FROM ' || V_TABLEINSERT3 || ' A 
            JOIN ' || V_TABLEINSERT6 || ' B 
                ON B.ECFDATE = A.ECFDATE                
                AND A.MASTERID = B.MASTERID                
                AND B.FLAG_CF = ''C'' AND B.STATUS = ''ACT''              
            JOIN ' || 'TMP_TC' || ' C 
                ON C.DOWNLOAD_DATE = A.ECFDATE                
                AND C.MASTERID = A.MASTERID                
            WHERE A.ID IN (                
                SELECT ID                
                FROM ' || 'TMP_P1' || '
            )                
            --20180108 EXCLUDE CF REV AND ITS PAIR                          
            AND B.CF_ID NOT IN (                
                SELECT CF_ID                
                FROM ' || V_TABLEINSERT1 || '
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
                AND FLAG_REVERSE = ''Y''                
                AND CF_ID_REV IS NOT NULL                
                                
                UNION ALL                
                                
                SELECT CF_ID_REV                
                FROM ' || V_TABLEINSERT1 || '
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
                AND FLAG_REVERSE = ''Y''                
                AND CF_ID_REV IS NOT NULL                
            ) ';
        EXECUTE (V_STR_QUERY);
    END IF;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' A 
        SET STATUS = TO_CHAR(''' || V_CURRDATE || '''::DATE, ''YYYYMMDD'')           
        WHERE STATUS = ''ACT''
        AND MASTERID IN (                
            SELECT MASTERID                
            FROM ' || V_TABLEINSERT4 || ' 
            WHERE TOTAL_AMT = 0                
            OR TOTAL_AMT_ACRU = 0                
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT8 || ' A 
        SET 
            AMORTSTOPDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ,AMORTSTOPMSG = ''SP_ACCT_EIR_ECF''                
        WHERE MASTERID IN (                
            SELECT MASTERID                
            FROM ' || V_TABLEINSERT4 || '
        )
        AND AMORTSTOPDATE IS NULL ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T1' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T1' || ' (MASTERID) 
        SELECT MASTERID                
        FROM ' || V_TABLEINSERT25 || ' 
        WHERE PREV_PMT_DATE = PMT_DATE                
        AND MASTERID IN (                
            SELECT B.MASTERID                
            FROM ' || V_TABLEINSERT4 || ' B
            WHERE (                
                (                
                    B.TOTAL_AMT <> 0                
                    AND B.TOTAL_AMT_ACRU <> 0                
                )                
                --OR B.STAFFLOAN = 1  --20180827                      
                OR (                
                    B.MASTERID IN (
                        SELECT DISTINCT MASTERID                
                        FROM ' || V_TABLEINSERT21 || ' 
                        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE              
                        AND EVENT_ID = 4          
                    )                
                )                
            )                
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT22 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT22 || ' (MASTERID)
        SELECT A.MASTERID 
        FROM ' || 'TMP_T1' || ' A ';
    EXECUTE (V_STR_QUERY);

    V_X = V_MIN_ID;
    V_X_INC = 500000;

    WHILE V_X <= V_MAX_ID 
    LOOP 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT24 || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT24 || ' 
            (                
                MASTERID                
                ,ACCTNO                
                ,PREV_PMT_DATE                
                ,PMT_DATE                
                ,INT_RATE                
                ,I_DAYS                
                ,COUNTER                
                ,OS_PRN_PREV                
                ,PRN_AMT                
                ,INT_AMT                
                ,OS_PRN                
                ,DISB_PERCENTAGE                
                ,DISB_AMOUNT                
                ,PLAFOND                
                ,ICC                
                ,GRACE_DATE                
            )                
            SELECT 
                MASTERID                
                ,ACCTNO                
                ,PREV_PMT_DATE                
                ,PMT_DATE                
                ,INTEREST_RATE                
                ,I_DAYS                
                ,COUNTER                
                ,OS_PRN_PREV                
                ,PRN_AMT                
                ,INT_AMT                
                ,OS_PRN                
                ,DISB_PERCENTAGE                
                ,DISB_AMOUNT                
                ,PLAFOND                
                ,ICC                
                ,GRACE_DATE                
            FROM ' || V_TABLEINSERT25 || '
            WHERE MASTERID IN (                
                SELECT MASTERID                
                FROM ' || V_TABLEINSERT22 || ' 
                WHERE ID >= ' || V_X || '
                AND ID < ' || (V_X + V_X_INC) || '               
            ) ';
        EXECUTE (V_STR_QUERY);

        CALL SP_IFRS_LBM_PAYM_CORE_PROC_NOP(P_RUNID, V_CURRDATE, P_PRC);

        V_X := V_X + V_X_INC;
    END LOOP;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT17 || ' A 
        SET NPV_RATE = B.NPV_RATE
        FROM ' || V_TABLEINSERT4 || ' B 
        WHERE B.STAFFLOAN = 1                
        AND A.MASTERID = B.MASTERID 
        AND COALESCE(B.NPV_RATE, 0) > 0 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT17 || ' A 
        SET NPV_INSTALLMENT = CASE                 
            WHEN TRUNC(F_CNT_DAYS_30_360(STARTAMORTDATE, PMT_DATE) / 30, 0) = 0 
            THEN N_INSTALLMENT / (POWER(1 + NULLIF(NPV_RATE, 0) / 360 / 100, F_CNT_DAYS_30_360(STARTAMORTDATE, PMT_DATE))) 
            ELSE N_INSTALLMENT / NULLIF((POWER(1 + NULLIF(NPV_RATE, 0) / 12 / 100, TRUNC(F_CNT_DAYS_30_360(STARTAMORTDATE, PMT_DATE) / 30, 0))), 0) 
        END                
        WHERE NPV_RATE > 0 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_B1' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_B2' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_B3' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_B1' || ' 
        (                
            MASTERID      
            ,N_OSPRN                
        ) SELECT 
            MASTERID                
            ,N_OSPRN                
        FROM ' || V_TABLEINSERT17 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE              
        AND PREV_PMT_DATE = PMT_DATE                
        AND NPV_RATE > 0 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_B2' || ' 
        (                
            MASTERID                
            ,NPV_SUM                
        )                
        SELECT 
            MASTERID                
            ,SUM(COALESCE(NPV_INSTALLMENT, 0)) AS NPV_SUM                
        FROM ' || V_TABLEINSERT17 || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
        AND NPV_RATE > 0                
        GROUP BY MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_B3' || ' 
        (                
            MASTERID                
            ,N_OSPRN                
            ,NPV_SUM                
            ,BENEFIT                
        ) SELECT 
            A.MASTERID                
            ,A.N_OSPRN                
            ,B.NPV_SUM                
            ,B.NPV_SUM - A.N_OSPRN AS BENEFIT                
        FROM ' || 'TMP_B1' || ' A                
        JOIN ' || 'TMP_B2' || ' B 
        ON B.MASTERID = A.MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT4 || ' A 
        SET BENEFIT = A.BENEFIT                
        FROM ' || 'TMP_B3' || ' B
        WHERE B.MASTERID = A.MASTERID ';
    EXECUTE (V_STR_QUERY);

    /*
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT6 || ' 
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
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  ECFDATE                
            ,C.MASTERID                
            ,C.BRCODE                
            ,C.CIFNO      
            ,C.FACNO                
            ,C.ACCTNO                
            ,C.DATASOURCE                
            ,C.CCY                
            ,C.PRD_CODE                
            ,C.TRX_CODE                
            ,C.FLAG_CF                
            ,C.FLAG_REVERSE                
            ,C.METHOD                
            ,C.STATUS                
            ,C.SRCPROCESS                
            ,C.AMOUNT                
            ,CURRENT_TIMESTAMP CREATEDDATE                
            ,''EIR_ECF_MAIN'' CREATEDBY                
            ,'''' SEQ                
            ,C.AMOUNT                
            ,C.ORG_CCY                
            ,C.ORG_CCY_EXRATE                
            ,C.PRD_TYPE                
            ,C.CF_ID                
        FROM ' || V_TABLEINSERT1 || ' C                
        JOIN ' || V_TABLEINSERT4 || ' B 
            ON B.MASTERID = C.MASTERID                
            AND B.TOTAL_AMT <> 0                
            AND B.TOTAL_AMT_ACRU <> 0                
        WHERE C.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
            AND C.MASTERID = B.MASTERID                
            AND C.STATUS = ''ACT''                
            AND C.METHOD = ''EIR''                
            --20180116 EXCLUDE CF REV AND ITS PAIR                          
            AND C.CF_ID NOT IN (                
                SELECT CF_ID                
                FROM ' || V_TABLEINSERT1 || '
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
                AND FLAG_REVERSE = ''Y''                
                AND CF_ID_REV IS NOT NULL                
                                
                UNION ALL                
                                
                SELECT CF_ID_REV                
                FROM ' || V_TABLEINSERT1 || ' 
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
                AND FLAG_REVERSE = ''Y''                
                AND CF_ID_REV IS NOT NULL                
            ) ';
    EXECUTE (V_STR_QUERY);
    */

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT6 || ' 
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
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  ECFDATE                
            ,C.MASTERID                
            ,C.BRCODE                
            ,C.CIFNO                
            ,C.FACNO                
            ,C.ACCTNO                
            ,C.DATASOURCE                
            ,C.CCY                
            ,C.PRDCODE                
            ,C.TRXCODE                
            ,C.FLAG_CF                
            ,C.FLAG_REVERSE                
            ,C.METHOD                
            ,C.STATUS                
            ,C.SRCPROCESS                
            ,C.AMOUNT                
            ,CURRENT_TIMESTAMP CREATEDDATE                
            ,''EIR_ECF_MAIN'' CREATEDBY                
            ,'''' SEQ                
            ,C.AMOUNT_ORG                
            ,C.ORG_CCY                
            ,C.ORG_CCY_EXRATE                
            ,C.PRDTYPE                
            ,C.CF_ID                
        FROM ' || V_TABLEINSERT7 || ' C                
        JOIN ' || 'VW_LBM_LAST_EIR_CF_PREV' || ' X 
            ON X.MASTERID = C.MASTERID                
            AND X.DOWNLOAD_DATE = C.DOWNLOAD_DATE                
            AND C.SEQ = X.SEQ                
        JOIN ' || V_TABLEINSERT4 || ' B 
            ON B.MASTERID = C.MASTERID                
            AND B.TOTAL_AMT <> 0                
            AND B.TOTAL_AMT_ACRU <> 0                
        --20160407 EIR STOP REV                          
        LEFT JOIN (                
            SELECT DISTINCT MASTERID                
            FROM ' || V_TABLEINSERT18 || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
        ) A 
            ON A.MASTERID = C.MASTERID                
        WHERE C.DOWNLOAD_DATE IN (                
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
            ,''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE 
        )                
        AND C.STATUS = ''ACT''                
        AND C.TRXCODE = ''BENEFIT''  
        --20160407 EIR STOP REV                          
        AND A.MASTERID IS NULL                
        --20180116 EXCLUDE CF REV AND ITS PAIR                          
        AND C.CF_ID NOT IN (                
            SELECT CF_ID                
            FROM ' || V_TABLEINSERT1 || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
            AND FLAG_REVERSE = ''Y''
            AND CF_ID_REV IS NOT NULL                
                            
            UNION ALL                
                            
            SELECT CF_ID_REV                
            FROM ' || V_TABLEINSERT1 || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
            AND FLAG_REVERSE = ''Y''
            AND CF_ID_REV IS NOT NULL                
        )                
        --20180426 EXCLUDE PREVDATE IF NOT FROM SP ACF ACCRU                  
        AND CASE                 
            WHEN C.DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE 
                AND C.SEQ <> ''2''                
            THEN 0                
            ELSE 1                
        END = 1 ';
    EXECUTE (V_STR_QUERY);

    IF V_PARAM_DISABLE_ACCRU_PREV != 0 
    THEN 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T1' || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T1' || ' (MASTERID) 
            SELECT DISTINCT MASTERID                
            FROM ' || V_TABLEINSERT3 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND DO_AMORT = ''Y'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T3' || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T3' || ' (MASTERID) 
            SELECT MASTERID                
            FROM ' || V_TABLEINSERT4 || ' 
            WHERE MASTERID NOT IN (                
                SELECT MASTERID                
                FROM ' || 'TMP_T1' || ' 
            ) ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_P1' || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_P1' || ' (ID)
            SELECT MAX(ID) AS ID                
            FROM ' || V_TABLEINSERT3 || ' 
            WHERE MASTERID IN (                
                SELECT MASTERID                
                FROM ' || 'TMP_T3' || ' 
            )                
            AND DO_AMORT = ''N''                
            AND DOWNLOAD_DATE < ' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE            
            AND DOWNLOAD_DATE >= ' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE 
            GROUP BY MASTERID ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_TF' || '';
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
                        THEN - 1 * A.AMOUNT                
                        ELSE A.AMOUNT                
                    END AS N_AMOUNT                
                    ,A.ECFDATE DOWNLOAD_DATE                
                    ,A.MASTERID                
                FROM ' || V_TABLEINSERT6 || ' A
                WHERE A.MASTERID IN (                
                    SELECT MASTERID                
                    FROM ' || 'TMP_T3' || ' 
                )                
                    AND A.STATUS = ''ACT''                
                    AND A.FLAG_CF = ''F''                
                    AND A.METHOD = ''EIR''                
            ) A                
            GROUP BY 
                A.DOWNLOAD_DATE                
                ,A.MASTERID ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_TC' || '';
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
                        THEN - 1 * A.AMOUNT                
                        ELSE A.AMOUNT                
                    END AS N_AMOUNT                
                    ,A.ECFDATE DOWNLOAD_DATE                
                    ,A.MASTERID                
                FROM ' || V_TABLEINSERT6 || ' A                
                WHERE A.MASTERID IN (                
                    SELECT MASTERID                
                    FROM ' || 'TMP_T3' || '
                )                
                    AND A.STATUS = ''ACT''                
                    AND A.FLAG_CF = ''C''                
                    AND A.METHOD = ''EIR''                
            ) A                
            GROUP BY 
                A.DOWNLOAD_DATE                
                ,A.MASTERID ';
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
                ,SRCPROCESS                
                ,ORG_CCY                
                ,ORG_CCY_EXRATE                
                ,PRDTYPE                
                ,CF_ID                
                ,BRCODE                
                ,METHOD                
            ) SELECT 
                A.FACNO                
                ,A.CIFNO                
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  ECFDATE                
                ,A.DATASOURCE                
                ,B.PRDCODE                
                ,B.TRXCODE                
                ,B.CCY                
                ,ROUND(CAST(CAST(CASE                 
                    WHEN B.FLAG_REVERSE = ''Y''                
                        THEN - 1 * B.AMOUNT                
                    ELSE B.AMOUNT                
                    END AS FLOAT) / NULLIF(CAST(C.SUM_AMT AS FLOAT), 0) AS DECIMAL(32, 20)) * A.N_ACCRU_FEE * - 1, ' || V_ROUND || ') AS N_AMOUNT                
                ,B.STATUS                
                ,CURRENT_TIMESTAMP                
                ,A.ACCTNO                
                ,A.MASTERID                
                ,B.FLAG_CF                
                ,''N''                
                ,''ECFACCRU''                
                ,B.ORG_CCY                
                ,B.ORG_CCY_EXRATE                
                ,B.PRDTYPE                
                ,B.CF_ID                
                ,B.BRCODE                
                ,B.METHOD                
            FROM ' || V_TABLEINSERT3 || ' A                
            JOIN ' || V_TABLEINSERT6 || ' B 
                ON B.ECFDATE = A.ECFDATE                
                AND A.MASTERID = B.MASTERID                
                AND B.FLAG_CF = ''F'' AND B.STATUS = ''ACT''            
                AND A.MASTERID NOT IN (          
                    SELECT DISTINCT MASTERID           
                    FROM ' || V_TABLEINSERT20 || '
                    WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
                )             
            JOIN TMP_TF C 
                ON C.DOWNLOAD_DATE = A.ECFDATE                
                AND C.MASTERID = A.MASTERID                
                --20160407 EIR STOP REV              
            LEFT JOIN (                
                SELECT DISTINCT MASTERID                
                FROM ' || V_TABLEINSERT18 || ' 
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
            ) D 
                ON A.MASTERID = D.MASTERID                
            WHERE A.ID IN (                
                SELECT ID                
                FROM ' || 'TMP_P1' || ' 
            )                
            --20160407 EIR STOP REV                          
            AND D.MASTERID IS NULL                
            --20180116 EXCLUDE CF REV AND ITS PAIR                          
            AND B.CF_ID NOT IN (                
                SELECT CF_ID                
                FROM IFRS_ACCT_COST_FEE                
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
                AND FLAG_REVERSE = ''Y''                
                AND CF_ID_REV IS NOT NULL                
                    
                UNION ALL                
                                
                SELECT CF_ID_REV                
                FROM IFRS_ACCT_COST_FEE                
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
                AND FLAG_REVERSE = ''Y''                
                AND CF_ID_REV IS NOT NULL                
            ) ';
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
                ,SRCPROCESS                
                ,ORG_CCY                
                ,ORG_CCY_EXRATE                
                ,PRDTYPE                
                ,CF_ID                
                ,BRCODE                
                ,METHOD                
            ) SELECT 
                A.FACNO                
                ,A.CIFNO                
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  ECFDATE                
                ,A.DATASOURCE                
                ,B.PRDCODE                
                ,B.TRXCODE                
                ,B.CCY                
                ,ROUND(CAST(CAST(CASE                 
                    WHEN B.FLAG_REVERSE = ''Y''                
                        THEN - 1 * B.AMOUNT                
                    ELSE B.AMOUNT                
                    END AS FLOAT) / NULLIF(CAST(C.SUM_AMT AS FLOAT), 0) AS DECIMAL(32, 20)) * A.N_ACCRU_COST * - 1, ' || V_ROUND || ') AS N_AMOUNT                
                ,B.STATUS                
                ,CURRENT_TIMESTAMP                
                ,A.ACCTNO                
                ,A.MASTERID                
                ,B.FLAG_CF                
                ,''N''                
                ,''ECFACCRU''                
                ,B.ORG_CCY                
                ,B.ORG_CCY_EXRATE                
                ,B.PRDTYPE                
                ,B.CF_ID                
                ,B.BRCODE                
                ,B.METHOD                
            FROM ' || V_TABLEINSERT3 || ' A                
            JOIN ' || V_TABLEINSERT6 || ' B 
                ON B.ECFDATE = A.ECFDATE                
                AND A.MASTERID = B.MASTERID                
                AND B.FLAG_CF = ''C'' AND B.STATUS = ''ACT''           
                AND A.MASTERID NOT IN (
                    SELECT DISTINCT MASTERID           
                    FROM ' || V_TABLEINSERT20 || ' 
                    WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
                )              
            JOIN ' || 'TMP_TC' || ' C 
                ON C.DOWNLOAD_DATE = A.ECFDATE                
                AND C.MASTERID = A.MASTERID                
            --20160407 EIR STOP REV                          
            LEFT JOIN (
                SELECT DISTINCT MASTERID                
                FROM ' || V_TABLEINSERT18 || '
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
            ) D 
                ON A.MASTERID = D.MASTERID                
            WHERE A.ID IN (                
                SELECT ID                
                FROM ' || 'TMP_P1' || ' 
            )                
            --20160407 EIR STOP REV                          
            AND D.MASTERID IS NULL                
            --20180108 EXCLUDE CF REV AND ITS PAIR                          
            AND B.CF_ID NOT IN (                
                SELECT CF_ID                
                FROM ' || V_TABLEINSERT1 || ' 
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
                AND FLAG_REVERSE = ''Y''
                AND CF_ID_REV IS NOT NULL                
                                
                UNION ALL                
                                
                SELECT CF_ID_REV                
                FROM ' || V_TABLEINSERT1 || '
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
                AND FLAG_REVERSE = ''Y''
                AND CF_ID_REV IS NOT NULL                
            ) ';
        EXECUTE (V_STR_QUERY);
    END IF;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T1' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T2' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T1' || ' (MASTERID) 
         SELECT DISTINCT MASTERID 
         FROM ' || 'V_TABLEINSERT6' || ' 
         WHERE ECFDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
         AND CREATEDBY = ''EIR_SWITCH'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T2' || ' (MASTERID)
        SELECT DISTINCT MASTERID 
        FROM ' || 'V_TABLEINSERT6' || ' 
        WHERE ECFDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND CREATEDBY != ''EIR_SWITCH'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || 'V_TABLEINSERT6' || ' A 
        SET STATUS = TO_CHAR(''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE, ''YYYYMMDD'')  
        WHERE ECFDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
        AND MASTERID IN (SELECT MASTERID FROM ' || 'TMP_T1' || ')  
        AND MASTERID IN (SELECT MASTERID FROM ' || 'TMP_T2' || ')  
        AND CREATEDBY = ''EIR_SWITCH''  
        AND STATUS = ''ACT'' ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LBM_ACCT_EIR_CF_ECF_GRP(P_RUNID, V_CURRDATE, P_PRC);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT10 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT14 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT5 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT5 || ' 
        (                
            MASTERID                
            ,FEE_AMT                
            ,COST_AMT                
            ,BENEFIT                
            ,STAFFLOAN                
            ,PREV_EIR                
            --20180226 COPY DATA                 
            ,TOTAL_AMT --20180517  ADD YACOP                 
            ,NEW_FEE_AMT                
            ,NEW_COST_AMT                
            ,NEW_TOTAL_AMT                
            ,GAIN_LOSS_CALC                
        ) SELECT 
            B.MASTERID                
            ,B.FEE_AMT                
            ,B.COST_AMT                
            ,B.BENEFIT                
            ,B.STAFFLOAN                
            ,B.PREV_EIR                
            ,B.TOTAL_AMT --20180517  ADD YACOP                    
            ,NEW_FEE_AMT                
            ,NEW_COST_AMT                
            ,NEW_TOTAL_AMT                
            ,GAIN_LOSS_CALC                
        FROM ' || V_TABLEINSERT4 || ' B
        WHERE (                
            B.TOTAL_AMT <> 0                
            AND B.TOTAL_AMT_ACRU <> 0                
        )                
        OR (                
            B.STAFFLOAN = 1                
            AND B.PREV_EIR IS NULL                
        )                
        --20170927, IVAN NOCF                          
        OR (                
            B.MASTERID IN (                
                SELECT DISTINCT MASTERID                
                FROM ' || V_TABLEINSERT21 || '
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
                AND EVENT_ID = ''4'' 
            )                
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT22 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT15 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT11 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT22 || ' (MASTERID) 
        SELECT A.MASTERID                
        FROM (                
            SELECT 
                MASTERID                
                ,PERIOD                
            FROM ' || V_TABLEINSERT17 || '
            WHERE PREV_PMT_DATE = PMT_DATE                
            AND MASTERID IN (                
                SELECT MASTERID                
                FROM ' || V_TABLEINSERT5 || '
                WHERE STAFFLOAN = 1                
                OR GAIN_LOSS_CALC = ''Y'' --20180226 PREPAYMENT                          
            )          
        ) A                
        ORDER BY PERIOD ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT MIN(ID) FROM ' || V_TABLEINSERT22 || '';
    EXECUTE (V_STR_QUERY) INTO V_MIN_ID;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT MAX(ID) FROM ' || V_TABLEINSERT22 || '';
    EXECUTE (V_STR_QUERY) INTO V_MAX_ID;

    V_X := V_MIN_ID;
    V_X_INC := 500000;

    WHILE V_X <= V_MAX_ID 
    LOOP 
        V_ID2 := V_X + V_X_INC - 1;

        CALL SP_IFRS_LBM_ACCT_EIR_GS_RANGE(P_RUNID, V_CURRDATE, P_PRC, P_ID1 => V_X, P_ID2 => V_ID2);

        CALL SP_IFRS_LBM_ACCT_EIR_GS_PROC3(P_RUNID, V_CURRDATE, P_PRC);

        V_X := V_X + V_X_INC;
    END LOOP;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT5 || ' A 
        SET BENEFIT = B.UNAMORT - B.GLOSS                
        FROM ' || V_TABLEINSERT15 || ' B 
        WHERE ( 
            B.MASTERID = A.MASTERID 
            AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            --20180226 ONLY FOR STAFF LOAN                          
            AND A.STAFFLOAN = 1                
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT4 || ' A 
        SET BENEFIT = B.UNAMORT - B.GLOSS                
        FROM ' || V_TABLEINSERT15 || ' B 
        WHERE ( 
            B.MASTERID = A.MASTERID 
            AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            --20180226 ONLY FOR STAFF LOAN                          
            AND A.STAFFLOAN = 1 
        ) ';
    EXECUTE (V_STR_QUERY);

    /*
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMP_YEST_UNAMORT';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_YEST_UNAMORT' || ' AS 
        SELECT 
            B.MASTERID                
            ,SUM(COALESCE(CASE             
                WHEN B.FLAG_CF = ''F'' 
                THEN CASE                 
                    WHEN B.FLAG_REVERSE = ''Y'' 
                    THEN - 1 * CASE                 
                        WHEN CFREV.MASTERID IS NULL                
                        THEN B.AMOUNT_ORG                
                        ELSE B.AMOUNT_ORG          
                    END                
                    ELSE CASE                 
                        WHEN CFREV.MASTERID IS NULL                
                        THEN B.AMOUNT_ORG                
                        ELSE B.AMOUNT_ORG                
                    END                
                END                
                ELSE 0                
            END, 0)) AS YFEE_AMT                
            ,SUM(COALESCE(CASE                 
                WHEN B.FLAG_CF = ''C'' 
                THEN CASE                 
                    WHEN B.FLAG_REVERSE = ''Y'' 
                    THEN - 1 * CASE                 
                        WHEN CFREV.MASTERID IS NULL                
                        THEN B.AMOUNT_ORG                
                        ELSE B.AMOUNT_ORG                
                    END                
                    ELSE CASE                 
                        WHEN CFREV.MASTERID IS NULL                
                        THEN B.AMOUNT_ORG                
                        ELSE B.AMOUNT_ORG                
                    END                
                END                
                ELSE 0                
            END, 0)) AS YCOST_AMT   
        FROM ' || V_TABLEINSERT7 || ' B              
        JOIN ' || 'VW_LBM_LAST_EIR_CF_PREV' || ' X 
            ON X.MASTERID = B.MASTERID                
            AND X.DOWNLOAD_DATE = B.DOWNLOAD_DATE                
            AND B.SEQ = X.SEQ                
        --20160407 EIR STOP REV                          
        LEFT JOIN (                
            SELECT DISTINCT MASTERID                
            FROM ' || V_TABLEINSERT18 || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE           
        ) A 
        ON A.MASTERID = B.MASTERID                
        --20180116 RESONA REQ                          
        LEFT JOIN ' || 'TMP_TODAYREV' || ' CFREV 
            ON CFREV.MASTERID = B.MASTERID                
            WHERE B.DOWNLOAD_DATE IN (                
                ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE           
                ,''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE 
            )                
        AND B.STATUS = ''ACT'' 
        AND A.MASTERID IS NULL                
        AND B.MASTERID IN (
            SELECT B.MASTERID          
            FROM ' || V_TABLEINSERT15 || ' B          
            WHERE B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        )          
        --20180116 EXCLUDE CF REVERSAL AND ITS PAIR                          
        AND B.CF_ID NOT IN (                
            SELECT CF_ID                
            FROM ' || V_TABLEINSERT1 || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE           
            AND FLAG_REVERSE = ''Y'' 
            AND CF_ID_REV IS NOT NULL                
                            
            UNION ALL         
                                    
            SELECT CF_ID_REV                
            FROM ' || V_TABLEINSERT1 || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE           
            AND FLAG_REVERSE = ''Y'' 
            AND CF_ID_REV IS NOT NULL                
        )                
        GROUP BY B.MASTERID ';
    EXECUTE (V_STR_QUERY);
    */

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT5 || ' A 
        SET 
            GAIN_LOSS_AMT = ROUND(B.GLOSS, ' || V_ROUND || ')                
            ,GAIN_LOSS_FEE_AMT = CASE                 
                WHEN FEE_AMT <> 0 AND COST_AMT = 0                
                THEN ROUND(B.GLOSS, ' || V_ROUND || ')                
                WHEN FEE_AMT = 0 AND COST_AMT <> 0                
                THEN 0                
                ELSE ROUND(B.GLOSS * FEE_AMT / TOTAL_AMT, ' || V_ROUND || ')                
            END                
            ,GAIN_LOSS_COST_AMT = CASE                 
                WHEN FEE_AMT = 0 AND COST_AMT <> 0                
                THEN ROUND(B.GLOSS, ' || V_ROUND || ')                
                WHEN FEE_AMT <> 0 AND COST_AMT = 0                
                THEN 0                
                ELSE ROUND(B.GLOSS, ' || V_ROUND || ') - ROUND(B.GLOSS * FEE_AMT / TOTAL_AMT, ' || V_ROUND || ')                
            END                
        FROM ' || V_TABLEINSERT15 || ' B                
        WHERE (                
            B.MASTERID = A.MASTERID 
            AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND A.STAFFLOAN = 0 
            AND A.GAIN_LOSS_CALC = ''Y'' 
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT4 || ' A 
        SET 
            GAIN_LOSS_AMT = ROUND(B.GLOSS, ' || V_ROUND || ') 
            ,GAIN_LOSS_FEE_AMT = CASE                 
                WHEN FEE_AMT <> 0 AND COST_AMT = 0                
                THEN ROUND(B.GLOSS, ' || V_ROUND || ') 
                WHEN FEE_AMT = 0 AND COST_AMT <> 0                
                THEN 0                
                ELSE ROUND(B.GLOSS * FEE_AMT / TOTAL_AMT, ' || V_ROUND || ') 
            END                
            ,GAIN_LOSS_COST_AMT = CASE                 
                WHEN FEE_AMT = 0 AND COST_AMT <> 0                
                THEN ROUND(B.GLOSS, ' || V_ROUND || ') 
                WHEN FEE_AMT <> 0 AND COST_AMT = 0                
                THEN 0                
                ELSE ROUND(B.GLOSS, ' || V_ROUND || ') - ROUND(B.GLOSS * FEE_AMT / TOTAL_AMT, ' || V_ROUND || ') 
            END                
        FROM ' || V_TABLEINSERT15 || ' B                
        WHERE (                
            B.MASTERID = A.MASTERID                
            AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE           
            AND A.STAFFLOAN = 0                
            AND A.GAIN_LOSS_CALC = ''Y''                
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_B1' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_B2' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_B3' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_B1' || ' 
        (                
            MASTERID                
            ,N_OSPRN                
        )                
        SELECT 
            MASTERID                
            ,N_OSPRN                
        FROM ' || V_TABLEINSERT17 || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND PREV_PMT_DATE = PMT_DATE                
        AND NPV_RATE > 0 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_B2' || ' 
        (                
            MASTERID                
            ,NPV_SUM                
        )           
        SELECT 
            A.MASTERID                
            ,(COALESCE(A.N_OSPRN, 0) + COALESCE(BENEFIT, 0)) AS NPV                
        FROM ' || 'TMP_B1' || ' A
        JOIN ' || V_TABLEINSERT4 || ' B 
        ON A.MASTERID = B.MASTERID                
        JOIN ' || V_TABLEINSERT15 || ' C 
        ON A.MASTERID = C.MASTERID                
        WHERE C.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_B3' || ' 
        (                
            MASTERID                
            ,N_OSPRN                
            ,NPV_SUM                
            ,BENEFIT                
        )                
        SELECT 
            A.MASTERID                
            ,A.N_OSPRN                
            ,B.NPV_SUM                
            ,B.NPV_SUM - A.N_OSPRN AS BENEFIT                
        FROM ' || 'TMP_B1' || ' A                
        JOIN ' || 'TMP_B2' || ' B 
        ON B.MASTERID = A.MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT4 || ' A 
        SET BENEFIT = B.BENEFIT                
        FROM ' || 'TMP_B3' || ' B                
        WHERE (A.MASTERID = B.MASTERID) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || 'V_TABLEINSERT6' || ' A 
        SET 
            STATUS = ''REV''  
            ,FLAG_REVERSE = ''Y''  
        FROM ' || V_TABLEINSERT4 || ' B 
        WHERE B.MASTERID = A.MASTERID  --ADD JOIN TO IFRS_LBM_ACCT_EIR_CF_ECF
        AND A.ECFDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND A.STATUS = ''ACT'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT6 || ' 
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
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ,A.MASTERID                
            ,M.BRANCH_CODE                
            ,M.CUSTOMER_NUMBER                
            ,M.FACILITY_NUMBER                
            ,M.ACCOUNT_NUMBER                
            ,M.DATA_SOURCE                
            ,M.CURRENCY                
            ,M.PRODUCT_CODE                
            ,''BENEFIT''                
            ,CASE                 
            WHEN A.BENEFIT < 0                
                THEN ''F''                
            ELSE ''C''                
            END                
            ,''N''                
            ,''EIR''                
            ,''ACT''                
            ,''STAFFLOAN''                
            ,A.BENEFIT                
            ,CURRENT_TIMESTAMP CREATEDDATE                
            ,''EIR_ECF_MAIN_TT'' CREATEDBY                
            ,'''' SEQ                
            ,A.BENEFIT                
            ,M.CURRENCY                
            ,1                
            ,M.PRODUCT_TYPE                
            ,0 AS CF_ID                
        FROM ' || 'TMP_B3' || ' A                
        JOIN ' || V_TABLEINSERT23 || ' M 
            ON M.MASTERID = A.MASTERID                
            AND M.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        JOIN ' || V_TABLEINSERT4 || ' C 
            ON C.MASTERID = A.MASTERID  ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' A 
        SET CF_ID = ID                
        WHERE CF_ID = 0                
        AND SRCPROCESS = ''STAFFLOAN''                
        AND DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT9 || ' 
        WHERE DOWNLOAD_DATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT16 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT12 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT14 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT10 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT22 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT22 || ' (MASTERID) 
        SELECT A.MASTERID                
        FROM (                
            SELECT 
                MASTERID                
                ,PERIOD                
            FROM ' || V_TABLEINSERT17 || '
            WHERE PREV_PMT_DATE = PMT_DATE                
        ) A                
        ORDER BY PERIOD ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT MIN(ID) FROM ' || V_TABLEINSERT22 || '';
    EXECUTE (V_STR_QUERY) INTO V_MIN_ID;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT MAX(ID) FROM ' || V_TABLEINSERT22 || '';
    EXECUTE (V_STR_QUERY) INTO V_MAX_ID;

    V_X := V_MIN_ID;
    V_X_INC := 500000;

    WHILE V_X <= V_MAX_ID 
    LOOP 
        V_ID2 = V_X + V_X_INC - 1;

        CALL SP_IFRS_LBM_ACCT_EIR_GS_RANGE(P_RUNID, V_CURRDATE, P_PRC, P_ID1 => V_X, P_ID2 => V_ID2);

        CALL SP_IFRS_LBM_ACCT_EIR_GS_ALL(P_RUNID, V_CURRDATE, P_PRC);

        V_X := V_X + V_X_INC;
    END LOOP;

    CALL SP_IFRS_LBM_ACCT_EIR_GS_INSERT4(P_RUNID, V_CURRDATE, P_PRC);

    CALL SP_IFRS_LBM_ACCT_EIR_ECF_ALIGN4(P_RUNID, V_CURRDATE, P_PRC);

    CALL SP_IFRS_LBM_ACCT_EIR_GS_INSERT(P_RUNID, V_CURRDATE, P_PRC);

    CALL SP_IFRS_LBM_ACCT_EIR_ECF_ALIGN(P_RUNID, V_CURRDATE, P_PRC);

    CALL SP_IFRS_LBM_ACCT_EIR_ECF_MERGE(P_RUNID, V_CURRDATE, P_PRC);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T1' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T1' || ' (MASTERID)
        SELECT DISTINCT MASTERID                
        FROM ' || V_TABLEINSERT8 || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T2' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T2' || ' (MASTERID)
        SELECT DISTINCT A.MASTERID                
        FROM ' || 'TMP_T1' || ' A                
        JOIN ' || V_TABLEINSERT8 || ' B 
        ON B.PREV_PMT_DATE = B.PMT_DATE                
        AND B.AMORTSTOPDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND B.MASTERID = A.MASTERID 
        UNION
        SELECT MASTERID                
        FROM ' || V_TABLEINSERT4 || '
        WHERE TOTAL_AMT = 0                
        OR TOTAL_AMT_ACRU = 0 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T1' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T1' || ' (MASTERID) 
        SELECT DISTINCT MASTERID                
        FROM ' || V_TABLEINSERT3 || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                
        AND DO_AMORT = ''Y'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T3' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T3' || ' 
        SELECT MASTERID 
        FROM ' || 'TMP_T2' || '
        WHERE MASTERID NOT IN (                
            SELECT MASTERID                
            FROM ' || 'TMP_T1' || '
        ) ';
    EXECUTE (V_STR_QUERY);

    IF V_PARAM_DISABLE_ACCRU_PREV = 0
    THEN 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_P1' || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_P1' || ' (ID)
            SELECT MAX(ID) AS ID                
            FROM ' || V_TABLEINSERT3 || '
            WHERE MASTERID IN (                
                SELECT MASTERID                
                FROM ' || 'TMP_T3' || '
            )                
            AND DO_AMORT = ''N''                
            AND DOWNLOAD_DATE < ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND DOWNLOAD_DATE >= ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE 
            GROUP BY MASTERID ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_TF' || '';
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
                        THEN - 1 * A.AMOUNT                
                        ELSE A.AMOUNT                
                    END AS N_AMOUNT                
                    ,A.ECFDATE DOWNLOAD_DATE                
                    ,A.MASTERID                
                FROM ' || V_TABLEINSERT6 || ' A                
                WHERE A.MASTERID IN (                
                    SELECT MASTERID                
                    FROM ' || 'TMP_T3' || '
                )                
                    AND A.STATUS = ''ACT''                
                    AND A.FLAG_CF = ''F''                
                    AND A.METHOD = ''EIR''                
            ) A                
            GROUP BY 
                A.DOWNLOAD_DATE                
                ,A.MASTERID ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_TC' || '';
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
                        THEN - 1 * A.AMOUNT                
                        ELSE A.AMOUNT                
                    END AS N_AMOUNT                
                    ,A.ECFDATE DOWNLOAD_DATE                
                    ,A.MASTERID                
                FROM ' || V_TABLEINSERT6 || ' A                
                WHERE A.MASTERID IN (                
                    SELECT MASTERID                
                    FROM ' || 'TMP_T3' || '
                )                
                    AND A.STATUS = ''ACT''                
                    AND A.FLAG_CF = ''C''                
                    AND A.METHOD = ''EIR''                
            ) A                
            GROUP BY 
                A.DOWNLOAD_DATE                
                ,A.MASTERID ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
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
                ,METHOD                
            ) SELECT 
                A.FACNO                
                ,A.CIFNO                
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
                ,A.ECFDATE                
                ,A.DATASOURCE                
                ,B.PRDCODE                
                ,B.TRXCODE                
                ,B.CCY                
                ,ROUND(CAST(CAST(CASE                 
                    WHEN B.FLAG_REVERSE = ''Y''                
                        THEN - 1 * B.AMOUNT                
                    ELSE B.AMOUNT                
                    END AS FLOAT) / NULLIF(CAST(C.SUM_AMT AS FLOAT), 0) AS DECIMAL(32, 20)) * A.N_ACCRU_FEE, ' || V_ROUND || ') AS N_AMOUNT                
                ,B.STATUS                
                ,CURRENT_TIMESTAMP                
                ,A.ACCTNO                
                ,A.MASTERID                
                ,B.FLAG_CF                
                ,''N''                
                ,NULL AS AMORTDATE                
                ,''ECF''                
                ,B.ORG_CCY                
                ,B.ORG_CCY_EXRATE                
                ,B.PRDTYPE                
                ,B.CF_ID                
                ,B.METHOD                
            FROM ' || V_TABLEINSERT3 || ' A                
            JOIN ' || V_TABLEINSERT6 || ' B 
                ON B.ECFDATE = A.ECFDATE                
                AND A.MASTERID = B.MASTERID                
                AND B.FLAG_CF = ''F'' AND B.STATUS = ''ACT''               
            JOIN ' || 'TMP_TF' || ' C 
                ON C.DOWNLOAD_DATE = A.ECFDATE                
                AND C.MASTERID = A.MASTERID                
            --20160407 EIR STOP REV                          
            LEFT JOIN (                
                SELECT DISTINCT MASTERID                
                FROM ' || V_TABLEINSERT18 || '
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
            ) D ON A.MASTERID = D.MASTERID                
            WHERE A.ID IN (                
                SELECT ID                
                FROM ' || 'TMP_P1' || '
            )                
            --20160407 EIR STOP REV                          
            AND D.MASTERID IS NULL                
            --20180108 EXCLUDE CF REV AND ITS PAIR                          
            AND B.CF_ID NOT IN (                
                SELECT CF_ID                
                FROM ' || V_TABLEINSERT1 || '
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
                AND FLAG_REVERSE = ''Y''                
                AND CF_ID_REV IS NOT NULL                
                                
                UNION ALL                
                                
                SELECT CF_ID_REV                
                FROM ' || V_TABLEINSERT1 || '
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
                AND FLAG_REVERSE = ''Y''                
                AND CF_ID_REV IS NOT NULL                
            ) ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
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
                ,METHOD                
            ) SELECT 
                A.FACNO                
                ,A.CIFNO                
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
                ,A.ECFDATE                
                ,A.DATASOURCE                
                ,B.PRDCODE                
                ,B.TRXCODE                
                ,B.CCY                
                ,ROUND(CAST(CAST(CASE                 
                    WHEN B.FLAG_REVERSE = ''Y''                
                        THEN - 1 * B.AMOUNT                
                    ELSE B.AMOUNT                
                    END AS FLOAT) / NULLIF(CAST(C.SUM_AMT AS FLOAT), 0) AS DECIMAL(32, 20)) * A.N_ACCRU_COST, ' || V_ROUND || ') AS N_AMOUNT                
                ,B.STATUS                
                ,CURRENT_TIMESTAMP                
                ,A.ACCTNO                
                ,A.MASTERID                
                ,B.FLAG_CF                
                ,''N''                
                ,NULL AS AMORTDATE                
                ,''ECF''                
                ,B.ORG_CCY                
                ,B.ORG_CCY_EXRATE                
                ,B.PRDTYPE                
                ,B.CF_ID                
                ,B.METHOD                
            FROM ' || V_TABLEINSERT3 || ' A                
            JOIN ' || V_TABLEINSERT6 || ' B 
                ON B.ECFDATE = A.ECFDATE                
                AND A.MASTERID = B.MASTERID                
                AND B.FLAG_CF = ''C'' AND B.STATUS = ''ACT''               
            JOIN ' || 'TMP_TC' || ' C 
                ON C.DOWNLOAD_DATE = A.ECFDATE                
                AND C.MASTERID = A.MASTERID                
            --20160407 EIR STOP REV                          
            LEFT JOIN (                
                SELECT DISTINCT MASTERID                
                FROM ' || V_TABLEINSERT18 || '
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
            ) D ON A.MASTERID = D.MASTERID                
            WHERE A.ID IN (                
                SELECT ID                
                FROM ' || 'TMP_P1' || '
            )                
            --20160407 EIR STOP REV                          
            AND D.MASTERID IS NULL                
            --20180108 EXCLUDE CF REV AND ITS PAIR                          
            AND B.CF_ID NOT IN (                
                SELECT CF_ID                
                FROM ' || V_TABLEINSERT1 || '
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
                AND FLAG_REVERSE = ''Y''                
                AND CF_ID_REV IS NOT NULL                
                                
                UNION ALL                
                                
                SELECT CF_ID_REV                
                FROM ' || V_TABLEINSERT1 || '
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
                AND FLAG_REVERSE = ''Y''                
                AND CF_ID_REV IS NOT NULL                
            ) ';
        EXECUTE (V_STR_QUERY);
    END IF;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' A 
        SET STATUS = TO_CHAR(''' || V_CURRDATE || '''::DATE, ''YYYYMMDD'') 
        WHERE STATUS = ''ACT''                
        AND MASTERID IN (                
            SELECT MASTERID                
            FROM ' || V_TABLEINSERT4 || '
            WHERE TOTAL_AMT = 0                
            OR TOTAL_AMT_ACRU = 0                
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_TF' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_TF' || ' 
        (                
            SUM_AMT                
            ,DOWNLOAD_DATE                
            ,MASTERID                
        )                
        SELECT 
            SUM(A.N_AMOUNT) AS SUM_AMT                
            ,A.DOWNLOAD_DATE                
            ,A.MASTERID                
        FROM (                
            SELECT 
                CASE                 
                    WHEN A.FLAG_REVERSE = ''Y''                
                    THEN - 1 * A.AMOUNT                
                    ELSE A.AMOUNT                
                END AS N_AMOUNT                
                ,A.ECFDATE DOWNLOAD_DATE                
                ,A.MASTERID                
            FROM ' || V_TABLEINSERT6 || ' A                
            WHERE A.ECFDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
            AND A.STATUS = ''ACT''                
            AND A.FLAG_CF = ''F''                
            AND A.METHOD = ''EIR''                
        ) A                
        GROUP BY 
            A.DOWNLOAD_DATE                
            ,A.MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_TC' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_TC' || ' 
        (                
            SUM_AMT                
            ,DOWNLOAD_DATE                
            ,MASTERID                
        )                
        SELECT 
            SUM(A.N_AMOUNT) AS SUM_AMT                
            ,A.DOWNLOAD_DATE                
            ,A.MASTERID                
        FROM (                
            SELECT 
                CASE                 
                    WHEN A.FLAG_REVERSE = ''Y''                
                    THEN - 1 * A.AMOUNT                
                    ELSE A.AMOUNT                
                END AS N_AMOUNT                
                ,A.ECFDATE DOWNLOAD_DATE                
                ,A.MASTERID                
            FROM ' || V_TABLEINSERT6 || ' A                
            WHERE A.ECFDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE  
            AND A.STATUS = ''ACT''                
            AND A.FLAG_CF = ''C''                
            AND A.METHOD = ''EIR''                
        ) A                
        GROUP BY 
            A.DOWNLOAD_DATE                
            ,A.MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT13 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT13 || ' 
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
            ,METHOD                
        ) SELECT 
            IMA.FACILITY_NUMBER                
            ,IMA.CUSTOMER_NUMBER                
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE        
            ,IMA.DATA_SOURCE                
            ,B.PRDCODE                
            ,B.TRXCODE                
            ,B.CCY                
            ,- 1 * --20180417 GAIN LOSS DIBALIK                    
            ROUND(CAST(CAST(CASE                 
                WHEN B.FLAG_REVERSE = ''Y''
                THEN - 1 * B.AMOUNT                
                ELSE B.AMOUNT                
                END AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS DECIMAL(32, 20)) * A.GAIN_LOSS_FEE_AMT, ' || V_ROUND || ') AS N_AMOUNT                
            ,B.STATUS                
            ,CURRENT_TIMESTAMP                
            ,IMA.ACCOUNT_NUMBER                
            ,A.MASTERID                
            ,B.FLAG_CF                
            ,''N''                
            ,NULL AS AMORTDATE                
            ,''ECF''                
            ,B.ORG_CCY                
            ,B.ORG_CCY_EXRATE                
            ,B.PRDTYPE                
            ,B.CF_ID                
            ,B.METHOD                
        FROM ' || V_TABLEINSERT4 || ' A                
        JOIN ' || V_TABLEINSERT23 || ' IMA 
            ON IMA.MASTERID = A.MASTERID                
        JOIN ' || V_TABLEINSERT6 || ' B 
            ON B.ECFDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
            AND A.MASTERID = B.MASTERID                
            AND B.FLAG_CF = ''F''                
        JOIN ' || 'TMP_TF' || ' C 
            ON C.MASTERID = A.MASTERID                
        WHERE COALESCE(A.GAIN_LOSS_AMT, 0) <> 0 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT13 || ' 
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
            ,METHOD                
        ) SELECT 
            IMA.FACILITY_NUMBER                
            ,IMA.CUSTOMER_NUMBER                
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE        
            ,IMA.DATA_SOURCE                
            ,B.PRDCODE                
            ,B.TRXCODE                
            ,B.CCY                
            ,- 1 * --20180417 GAIN LOSS DIBALIK                    
            ROUND(CAST(CAST(CASE                 
                WHEN B.FLAG_REVERSE = ''Y''
                THEN - 1 * B.AMOUNT                
                ELSE B.AMOUNT                
                END AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS DECIMAL(32, 20)) * A.GAIN_LOSS_COST_AMT, ' || V_ROUND || ') AS N_AMOUNT                
            ,B.STATUS                
            ,CURRENT_TIMESTAMP                
            ,IMA.ACCOUNT_NUMBER                
            ,A.MASTERID                
            ,B.FLAG_CF                
            ,''N''                
            ,NULL AS AMORTDATE                
            ,''ECF''                
            ,B.ORG_CCY                
            ,B.ORG_CCY_EXRATE                
            ,B.PRDTYPE                
            ,B.CF_ID                
            ,B.METHOD                
        FROM ' || V_TABLEINSERT4 || ' A                
        JOIN ' || V_TABLEINSERT23 || ' IMA 
            ON IMA.MASTERID = A.MASTERID                
        JOIN ' || V_TABLEINSERT6 || ' B 
            ON B.ECFDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                 
            AND A.MASTERID = B.MASTERID                
            AND B.FLAG_CF = ''C''                
        JOIN ' || 'TMP_TC' || ' C 
            ON C.MASTERID = A.MASTERID                
        WHERE COALESCE(A.GAIN_LOSS_AMT, 0) <> 0 ';
    EXECUTE (V_STR_QUERY);

    /*
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TEMP_TFC';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TEMP_TFC' || ' AS 
        SELECT MASTERID        
        ,SUM(CASE WHEN FLAG_CF = ''F'' THEN AMOUNT ELSE 0 END) AS TFEE        
        ,SUM(CASE WHEN FLAG_CF = ''C'' THEN AMOUNT ELSE 0 END) AS TCOST        
        FROM ' || 'V_TABLEINSERT6' || ' 
        WHERE ECFDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        GROUP BY MASTERID ';
    EXECUTE (V_STR_QUERY);
    */

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' C 
        SET AMOUNT = ((A.FEE_AMT + A.GAIN_LOSS_FEE_AMT) / A.FEE_AMT) * AMOUNT                 
        FROM ' || V_TABLEINSERT4 || ' A                
        WHERE A.MASTERID = C.MASTERID                
        AND C.ECFDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND COALESCE(A.GAIN_LOSS_FEE_AMT, 0) <> 0                
        AND C.FLAG_CF = ''F'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' C 
        SET AMOUNT = ((A.COST_AMT + A.GAIN_LOSS_COST_AMT) / A.COST_AMT) * AMOUNT                
        FROM ' || V_TABLEINSERT4 || ' A                
        WHERE A.MASTERID = C.MASTERID                
        AND C.ECFDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND COALESCE(A.GAIN_LOSS_COST_AMT, 0) <> 0                
        AND C.FLAG_CF = ''C'' ';
    EXECUTE (V_STR_QUERY);

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_LBM_ACCT_EIR_ECF_MAIN', '');

    RAISE NOTICE 'SP_IFRS_LBM_ACCT_EIR_ECF_MAIN | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT4;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_LBM_ACCT_EIR_ECF_MAIN';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT4 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;