---- DROP PROCEDURE SP_IFRS_NOMINATIF;

CREATE OR REPLACE PROCEDURE SP_IFRS_NOMINATIF(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000',
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S')
LANGUAGE PLPGSQL AS $$
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
        V_TABLEINSERT1 := 'IFRS_LOAN_REPORT_RECON_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_NOMINATIF_MONTHLY_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_PRODUCT_PARAM_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_LOAN_REPORT_RECON';
        V_TABLEINSERT2 := 'IFRS_NOMINATIF_MONTHLY';
        V_TABLEINSERT3 := 'IFRS_PRODUCT_PARAM';
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
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT2 || ' AS SELECT * FROM IFRS_NOMINATIF_MONTHLY WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_NOMINATIF', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_LOAN_REPORT' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_LOAN_REPORT' || ' 
        (    
            MASTERID    
            ,ACCOUNT_NUMBER    
            ,FACILITY_NUMBER    
            ,CUSTOMER_NAME    
            ,DATA_SOURCE    
            ,PRODUCT_CODE    
            ,CCY    
            ,EXCHANGE_RATE    
            ,INTEREST_RATE    
            ,EIR    
            ,BRANCH_CODE    
            ,OUTSTANDING    
            ,AMORT_TYPE    
            ,INITIAL_FEE    
            ,INITIAL_COST    
            ,UNAMORT_FEE    
            ,UNAMORT_COST  
            ,AMORT_FEE    
            ,AMORT_COST  
            ,ACCUMULATIVE_ACCRUED  
            ,UNAMORT_AMT_TOTAL  
            ,AMORT_FEE_MTD    
            ,AMORT_COST_MTD    
            ,AMORT_FEE_YTD    
            ,AMORT_COST_YTD    
        ) SELECT 
            A.MASTERID    
            ,A.ACCOUNT_NUMBER    
            ,A.FACILITY_NUMBER    
            ,A.CUSTOMER_NAME    
            ,A.DATA_SOURCE    
            ,A.PRODUCT_CODE    
            ,A.CCY    
            ,A.EXCHANGE_RATE    
            ,A.INTEREST_RATE    
            ,A.EIR    
            ,A.BRANCH_CODE    
            ,A.OUTSTANDING    
            ,A.METHOD    
            ,SUM(A.INITIAL_TRX_FEE_AMT) AS INITIAL_FEE    
            ,SUM(A.INITIAL_TRX_COST_AMT) AS INITIAL_COST    
            ,SUM(A.UNAMORT_GL_FEE_AMT) AS UNAMORT_FEE    
            ,SUM(A.UNAMORT_GL_COST_AMT) AS UNAMORT_COST    
            ,SUM(A.AMORT_GL_FEE_AMT) AS AMORT_FEE    
            ,SUM(A.AMORT_GL_COST_AMT) AS AMORT_COST  
            ,A.AMORT_GL_COST_AMT + A.AMORT_GL_FEE_AMT AS ACCUMULATIVE_ACCRUED  
            ,A.UNAMORT_GL_COST_AMT + A.UNAMORT_GL_FEE_AMT AS UNAMORT_AMT_TOTAL  
            ,SUM(A.MTD_AMORT_GL_FEE_AMT) AS AMORT_FEE_MTD    
            ,SUM(A.MTD_AMORT_GL_COST_AMT) AS AMORT_COST_MTD    
            ,SUM(A.YTD_AMORT_GL_FEE_AMT) AS AMORT_FEE_YTD    
            ,SUM(A.YTD_AMORT_GL_COST_AMT) AS AMORT_COST_YTD    
        FROM ' || V_TABLEINSERT1 || ' A    
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE     
        GROUP BY 
            A.MASTERID    
            ,A.ACCOUNT_NUMBER    
            ,A.FACILITY_NUMBER    
            ,A.CUSTOMER_NAME    
            ,A.DATA_SOURCE    
            ,A.PRODUCT_CODE    
            ,A.CCY    
            ,A.EXCHANGE_RATE    
            ,A.INTEREST_RATE    
            ,A.EIR    
            ,A.OUTSTANDING    
            ,A.BRANCH_CODE    
            ,A.METHOD  
            ,A.AMORT_GL_COST_AMT  
            ,A.AMORT_GL_FEE_AMT  
            ,A.UNAMORT_GL_COST_AMT  
            ,A.UNAMORT_GL_FEE_AMT ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT2 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRMONTH AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
        (    
            DOWNLOAD_DATE    
            ,MASTERID    
            ,ACCOUNT_NUMBER    
            ,FACILITY_NUMBER    
            ,CUSTOMER_NUMBER    
            ,CUSTOMER_NAME    
            ,DATA_SOURCE    
            ,PRODUCT_TYPE    
            ,PRODUCT_CODE    
            ,MASTER_ACCOUNT_CODE  
            ,IIP_AMOUNT  
            ,PIP_AMOUNT  
            ,BRANCH_CODE    
            ,ACCOUNT_STATUS    
            ,STAFF_LOAN_FLAG    
            ,CURRENCY    
            ,EXCHANGE_RATE    
            ,INTEREST_RATE    
            ,EIR    
            ,OUTSTANDING    
            ,FAIR_VALUE    
            ,INITIAL_FEE    
            ,INITIAL_COST    
            ,UNAMORT_FEE    
            ,UNAMORT_COST    
            ,AMORT_FEE    
            ,AMORT_COST    
            ,AMORT_FEE_MTD    
            ,AMORT_COST_MTD    
            ,AMORT_FEE_YTD    
            ,AMORT_COST_YTD    
            ,BI_COLLECTABILITY    
            ,DAY_PAST_DUE    
            ,IMPAIRED_FLAG    
            ,SEGMENT    
            ,BUCKET_ID    
            ,EAD    
            ,CKPN_AMOUNT    
            ,UNWINDING_AMOUNT    
            ,BEGINNING_BALANCE    
            ,ENDING_BALANCE    
            ,WRITEBACK    
            ,CHARGE    
            ,AMORT_TYPE    
            ,INTEREST_ACCRUED  
        ) SELECT 
            ''' || CAST(V_CURRMONTH AS VARCHAR(10)) || '''::DATE 
            ,IMA.MASTERID    
            ,IMA.ACCOUNT_NUMBER    
            ,IMA.FACILITY_NUMBER    
            ,IMA.CUSTOMER_NUMBER    
            ,IMA.CUSTOMER_NAME    
            ,IMA.DATA_SOURCE    
            ,IMA.PRODUCT_TYPE    
            ,IMA.PRODUCT_CODE    
            ,IMA.MASTER_ACCOUNT_CODE  
            ,CASE 
                WHEN IMA.IMPAIRED_FLAG = ''I''  
                THEN IMA.ECL_AMOUNT  
                ELSE 0  
            END AS IIP_AMOUNT  
            ,CASE 
                WHEN IMA.IMPAIRED_FLAG = ''C''  
                THEN IMA.ECL_AMOUNT  
                ELSE 0  
            END AS PIP_AMOUNT  
            ,IMA.BRANCH_CODE    
            ,IMA.ACCOUNT_STATUS    
            ,IMA.STAFF_LOAN_FLAG    
            ,IMA.CURRENCY    
            ,IMA.EXCHANGE_RATE    
            ,IMA.INTEREST_RATE    
            ,IMA.EIR    
            ,IMA.OUTSTANDING  
            ,IMA.FAIR_VALUE_AMOUNT AS FAIR_VALUE_AMOUNT    
            ,CASE     
                WHEN IMA.IAS_CLASS = ''L''    
                THEN COALESCE(ILR.INITIAL_FEE, 0)    
                ELSE COALESCE(ILR.INITIAL_FEE, 0) * - 1    
            END AS INITIAL_FEE   
            ,CASE     
                WHEN IMA.IAS_CLASS = ''L''    
                THEN COALESCE(ILR.INITIAL_COST, 0) * - 1    
                ELSE COALESCE(ILR.INITIAL_COST, 0)    
            END AS INITIAL_COST   
            ,CASE     
                WHEN IMA.IAS_CLASS = ''L''    
                THEN COALESCE(ILR.UNAMORT_FEE, 0)    
                ELSE COALESCE(ILR.UNAMORT_FEE, 0) * - 1    
            END AS UNAMORT_FEE   
            ,CASE     
                WHEN IMA.IAS_CLASS = ''L''    
                THEN COALESCE(ILR.UNAMORT_COST, 0) * - 1    
                ELSE COALESCE(ILR.UNAMORT_COST, 0)    
            END AS UNAMORT_COST   
            ,COALESCE(ILR.AMORT_FEE, 0) AS AMORT_FEE    
            ,COALESCE(ILR.AMORT_COST, 0) * - 1 AS AMORT_COST   
            ,COALESCE(ILR.AMORT_FEE_MTD, 0) AS AMORT_FEE_MTD   
            ,COALESCE(ILR.AMORT_COST_MTD, 0) * - 1 AS AMORT_COST_MTD   
            ,COALESCE(ILR.AMORT_FEE_YTD, 0) AS AMORT_FEE_YTD   
            ,COALESCE(ILR.AMORT_COST_YTD, 0) * - 1 AS AMORT_COST_YTD   
            ,IMA.BI_COLLECTABILITY    
            ,IMA.DAY_PAST_DUE    
            ,IMA.IMPAIRED_FLAG    
            ,IMA.SEGMENT    
            ,IMA.BUCKET_ID    
            ,COALESCE(IMA.FAIR_VALUE_AMOUNT, IMA.OUTSTANDING) AS EAD    
            ,COALESCE(IMA.ECL_AMOUNT, 0) AS CKPN_AMOUNT    
            ,CASE     
                WHEN IMA.IMPAIRED_FLAG = ''C''    
                THEN COALESCE(IMA.CA_UNWINDING_AMOUNT, 0)    
                ELSE COALESCE(IMA.IA_UNWINDING_SUM_AMOUNT, 0)    
            END AS UNWINDING_AMOUNT    
            ,IMA.BEGINNING_BALANCE    
            ,IMA.ENDING_BALANCE     
            ,IMA.WRITEBACK_AMOUNT    
            ,IMA.CHARGE_AMOUNT    
            ,IMA.AMORT_TYPE  
            ,COALESCE(ILR.ACCUMULATIVE_ACCRUED, 0)  
        FROM ' || V_TABLENAME || ' IMA    
        LEFT JOIN ' || 'TMP_LOAN_REPORT' || ' ILR 
            ON IMA.MASTERID = ILR.MASTERID  
        WHERE IMA.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE     
            AND IMA.PRODUCT_GROUP <> ''STAFF_LOAN'' ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
        (    
            DOWNLOAD_DATE    
            ,MASTERID    
            ,ACCOUNT_NUMBER    
            ,FACILITY_NUMBER    
            ,CUSTOMER_NAME    
            ,DATA_SOURCE    
            ,PRODUCT_TYPE    
            ,PRODUCT_CODE    
            ,BRANCH_CODE    
            ,ACCOUNT_STATUS    
            ,CURRENCY    
            ,EXCHANGE_RATE    
            ,INTEREST_RATE    
            ,EIR    
            ,OUTSTANDING  
            ,FAIR_VALUE  
            ,INITIAL_FEE    
            ,INITIAL_COST    
            ,UNAMORT_FEE    
            ,UNAMORT_COST    
            ,AMORT_FEE    
            ,AMORT_COST  
            ,AMORT_FEE_MTD    
            ,AMORT_COST_MTD    
            ,AMORT_FEE_YTD    
            ,AMORT_COST_YTD    
            ,AMORT_TYPE    
            ,INTEREST_ACCRUED  
        ) SELECT 
            ''' || CAST(V_CURRMONTH AS VARCHAR(10)) || '''::DATE 
            ,ILR.MASTERID    
            ,ILR.ACCOUNT_NUMBER    
            ,ILR.FACILITY_NUMBER    
            ,ILR.CUSTOMER_NAME    
            ,ILR.DATA_SOURCE    
            ,IMP.PRD_TYPE    
            ,ILR.PRODUCT_CODE    
            ,ILR.BRANCH_CODE    
            ,''C'' AS ACCOUNT_STATUS    
            ,ILR.CCY    
            ,ILR.EXCHANGE_RATE    
            ,ILR.INTEREST_RATE    
            ,ILR.EIR    
            ,ILR.OUTSTANDING  
            ,CASE 
                WHEN CCD.VALUE1 IS NULL  
                THEN COALESCE(ILR.OUTSTANDING, 0) + COALESCE(ILR.UNAMORT_AMT_TOTAL, 0)  
                ELSE ILR.OUTSTANDING  
            END AS FAIR_VALUE_AMOUNT  
            ,CASE     
                WHEN IMP.FLAG_AL = ''L''    
                THEN COALESCE(ILR.INITIAL_FEE, 0)    
                ELSE COALESCE(ILR.INITIAL_FEE, 0) * - 1    
            END    
            ,CASE     
                WHEN IMP.FLAG_AL = ''L''    
                THEN COALESCE(ILR.INITIAL_COST, 0) * - 1    
                ELSE COALESCE(ILR.INITIAL_COST, 0)    
            END    
            ,CASE     
                WHEN IMP.FLAG_AL = ''L''    
                THEN COALESCE(ILR.UNAMORT_FEE, 0)    
                ELSE COALESCE(ILR.UNAMORT_FEE, 0) * - 1    
            END    
            ,CASE     
                WHEN IMP.FLAG_AL = ''L''    
                THEN COALESCE(ILR.UNAMORT_COST, 0) * - 1    
                ELSE COALESCE(ILR.UNAMORT_COST, 0)    
            END    
            ,COALESCE(ILR.AMORT_FEE, 0)    
            ,COALESCE(ILR.AMORT_COST, 0) * - 1    
            ,COALESCE(ILR.AMORT_FEE_MTD, 0)    
            ,COALESCE(ILR.AMORT_COST_MTD, 0) * - 1    
            ,COALESCE(ILR.AMORT_FEE_YTD, 0)    
            ,COALESCE(ILR.AMORT_COST_YTD, 0) * - 1    
            ,ILR.AMORT_TYPE    
            ,COALESCE(ILR.ACCUMULATIVE_ACCRUED, 0)  
        FROM ' || 'TMP_LOAN_REPORT' || ' ILR    
        LEFT JOIN ' || V_TABLEINSERT3 || ' IMP 
            ON ILR.DATA_SOURCE = IMP.DATA_SOURCE 
            AND ILR.PRODUCT_CODE = IMP.PRD_CODE  
        LEFT JOIN TBLM_COMMONCODEDETAIL CCD 
            ON COMMONCODE = ''S1022'' 
            AND ILR.DATA_SOURCE = CCD.VALUE1 
            AND ILR.PRODUCT_CODE = CCD.VALUE2  
        WHERE ILR.MASTERID NOT IN (    
            SELECT MASTERID    
            FROM ' || V_TABLENAME || '
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE     
        )    
        AND IMP.PRD_TYPE <> ''STAFF_LOAN'' ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_NOMINATIF', '');

    RAISE NOTICE 'SP_IFRS_NOMINATIF | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT2;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_NOMINATIF';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT2 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;