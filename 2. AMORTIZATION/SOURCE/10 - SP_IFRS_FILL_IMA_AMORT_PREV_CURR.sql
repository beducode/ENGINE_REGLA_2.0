CREATE OR REPLACE PROCEDURESP_IFRS_FILL_IMA_AMORT_PREV_CURR(IN P_RUNID CHARACTER VARYING DEFAULT 'S_00000_0000'::CHARACTER VARYING, IN P_DOWNLOAD_DATE DATE DEFAULT NULL::DATE, IN P_PRC CHARACTER VARYING DEFAULT 'S'::CHARACTER VARYING)
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
        V_TABLEINSERT1 := 'IFRS_IMA_AMORT_PREV_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_IMA_AMORT_CURR_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_IMA_AMORT_PREV';
        V_TABLEINSERT2 := 'IFRS_IMA_AMORT_CURR';
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
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM IFRS_IMA_AMORT_PREV WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT2 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT2 || ' AS SELECT * FROM IFRS_IMA_AMORT_CURR WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_FILL_IMA_AMORT_PREV_CURR', '');

    ---- CLEAN UP
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT1 || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT2 || ' ';
    EXECUTE (V_STR_QUERY);

    ---- FILL IMA_AMORT_PREV
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_FILL_IMA_AMORT_PREV_CURR', 'FILL IMA_AMORT_PREV');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (
            DOWNLOAD_DATE 
            ,MASTERID 
            ,PRODUCT_ENTITY 
            ,JF_FLAG 
            ,DATA_SOURCE 
            ,PRODUCT_TYPE 
            ,PRODUCT_CODE 
            ,PRODUCT_GROUP 
            ,BRANCH_CODE 
            ,CURRENCY 
            ,ACCOUNT_NUMBER 
            ,PREVIOUS_ACCOUNT_NUMBER 
            ,CUSTOMER_NUMBER 
            ,CUSTOMER_NAME 
            ,GLOBAL_CUSTOMER_NUMBER 
            ,EXCHANGE_RATE 
            ,MARKET_RATE 
            ,INITIAL_OUTSTANDING 
            ,OUTSTANDING 
            ,OUTSTANDING_JF 
            ,OUTSTANDING_BANK 
            ,OUTSTANDING_PASTDUE 
            ,OUTSTANDING_IDC 
            ,PLAFOND 
            ,PLAFOND_CASH 
            ,UNUSED_AMOUNT 
            ,DOWN_PAYMENT_AMOUNT 
            ,INTEREST_RATE 
            ,ACCOUNT_STATUS 
            ,FACILITY_NUMBER 
            ,LOAN_START_DATE 
            ,LOAN_DUE_DATE 
            ,INSTALLMENT_GRACE_PERIOD 
            ,LOAN_START_AMORTIZATION 
            ,LOAN_END_AMORTIZATION 
            ,AMORT_TYPE 
            ,NEXT_PAYMENT_DATE 
            ,LAST_PAYMENT_DATE 
            ,FIRST_INSTALLMENT_DATE 
            ,TENOR 
            ,PAYMENT_CODE 
            ,PAYMENT_TERM 
            ,INTEREST_PAYMENT_TERM 
            ,EIR_STATUS 
            ,ECF_STATUS 
            ,INTEREST_CALCULATION_CODE 
            ,EIR 
            ,EIR_AMOUNT 
            ,FAIR_VALUE_AMOUNT 
            ,UNAMORT_COST_AMT 
            ,UNAMORT_FEE_AMT 
            ,DAILY_AMORT_AMT 
            ,DAY_PAST_DUE 
            ,ORIGINAL_COLLECTABILITY 
            ,BI_COLLECTABILITY 
            ,GL_CONSTNAME 
            ,SEGMENT 
            ,PD_SEGMENT 
            ,LGD_SEGMENT 
            ,IMPAIRED_FLAG 
            ,IS_IMPAIRED 
            ,CA_UNWINDING_AMOUNT 
            ,IA_UNWINDING_AMOUNT 
            ,BEGINNING_BALANCE 
            ,ENDING_BALANCE 
            ,WRITEBACK 
            ,CHARGE 
            ,WRITEOFF_FLAG 
            ,WRITEOFF_DATE 
            ,OUTSTANDING_WO 
            ,RESTRUCTURE_DATE 
            ,RESTRUCTURE_FLAG 
            ,STAFF_LOAN_FLAG 
            ,BELOW_MARKET_FLAG 
            ,COMMITTED_FLAG 
            ,INSTALLMENT_AMOUNT 
            ,INITIAL_UNAMORT_TXN_COST 
            ,INITIAL_UNAMORT_ORG_FEE 
            ,IAS_CLASS 
            ,REVOLVING_FLAG 
            ,BUCKET_ID 
            ,PRODUCT_TYPE_GROUP 
            ,NPL_DATE 
            ,NPL_FLAG 
            ,BRANCH_CODE_OPEN 
            ,UNAMORT_AMT_TOTAL_JF 
            ,UNAMORT_FEE_AMT_JF 
            ,UNAMORT_COST_AMT_JF 
            ,IFRS9_CLASS 
            ,SPPI_RESULT 
            ,BM_RESULT 
            ,INTEREST_ACCRUED 
            ,ACCOUNT_TYPE 
            ,CUSTOMER_TYPE 
            ,PRODUCT_TYPE_1 
            ,CCF 
            ,CCF_RULE_ID 
            ,CCF_EFF_DATE 
            ,ECL_AMOUNT 
            ,ECL_AMOUNT_BFL 
            ,AVG_EIR 
            ,ECL_MODEL_ID 
            ,SEGMENTATION_ID 
            ,PD_ME_MODEL_ID 
            ,DEFAULT_RULE_ID 
            ,DPD_CIF 
            ,BI_COLLECT_CIF 
            ,RESTRUCTURE_COLLECT_FLAG 
            ,PLAFOND_CIF 
            ,RATING_CODE 
            ,SOURCE_SYSTEM 
            ,LOB_CODE 
            ,EARLY_PAYMENT 
            ,EARLY_PAYMENT_FLAG 
            ,SEGMENT_FLAG 
        ) SELECT 
            DOWNLOAD_DATE 
            ,MASTERID 
            ,PRODUCT_ENTITY 
            ,JF_FLAG 
            ,DATA_SOURCE 
            ,PRODUCT_TYPE 
            ,PRODUCT_CODE 
            ,PRODUCT_GROUP 
            ,BRANCH_CODE 
            ,CURRENCY 
            ,ACCOUNT_NUMBER 
            ,PREVIOUS_ACCOUNT_NUMBER 
            ,CUSTOMER_NUMBER 
            ,CUSTOMER_NAME 
            ,GLOBAL_CUSTOMER_NUMBER 
            ,EXCHANGE_RATE 
            ,MARKET_RATE 
            ,INITIAL_OUTSTANDING 
            ,OUTSTANDING 
            ,OUTSTANDING_JF 
            ,OUTSTANDING_BANK 
            ,OUTSTANDING_PASTDUE 
            ,OUTSTANDING_IDC 
            ,PLAFOND 
            ,PLAFOND_CASH 
            ,UNUSED_AMOUNT 
            ,DOWN_PAYMENT_AMOUNT 
            ,INTEREST_RATE 
            ,ACCOUNT_STATUS 
            ,FACILITY_NUMBER 
            ,LOAN_START_DATE 
            ,LOAN_DUE_DATE 
            ,INSTALLMENT_GRACE_PERIOD 
            ,LOAN_START_AMORTIZATION 
            ,LOAN_END_AMORTIZATION 
            ,AMORT_TYPE 
            ,NEXT_PAYMENT_DATE 
            ,LAST_PAYMENT_DATE 
            ,FIRST_INSTALLMENT_DATE 
            ,TENOR 
            ,PAYMENT_CODE 
            ,PAYMENT_TERM 
            ,INTEREST_PAYMENT_TERM 
            ,EIR_STATUS 
            ,ECF_STATUS 
            ,INTEREST_CALCULATION_CODE 
            ,EIR 
            ,EIR_AMOUNT 
            ,FAIR_VALUE_AMOUNT 
            ,UNAMORT_COST_AMT 
            ,UNAMORT_FEE_AMT 
            ,DAILY_AMORT_AMT 
            ,DAY_PAST_DUE 
            ,ORIGINAL_COLLECTABILITY 
            ,BI_COLLECTABILITY 
            ,GL_CONSTNAME 
            ,SEGMENT 
            ,PD_SEGMENT 
            ,LGD_SEGMENT 
            ,IMPAIRED_FLAG 
            ,IS_IMPAIRED 
            ,CA_UNWINDING_AMOUNT 
            ,IA_UNWINDING_AMOUNT 
            ,BEGINNING_BALANCE 
            ,ENDING_BALANCE 
            ,WRITEBACK_AMOUNT 
            ,CHARGE_AMOUNT 
            ,WRITEOFF_FLAG 
            ,WRITEOFF_DATE 
            ,OUTSTANDING_WO 
            ,RESTRUCTURE_DATE 
            ,RESTRUCTURE_FLAG 
            ,CASE WHEN STAFF_LOAN_FLAG = 1 THEN ''Y'' ELSE ''N'' END 
            ,BELOW_MARKET_FLAG 
            ,COMMITTED_FLAG 
            ,INSTALLMENT_AMOUNT 
            ,INITIAL_UNAMORT_TXN_COST 
            ,INITIAL_UNAMORT_ORG_FEE 
            ,IAS_CLASS 
            ,REVOLVING_FLAG 
            ,BUCKET_ID 
            ,NULL AS PRODUCT_TYPE_GROUP 
            ,NPL_DATE 
            ,NPL_FLAG 
            ,BRANCH_CODE_OPEN 
            ,UNAMORT_AMT_TOTAL_JF 
            ,UNAMORT_FEE_AMT_JF 
            ,UNAMORT_COST_AMT_JF 
            ,IFRS9_CLASS 
            ,SPPI_RESULT 
            ,BM_RESULT 
            ,INTEREST_ACCRUED 
            ,ACCOUNT_TYPE 
            ,CUSTOMER_TYPE 
            ,PRODUCT_TYPE_1 
            ,CCF::REAL 
            ,CCF_RULE_ID::INT 
            ,CCF_EFF_DATE::DATE 
            ,ECL_AMOUNT 
            ,ECL_AMOUNT_BFL 
            ,AVG_EIR 
            ,ECL_MODEL_ID 
            ,SEGMENTATION_ID 
            ,PD_ME_MODEL_ID 
            ,DEFAULT_RULE_ID 
            ,DPD_CIF 
            ,BI_COLLECT_CIF 
            ,RESTRUCTURE_COLLECT_FLAG 
            ,PLAFOND_CIF 
            ,RATING_CODE 
            ,SOURCE_SYSTEM 
            ,LOB_CODE 
            ,EARLY_PAYMENT 
            ,EARLY_PAYMENT_FLAG 
            ,SEGMENT_FLAG 
        FROM ' || V_TABLENAME || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    ---- FILL IMA_AMORT_CURR
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_FILL_IMA_AMORT_PREV_CURR', 'FILL IMA_AMORT_CURR');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
        (
            DOWNLOAD_DATE 
            ,MASTERID 
            ,PRODUCT_ENTITY 
            ,JF_FLAG 
            ,DATA_SOURCE 
            ,PRODUCT_TYPE 
            ,PRODUCT_CODE 
            ,PRODUCT_GROUP 
            ,BRANCH_CODE 
            ,CURRENCY 
            ,ACCOUNT_NUMBER 
            ,PREVIOUS_ACCOUNT_NUMBER 
            ,CUSTOMER_NUMBER 
            ,CUSTOMER_NAME 
            ,GLOBAL_CUSTOMER_NUMBER 
            ,EXCHANGE_RATE 
            ,MARKET_RATE 
            ,INITIAL_OUTSTANDING 
            ,OUTSTANDING 
            ,OUTSTANDING_JF 
            ,OUTSTANDING_BANK 
            ,OUTSTANDING_PASTDUE 
            ,OUTSTANDING_IDC 
            ,PLAFOND 
            ,PLAFOND_CASH 
            ,UNUSED_AMOUNT 
            ,DOWN_PAYMENT_AMOUNT 
            ,INTEREST_RATE 
            ,ACCOUNT_STATUS 
            ,FACILITY_NUMBER 
            ,LOAN_START_DATE 
            ,LOAN_DUE_DATE 
            ,INSTALLMENT_GRACE_PERIOD 
            ,LOAN_START_AMORTIZATION 
            ,LOAN_END_AMORTIZATION 
            ,AMORT_TYPE 
            ,NEXT_PAYMENT_DATE 
            ,LAST_PAYMENT_DATE 
            ,FIRST_INSTALLMENT_DATE 
            ,TENOR 
            ,PAYMENT_CODE 
            ,PAYMENT_TERM 
            ,INTEREST_PAYMENT_TERM 
            ,EIR_STATUS 
            ,ECF_STATUS 
            ,INTEREST_CALCULATION_CODE 
            ,EIR 
            ,EIR_AMOUNT 
            ,FAIR_VALUE_AMOUNT 
            ,UNAMORT_COST_AMT 
            ,UNAMORT_FEE_AMT 
            ,DAILY_AMORT_AMT 
            ,DAY_PAST_DUE 
            ,ORIGINAL_COLLECTABILITY 
            ,BI_COLLECTABILITY 
            ,GL_CONSTNAME 
            ,SEGMENT 
            ,PD_SEGMENT 
            ,LGD_SEGMENT 
            ,IMPAIRED_FLAG 
            ,IS_IMPAIRED 
            ,CA_UNWINDING_AMOUNT 
            ,IA_UNWINDING_AMOUNT 
            ,BEGINNING_BALANCE 
            ,ENDING_BALANCE 
            ,WRITEBACK 
            ,CHARGE 
            ,WRITEOFF_FLAG 
            ,WRITEOFF_DATE 
            ,OUTSTANDING_WO 
            ,RESTRUCTURE_DATE 
            ,RESTRUCTURE_FLAG 
            ,STAFF_LOAN_FLAG 
            ,BELOW_MARKET_FLAG 
            ,COMMITTED_FLAG 
            ,INSTALLMENT_AMOUNT 
            ,INITIAL_UNAMORT_TXN_COST 
            ,INITIAL_UNAMORT_ORG_FEE 
            ,IAS_CLASS 
            ,REVOLVING_FLAG 
            ,BUCKET_ID 
            ,PRODUCT_TYPE_GROUP 
            ,NPL_DATE 
            ,NPL_FLAG 
            ,BRANCH_CODE_OPEN 
            ,UNAMORT_AMT_TOTAL_JF 
            ,UNAMORT_FEE_AMT_JF 
            ,UNAMORT_COST_AMT_JF 
            ,IFRS9_CLASS 
            ,SPPI_RESULT 
            ,BM_RESULT 
            ,INTEREST_ACCRUED 
            ,ACCOUNT_TYPE 
            ,CUSTOMER_TYPE 
            ,PRODUCT_TYPE_1 
            ,CCF 
            ,CCF_RULE_ID 
            ,CCF_EFF_DATE 
            ,ECL_AMOUNT 
            ,ECL_AMOUNT_BFL 
            ,AVG_EIR 
            ,ECL_MODEL_ID 
            ,SEGMENTATION_ID 
            ,PD_ME_MODEL_ID 
            ,DEFAULT_RULE_ID 
            ,DPD_CIF 
            ,BI_COLLECT_CIF 
            ,RESTRUCTURE_COLLECT_FLAG 
            ,PLAFOND_CIF 
            ,RATING_CODE 
            ,SOURCE_SYSTEM 
            ,LOB_CODE 
            ,EARLY_PAYMENT 
            ,EARLY_PAYMENT_FLAG 
            ,SEGMENT_FLAG 
        ) SELECT 
            DOWNLOAD_DATE 
            ,MASTERID 
            ,PRODUCT_ENTITY 
            ,JF_FLAG 
            ,DATA_SOURCE 
            ,PRODUCT_TYPE 
            ,PRODUCT_CODE 
            ,PRODUCT_GROUP 
            ,BRANCH_CODE 
            ,CURRENCY 
            ,ACCOUNT_NUMBER 
            ,PREVIOUS_ACCOUNT_NUMBER 
            ,CUSTOMER_NUMBER 
            ,CUSTOMER_NAME 
            ,GLOBAL_CUSTOMER_NUMBER 
            ,EXCHANGE_RATE 
            ,MARKET_RATE 
            ,INITIAL_OUTSTANDING 
            ,OUTSTANDING 
            ,OUTSTANDING_JF 
            ,OUTSTANDING_BANK 
            ,OUTSTANDING_PASTDUE 
            ,OUTSTANDING_IDC 
            ,PLAFOND 
            ,PLAFOND_CASH 
            ,UNUSED_AMOUNT 
            ,DOWN_PAYMENT_AMOUNT 
            ,INTEREST_RATE 
            ,ACCOUNT_STATUS 
            ,FACILITY_NUMBER 
            ,LOAN_START_DATE 
            ,LOAN_DUE_DATE 
            ,INSTALLMENT_GRACE_PERIOD 
            ,LOAN_START_AMORTIZATION 
            ,LOAN_END_AMORTIZATION 
            ,AMORT_TYPE 
            ,NEXT_PAYMENT_DATE 
            ,LAST_PAYMENT_DATE 
            ,FIRST_INSTALLMENT_DATE 
            ,TENOR 
            ,PAYMENT_CODE 
            ,PAYMENT_TERM 
            ,INTEREST_PAYMENT_TERM 
            ,EIR_STATUS 
            ,ECF_STATUS 
            ,INTEREST_CALCULATION_CODE 
            ,EIR 
            ,EIR_AMOUNT 
            ,FAIR_VALUE_AMOUNT 
            ,UNAMORT_COST_AMT 
            ,UNAMORT_FEE_AMT 
            ,DAILY_AMORT_AMT 
            ,DAY_PAST_DUE 
            ,ORIGINAL_COLLECTABILITY 
            ,BI_COLLECTABILITY 
            ,GL_CONSTNAME 
            ,SEGMENT 
            ,PD_SEGMENT 
            ,LGD_SEGMENT 
            ,IMPAIRED_FLAG 
            ,IS_IMPAIRED 
            ,CA_UNWINDING_AMOUNT 
            ,IA_UNWINDING_AMOUNT 
            ,BEGINNING_BALANCE 
            ,ENDING_BALANCE 
            ,WRITEBACK_AMOUNT 
            ,CHARGE_AMOUNT 
            ,WRITEOFF_FLAG 
            ,WRITEOFF_DATE 
            ,OUTSTANDING_WO 
            ,RESTRUCTURE_DATE 
            ,RESTRUCTURE_FLAG 
            ,CASE WHEN STAFF_LOAN_FLAG = 1 THEN ''Y'' ELSE ''N'' END 
            ,BELOW_MARKET_FLAG 
            ,COMMITTED_FLAG 
            ,INSTALLMENT_AMOUNT 
            ,INITIAL_UNAMORT_TXN_COST 
            ,INITIAL_UNAMORT_ORG_FEE 
            ,IAS_CLASS 
            ,REVOLVING_FLAG 
            ,BUCKET_ID 
            ,NULL AS PRODUCT_TYPE_GROUP 
            ,NPL_DATE 
            ,NPL_FLAG 
            ,BRANCH_CODE_OPEN 
            ,UNAMORT_AMT_TOTAL_JF 
            ,UNAMORT_FEE_AMT_JF 
            ,UNAMORT_COST_AMT_JF 
            ,IFRS9_CLASS 
            ,SPPI_RESULT 
            ,BM_RESULT 
            ,INTEREST_ACCRUED 
            ,ACCOUNT_TYPE 
            ,CUSTOMER_TYPE 
            ,PRODUCT_TYPE_1 
            ,CCF::REAL 
            ,CCF_RULE_ID::INT 
            ,CCF_EFF_DATE::DATE 
            ,ECL_AMOUNT 
            ,ECL_AMOUNT_BFL 
            ,AVG_EIR 
            ,ECL_MODEL_ID 
            ,SEGMENTATION_ID 
            ,PD_ME_MODEL_ID 
            ,DEFAULT_RULE_ID 
            ,DPD_CIF 
            ,BI_COLLECT_CIF 
            ,RESTRUCTURE_COLLECT_FLAG 
            ,PLAFOND_CIF 
            ,RATING_CODE 
            ,SOURCE_SYSTEM 
            ,LOB_CODE 
            ,EARLY_PAYMENT 
            ,EARLY_PAYMENT_FLAG 
            ,SEGMENT_FLAG 
        FROM ' || V_TABLENAME || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_FILL_IMA_AMORT_PREV_CURR', '');

    RAISE NOTICE 'SP_IFRS_FILL_IMA_AMORT_PREV_CURR | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT2;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_FILL_IMA_AMORT_PREV_CURR';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT2 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$
