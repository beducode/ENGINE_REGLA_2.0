CREATE OR REPLACE SP_IFRS_COST_FEE_STATUS(IN P_RUNID CHARACTER VARYING DEFAULT 'S_00000_0000'::CHARACTER VARYING, IN P_DOWNLOAD_DATE DATE DEFAULT NULL::DATE, IN P_PRC CHARACTER VARYING DEFAULT 'S'::CHARACTER VARYING, IN P_MAT_LEVEL INTEGER DEFAULT 0, IN P_UNMAT_PL INTEGER DEFAULT 0)
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

    ---- VARIABLE PROCESS
    V_CX INT;
    V_HAS_NULL_PAIR BOOLEAN;
    
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
        V_TABLEINSERT1 := 'IFRS_ACCT_COST_FEE_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_IMA_AMORT_CURR_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_ACCT_CLOSED_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_PRODUCT_PARAM_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_TRANSACTION_PARAM_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_ACCT_COST_FEE';
        V_TABLEINSERT2 := 'IFRS_IMA_AMORT_CURR';
        V_TABLEINSERT3 := 'IFRS_ACCT_CLOSED';
        V_TABLEINSERT4 := 'IFRS_PRODUCT_PARAM';
        V_TABLEINSERT5 := 'IFRS_TRANSACTION_PARAM';
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

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_COST_FEE_STATUS', '');

    ---- RESET
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' 
        SET STATUS = ''FRZNF'', METHOD = ''X'' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''':: DATE 
        AND STATUS <> ''PARAM'' ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_COST_FEE_STATUS', '1');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET 
            STATUS = CASE 
                WHEN A.STATUS = ''PARAM'' THEN ''PARAM'' 
                WHEN A.CCY != B.CURRENCY THEN ''FRZCCY'' 
                WHEN (
                    B.LOAN_DUE_DATE <= A.DOWNLOAD_DATE 
                    OR B.ACCOUNT_STATUS IN (''W'', ''C'', ''E'', ''CE'', ''CT'', ''CN'') 
                ) THEN ''PNL'' 
                ELSE ''ACT'' 
            END 
            ,POS_AMOUNT = CASE 
                WHEN B.OUTSTANDING = 0 THEN 0 
                ELSE A.AMOUNT / B.OUTSTANDING 
            END 
            ,MASTERID = B.MASTERID 
            ,DATASOURCE = B.DATA_SOURCE 
            ,PRD_TYPE = B.PRODUCT_TYPE 
            ,PRD_CODE = B.PRODUCT_CODE 
        FROM ' || V_TABLEINSERT2 || ' B 
        WHERE B.MASTERID = A.MASTERID 
        AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_COST_FEE_STATUS', '2');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET STATUS = ''PNL'', CREATEDBY = B.CREATEDBY 
        FROM (
            SELECT MASTERID, CREATEDBY 
            FROM ' || V_TABLEINSERT3 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''':: DATE 
        ) B 
        WHERE A.MASTERID = B.MASTERID 
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''':: DATE ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_COST_FEE_STATUS', '3');

    ---- MARK AMORT METHOD BASED ON PRODUCT PARAM
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET METHOD = B.AMORT_TYPE 
        FROM ( 
            SELECT X.*, Y.* 
            FROM ' || V_TABLEINSERT4 || ' X 
            CROSS JOIN IFRS_PRC_DATE_AMORT Y 
        ) B 
        WHERE A.DATASOURCE = B.DATA_SOURCE 
        AND A.PRD_TYPE = B.PRD_TYPE 
        AND A.PRD_CODE = B.PRD_CODE 
        AND ( 
            A.CCY = B.CCY 
            OR B.CCY = ''ALL'' 
        ) AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''':: DATE ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_COST_FEE_STATUS', '4');

    ---- EIR WITH ZERO OS WILL GO PNL
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' 
        SET STATUS = ''PNL'', CREATEDBY = ''EIR_ZERO_OS''  
        WHERE STATUS = ''ACT'' 
        AND METHOD = ''EIR'' 
        AND DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''':: DATE 
        AND MASTERID IN (
            SELECT MASTERID 
            FROM ' || V_TABLEINSERT2 || ' 
            WHERE COALESCE(OUTSTANDING, 0) <= 0 
        ) ';
    EXECUTE (V_STR_QUERY);
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_COST_FEE_STATUS', '5');

    ---- START UNDER MATERIALITY TO PNL
    IF P_UNMAT_PL = 1 THEN 
        IF P_MAT_LEVEL = 0 THEN 
            ---- ABS MATERIALITY FEE BY PRODUCT
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' 
                SET CREATEDBY = ''ABS_MAT_FEE'', STATUS = ''PNL'' 
                FROM ' || V_TABLEINSERT1 || ' A 
                JOIN ( 
                    SELECT X.*, Y.* 
                    FROM ' || V_TABLEINSERT4 || ' X
                    CROSS JOIN IFRS_PRC_DATE_AMORT Y
                ) B 
                ON (
                    A.DATASOURCE = B.DATA_SOURCE 
                    OR B.DATA_SOURCE = ''ALL'' 
                ) AND (
                    A.PRD_TYPE = B.PRD_TYPE 
                    OR B.PRD_TYPE = ''ALL'' 
                ) AND (
                    A.PRD_CODE = B.PRD_CODE 
                    OR B.PRD_CODE = ''ALL'' 
                ) AND (
                    A.CCY = B.CCY 
                    OR B.CCY = ''ALL'' 
                ) AND A.DOWNLOAD_DATE = B.CURRDATE 
                JOIN ' || V_TABLEINSERT2 || ' C 
                ON A.MASTERID = C.MASTERID 
                AND A.DOWNLOAD_DATE = C.DOWNLOAD_DATE 
                WHERE A.FLAG_CF = ''F'' 
                AND A.STATUS = ''ACT'' 
                AND B.FEE_MAT_TYPE IN ('''', ''ABS'') 
                AND ABS(A.AMOUNT * COALESCE(C.EXCHANGE_RATE, 1)) < B.FEE_MAT_AMT 
                AND A.SOURCE_TABLE <> ''MAIN_M_LOAN_PSAK'' ';
            EXECUTE (V_STR_QUERY);
        END IF;

        IF P_MAT_LEVEL = 1 THEN 
            ---- ABS MATERIALITY FEE BY TRANSACTION
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' 
                SET CREATEDBY = ''ABS_MAT_FEE'', STATUS = ''PNL'' 
                FROM ' || V_TABLEINSERT1 || ' A 
                JOIN ( 
                    SELECT X.*, Y.* 
                    FROM ' || V_TABLEINSERT5 || ' X
                    CROSS JOIN IFRS_PRC_DATE_AMORT Y
                ) B 
                ON (
                    A.DATASOURCE = B.DATA_SOURCE 
                    OR B.DATA_SOURCE = ''ALL'' 
                ) AND (
                    A.PRD_TYPE = B.PRD_TYPE 
                    OR B.PRD_TYPE = ''ALL'' 
                ) AND (
                    A.PRD_CODE = B.PRD_CODE 
                    OR B.PRD_CODE = ''ALL'' 
                ) AND (
                    A.TRX_CODE = B.TRX_CODE 
                    OR B.TRX_CODE = ''ALL'' 
                ) AND (
                    A.CCY = B.CCY 
                    OR B.CCY = ''ALL'' 
                ) AND A.DOWNLOAD_DATE = B.CURRDATE 
                JOIN ' || V_TABLEINSERT2 || ' C 
                ON A.MASTERID = C.MASTERID 
                AND A.DOWNLOAD_DATE = C.DOWNLOAD_DATE 
                WHERE A.FLAG_CF = ''F'' 
                AND A.STATUS = ''ACT'' 
                AND B.FEE_MAT_TYPE IN ('''', ''ABS'') 
                AND ABS(A.AMOUNT * COALESCE(C.EXCHANGE_RATE, 1)) < B.FEE_MAT_AMT 
                AND A.SOURCE_TABLE <> ''MAIN_M_LOAN_PSAK'' ';
            EXECUTE (V_STR_QUERY);
        END IF;

        CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_COST_FEE_STATUS', '6');

        IF P_MAT_LEVEL = 0 THEN 
            ---- ABS MATERIALITY COST BY PRODUCT
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' 
                SET CREATEDBY = ''ABS_MAT_COST'', STATUS = ''PNL'' 
                FROM ' || V_TABLEINSERT1 || ' A 
                JOIN ( 
                    SELECT X.*, Y.* 
                    FROM ' || V_TABLEINSERT4 || ' X
                    CROSS JOIN IFRS_PRC_DATE_AMORT Y
                ) B 
                ON (
                    A.DATASOURCE = B.DATA_SOURCE 
                    OR B.DATA_SOURCE = ''ALL'' 
                ) AND (
                    A.PRD_TYPE = B.PRD_TYPE 
                    OR B.PRD_TYPE = ''ALL'' 
                ) AND (
                    A.PRD_CODE = B.PRD_CODE 
                    OR B.PRD_CODE = ''ALL'' 
                ) AND (
                    A.CCY = B.CCY 
                    OR B.CCY = ''ALL'' 
                ) AND A.DOWNLOAD_DATE = B.CURRDATE 
                JOIN ' || V_TABLEINSERT2 || ' C 
                ON A.MASTERID = C.MASTERID 
                AND A.DOWNLOAD_DATE = C.DOWNLOAD_DATE 
                WHERE A.FLAG_CF = ''C'' 
                AND A.STATUS = ''ACT'' 
                AND B.FEE_MAT_TYPE IN ('''', ''ABS'') 
                AND ABS(A.AMOUNT * COALESCE(C.EXCHANGE_RATE, 1)) < B.COST_MAT_AMT 
                AND A.SOURCE_TABLE <> ''MAIN_M_LOAN_PSAK'' ';
            EXECUTE (V_STR_QUERY);
        END IF;

        IF P_MAT_LEVEL = 1 THEN 
            ---- ABS MATERIALITY COST BY TRANSACTION
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' 
                SET CREATEDBY = ''ABS_MAT_COST'', STATUS = ''PNL'' 
                FROM ' || V_TABLEINSERT1 || ' A 
                JOIN ( 
                    SELECT X.*, Y.* 
                    FROM ' || V_TABLEINSERT5 || ' X
                    CROSS JOIN IFRS_PRC_DATE_AMORT Y
                ) B 
                ON (
                    A.DATASOURCE = B.DATA_SOURCE 
                    OR B.DATA_SOURCE = ''ALL'' 
                ) AND (
                    A.PRD_TYPE = B.PRD_TYPE 
                    OR B.PRD_TYPE = ''ALL'' 
                ) AND (
                    A.PRD_CODE = B.PRD_CODE 
                    OR B.PRD_CODE = ''ALL'' 
                ) AND (
                    A.TRX_CODE = B.TRX_CODE 
                    OR B.TRX_CODE = ''ALL'' 
                ) AND (
                    A.CCY = B.CCY 
                    OR B.CCY = ''ALL'' 
                ) AND A.DOWNLOAD_DATE = B.CURRDATE 
                JOIN ' || V_TABLEINSERT2 || ' C 
                ON A.MASTERID = C.MASTERID 
                AND A.DOWNLOAD_DATE = C.DOWNLOAD_DATE 
                WHERE A.FLAG_CF = ''C'' 
                AND A.STATUS = ''ACT'' 
                AND B.FEE_MAT_TYPE IN ('''', ''ABS'') 
                AND ABS(A.AMOUNT * COALESCE(C.EXCHANGE_RATE, 1)) < B.COST_MAT_AMT 
                AND A.SOURCE_TABLE <> ''MAIN_M_LOAN_PSAK'' ';
            EXECUTE (V_STR_QUERY);
        END IF;

        CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_COST_FEE_STATUS', '7');

        IF P_MAT_LEVEL = 0 THEN 
            ---- PERCENT OF OS FEE PRODUCT PARAM
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A
                SET CREATEDBY = ''POS_MAT_FEE'', STATUS = ''PNL'' 
                FROM ( 
                    SELECT X.*, Y.* 
                    FROM ' || V_TABLEINSERT4 || ' X
                    CROSS JOIN IFRS_PRC_DATE_AMORT Y
                ) B 
                WHERE A.DATASOURCE = B.DATA_SOURCE 
                AND A.PRD_TYPE = B.PRD_TYPE 
                AND A.PRD_CODE = B.PRD_CODE 
                AND (
                    A.CCY = B.CCY 
                    OR B.CCY = ''ALL'' 
                ) AND A.DOWNLOAD_DATE = B.CURRDATE 
                AND A.FLAG_CF = ''F'' 
                AND A.STATUS = ''ACT'' 
                AND B.FEE_MAT_TYPE = ''POS'' 
                AND ABS(A.POS_AMOUNT) < B.FEE_MAT_AMT 
                AND A.SOURCE_TABLE <> ''MAIN_M_LOAN_PSAK'' ';
            EXECUTE (V_STR_QUERY);
        END IF;

        IF P_MAT_LEVEL = 1 THEN 
            ---- POS MATERIALITY FEE BY TRANSACTION
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
                SET CREATEDBY = ''POS_MAT_FEE'', STATUS = ''PNL'' 
                FROM ( 
                    SELECT X.*, Y.* 
                    FROM ' || V_TABLEINSERT5 || ' X
                    CROSS JOIN IFRS_PRC_DATE_AMORT Y
                ) B 
                ON (
                    A.DATASOURCE = B.DATA_SOURCE 
                    OR B.DATA_SOURCE = ''ALL'' 
                ) AND (
                    A.PRD_TYPE = B.PRD_TYPE 
                    OR B.PRD_TYPE = ''ALL'' 
                ) AND (
                    A.PRD_CODE = B.PRD_CODE 
                    OR B.PRD_CODE = ''ALL'' 
                ) AND (
                    A.TRX_CODE = B.TRX_CODE 
                    OR B.TRX_CODE = ''ALL'' 
                ) AND (
                    A.CCY = B.CCY 
                    OR B.CCY = ''ALL'' 
                ) AND A.DOWNLOAD_DATE = B.CURRDATE 
                AND A.FLAG_CF = ''F'' 
                AND A.STATUS = ''ACT'' 
                AND B.FEE_MAT_TYPE = ''POS'' 
                AND ABS(A.POS_AMOUNT) < B.FEE_MAT_AMT 
                AND A.SOURCE_TABLE <> ''MAIN_M_LOAN_PSAK'' ';
            EXECUTE (V_STR_QUERY);
        END IF;

        CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_COST_FEE_STATUS', '8');

        IF P_MAT_LEVEL = 0 THEN 
            ---- PERCENT OF OS COST
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A
                SET CREATEDBY = ''POS_MAT_COST'', STATUS = ''PNL'' 
                FROM ( 
                    SELECT X.*, Y.* 
                    FROM ' || V_TABLEINSERT4 || ' X
                    CROSS JOIN IFRS_PRC_DATE_AMORT Y
                ) B 
                WHERE A.DATASOURCE = B.DATA_SOURCE 
                AND A.PRD_TYPE = B.PRD_TYPE 
                AND A.PRD_CODE = B.PRD_CODE 
                AND (
                    A.CCY = B.CCY 
                    OR B.CCY = ''ALL'' 
                ) AND A.DOWNLOAD_DATE = B.CURRDATE 
                AND A.FLAG_CF = ''C'' 
                AND A.STATUS = ''ACT'' 
                AND B.FEE_MAT_TYPE = ''POS'' 
                AND ABS(A.POS_AMOUNT) < B.COST_MAT_AMT 
                AND A.SOURCE_TABLE <> ''MAIN_M_LOAN_PSAK'' ';
            EXECUTE (V_STR_QUERY);
        END IF;

        IF P_MAT_LEVEL = 1 THEN 
            ---- POS MATERIALITY COST BY TRANSACTION
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
                SET CREATEDBY = ''POS_MAT_COST'', STATUS = ''PNL'' 
                FROM ( 
                    SELECT X.*, Y.* 
                    FROM ' || V_TABLEINSERT5 || ' X
                    CROSS JOIN IFRS_PRC_DATE_AMORT Y
                ) B 
                ON (
                    A.DATASOURCE = B.DATA_SOURCE 
                    OR B.DATA_SOURCE = ''ALL'' 
                ) AND (
                    A.PRD_TYPE = B.PRD_TYPE 
                    OR B.PRD_TYPE = ''ALL'' 
                ) AND (
                    A.PRD_CODE = B.PRD_CODE 
                    OR B.PRD_CODE = ''ALL'' 
                ) AND (
                    A.TRX_CODE = B.TRX_CODE 
                    OR B.TRX_CODE = ''ALL'' 
                ) AND (
                    A.CCY = B.CCY 
                    OR B.CCY = ''ALL'' 
                ) AND A.DOWNLOAD_DATE = B.CURRDATE 
                AND A.FLAG_CF = ''C'' 
                AND A.STATUS = ''ACT'' 
                AND B.FEE_MAT_TYPE = ''POS'' 
                AND ABS(A.POS_AMOUNT) < B.COST_MAT_AMT 
                AND A.SOURCE_TABLE <> ''MAIN_M_LOAN_PSAK'' ';
            EXECUTE (V_STR_QUERY);
        END IF;

        CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_COST_FEE_STATUS', '9');
    END IF;
    ---- END UNDER MATERIALITY TO PNL

    ---- START UNDER MATIRIALITY NOT PROCESSED
    IF P_UNMAT_PL = 0 THEN 
        IF P_MAT_LEVEL = 0 THEN 
            ---- ABS MATERIALITY FEE BY PRODUCT
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' 
                SET CREATEDBY = ''ABS_MAT_FEE'', STATUS = ''UNMAT'' 
                FROM ' || V_TABLEINSERT1 || ' A 
                JOIN ( 
                    SELECT X.*, Y.* 
                    FROM ' || V_TABLEINSERT4 || ' X
                    CROSS JOIN IFRS_PRC_DATE_AMORT Y
                ) B 
                ON (
                    A.DATASOURCE = B.DATA_SOURCE 
                    OR B.DATA_SOURCE = ''ALL'' 
                ) AND (
                    A.PRD_TYPE = B.PRD_TYPE 
                    OR B.PRD_TYPE = ''ALL'' 
                ) AND (
                    A.PRD_CODE = B.PRD_CODE 
                    OR B.PRD_CODE = ''ALL'' 
                ) AND (
                    A.CCY = B.CCY 
                    OR B.CCY = ''ALL'' 
                ) AND A.DOWNLOAD_DATE = B.CURRDATE 
                JOIN ' || V_TABLEINSERT2 || ' C 
                ON A.MASTERID = C.MASTERID 
                AND A.DOWNLOAD_DATE = C.DOWNLOAD_DATE 
                WHERE A.FLAG_CF = ''F'' 
                AND A.STATUS = ''ACT'' 
                AND B.FEE_MAT_TYPE IN ('''', ''ABS'') 
                AND ABS(A.AMOUNT * COALESCE(C.EXCHANGE_RATE, 1)) < B.FEE_MAT_AMT 
                AND A.SOURCE_TABLE <> ''MAIN_M_LOAN_PSAK'' ';
            EXECUTE (V_STR_QUERY);
        END IF;

        IF P_MAT_LEVEL = 1 THEN 
            ---- ABS MATERIALITY FEE BY TRANSACTION
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' 
                SET CREATEDBY = ''ABS_MAT_FEE'', STATUS = ''UNMAT'' 
                FROM ' || V_TABLEINSERT1 || ' A 
                JOIN ( 
                    SELECT X.*, Y.* 
                    FROM ' || V_TABLEINSERT5 || ' X
                    CROSS JOIN IFRS_PRC_DATE_AMORT Y
                ) B 
                ON (
                    A.DATASOURCE = B.DATA_SOURCE 
                    OR B.DATA_SOURCE = ''ALL'' 
                ) AND (
                    A.PRD_TYPE = B.PRD_TYPE 
                    OR B.PRD_TYPE = ''ALL'' 
                ) AND (
                    A.PRD_CODE = B.PRD_CODE 
                    OR B.PRD_CODE = ''ALL'' 
                ) AND (
                    A.TRX_CODE = B.TRX_CODE 
                    OR B.TRX_CODE = ''ALL'' 
                ) AND (
                    A.CCY = B.CCY 
                    OR B.CCY = ''ALL'' 
                ) AND A.DOWNLOAD_DATE = B.CURRDATE 
                JOIN ' || V_TABLEINSERT2 || ' C 
                ON A.MASTERID = C.MASTERID 
                AND A.DOWNLOAD_DATE = C.DOWNLOAD_DATE 
                WHERE A.FLAG_CF = ''F'' 
                AND A.STATUS = ''ACT'' 
                AND B.FEE_MAT_TYPE IN ('''', ''ABS'') 
                AND ABS(A.AMOUNT * COALESCE(C.EXCHANGE_RATE, 1)) < B.FEE_MAT_AMT 
                AND A.SOURCE_TABLE <> ''MAIN_M_LOAN_PSAK'' ';
            EXECUTE (V_STR_QUERY);
        END IF;

        CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_COST_FEE_STATUS', '6');

        IF P_MAT_LEVEL = 0 THEN 
            ---- ABS MATERIALITY COST BY PRODUCT
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' 
                SET CREATEDBY = ''ABS_MAT_COST'', STATUS = ''UNMAT'' 
                FROM ' || V_TABLEINSERT1 || ' A 
                JOIN ( 
                    SELECT X.*, Y.* 
                    FROM ' || V_TABLEINSERT4 || ' X
                    CROSS JOIN IFRS_PRC_DATE_AMORT Y
                ) B 
                ON (
                    A.DATASOURCE = B.DATA_SOURCE 
                    OR B.DATA_SOURCE = ''ALL'' 
                ) AND (
                    A.PRD_TYPE = B.PRD_TYPE 
                    OR B.PRD_TYPE = ''ALL'' 
                ) AND (
                    A.PRD_CODE = B.PRD_CODE 
                    OR B.PRD_CODE = ''ALL'' 
                ) AND (
                    A.CCY = B.CCY 
                    OR B.CCY = ''ALL'' 
                ) AND A.DOWNLOAD_DATE = B.CURRDATE 
                JOIN ' || V_TABLEINSERT2 || ' C 
                ON A.MASTERID = C.MASTERID 
                AND A.DOWNLOAD_DATE = C.DOWNLOAD_DATE 
                WHERE A.FLAG_CF = ''C'' 
                AND A.STATUS = ''ACT'' 
                AND B.FEE_MAT_TYPE IN ('''', ''ABS'') 
                AND ABS(A.AMOUNT * COALESCE(C.EXCHANGE_RATE, 1)) < B.COST_MAT_AMT 
                AND A.SOURCE_TABLE <> ''MAIN_M_LOAN_PSAK'' ';
            EXECUTE (V_STR_QUERY);
        END IF;

        IF P_MAT_LEVEL = 1 THEN 
            ---- ABS MATERIALITY COST BY TRANSACTION
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' 
                SET CREATEDBY = ''ABS_MAT_COST'', STATUS = ''UNMAT'' 
                FROM ' || V_TABLEINSERT1 || ' A 
                JOIN ( 
                    SELECT X.*, Y.* 
                    FROM ' || V_TABLEINSERT5 || ' X
                    CROSS JOIN IFRS_PRC_DATE_AMORT Y
                ) B 
                ON (
                    A.DATASOURCE = B.DATA_SOURCE 
                    OR B.DATA_SOURCE = ''ALL'' 
                ) AND (
                    A.PRD_TYPE = B.PRD_TYPE 
                    OR B.PRD_TYPE = ''ALL'' 
                ) AND (
                    A.PRD_CODE = B.PRD_CODE 
                    OR B.PRD_CODE = ''ALL'' 
                ) AND (
                    A.TRX_CODE = B.TRX_CODE 
                    OR B.TRX_CODE = ''ALL'' 
                ) AND (
                    A.CCY = B.CCY 
                    OR B.CCY = ''ALL'' 
                ) AND A.DOWNLOAD_DATE = B.CURRDATE 
                JOIN ' || V_TABLEINSERT2 || ' C 
                ON A.MASTERID = C.MASTERID 
                AND A.DOWNLOAD_DATE = C.DOWNLOAD_DATE 
                WHERE A.FLAG_CF = ''C'' 
                AND A.STATUS = ''ACT'' 
                AND B.FEE_MAT_TYPE IN ('''', ''ABS'') 
                AND ABS(A.AMOUNT * COALESCE(C.EXCHANGE_RATE, 1)) < B.COST_MAT_AMT 
                AND A.SOURCE_TABLE <> ''MAIN_M_LOAN_PSAK'' ';
            EXECUTE (V_STR_QUERY);
        END IF;

        CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_COST_FEE_STATUS', '7');

        IF P_MAT_LEVEL = 0 THEN 
            ---- PERCENT OF OS FEE PRODUCT PARAM
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A
                SET CREATEDBY = ''POS_MAT_FEE'', STATUS = ''UNMAT'' 
                FROM ( 
                    SELECT X.*, Y.* 
                    FROM ' || V_TABLEINSERT4 || ' X
                    CROSS JOIN IFRS_PRC_DATE_AMORT Y
                ) B 
                WHERE A.DATASOURCE = B.DATA_SOURCE 
                AND A.PRD_TYPE = B.PRD_TYPE 
                AND A.PRD_CODE = B.PRD_CODE 
                AND (
                    A.CCY = B.CCY 
                    OR B.CCY = ''ALL'' 
                ) AND A.DOWNLOAD_DATE = B.CURRDATE 
                AND A.FLAG_CF = ''F'' 
                AND A.STATUS = ''ACT'' 
                AND B.FEE_MAT_TYPE = ''POS'' 
                AND ABS(A.POS_AMOUNT) < B.FEE_MAT_AMT 
                AND A.SOURCE_TABLE <> ''MAIN_M_LOAN_PSAK'' ';
            EXECUTE (V_STR_QUERY);
        END IF;

        IF P_MAT_LEVEL = 1 THEN 
            ---- POS MATERIALITY FEE BY TRANSACTION
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
                SET CREATEDBY = ''POS_MAT_FEE'', STATUS = ''UNMAT'' 
                FROM ( 
                    SELECT X.*, Y.* 
                    FROM ' || V_TABLEINSERT5 || ' X
                    CROSS JOIN IFRS_PRC_DATE_AMORT Y
                ) B 
                ON (
                    A.DATASOURCE = B.DATA_SOURCE 
                    OR B.DATA_SOURCE = ''ALL'' 
                ) AND (
                    A.PRD_TYPE = B.PRD_TYPE 
                    OR B.PRD_TYPE = ''ALL'' 
                ) AND (
                    A.PRD_CODE = B.PRD_CODE 
                    OR B.PRD_CODE = ''ALL'' 
                ) AND (
                    A.TRX_CODE = B.TRX_CODE 
                    OR B.TRX_CODE = ''ALL'' 
                ) AND (
                    A.CCY = B.CCY 
                    OR B.CCY = ''ALL'' 
                ) AND A.DOWNLOAD_DATE = B.CURRDATE 
                AND A.FLAG_CF = ''F'' 
                AND A.STATUS = ''ACT'' 
                AND B.FEE_MAT_TYPE = ''POS'' 
                AND ABS(A.POS_AMOUNT) < B.FEE_MAT_AMT 
                AND A.SOURCE_TABLE <> ''MAIN_M_LOAN_PSAK'' ';
            EXECUTE (V_STR_QUERY);
        END IF;

        CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_COST_FEE_STATUS', '8');

        IF P_MAT_LEVEL = 0 THEN 
            ---- PERCENT OF OS COST
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A
                SET CREATEDBY = ''POS_MAT_COST'', STATUS = ''UNMAT'' 
                FROM ( 
                    SELECT X.*, Y.* 
                    FROM ' || V_TABLEINSERT4 || ' X
                    CROSS JOIN IFRS_PRC_DATE_AMORT Y
                ) B 
                WHERE A.DATASOURCE = B.DATA_SOURCE 
                AND A.PRD_TYPE = B.PRD_TYPE 
                AND A.PRD_CODE = B.PRD_CODE 
                AND (
                    A.CCY = B.CCY 
                    OR B.CCY = ''ALL'' 
                ) AND A.DOWNLOAD_DATE = B.CURRDATE 
                AND A.FLAG_CF = ''C'' 
                AND A.STATUS = ''ACT'' 
                AND B.FEE_MAT_TYPE = ''POS'' 
                AND ABS(A.POS_AMOUNT) < B.COST_MAT_AMT 
                AND A.SOURCE_TABLE <> ''MAIN_M_LOAN_PSAK'' ';
            EXECUTE (V_STR_QUERY);
        END IF;

        IF P_MAT_LEVEL = 1 THEN 
            ---- POS MATERIALITY COST BY TRANSACTION
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
                SET CREATEDBY = ''POS_MAT_COST'', STATUS = ''UNMAT'' 
                FROM ( 
                    SELECT X.*, Y.* 
                    FROM ' || V_TABLEINSERT5 || ' X
                    CROSS JOIN IFRS_PRC_DATE_AMORT Y
                ) B 
                ON (
                    A.DATASOURCE = B.DATA_SOURCE 
                    OR B.DATA_SOURCE = ''ALL'' 
                ) AND (
                    A.PRD_TYPE = B.PRD_TYPE 
                    OR B.PRD_TYPE = ''ALL'' 
                ) AND (
                    A.PRD_CODE = B.PRD_CODE 
                    OR B.PRD_CODE = ''ALL'' 
                ) AND (
                    A.TRX_CODE = B.TRX_CODE 
                    OR B.TRX_CODE = ''ALL'' 
                ) AND (
                    A.CCY = B.CCY 
                    OR B.CCY = ''ALL'' 
                ) AND A.DOWNLOAD_DATE = B.CURRDATE 
                AND A.FLAG_CF = ''C'' 
                AND A.STATUS = ''ACT'' 
                AND B.FEE_MAT_TYPE = ''POS'' 
                AND ABS(A.POS_AMOUNT) < B.COST_MAT_AMT 
                AND A.SOURCE_TABLE <> ''MAIN_M_LOAN_PSAK'' ';
            EXECUTE (V_STR_QUERY);
        END IF;

        CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_COST_FEE_STATUS', '9');
    END IF;

    ---- ACT WITH METHOD SL BUT LOAN_START_DATE OR LOAN_DUE DATE IS NULL WILL GO TO PNL
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' 
        SET STATUS = ''PNL'', CREATEDBY = ''SL_START_ENDDT_NULL'' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''':: DATE 
        AND STATUS = ''ACT'' 
        AND METHOD = ''SL'' 
        AND FLAG_AL = ''A'' 
        AND MASTERID IN (
            SELECT ACCOUNT_NUMBER 
            FROM ' || V_TABLEINSERT2 || ' 
            WHERE AMORT_TYPE = ''SL'' 
            AND DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''':: DATE 
            AND (
                LOAN_START_DATE IS NULL 
                OR LOAN_DUE_DATE IS NULL 
                OR (LOAN_START_DATE > LOAN_DUE_DATE) 
            )
        ) ';
    EXECUTE (V_STR_QUERY);

    ---- IF DIFF DATE BETWEEN COST FEE DATE AND LOAN DUE DATE ONLY HAVE DIFFERENT 1 DAY, WILL TREAT AS PNL (CAUSING AN ERROR WHEN GOALSEEK)
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET STATUS = ''PNL'', CREATEDBY = ''EIR_DUE_DATE_1_DAY'' 
        FROM ' || V_TABLEINSERT2 || ' B 
        WHERE A.MASTERID = B.MASTERID 
        AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE 
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''':: DATE 
        AND A.STATUS = ''ACT'' 
        AND A.METHOD = ''EIR'' 
        AND A.FLAG_AL = ''A'' 
        AND (B.LOAN_DUE_DATE::DATE - A.DOWNLOAD_DATE::DATE) <= 1 ';
    EXECUTE (V_STR_QUERY);

    ---- ACT BUT NO METHOD WILL GO TO FRZMTD
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' 
        SET STATUS = ''FRZMTD'' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''':: DATE 
        AND STATUS = ''ACT'' 
        AND METHOD NOT IN (''EIR'', ''SL'') ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_COST_FEE_STATUS', '10');

    ---- CAN NOT PROCESS ACT REVERSE FEE/COST IF NO PREV COST FEE FOR THAT ACCOUNT
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A
        SET STATUS = ''FRZREV'' 
        WHERE ID IN ( 
            SELECT A.ID 
            FROM ' || V_TABLEINSERT1 || ' A 
            LEFT JOIN ' || V_TABLEINSERT1 || ' B 
            ON B.DOWNLOAD_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''':: DATE 
            AND B.FLAG_REVERSE = ''N'' 
            AND B.AMOUNT = A.AMOUNT 
            AND A.MASTERID = B.MASTERID 
            AND B.ID = B.CF_ID 
            WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''':: DATE 
            AND A.FLAG_REVERSE = ''Y'' 
            AND A.STATUS = ''ACT'' 
            AND B.MASTERID IS NULL 
        ) ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_COST_FEE_STATUS', '11');

    ---- FILL CF ID
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET CF_ID = ID 
        WHERE STATUS IN (''ACT'', ''PNL'') 
        AND DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''':: DATE ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'DEBUG', 'SP_IFRS_COST_FEE_STATUS', 'REVERSAL-PAIRING');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMP_REV_PAIR ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TMP_REV_PAIR AS 
        SELECT 
            A.ID 
            ,A.CF_ID 
            ,MIN(B.ID) AS PAIR_ID 
        FROM ' || V_TABLEINSERT1 || ' A 
        LEFT JOIN ' || V_TABLEINSERT1 || ' B 
        ON B.FLAG_REVERSE = ''N'' 
        AND B.CF_ID_REV IS NULL 
        AND B.MASTERID = A.MASTERID 
        AND B.AMOUNT = A.AMOUNT 
        AND B.CCY = A.CCY 
        AND B.FLAG_CF = A.FLAG_CF 
        AND B.TRX_CODE = A.TRX_CODE 
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''':: DATE 
        AND A.FLAG_REVERSE = ''Y'' 
        GROUP BY A.ID, A.CF_ID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE TMP_REV_PAIR A 
        SET PAIR_ID = NULL 
        FROM ( 
            SELECT PAIR_ID, MIN(ID) AS ALLOWED_ID 
            FROM TMP_REV_PAIR 
            GROUP BY PAIR_ID 
            HAVING COUNT(PAIR_ID) > 1 
        ) B 
        WHERE A.PAIR_ID = B.PAIR_ID 
        AND A.ID <> B.ALLOWED_ID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS TMP_REV_PAIR2';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE TMP_REV_PAIR2
        (
            ID2 BIGINT 
            ,CF_ID2 BIGINT 
            ,PAIR_ID2 BIGINT 
        ) ';
    EXECUTE (V_STR_QUERY);

    V_CX := 1;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT EXISTS (SELECT * FROM TMP_REV_PAIR WHERE PAIR_ID IS NULL) ';
    EXECUTE (V_STR_QUERY) INTO V_HAS_NULL_PAIR;

    WHILE V_CX <= 5 AND V_HAS_NULL_PAIR LOOP 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM TMP_REV_PAIR2';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO TMP_REV_PAIR2 
            (ID2, CF_ID2, PAIR_ID2) 
            SELECT A.ID, A.CF_ID, MIN(B.ID) 
            FROM ' || V_TABLEINSERT1 || ' A 
            LEFT JOIN ' || V_TABLEINSERT1 || ' B 
            ON B.FLAG_REVERSE = ''N'' 
            AND B.CF_ID_REV IS NULL 
            AND B.MASTERID = A.MASTERID 
            AND B.AMOUNT = A.AMOUNT 
            AND B.CCY = A.CCY 
            AND B.FLAG_CF = A.FLAG_CF 
            AND B.TRX_CODE = A.TRX_CODE 
            AND B.ID NOT IN ( 
                SELECT PAIR_ID 
                FROM TMP_REV_PAIR 
                WHERE PAIR_ID IS NOT NULL
            )
            WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''':: DATE 
            AND A.FLAG_REVERSE = ''Y'' 
            AND A.ID IN (
                SELECT ID 
                FROM TMP_REV_PAIR 
                WHERE PAIR_ID IS NULL
            )
            GROUP BY A.ID, A.CF_ID ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE TMP_REV_PAIR_2 A 
            SET PAIR_ID2 = NULL 
            FROM (
                SELECT PAIR_ID2 AS PAIR_ID, MIN(ID2) AS ALLOWED_ID 
                FROM TMP_REV_PAIR2 
                GROUP BY PAIR_ID2 
                HAVING COUNT(PAIR_ID2) > 1 
            ) B 
            WHERE A.PAIR_ID2 = B.PAIR_ID 
            AND A.ID2 <> B.ALLOWED_ID ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE TMP_REV_PAIR A 
            SET PAIR_ID = B.PAIR_ID2 
            FROM TMP_REV_PAIR2 B 
            WHERE A.ID = B.ID2 
            AND B.PAIR_ID2 IS NOT NULL ';
        EXECUTE (V_STR_QUERY);

        V_CX := V_CX + 1;

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'EXISTS(SELECT * FROM TMP_REV_PAIR WHERE PAIR_ID IS NULL) ';
        EXECUTE (V_STR_QUERY) INTO V_HAS_NULL_PAIR;
    END LOOP;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET STATUS = ''FRZREVPRO'' 
        FROM TMP_REV_PAIR B 
        WHERE A.CF_ID = B.CF_ID 
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''':: DATE 
        AND B.PAIR_ID IS NULL ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM TMP_REV_PAIR WHERE PAIR_ID IS NULL ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET CF_ID_REV = B.PAIR_ID 
        FROM TMP_REV_PAIR B 
        WHERE A.CF_ID = B.CF_ID 
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''':: DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET STATUS = ''FRZNF'', CREATEDBY = ''SP_IFRS_TRAN_DAILY'' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''':: DATE 
        AND MASTERID NOT IN (
            SELECT MASTERID 
            FROM ' || V_TABLEINSERT2 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || ''':: DATE 
        ) ';
    EXECUTE (V_STR_QUERY);

    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_COST_FEE_STATUS', '');

    RAISE NOTICE 'SP_IFRS_COST_FEE_STATUS | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT1;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_COST_FEE_STATUS';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT1 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$
