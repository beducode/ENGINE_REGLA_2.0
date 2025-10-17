CREATE OR REPLACE SP_IFRS_REPORT_RECON(IN P_RUNID CHARACTER VARYING DEFAULT 'S_00000_0000'::CHARACTER VARYING, IN P_DOWNLOAD_DATE DATE DEFAULT NULL::DATE, IN P_PRC CHARACTER VARYING DEFAULT 'S'::CHARACTER VARYING)
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

    ---- VARIABLE PROCESS
    V_ISBOM INT;
    V_ISBOY INT;
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
        V_TABLEINSERT1 := 'IFRS_ACCT_COST_FEE_SUMM_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_ACCT_JOURNAL_DATA_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_JOURNAL_PARAM_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_LOAN_REPORT_RECON_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_ACCT_COST_FEE_SUMM';
        V_TABLEINSERT2 := 'IFRS_ACCT_JOURNAL_DATA';
        V_TABLEINSERT3 := 'IFRS_JOURNAL_PARAM';
        V_TABLEINSERT4 := 'IFRS_LOAN_REPORT_RECON';
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

    SELECT B.VALUE1, B.VALUE2 INTO V_ROUND, V_FUNCROUND
    FROM TBLM_COMMONCODEHEADER A 
    JOIN TBLM_COMMONCODEDETAIL B 
    ON A.COMMONCODE = B.COMMONCODE 
    WHERE A.COMMONCODE = 'SCM003';
    
    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- RECORD RUN_ID --------
    CALL SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, PG_BACKEND_PID(), CURRENT_DATE);
    -------- RECORD RUN_ID --------

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT4 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT4 || ' AS SELECT * FROM IFRS_LOAN_REPORT_RECON WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_REPORT_RECON', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT 
        CASE 
            WHEN TO_CHAR(''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE, ''YYYYMM'') <> TO_CHAR(''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE, ''YYYYMM'') 
            THEN 1 
            ELSE 0 
        END
        ,CASE 
            WHEN TO_CHAR(''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE, ''YYYY'') <> TO_CHAR(''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE, ''YYYY'') 
            THEN 1 
            ELSE 0 
        END ';
    EXECUTE (V_STR_QUERY) INTO V_ISBOM, V_ISBOY;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT4 || ' 
        WHERE DOWNLOAD_DATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_JOURNAL_PARAM' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_JOURNAL_PARAM' || ' 
        (        
            GL_CONSTNAME        
            ,TRX_CODE        
            ,FLAG_CF        
            ,JOURNALCODE        
            ,DRCR        
            ,GLNO        
            ,GL_INTERNAL_CODE        
            ,COSTCENTER        
            ,JOURNAL_DESC        
            ,CCY        
        ) SELECT 
            GL_CONSTNAME        
            ,TRX_CODE        
            ,FLAG_CF        
            ,JOURNALCODE        
            ,DRCR        
            ,GLNO        
            ,GL_INTERNAL_CODE        
            ,COSTCENTER        
            ,JOURNAL_DESC        
            ,CCY        
        FROM ' || V_TABLEINSERT3 || '
        WHERE JOURNALCODE IN (        
            ''ACCRU''        
            ,''ACCRU_NE''        
            ,''ITRCG''        
            ,''ITRCG_SL''        
            ,''ACCRU_SL''        
            ,''ADJMR''        
            ,''ITRCG_NE''        
            ,''ITRCG2''        
            ,''ITRCG2_SL''        
            ,''ITRCG1''       
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_JOURNAL' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_JOURNAL' || ' 
        (        
            DOWNLOAD_DATE        
            ,MASTERID        
            ,ACCTNO        
            ,DATASOURCE        
            ,PRDTYPE        
            ,PRDCODE        
            ,TRXCODE        
            ,BRANCH        
            ,CCY        
            ,JOURNALCODE        
            ,DRCR        
            ,FLAG_CF        
            ,REVERSE        
            ,VALCTR_CODE        
            ,GLNO        
            ,ORG_AMOUNT        
            ,IDR_AMOUNT        
            ,CLS_AMOUNT        
            ,ACT_AMOUNT        
            ,METHOD        
        ) SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE         
            ,GL.MASTERID        
            ,GL.ACCTNO        
            ,GL.DATASOURCE        
            ,GL.PRDTYPE        
            ,GL.PRDCODE        
            ,GL.TRXCODE        
            ,GL.BRANCH        
            ,GL.CCY        
            ,GL.JOURNALCODE        
            ,GL.DRCR        
            ,GL.FLAG_CF        
            ,GL.REVERSE        
            ,GL.VALCTR_CODE        
            ,GL.GLNO        
            ,SUM(CASE         
                WHEN GL.DRCR = ''C''        
                THEN GL.N_AMOUNT        
                ELSE GL.N_AMOUNT * - 1        
            END) AS ORG_AMOUNT        
            ,SUM(CASE         
                WHEN GL.DRCR = ''C''        
                THEN GL.N_AMOUNT_IDR * COALESCE(1, 1)        
                ELSE GL.N_AMOUNT_IDR * COALESCE(1, 1) * - 1        
            END) AS IDR_AMOUNT        
            ,0 CLS_AMOUNT        
            ,0 ACT_AMOUNT        
            ,METHOD        
        FROM ' || V_TABLEINSERT2 || ' GL        
        WHERE GL.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE   
            AND GL.TRXCODE <> ''BENEFIT''  
            AND GL.JOURNALCODE NOT IN (''OCIMTM'',''PLMTM'') 
        GROUP BY 
            GL.MASTERID        
            ,GL.ACCTNO        
            ,GL.DATASOURCE        
            ,GL.PRDTYPE        
            ,GL.PRDCODE        
            ,GL.TRXCODE        
            ,GL.BRANCH        
            ,GL.CCY        
            ,GL.JOURNALCODE        
            ,GL.DRCR        
            ,GL.FLAG_CF        
            ,GL.REVERSE        
            ,GL.VALCTR_CODE        
            ,GL.GLNO        
            ,METHOD ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_LOAN_REPORT_RECON' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_LOAN_REPORT_RECON' || ' 
        (        
            DOWNLOAD_DATE        
            ,MASTERID        
            ,ACCOUNT_NUMBER        
            ,BRANCH_CODE        
            ,TRANSACTION_CODE        
            ,CCY        
            ,INITIAL_GL_FEE_AMT        
            ,INITIAL_GL_COST_AMT        
            ,UNAMORT_GL_FEE_AMT        
            ,UNAMORT_GL_COST_AMT        
            ,AMORT_GL_FEE_AMT        
            ,DAILY_AMORT_GL_FEE_AMT        
            ,MTD_AMORT_GL_FEE_AMT        
            ,YTD_AMORT_GL_FEE_AMT        
            ,AMORT_GL_COST_AMT        
            ,DAILY_AMORT_GL_COST_AMT        
            ,MTD_AMORT_GL_COST_AMT        
            ,YTD_AMORT_GL_COST_AMT        
            ,METHOD        
        ) SELECT 
            A.DOWNLOAD_DATE        
            ,A.MASTERID        
            ,A.ACCTNO        
            ,A.BRANCH        
            ,NULL        
            ,A.CCY        
            ,SUM(CASE         
                WHEN A.JOURNALCODE = ''DEFA0''   
                    AND FLAG_CF = ''F''      
                    AND A.GLNO = X1.GLNO        
                THEN A.ORG_AMOUNT        
                ELSE 0        
            END) AS INITIAL_GL_FEE_AMT        
            ,SUM(CASE         
                WHEN A.JOURNALCODE = ''DEFA0''   
                    AND FLAG_CF = ''C''     
                    AND A.GLNO = X4.GLNO        
                THEN A.ORG_AMOUNT        
                ELSE 0        
            END) AS INITIAL_GL_COST_AMT        
            ,SUM(CASE         
                WHEN A.FLAG_CF = ''F''        
                    AND A.GLNO = X2.GLNO        
                THEN A.ORG_AMOUNT        
                ELSE 0        
            END) AS UNAMORT_GL_FEE_AMT        
            ,SUM(CASE         
                WHEN A.FLAG_CF = ''C''        
                    AND A.GLNO = X5.GLNO        
                THEN A.ORG_AMOUNT        
                ELSE 0        
            END) AS UNAMORT_GL_COST_AMT        
            ,SUM(CASE         
                WHEN A.FLAG_CF = ''F''        
                    AND A.GLNO = X3.GLNO        
                THEN A.ORG_AMOUNT        
                ELSE 0        
            END) AS AMORT_GL_FEE_AMT        
            ,SUM(CASE         
                WHEN A.FLAG_CF = ''F''        
                    AND A.GLNO = X3.GLNO        
                THEN A.ORG_AMOUNT        
                ELSE 0        
            END) AS DAILY_AMORT_GL_FEE_AMT        
            ,SUM(CASE         
                WHEN A.FLAG_CF = ''F''        
                    AND A.GLNO = X3.GLNO        
                THEN A.ORG_AMOUNT        
                ELSE 0        
            END) AS MTD_AMORT_GL_FEE_AMT        
            ,SUM(CASE         
                WHEN A.FLAG_CF = ''F''        
                    AND A.GLNO = X3.GLNO        
                THEN A.ORG_AMOUNT        
                ELSE 0        
            END) AS YTD_AMORT_GL_FEE_AMT        
            ,SUM(CASE         
                WHEN A.FLAG_CF = ''C''        
                    AND A.GLNO = X6.GLNO        
                THEN A.ORG_AMOUNT        
                ELSE 0        
            END) AS AMORT_GL_COST_AMT        
            ,SUM(CASE         
                WHEN A.FLAG_CF = ''C''        
                    AND A.GLNO = X6.GLNO        
                THEN A.ORG_AMOUNT        
                ELSE 0        
            END) AS DAILY_AMORT_GL_COST_AMT        
            ,SUM(CASE         
                WHEN A.FLAG_CF = ''C''        
                    AND A.GLNO = X6.GLNO        
                THEN A.ORG_AMOUNT        
                ELSE 0        
            END) AS MTD_AMORT_GL_COST_AMT        
            ,SUM(CASE         
                WHEN A.FLAG_CF = ''C''        
                    AND A.GLNO = X6.GLNO        
                THEN A.ORG_AMOUNT        
                ELSE 0        
            END) AS YTD_AMORT_GL_COST_AMT        
            ,METHOD        
        FROM ' || 'TMP_JOURNAL' || ' A        
        LEFT JOIN (        
            SELECT DISTINCT GLNO        
            FROM ' || 'TMP_JOURNAL_PARAM' || '
            WHERE JOURNALCODE IN (        
                ''ITRCG''        
                ,''ITRCG_SL''        
                ,''ITRCG2''        
                ,''ITRCG2_SL''        
                ,''ITRCG_NE''        
                ,''ITRCG1''        
            )        
            AND DRCR = ''C''        
            AND FLAG_CF = ''F''        
        ) X1 
        ON A.GLNO = X1.GLNO        
        --UNAMORT FEE GL                    
        LEFT JOIN (        
            SELECT DISTINCT GLNO        
            FROM ' || 'TMP_JOURNAL_PARAM' || '
            WHERE JOURNALCODE IN (        
                ''ACCRU''        
                ,''ACCRU_SL''        
                ,''ACCRU_NE''        
                ,''OTHER''      
            )        
            AND DRCR = ''D''        
            AND FLAG_CF = ''F''        
        ) X2 
        ON A.GLNO = X2.GLNO        
        --AMORT FEE GL                    
        LEFT JOIN (        
            SELECT DISTINCT GLNO        
            FROM ' || 'TMP_JOURNAL_PARAM' || '
            WHERE JOURNALCODE IN (        
                ''ACCRU''        
                ,''ACCRU_SL''        
                ,''ACCRU_NE''        
                ,''OTHER''      
            )        
            AND DRCR = ''C''        
            AND FLAG_CF = ''F''        
        ) X3 
        ON A.GLNO = X3.GLNO        
        --INITIAL COST GL                   
        LEFT JOIN (        
            SELECT DISTINCT GLNO        
            FROM ' || 'TMP_JOURNAL_PARAM' || '
            WHERE JOURNALCODE IN (        
                ''ITRCG''        
                ,''ITRCG_SL''        
                ,''ITRCG2''        
                ,''ITRCG2_SL''        
                ,''ITRCG_NE''        
                ,''ITRCG1''        
            )        
            AND DRCR = ''D''        
            AND FLAG_CF = ''C''        
        ) X4 
        ON A.GLNO = X4.GLNO        
        LEFT JOIN (        
            SELECT DISTINCT GLNO        
            FROM ' || 'TMP_JOURNAL_PARAM' || '
            WHERE JOURNALCODE IN (        
                ''ACCRU''        
                ,''ACCRU_SL''        
                ,''ACCRU_NE''        
                ,''OTHER''      
            )        
            AND DRCR = ''C''        
            AND FLAG_CF = ''C''        
        ) X5 
        ON A.GLNO = X5.GLNO        
        LEFT JOIN (        
            SELECT DISTINCT GLNO        
            FROM ' || 'TMP_JOURNAL_PARAM' || '
            WHERE JOURNALCODE IN (        
                ''ACCRU''        
                ,''ACCRU_SL''        
                ,''ACCRU_NE''        
                ,''OTHER''      
            )        
            AND DRCR = ''D''        
            AND FLAG_CF = ''C''        
        ) X6 
        ON A.GLNO = X6.GLNO        
        GROUP BY 
            A.DOWNLOAD_DATE        
            ,A.MASTERID        
            ,A.ACCTNO        
            ,A.BRANCH         
            ,A.CCY        
            ,A.METHOD ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_LOAN_REPORT_RECON' || ' 
        (        
            DOWNLOAD_DATE        
            ,MASTERID        
            ,ACCOUNT_NUMBER        
            ,BRANCH_CODE        
            ,TRANSACTION_CODE        
            ,CCY        
            ,INITIAL_GL_FEE_AMT      
            ,INITIAL_GL_COST_AMT        
            ,UNAMORT_GL_FEE_AMT        
            ,UNAMORT_GL_COST_AMT        
            ,AMORT_GL_FEE_AMT        
            ,DAILY_AMORT_GL_FEE_AMT        
            ,MTD_AMORT_GL_FEE_AMT        
            ,YTD_AMORT_GL_FEE_AMT        
            ,AMORT_GL_COST_AMT        
            ,DAILY_AMORT_GL_COST_AMT        
            ,MTD_AMORT_GL_COST_AMT        
            ,YTD_AMORT_GL_COST_AMT        
            ,METHOD        
        ) SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE         
            ,A.MASTERID        
            ,A.ACCOUNT_NUMBER        
            ,A.BRANCH_CODE        
            ,A.TRANSACTION_CODE        
            ,A.CCY        
            ,A.INITIAL_GL_FEE_AMT        
            ,A.INITIAL_GL_COST_AMT        
            ,A.UNAMORT_GL_FEE_AMT        
            ,A.UNAMORT_GL_COST_AMT        
            ,A.AMORT_GL_FEE_AMT        
            ,0 AS DAILY_AMORT_GL_FEE_AMT        
            ,CASE         
                WHEN ' || V_ISBOM || ' = 0        
                THEN MTD_AMORT_GL_FEE_AMT        
                ELSE 0        
            END AS MTD_AMORT_GL_FEE_AMT        
            ,CASE         
                WHEN ' || V_ISBOY || ' = 0        
                THEN YTD_AMORT_GL_FEE_AMT        
                ELSE 0        
            END AS YTD_AMORT_GL_FEE_AMT        
            ,A.AMORT_GL_COST_AMT        
            ,0 AS DAILY_AMORT_GL_COST_AMT        
            ,CASE         
                WHEN ' || V_ISBOM || ' = 0        
                THEN MTD_AMORT_GL_COST_AMT        
                ELSE 0        
            END AS MTD_AMORT_GL_COST_AMT        
            ,CASE         
                WHEN ' || V_ISBOY || ' = 0        
                THEN YTD_AMORT_GL_COST_AMT        
                ELSE 0        
            END AS YTD_AMORT_GL_COST_AMT        
            ,METHOD        
        FROM ' || V_TABLEINSERT4 || ' A        
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || ' 
        (        
            DOWNLOAD_DATE        
            ,MASTERID        
            ,ACCOUNT_NUMBER        
            ,TRANSACTION_CODE        
            ,CCY    
            ,INITIAL_GL_FEE_AMT  
            ,INITIAL_GL_COST_AMT        
            ,UNAMORT_GL_FEE_AMT        
            ,UNAMORT_GL_COST_AMT        
            ,AMORT_GL_FEE_AMT        
            ,DAILY_AMORT_GL_FEE_AMT        
            ,MTD_AMORT_GL_FEE_AMT        
            ,YTD_AMORT_GL_FEE_AMT        
            ,AMORT_GL_COST_AMT        
            ,DAILY_AMORT_GL_COST_AMT        
            ,MTD_AMORT_GL_COST_AMT        
            ,YTD_AMORT_GL_COST_AMT        
            ,METHOD        
        ) SELECT 
            X.DOWNLOAD_DATE              
            ,X.MASTERID              
            ,X.ACCOUNT_NUMBER              
            ,X.TRANSACTION_CODE        
            ,X.CCY    
            ,SUM(INITIAL_GL_FEE_AMT)  
            ,SUM(INITIAL_GL_COST_AMT)              
            ,SUM(X.UNAMORT_GL_FEE_AMT)           
            ,SUM(X.UNAMORT_GL_COST_AMT)        
            ,SUM(X.AMORT_GL_FEE_AMT)         
            ,SUM(X.DAILY_AMORT_GL_FEE_AMT)        
            ,SUM(X.MTD_AMORT_GL_FEE_AMT)        
            ,SUM(X.YTD_AMORT_GL_FEE_AMT)        
            ,SUM(X.AMORT_GL_COST_AMT)        
            ,SUM(X.DAILY_AMORT_GL_COST_AMT)        
            ,SUM(X.MTD_AMORT_GL_COST_AMT)        
            ,SUM(X.YTD_AMORT_GL_COST_AMT)                 
            ,X.METHOD         
        FROM ' || 'TMP_LOAN_REPORT_RECON' || ' X              
        WHERE X.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE           
        GROUP BY 
            X.DOWNLOAD_DATE              
            ,X.MASTERID              
            ,X.ACCOUNT_NUMBER              
            ,X.TRANSACTION_CODE        
            ,X.CCY            
            ,X.METHOD  ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT4 || ' A 
        SET 
            FACILITY_NUMBER = B.FACILITY_NUMBER        
            ,CUSTOMER_NAME = B.CUSTOMER_NAME        
            ,BRANCH_CODE = B.BRANCH_CODE        
            ,DATA_SOURCE = B.DATA_SOURCE        
            ,PRODUCT_CODE = B.PRODUCT_CODE        
            ,PRODUCT_TYPE = B.PRODUCT_TYPE        
            ,JF_FLAG = B.JF_FLAG        
            ,EXCHANGE_RATE = B.EXCHANGE_RATE        
            ,BI_COLLECTABILITY = B.BI_COLLECTABILITY        
            ,DAY_PAST_DUE = B.DAY_PAST_DUE        
            ,INTEREST_RATE = B.INTEREST_RATE        
            ,EIR = B.EIR        
            ,LOAN_START_DATE = B.LOAN_START_DATE        
            ,LOAN_DUE_DATE = B.LOAN_DUE_DATE        
            ,OUTSTANDING = B.OUTSTANDING        
            ,OUTSTANDING_JF = B.OUTSTANDING_JF        
            ,OUTSTANDING_BANK = B.OUTSTANDING_BANK        
            ,PLAFOND = B.PLAFOND        
            ,METHOD = B.METHOD        
        FROM ' || V_TABLEINSERT4 || ' B 
        WHERE A.MASTERID = B.MASTERID        
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE         
        AND B.DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'MERGE INTO ' || V_TABLEINSERT4 || ' A
        USING ' || 'IFRS_MASTER_ACCOUNT' || ' B 
        ON (
            A.MASTERID = B.MASTERID        
            AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE         
            AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE         
        ) WHEN MATCHED THEN UPDATE SET 
            FACILITY_NUMBER = B.FACILITY_NUMBER        
            ,CUSTOMER_NAME = B.CUSTOMER_NAME        
            ,BRANCH_CODE = B.BRANCH_CODE        
            ,DATA_SOURCE = B.DATA_SOURCE        
            ,PRODUCT_CODE = B.PRODUCT_CODE        
            ,PRODUCT_TYPE = B.PRODUCT_TYPE        
            ,JF_FLAG = B.JF_FLAG        
            ,EXCHANGE_RATE = B.EXCHANGE_RATE        
            ,BI_COLLECTABILITY = B.BI_COLLECTABILITY        
            ,DAY_PAST_DUE = B.DAY_PAST_DUE        
            ,INTEREST_RATE = B.INTEREST_RATE        
            ,EIR = B.EIR        
            ,LOAN_START_DATE = B.LOAN_START_DATE        
            ,LOAN_DUE_DATE = B.LOAN_DUE_DATE        
            ,OUTSTANDING = B.OUTSTANDING        
            ,OUTSTANDING_JF = B.OUTSTANDING_JF        
            ,OUTSTANDING_BANK = B.OUTSTANDING_BANK        
            ,PLAFOND = B.PLAFOND        
            ,UNAMORT_MASTER_FEE_AMT = COALESCE(B.UNAMORT_FEE_AMT, 0)  
            ,UNAMORT_MASTER_FEE_NONEIR_AMT = COALESCE(B.UNAMORT_FEE_AMT_JF, 0)        
            ,UNAMORT_MASTER_COST_AMT = COALESCE(B.UNAMORT_COST_AMT, 0)        
            ,UNAMORT_MASTER_COST_NONEIR_AMT = COALESCE(B.UNAMORT_COST_AMT_JF, 0)        
            ,METHOD = B.AMORT_TYPE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT4 || ' 
        SET 
            INITIAL_TRX_FEE_AMT = COALESCE(C.FEE_AMOUNT, B.INITIAL_TRX_FEE_AMT) 
            ,INITIAL_TRX_COST_AMT = COALESCE(C.COST_AMOUNT,B.INITIAL_TRX_COST_AMT)
	    FROM ' || V_TABLEINSERT4 || ' A          
	    LEFT JOIN ' || V_TABLEINSERT4 || ' B 
            ON A.MASTERID = B.MASTERID          
            AND B.DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE           
	    LEFT JOIN (
            SELECT  
                TRX.MASTERID 
                ,SUM(TRX.AMOUNT_FEE) AS FEE_AMOUNT 
                ,SUM(TRX.AMOUNT_COST) AS COST_AMOUNT
            FROM ' || V_TABLEINSERT1 || ' TRX 
            WHERE TRX.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE           
            GROUP BY TRX.MASTERID          
	    ) C 
        ON A.MASTERID = C.MASTERID          
	    WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT4 || ' 
        SET 
            COUNTER_CHECK_FEE = CASE         
                WHEN Z.FLAG_AL = ''L''        
                THEN ROUND((SUMMARY.INITIAL_GL_FEE_AMT - SUMMARY.UNAMORT_GL_FEE_AMT - SUMMARY.AMORT_GL_FEE_AMT) + (A.UNAMORT_MASTER_FEE_AMT - SUMMARY.UNAMORT_GL_FEE_AMT), ' || V_ROUND || ')        
                ELSE ROUND((SUMMARY.INITIAL_GL_FEE_AMT - SUMMARY.UNAMORT_GL_FEE_AMT - SUMMARY.AMORT_GL_FEE_AMT) + (A.UNAMORT_MASTER_FEE_AMT + SUMMARY.UNAMORT_GL_FEE_AMT), ' || V_ROUND || ')        
            END        
            ,COUNTER_CHECK_COST = CASE         
                WHEN Z.FLAG_AL = ''L''        
                THEN ROUND((SUMMARY.INITIAL_GL_COST_AMT - SUMMARY.UNAMORT_GL_COST_AMT - SUMMARY.AMORT_GL_COST_AMT) + (A.UNAMORT_MASTER_COST_AMT - SUMMARY.UNAMORT_GL_COST_AMT), ' || V_ROUND || ')        
                ELSE ROUND((SUMMARY.INITIAL_GL_COST_AMT- SUMMARY.UNAMORT_GL_COST_AMT - SUMMARY.AMORT_GL_COST_AMT) + (A.UNAMORT_MASTER_COST_AMT + SUMMARY.UNAMORT_GL_COST_AMT), ' || V_ROUND || ')        
            END        
        FROM ' || V_TABLEINSERT4 || ' A        
        INNER JOIN (        
            SELECT 
                MASTERID        
                ,SUM(X.INITIAL_GL_FEE_AMT) INITIAL_GL_FEE_AMT        
                ,SUM(X.UNAMORT_GL_FEE_AMT) UNAMORT_GL_FEE_AMT        
                ,SUM(X.AMORT_GL_FEE_AMT) AMORT_GL_FEE_AMT        
                ,SUM(X.INITIAL_GL_COST_AMT) INITIAL_GL_COST_AMT        
                ,SUM(X.UNAMORT_GL_COST_AMT) UNAMORT_GL_COST_AMT        
                ,SUM(X.AMORT_GL_COST_AMT) AMORT_GL_COST_AMT        
            FROM ' || V_TABLEINSERT4 || ' X        
            WHERE X.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE         
            GROUP BY MASTERID        
        ) SUMMARY 
        ON SUMMARY.MASTERID = A.MASTERID        
        LEFT JOIN IFRS_PRODUCT_PARAM Z 
            ON A.DATA_SOURCE = Z.DATA_SOURCE        
            AND A.PRODUCT_CODE = Z.PRD_CODE        
            AND A.PRODUCT_TYPE = Z.PRD_TYPE        
            AND (        
                A.CCY = Z.CCY        
                OR Z.CCY = ''ALL''        
            )        
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_REPORT_RECON', '');

    RAISE NOTICE 'SP_IFRS_REPORT_RECON | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT4;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_REPORT_RECON';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT4 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$
