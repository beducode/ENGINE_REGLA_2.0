---- DROP PROCEDURE SP_IFRS_ACCT_JOURNAL_DATA;

CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_JOURNAL_DATA(
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
    V_TABLENAME VARCHAR(100);
    V_TABLEINSERT1 VARCHAR(100);
    V_TABLEINSERT2 VARCHAR(100);
    V_TABLEINSERT3 VARCHAR(100);
    V_TABLEINSERT4 VARCHAR(100);
    V_TABLEINSERT5 VARCHAR(100);
    V_TABLEINSERT6 VARCHAR(100);
    V_TABLEINSERT7 VARCHAR(100);

    ---- VARIABLE PROCESS
    V_MIGRATIONDATE DATE;
    
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
        V_TABLEINSERT1 := 'IFRS_ACCT_COST_FEE_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_ACCT_JOURNAL_DATA_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_ACCT_JOURNAL_INTM_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_IMA_AMORT_CURR_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_IMA_AMORT_PREV_' || P_RUNID || '';
        V_TABLEINSERT6 := 'IFRS_JOURNAL_PARAM_' || P_RUNID || '';
        V_TABLEINSERT7 := 'IFRS_TRX_FACILITY_' || P_RUNID || '';
    ELSE 
        V_TABLENAME := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT1 := 'IFRS_ACCT_COST_FEE';
        V_TABLEINSERT2 := 'IFRS_ACCT_JOURNAL_DATA';
        V_TABLEINSERT3 := 'IFRS_ACCT_JOURNAL_INTM';
        V_TABLEINSERT4 := 'IFRS_IMA_AMORT_CURR';
        V_TABLEINSERT5 := 'IFRS_IMA_AMORT_PREV';
        V_TABLEINSERT6 := 'IFRS_JOURNAL_PARAM';
        V_TABLEINSERT7 := 'IFRS_TRX_FACILITY';
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

    SELECT VALUE2 INTO V_MIGRATIONDATE 
    FROM TBLM_COMMONCODEDETAIL
    WHERE VALUE1 = 'ITRCGM';
    
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
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT2 || ' AS SELECT * FROM IFRS_ACCT_JOURNAL_DATA WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT7 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT7 || ' AS SELECT * FROM IFRS_TRX_FACILITY WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_ACCT_JOURNAL_DATA', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT3 || ' A 
        SET METHOD = ''EIR''
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND SUBSTRING(SOURCEPROCESS, 1, 3) = ''EIR'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT3 || ' 
        SET 
            FLAG_AL = CASE 
                WHEN COALESCE(B.IAS_CLASS, '''') = '''' 
                THEN D.INST_CLS_VALUE 
                ELSE B.IAS_CLASS 
            END 
            ,N_AMOUNT_IDR = A.N_AMOUNT * COALESCE(C.RATE_AMOUNT, 1)       
        FROM ' || V_TABLEINSERT3 || ' A       
        LEFT JOIN ' || V_TABLEINSERT4 || ' B       
            ON A.MASTERID = B.MASTERID       
        LEFT JOIN ' || 'IFRS_MASTER_EXCHANGE_RATE' || ' C      
            ON A.CCY = C.CURRENCY  
            AND A.DOWNLOAD_DATE = C.DOWNLOAD_DATE       
        LEFT JOIN ' || 'IFRS_MASTER_PRODUCT_PARAM' || ' D 
            ON A.DATASOURCE = D.DATA_SOURCE  
            AND A.PRDCODE = D.PRD_CODE                                            
            AND (                                            
                A.CCY = D.CCY  
                OR D.CCY = ''ALL'' 
            )                                        
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT2 || ' 
        WHERE DOWNLOAD_DATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    IF V_CURRDATE = V_MIGRATIONDATE 
    THEN 
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
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
                ,A.REVERSE              
                ,A.FLAG_CF              
                ,CASE               
                    WHEN (              
                        A.REVERSE = ''N''              
                        AND A.FLAG_AL IN (''A'', ''O'')              
                    ) OR (              
                        A.REVERSE = ''Y''              
                        AND A.FLAG_AL NOT IN (''A'', ''O'')         
                    )              
                    THEN CASE               
                        WHEN A.N_AMOUNT >= 0              
                            AND A.FLAG_CF = ''F''              
                            AND A.JOURNALCODE IN (              
                                ''ACCRU''              
                                ,''AMORT''              
                            )              
                        THEN B.DRCR              
                        WHEN A.N_AMOUNT <= 0              
                            AND A.FLAG_CF = ''C''              
                            AND A.JOURNALCODE IN ( 
                                ''ACCRU''          
                                ,''AMORT''              
                            )              
                        THEN B.DRCR              
                        WHEN A.N_AMOUNT <= 0              
                            AND A.FLAG_CF = ''F''              
                            AND A.JOURNALCODE IN (''DEFA0'')              
                        THEN B.DRCR              
                        WHEN A.N_AMOUNT >= 0              
                            AND A.FLAG_CF = ''C''              
                            AND A.JOURNALCODE IN (''DEFA0'')              
                        THEN B.DRCR              
                        ELSE CASE               
                            WHEN B.DRCR = ''D''              
                            THEN ''C''              
                            ELSE ''D''              
                        END              
                    END              
                    ELSE CASE               
                        WHEN A.N_AMOUNT <= 0              
                            AND A.FLAG_CF = ''F''              
                            AND A.JOURNALCODE IN (              
                                ''ACCRU''              
                                ,''AMORT''              
                            )              
                        THEN B.DRCR              
                        WHEN A.N_AMOUNT >= 0              
                            AND A.FLAG_CF = ''C''              
                            AND A.JOURNALCODE IN (              
                                ''ACCRU''              
                                ,''AMORT''              
                            )              
                        THEN B.DRCR              
                        WHEN A.N_AMOUNT >= 0              
                            AND A.FLAG_CF = ''F''              
                            AND A.JOURNALCODE IN (''DEFA0'')              
                        THEN B.DRCR              
                        WHEN A.N_AMOUNT <= 0              
                            AND A.FLAG_CF = ''C''              
                            AND A.JOURNALCODE IN (''DEFA0'')              
                        THEN B.DRCR              
                        ELSE CASE               
                            WHEN B.DRCR = ''D''              
                            THEN ''C''              
                            ELSE ''D''              
                        END              
                    END              
                END AS DRCR              
                ,B.GLNO              
                ,ABS(A.N_AMOUNT)              
                ,ABS(A.N_AMOUNT_IDR)              
                ,A.SOURCEPROCESS              
                ,A.ID             
                ,CURRENT_TIMESTAMP              
                ,''SP_JOURNAL_DATA2''              
                ,A.BRANCH              
                ,B.JOURNALCODE AS JOURNALCODE2              
                ,B.JOURNAL_DESC              
                ,B.JOURNALCODE              
                ,B.COSTCENTER || ''-'' || COALESCE(IMC.APPLICATION_NO, IMP.APPLICATION_NO, '''')              
                ,B.GL_INTERNAL_CODE              
                ,A.METHOD         
                ,IMC.ACCOUNT_TYPE AS ACCOUNT_TYPE    
                ,IMC.CUSTOMER_TYPE AS CUSTOMER_TYPE       
            FROM ' || V_TABLEINSERT3 || ' A    
            JOIN ' || V_TABLEINSERT1 || ' FEE 
                ON A.DOWNLOAD_DATE = FEE.DOWNLOAD_DATE 
                AND A.CF_ID = FEE.CF_ID 
                AND FEE.TRX_LEVEL <> ''FAC'' 
                AND FEE.SOURCE_TABLE = ''TBLU_TRANS_ASSET''          
            LEFT JOIN ' || V_TABLEINSERT4 || ' IMC 
                ON A.MASTERID = IMC.MASTERID              
            LEFT JOIN ' || V_TABLEINSERT5 || ' IMP 
                ON A.MASTERID = IMP.MASTERID              
            JOIN ' || V_TABLEINSERT6 || ' B 
                ON B.JOURNALCODE = ''ITRCGM''           
                AND (              
                    B.CCY = A.CCY              
                    OR B.CCY = ''ALL''              
                )              
                AND B.FLAG_CF = ''B''              
                AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME, IMP.GL_CONSTNAME, '''')              
                AND (              
                    A.TRXCODE = B.TRX_CODE              
                    OR B.TRX_CODE = ''ALL''              
                )        
            WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
                AND A.JOURNALCODE = ''DEFA0''              
                AND A.TRXCODE = ''BENEFIT''              
                AND A.METHOD = ''EIR''    
                AND A.DATASOURCE IN (''LOAN_T24'',''LIMIT_T24'')           
                AND A.SOURCEPROCESS NOT IN (''EIR_REV_SWITCH'',''EIR_SWITCH'') ';
        EXECUTE (V_STR_QUERY);

        GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
        V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
        V_RETURNROWS := 0;
    END IF;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
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
            ,A.REVERSE              
            ,A.FLAG_CF              
            ,CASE               
                WHEN (              
                    A.REVERSE = ''N''              
                    AND A.FLAG_AL IN (''A'', ''O'')              
                )              
                OR (              
                    A.REVERSE = ''Y''     
                    AND A.FLAG_AL NOT IN (''A'', ''O'')            
                )              
                THEN CASE               
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (          
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    ELSE CASE               
                        WHEN B.DRCR = ''D''              
                        THEN ''C''              
                        ELSE ''D''              
                    END              
                END              
                ELSE CASE               
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    ELSE CASE               
                        WHEN B.DRCR = ''D''              
                        THEN ''C''              
                        ELSE ''D''              
                    END              
                END              
            END AS DRCR              
            ,B.GLNO              
            ,ABS(A.N_AMOUNT)              
            ,ABS(A.N_AMOUNT_IDR)              
            ,A.SOURCEPROCESS              
            ,A.ID              
            ,CURRENT_TIMESTAMP              
            ,''SP_JOURNAL_DATA2''              
            ,A.BRANCH              
            ,A.JOURNALCODE2              
            ,B.JOURNAL_DESC              
            ,B.JOURNALCODE              
            ,B.COSTCENTER || ''-'' || COALESCE(IMC.APPLICATION_NO, IMP.APPLICATION_NO, '''')              
            ,B.GL_INTERNAL_CODE              
            ,A.METHOD      
            ,IMC.ACCOUNT_TYPE AS ACCOUNT_TYPE    
            ,IMC.CUSTOMER_TYPE AS CUSTOMER_TYPE           
        FROM ' || V_TABLEINSERT3 || ' A  
        JOIN ' || V_TABLEINSERT1 || ' FEE 
            ON A.DOWNLOAD_DATE = FEE.DOWNLOAD_DATE 
            AND A.CF_ID = FEE.CF_ID 
            AND FEE.TRX_LEVEL = ''FAC''              
        LEFT JOIN ' || V_TABLEINSERT4 || ' IMC 
            ON A.MASTERID = IMC.MASTERID              
        LEFT JOIN ' || V_TABLEINSERT5 || ' IMP 
            ON A.MASTERID = IMP.MASTERID              
        JOIN ' || V_TABLEINSERT6 || ' B 
            ON B.JOURNALCODE IN (              
                ''ITRCG''              
                ,''ITRCG1''              
                ,''ITRCG2''              
            )              
            AND (              
                B.CCY = A.CCY              
                OR B.CCY = ''ALL''              
            )              
            AND B.FLAG_CF = A.FLAG_CF              
            AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME, IMP.GL_CONSTNAME, '''')              
            AND (              
                A.TRXCODE = B.TRX_CODE              
                OR B.TRX_CODE = ''ALL''              
            )              
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
            AND A.JOURNALCODE = ''DEFA0''              
            AND A.TRXCODE <> ''BENEFIT''              
            AND A.METHOD = ''EIR''  
            AND A.DATASOURCE IN (''LOAN_T24'',''LIMIT_T24'')   
            AND A.DOWNLOAD_DATE = ''' || CAST(V_MIGRATIONDATE AS VARCHAR(10)) || '''::DATE           
            AND A.SOURCEPROCESS NOT IN (''EIR_REV_SWITCH'',''EIR_SWITCH'') ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
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
            ,A.REVERSE              
            ,A.FLAG_CF              
            ,CASE               
                WHEN (              
                    A.REVERSE = ''N''              
                    AND A.FLAG_AL IN (''A'', ''O'')              
                )              
                OR (              
                    A.REVERSE = ''Y''              
                    AND A.FLAG_AL NOT IN (''A'', ''O'')         
                )              
                THEN CASE               
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    ELSE CASE               
                        WHEN B.DRCR = ''D''              
                        THEN ''C''              
                        ELSE ''D''              
                    END              
                END              
                ELSE CASE               
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''  
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    ELSE CASE               
                        WHEN B.DRCR = ''D''              
                        THEN ''C''              
                        ELSE ''D''              
                    END              
                END              
            END AS DRCR              
            ,B.GLNO              
            ,ABS(A.N_AMOUNT)              
            ,ABS(A.N_AMOUNT_IDR)              
            ,A.SOURCEPROCESS              
            ,A.ID             
            ,CURRENT_TIMESTAMP              
            ,''SP_JOURNAL_DATA2''              
            ,A.BRANCH              
            ,B.JOURNALCODE              
            ,B.JOURNAL_DESC              
            ,B.JOURNALCODE              
            ,B.COSTCENTER || ''-'' || COALESCE(IMC.APPLICATION_NO, IMP.APPLICATION_NO, '''')              
            ,B.GL_INTERNAL_CODE              
            ,A.METHOD       
            ,IMC.ACCOUNT_TYPE AS ACCOUNT_TYPE    
            ,IMC.CUSTOMER_TYPE AS CUSTOMER_TYPE          
        FROM ' || V_TABLEINSERT3 || ' A  
        JOIN ' || V_TABLEINSERT1 || ' FEE 
            ON A.DOWNLOAD_DATE = FEE.DOWNLOAD_DATE 
            AND A.CF_ID = FEE.CF_ID 
            AND FEE.TRX_LEVEL = ''FAC''             
        LEFT JOIN ' || V_TABLEINSERT4 || ' IMC 
            ON A.MASTERID = IMC.MASTERID              
        LEFT JOIN ' || V_TABLEINSERT5 || ' IMP 
            ON A.MASTERID = IMP.MASTERID           
        JOIN ' || V_TABLEINSERT6 || ' B 
            ON B.JOURNALCODE IN (              
                ''ITRCG''              
                ,''ITRCG1''              
                ,''ITRCG2''              
                ,''ITEMB''              
            )              
            AND (              
                B.CCY = A.CCY              
                OR B.CCY = ''ALL''              
            )              
            AND B.FLAG_CF = ''B''              
            AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME, IMP.GL_CONSTNAME, '''')              
            AND (              
                A.TRXCODE = B.TRX_CODE              
                OR B.TRX_CODE = ''ALL''              
            )        
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
        AND A.JOURNALCODE = ''DEFA0''              
        AND A.TRXCODE = ''BENEFIT''              
        AND A.METHOD = ''EIR''    
        AND A.DATASOURCE IN (''LOAN_T24'',''LIMIT_T24'')   
        AND A.DOWNLOAD_DATE = ''' || CAST(V_MIGRATIONDATE AS VARCHAR(10)) || '''::DATE              
        AND A.SOURCEPROCESS NOT IN (''EIR_REV_SWITCH'',''EIR_SWITCH'') ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
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
            ,A.REVERSE              
            ,A.FLAG_CF              
            ,CASE   
                WHEN (     
                    A.REVERSE = ''N''              
                    AND A.FLAG_AL IN (''A'', ''O'')              
                )              
                OR (              
                    A.REVERSE = ''Y''     
                    AND A.FLAG_AL NOT IN (''A'', ''O'')            
                )              
                THEN CASE               
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (          
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    ELSE CASE               
                        WHEN B.DRCR = ''D''              
                        THEN ''C''              
                        ELSE ''D''              
                    END              
                END              
                ELSE CASE               
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    ELSE CASE               
                        WHEN B.DRCR = ''D''              
                        THEN ''C''              
                        ELSE ''D''              
                    END              
                END              
            END AS DRCR              
            ,B.GLNO              
            ,ABS(A.N_AMOUNT)              
            ,ABS(A.N_AMOUNT_IDR)              
            ,A.SOURCEPROCESS              
            ,A.ID              
            ,CURRENT_TIMESTAMP              
            ,''SP_JOURNAL_DATA2''              
            ,A.BRANCH              
            ,A.JOURNALCODE2              
            ,B.JOURNAL_DESC              
            ,B.JOURNALCODE              
            ,B.COSTCENTER || ''-'' || COALESCE(IMC.APPLICATION_NO, IMP.APPLICATION_NO, '''')              
            ,B.GL_INTERNAL_CODE              
            ,A.METHOD      
            ,IMC.ACCOUNT_TYPE AS ACCOUNT_TYPE    
            ,IMC.CUSTOMER_TYPE AS CUSTOMER_TYPE           
        FROM ' || ' IFRS_ACCT_JOURNAL_INTM' || ' A  
        INNER JOIN ' || ' IFRS_ACCT_COST_FEE' || ' FEE 
            ON A.DOWNLOAD_DATE = FEE.DOWNLOAD_DATE 
            AND A.CF_ID = FEE.CF_ID 
            AND COALESCE(FEE.TRX_LEVEL,'''') <> ''FAC'' 
            AND FEE.SOURCE_TABLE <> ''TBLU_TRANS_ASSET''              
        LEFT JOIN ' || ' IFRS_IMA_AMORT_CURR' || ' IMC 
            ON A.MASTERID = IMC.MASTERID              
        LEFT JOIN ' || ' IFRS_IMA_AMORT_PREV' || ' IMP 
            ON A.MASTERID = IMP.MASTERID              
        JOIN ' || ' IFRS_JOURNAL_PARAM' || ' B 
            ON B.JOURNALCODE IN (              
                ''ITRCG''              
                ,''ITRCG1''              
                ,''ITRCG2''              
            )              
            AND (              
                B.CCY = A.CCY              
                OR B.CCY = ''ALL''              
            )              
            AND B.FLAG_CF = A.FLAG_CF              
            AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME, IMP.GL_CONSTNAME, '''')              
            AND (              
                A.TRXCODE = B.TRX_CODE              
                OR B.TRX_CODE = ''ALL''              
            )              
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
            AND A.JOURNALCODE = ''DEFA0''              
            AND A.TRXCODE <> ''BENEFIT''              
            AND A.METHOD = ''EIR''  
            AND A.DATASOURCE IN (''LOAN_T24'',''LIMIT_T24'')   
            AND A.DOWNLOAD_DATE = ''' || CAST(V_MIGRATIONDATE AS VARCHAR(10)) || '''::DATE            
            AND A.SOURCEPROCESS NOT IN (''EIR_REV_SWITCH'',''EIR_SWITCH'') ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
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
            ,A.REVERSE              
            ,A.FLAG_CF              
            ,CASE               
                WHEN (              
                    A.REVERSE = ''N''              
                    AND A.FLAG_AL IN (''A'', ''O'')              
                )              
                OR (              
                    A.REVERSE = ''Y''              
                    AND A.FLAG_AL NOT IN (''A'', ''O'')         
                )              
                THEN CASE               
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    ELSE CASE               
                        WHEN B.DRCR = ''D''              
                        THEN ''C''              
                        ELSE ''D''              
                    END              
                END              
                ELSE CASE               
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''  
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    ELSE CASE               
                        WHEN B.DRCR = ''D''              
                        THEN ''C''              
                        ELSE ''D''              
                    END              
                END              
            END AS DRCR              
            ,B.GLNO              
            ,ABS(A.N_AMOUNT)              
            ,ABS(A.N_AMOUNT_IDR)              
            ,A.SOURCEPROCESS              
            ,A.ID             
            ,CURRENT_TIMESTAMP              
            ,''SP_JOURNAL_DATA2''              
            ,A.BRANCH              
            ,B.JOURNALCODE              
            ,B.JOURNAL_DESC              
            ,B.JOURNALCODE              
            ,B.COSTCENTER || ''-'' || COALESCE(IMC.APPLICATION_NO, IMP.APPLICATION_NO, '''')              
            ,B.GL_INTERNAL_CODE              
            ,A.METHOD       
            ,IMC.ACCOUNT_TYPE AS ACCOUNT_TYPE    
            ,IMC.CUSTOMER_TYPE AS CUSTOMER_TYPE          
        FROM ' || ' IFRS_ACCT_JOURNAL_INTM' || ' A  
        INNER JOIN ' || ' IFRS_ACCT_COST_FEE' || ' FEE 
            ON A.DOWNLOAD_DATE = FEE.DOWNLOAD_DATE 
            AND A.CF_ID = FEE.CF_ID 
            AND COALESCE(FEE.TRX_LEVEL,'''') <> ''FAC'' 
            AND FEE.SOURCE_TABLE <> ''TBLU_TRANS_ASSET''            
        LEFT JOIN ' || ' IFRS_IMA_AMORT_CURR' || ' IMC 
            ON A.MASTERID = IMC.MASTERID              
        LEFT JOIN ' || ' IFRS_IMA_AMORT_PREV' || ' IMP 
            ON A.MASTERID = IMP.MASTERID           
        JOIN ' || ' IFRS_JOURNAL_PARAM' || ' B 
            ON B.JOURNALCODE IN (              
                ''ITRCG''              
                ,''ITRCG1''              
                ,''ITRCG2''              
                ,''ITEMB''              
            )              
            AND (              
                B.CCY = A.CCY              
                OR B.CCY = ''ALL''              
            )              
            AND B.FLAG_CF = ''B''              
            AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME, IMP.GL_CONSTNAME, '''')              
            AND (              
                A.TRXCODE = B.TRX_CODE              
                OR B.TRX_CODE = ''ALL''              
            )        
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
            AND A.JOURNALCODE = ''DEFA0''              
            AND A.TRXCODE = ''BENEFIT''              
            AND A.METHOD = ''EIR''    
            AND A.DATASOURCE IN (''LOAN_T24'',''LIMIT_T24'')   
            AND A.DOWNLOAD_DATE = ''' || CAST(V_MIGRATIONDATE AS VARCHAR(10)) || '''::DATE              
            AND A.SOURCEPROCESS NOT IN (''EIR_REV_SWITCH'',''EIR_SWITCH'') ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
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
            ,A.REVERSE              
            ,A.FLAG_CF              
            ,CASE               
                WHEN ( 
                    A.REVERSE = ''N''              
                    AND A.FLAG_AL IN (''A'', ''O'')              
                )              
                OR (              
                    A.REVERSE = ''Y''              
                    AND A.FLAG_AL NOT IN (''A'', ''O'')            
                )              
                THEN CASE               
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (          
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                       )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    ELSE CASE               
                        WHEN B.DRCR = ''D''              
                        THEN ''C''              
                        ELSE ''D''              
                    END              
                END              
                ELSE CASE               
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''F''         
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    ELSE CASE               
                        WHEN B.DRCR = ''D''              
                        THEN ''C''              
                        ELSE ''D''              
                    END              
                END              
            END AS DRCR              
            ,B.GLNO              
            ,ABS(A.N_AMOUNT)              
            ,ABS(A.N_AMOUNT_IDR)              
            ,A.SOURCEPROCESS              
            ,A.ID              
            ,CURRENT_TIMESTAMP              
            ,''SP_JOURNAL_DATA2''              
            ,A.BRANCH              
            ,A.JOURNALCODE2              
            ,B.JOURNAL_DESC              
            ,B.JOURNALCODE              
            ,B.COSTCENTER || ''-'' || COALESCE(IMC.APPLICATION_NO, IMP.APPLICATION_NO, '''')              
            ,B.GL_INTERNAL_CODE              
            ,METHOD      
            ,IMC.ACCOUNT_TYPE AS ACCOUNT_TYPE    
            ,IMC.CUSTOMER_TYPE AS CUSTOMER_TYPE           
        FROM ' || ' IFRS_ACCT_JOURNAL_INTM' || ' A              
        LEFT JOIN ' || ' IFRS_IMA_AMORT_CURR' || ' IMC 
            ON A.MASTERID = IMC.MASTERID              
        LEFT JOIN ' || ' IFRS_IMA_AMORT_PREV' || ' IMP 
            ON A.MASTERID = IMP.MASTERID              
        JOIN ' || ' IFRS_JOURNAL_PARAM' || ' B 
            ON B.JOURNALCODE IN (              
                ''ITRCG''              
                ,''ITRCG1''              
                ,''ITRCG2''              
            )              
            AND (              
                B.CCY = A.CCY              
                OR B.CCY = ''ALL''              
            )              
            AND B.FLAG_CF = A.FLAG_CF              
            AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME, IMP.GL_CONSTNAME, '''')              
            AND (              
                A.TRXCODE = B.TRX_CODE              
                OR B.TRX_CODE = ''ALL''              
            )              
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
            AND A.JOURNALCODE = ''DEFA0''              
            AND A.TRXCODE <> ''BENEFIT''              
            AND A.METHOD = ''EIR''  
            AND (A.DATASOURCE NOT IN (''LOAN_T24'',''LIMIT_T24'') OR (A.DATASOURCE IN (''LOAN_T24'',''LIMIT_T24'') AND A.DOWNLOAD_DATE <> ''' || CAST(V_MIGRATIONDATE AS VARCHAR(10)) || '''::DATE ))           
            AND A.SOURCEPROCESS NOT IN (''EIR_REV_SWITCH'',''EIR_SWITCH'') ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
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
            ,A.REVERSE              
            ,A.FLAG_CF              
            ,CASE               
                WHEN (              
                    A.REVERSE = ''N''              
                    AND A.FLAG_AL IN (''A'', ''O'')              
                )              
                OR (              
                    A.REVERSE = ''Y''              
                    AND A.FLAG_AL NOT IN (''A'', ''O'')         
                )              
                THEN CASE               
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    ELSE CASE               
                        WHEN B.DRCR = ''D''              
                        THEN ''C''              
                        ELSE ''D''              
                    END              
                END              
                ELSE CASE               
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    ELSE CASE               
                        WHEN B.DRCR = ''D''              
                        THEN ''C''              
                        ELSE ''D''              
                    END              
                END              
            END AS DRCR              
            ,B.GLNO              
            ,ABS(A.N_AMOUNT)              
            ,ABS(A.N_AMOUNT_IDR)              
            ,A.SOURCEPROCESS              
            ,A.ID             
            ,CURRENT_TIMESTAMP              
            ,''SP_JOURNAL_DATA2''              
            ,A.BRANCH              
            ,B.JOURNALCODE              
            ,B.JOURNAL_DESC              
            ,B.JOURNALCODE              
            ,B.COSTCENTER || ''-'' || COALESCE(IMC.APPLICATION_NO, IMP.APPLICATION_NO, '''')              
            ,B.GL_INTERNAL_CODE              
            ,METHOD       
            ,IMC.ACCOUNT_TYPE AS ACCOUNT_TYPE    
            ,IMC.CUSTOMER_TYPE AS CUSTOMER_TYPE          
        FROM ' || ' IFRS_ACCT_JOURNAL_INTM' || ' A              
        LEFT JOIN ' || ' IFRS_IMA_AMORT_CURR' || ' IMC 
            ON A.MASTERID = IMC.MASTERID              
        LEFT JOIN ' || ' IFRS_IMA_AMORT_PREV' || ' IMP 
            ON A.MASTERID = IMP.MASTERID              
        JOIN ' || ' IFRS_JOURNAL_PARAM' || ' B 
            ON B.JOURNALCODE IN (              
                ''ITRCG''              
                ,''ITRCG1''              
                ,''ITRCG2''              
                ,''ITEMB''              
            )              
            AND (              
                B.CCY = A.CCY              
                OR B.CCY = ''ALL''              
            )              
            AND B.FLAG_CF = ''B''              
            AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME, IMP.GL_CONSTNAME, '''')      
            AND (              
                A.TRXCODE = B.TRX_CODE              
                OR B.TRX_CODE = ''ALL''              
            )        
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
            AND A.JOURNALCODE = ''DEFA0''              
            AND A.TRXCODE = ''BENEFIT''              
            AND A.METHOD = ''EIR''      
            AND (A.DATASOURCE NOT IN (''LOAN_T24'',''LIMIT_T24'') OR (A.DATASOURCE IN (''LOAN_T24'',''LIMIT_T24'') AND A.DOWNLOAD_DATE <> ''' || CAST(V_MIGRATIONDATE AS VARCHAR(10)) || '''::DATE ))          
            AND A.SOURCEPROCESS NOT IN (''EIR_REV_SWITCH'',''EIR_SWITCH'') ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
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
            ,BRANCH    ,JOURNALCODE2              
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
            ,A.REVERSE              
            ,A.FLAG_CF              
            ,CASE               
                WHEN (              
                    A.REVERSE = ''N''              
                    AND A.FLAG_AL IN (''A'', ''O'')              
                )              
                OR (              
                    A.REVERSE = ''Y''              
                    AND A.FLAG_AL NOT IN (''A'', ''O'')    
                )              
                THEN CASE               
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    ELSE CASE               
                        WHEN B.DRCR = ''D''              
                        THEN ''C''              
                        ELSE ''D''              
                    END              
                END              
                ELSE CASE               
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''C''             
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    ELSE CASE               
                        WHEN B.DRCR = ''D''              
                        THEN ''C''              
                        ELSE ''D''              
                    END              
                END              
            END AS DRCR              
            ,B.GLNO              
            ,ABS(A.N_AMOUNT)              
            ,ABS(A.N_AMOUNT_IDR)              
            ,A.SOURCEPROCESS              
            ,A.ID              
            ,CURRENT_TIMESTAMP              
            ,''SP_ACCT_JOURNAL_DATA2''              
            ,A.BRANCH              
            ,A.JOURNALCODE2              
            ,B.JOURNAL_DESC              
            ,B.JOURNALCODE              
            ,B.COSTCENTER || ''-'' || COALESCE(IMC.APPLICATION_NO, IMP.APPLICATION_NO, '''')              
            ,B.GL_INTERNAL_CODE              
            ,METHOD        
            ,IMC.ACCOUNT_TYPE AS ACCOUNT_TYPE    
            ,IMC.CUSTOMER_TYPE AS CUSTOMER_TYPE         
        FROM ' || ' IFRS_ACCT_JOURNAL_INTM' || ' A              
        LEFT JOIN ' || ' IFRS_IMA_AMORT_CURR' || ' IMC 
            ON A.MASTERID = IMC.MASTERID              
        LEFT JOIN ' || ' IFRS_IMA_AMORT_PREV' || ' IMP 
            ON A.MASTERID = IMP.MASTERID              
        JOIN ' || ' IFRS_JOURNAL_PARAM' || ' B 
            ON B.JOURNALCODE IN (              
                ''ACCRU''    
                ,''EMPBE''              
                ,''EMACR''              
            )              
            AND (              
                B.CCY = A.CCY              
                OR B.CCY = ''ALL''              
            )              
            AND B.FLAG_CF = A.FLAG_CF              
            AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME, IMP.GL_CONSTNAME, '''')              
            AND (              
                A.TRXCODE = B.TRX_CODE              
                OR B.TRX_CODE = ''ALL''              
            )        
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
            AND A.JOURNALCODE IN (              
                ''ACCRU''              
                ,''AMORT''              
            )              
            AND A.TRXCODE <> ''BENEFIT''              
            AND A.METHOD = ''EIR'' ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
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
            ,A.REVERSE              
            ,A.FLAG_CF              
            ,CASE               
                WHEN (              
                    A.REVERSE = ''N''              
                    AND A.FLAG_AL IN (''A'', ''O'')             
                )              
                OR (              
                    A.REVERSE = ''Y''              
                    AND A.FLAG_AL NOT IN (''A'', ''O'')          
                )              
                THEN CASE               
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    ELSE CASE               
                        WHEN B.DRCR = ''D''              
                        THEN ''C''              
                        ELSE ''D''              
                    END              
                END         
                ELSE CASE               
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (              
                            ''ACCRU''              
                            ,''AMORT''              
                        )              
                    THEN B.DRCR              
                    WHEN A.N_AMOUNT >= 0              
                        AND A.FLAG_CF = ''F''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR       
                    WHEN A.N_AMOUNT <= 0              
                        AND A.FLAG_CF = ''C''              
                        AND A.JOURNALCODE IN (''DEFA0'')              
                    THEN B.DRCR              
                    ELSE CASE               
                        WHEN B.DRCR = ''D''              
                        THEN ''C''              
                        ELSE ''D''              
                    END              
                END              
            END AS DRCR              
            ,B.GLNO              
            ,ABS(A.N_AMOUNT)              
            ,ABS(A.N_AMOUNT_IDR)              
            ,A.SOURCEPROCESS              
            ,A.ID              
            ,CURRENT_TIMESTAMP              
            ,''SP_ACCT_JOURNAL_DATA2''              
            ,A.BRANCH              
            ,B.JOURNALCODE              
            ,B.JOURNAL_DESC              
            ,B.JOURNALCODE              
            ,B.COSTCENTER || ''-'' || COALESCE(IMC.APPLICATION_NO, IMP.APPLICATION_NO, '''')              
            ,B.GL_INTERNAL_CODE              
            ,METHOD      
            ,IMC.ACCOUNT_TYPE AS ACCOUNT_TYPE    
            ,IMC.CUSTOMER_TYPE AS CUSTOMER_TYPE           
        FROM ' || ' IFRS_ACCT_JOURNAL_INTM' || ' A              
        LEFT JOIN ' || ' IFRS_IMA_AMORT_CURR' || ' IMC 
            ON A.MASTERID = IMC.MASTERID              
        LEFT JOIN ' || ' IFRS_IMA_AMORT_PREV' || ' IMP 
            ON A.MASTERID = IMP.MASTERID              
        JOIN ' || ' IFRS_JOURNAL_PARAM' || ' B 
            ON B.JOURNALCODE IN (              
                ''ACCRU''              
                ,''EMPBE''              
                ,''EMACR''              
                ,''EBCTE''              
            )              
            AND (              
                B.CCY = A.CCY              
                OR B.CCY = ''ALL''              
            )              
            AND B.FLAG_CF = ''B''             
            AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME, IMP.GL_CONSTNAME, '''')              
            AND (              
                A.TRXCODE = B.TRX_CODE              
                OR B.TRX_CODE = ''ALL''              
            )          
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
            AND A.JOURNALCODE IN (              
                ''ACCRU''              
                ,''AMORT''              
            )              
            AND A.TRXCODE = ''BENEFIT''              
            AND A.METHOD = ''EIR'' ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
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
            ,A.REVERSE              
            ,A.FLAG_CF              
            ,CASE               
                WHEN A.REVERSE = ''N''              
                    AND N_AMOUNT > 0              
                THEN B.DRCR              
                WHEN A.REVERSE = ''Y''              
                    AND N_AMOUNT <= 0              
                THEN B.DRCR              
                ELSE CASE               
                    WHEN B.DRCR = ''C''              
                    THEN ''D''              
                    ELSE ''C''              
                END              
            END AS DRCR              
            ,B.GLNO              
            ,ABS(A.N_AMOUNT) N_AMOUNT              
            ,ABS(A.N_AMOUNT_IDR) N_AMOUNT_IDR              
            ,A.SOURCEPROCESS              
            ,A.ID              
            ,CURRENT_TIMESTAMP AS CREATEDDATE              
            ,''SP_ACCT_JOURNAL_DATA2'' CREATEDBY              
            ,A.BRANCH              
            ,A.JOURNALCODE2              
            ,B.JOURNAL_DESC              
            ,B.JOURNALCODE AS NOREF              
            ,B.COSTCENTER || ''-'' || COALESCE(IMC.APPLICATION_NO, IMP.APPLICATION_NO, '''')              
            ,B.GL_INTERNAL_CODE              
            ,A.METHOD            
            ,IMC.ACCOUNT_TYPE AS ACCOUNT_TYPE    
            ,IMC.CUSTOMER_TYPE AS CUSTOMER_TYPE     
        FROM ' || ' IFRS_ACCT_JOURNAL_INTM' || ' A              
        LEFT JOIN ' || ' IFRS_IMA_AMORT_CURR' || ' IMC 
            ON A.MASTERID = IMC.MASTERID              
        LEFT JOIN ' || ' IFRS_IMA_AMORT_PREV' || ' IMP 
            ON A.MASTERID = IMP.MASTERID              
        JOIN ' || ' IFRS_JOURNAL_PARAM' || ' B 
            ON B.JOURNALCODE = ''ACRU4''              
            AND (              
                B.CCY = A.CCY              
                OR B.CCY = ''ALL''              
            )              
            AND B.FLAG_CF = A.FLAG_CF              
            AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME, IMP.GL_CONSTNAME, '''')              
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
            AND A.JOURNALCODE IN (              
                ''ACRU4''              
                ,''AMRT4''              
            )              
            AND A.TRXCODE <> ''BENEFIT''              
            AND A.METHOD = ''EIR'' ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
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
            ,A.REVERSE                
            ,A.FLAG_CF                
            ,B.DRCR           
            ,B.GLNO                
            ,ABS(A.N_AMOUNT)                
            ,ABS(A.N_AMOUNT_IDR)                
            ,A.SOURCEPROCESS                
            ,A.ID                
            ,CURRENT_TIMESTAMP                
            ,''SP_JOURNAL_DATA2''                
            ,A.BRANCH                
            ,B.JOURNALCODE                
            ,B.JOURNAL_DESC                
            ,B.JOURNALCODE                
            ,B.COSTCENTER || ''-'' || COALESCE(IMC.APPLICATION_NO, IMP.APPLICATION_NO, '''')                
            ,B.GL_INTERNAL_CODE                
            ,METHOD           
            ,IMC.ACCOUNT_TYPE AS ACCOUNT_TYPE    
            ,IMC.CUSTOMER_TYPE AS CUSTOMER_TYPE      
        FROM ' || ' IFRS_ACCT_JOURNAL_INTM' || ' A                
        LEFT JOIN ' || ' IFRS_IMA_AMORT_CURR' || ' IMC 
            ON A.MASTERID = IMC.MASTERID                
        LEFT JOIN ' || ' IFRS_IMA_AMORT_PREV' || ' IMP 
            ON A.MASTERID = IMP.MASTERID                
        JOIN ' || ' IFRS_JOURNAL_PARAM' || ' B 
            ON B.JOURNALCODE IN (''RCLV'')        
            AND (                
                B.CCY = A.CCY                
                OR B.CCY = ''ALL''                
            )                
            AND B.FLAG_CF = A.FLAG_CF                
            AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME, IMP.GL_CONSTNAME, '''')                
            AND (                
                A.TRXCODE = B.TRX_CODE                
                OR B.TRX_CODE = ''ALL''                
            )                 
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
            AND A.JOURNALCODE = ''DEFA0''                
            AND A.METHOD = ''EIR''         
            AND A.SOURCEPROCESS = ''EIR_REV_SWITCH'' ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;
    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
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
            ,A.REVERSE                
            ,A.FLAG_CF                
            ,B.DRCR         
            ,B.GLNO                
            ,ABS(A.N_AMOUNT)                
            ,ABS(A.N_AMOUNT_IDR)                
            ,A.SOURCEPROCESS                
            ,A.ID                
            ,CURRENT_TIMESTAMP                
            ,''SP_JOURNAL_DATA2''                
            ,A.BRANCH                
            ,B.JOURNALCODE                
            ,B.JOURNAL_DESC              
            ,B.JOURNALCODE                
            ,B.COSTCENTER || ''-'' || COALESCE(IMC.APPLICATION_NO, IMP.APPLICATION_NO, '''')                
            ,B.GL_INTERNAL_CODE                
            ,METHOD        
            ,IMC.ACCOUNT_TYPE AS ACCOUNT_TYPE    
            ,IMC.CUSTOMER_TYPE AS CUSTOMER_TYPE   
        FROM ' || ' IFRS_ACCT_JOURNAL_INTM' || ' A                
        LEFT JOIN ' || ' IFRS_IMA_AMORT_CURR' || ' IMC 
            ON A.MASTERID = IMC.MASTERID                
        LEFT JOIN ' || ' IFRS_IMA_AMORT_PREV' || ' IMP 
            ON A.MASTERID = IMP.MASTERID                
        JOIN ' || ' IFRS_JOURNAL_PARAM' || ' B 
            ON B.JOURNALCODE IN (''RCLS'')        
            AND (                
                B.CCY = A.CCY                
                OR B.CCY = ''ALL''                
            )                
            AND B.FLAG_CF = A.FLAG_CF                
            AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME, IMP.GL_CONSTNAME, '''')                
            AND (                
                A.TRXCODE = B.TRX_CODE                
                OR B.TRX_CODE = ''ALL''                
            )                 
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
            AND A.JOURNALCODE = ''DEFA0''                 
            AND A.METHOD = ''EIR''               
            AND A.SOURCEPROCESS = ''EIR_SWITCH'' ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    -- CALL SP_IFRS_ACCT_JRNL_DATA_SL();

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
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
            B.DOWNLOAD_DATE              
            ,A.TRX_FACILITY_NO              
            ,A.TRX_FACILITY_NO              
            ,NULL              
            ,A.TRX_FACILITY_NO              
            ,NULL              
            ,NULL              
            ,NULL              
            ,A.TRX_CODE              
            ,A.TRX_CCY              
            ,''PNL''              
            ,''ACT'' STATUS              
            ,''N'' REVERSE              
            ,D.VALUE1 FLAG_CF              
            ,LEFT(D.VALUE2, 1)              
            ,D.VALUE3 GLNO              
            ,A.REMAINING         
            ,A.REMAINING * RATE.RATE_AMOUNT              
            ,''CORP FACILITY EXP'' AS SOURCEPROCESS              
            ,NULL              
            ,CURRENT_TIMESTAMP              
            ,''SP_ACCT_JOURNAL_DATA''              
            ,E.BRANCH_CODE              
            ,''PNL'' JOURNALCODE2              
            ,D.DESCRIPTION              
            ,NULL              
            ,NULL              
            ,NULL GL_INTERNAL_CODE              
            ,NULL METHOD     
            ,MPB.ACCOUNT_TYPE  
            ,E.CUSTOMER_TYPE            
        FROM ' || ' IFRS_TRX_FACILITY' || ' A              
        LEFT JOIN ' || ' IFRS_MASTER_PARENT_LIMIT' || ' B 
            ON A.TRX_FACILITY_NO = B.LIMIT_PARENT_NO              
            AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE              
        LEFT JOIN ' || ' IFRS_TRANSACTION_PARAM' || ' C 
            ON A.TRX_CODE = C.TRX_CODE              
            AND (              
                A.TRX_CCY = C.CCY              
                OR C.CCY = ''ALL''              
            )              
        LEFT JOIN DBLINK(''ifrs9_stg'', ''SELECT * FROM TBL_MASTER_PRODUCT_BANKWIDE'') 
            AS MPB (
                PRODUCT_CODE VARCHAR(10)
                ,PRODUCT_DESC VARCHAR(70)
                ,ACCOUNT_TYPE VARCHAR(2)
                ,STATUS VARCHAR(15)
                ,SOURCE_DATA VARCHAR(10)
                ,LAST_UPDATE_DATE TIMESTAMP
            )
            ON C.PRD_CODE =  MPB.PRODUCT_CODE      
        LEFT JOIN TBLM_COMMONCODEDETAIL D 
            ON LEFT(C.IFRS_TXN_CLASS, 1) = D.VALUE1              
            AND D.COMMONCODE = ''B103''              
        LEFT JOIN ' || ' IFRS_MASTER_EXCHANGE_RATE' || ' RATE 
            ON A.TRX_CCY = RATE.CURRENCY              
            AND RATE.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
        LEFT JOIN ' || ' IFRS_IMA_LIMIT' || ' E 
            ON A.TRX_FACILITY_NO = E.MASTERID              
            AND E.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
        WHERE A.REMAINING > 0              
            AND (A.FACILITY_EXPIRED_DATE + 1)::DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
            AND A.STATUS = ''P''              
            AND A.REVID IS NULL              
            AND A.PKID NOT IN (              
                SELECT DISTINCT REVID              
                FROM ' || ' IFRS_TRX_FACILITY' || '              
                WHERE REVID IS NOT NULL              
            )              
            AND B.SME_FLAG = 0 ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' 
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
            B.DOWNLOAD_DATE              
            ,A.TRX_FACILITY_NO              
            ,A.TRX_FACILITY_NO              
            ,NULL              
            ,A.TRX_FACILITY_NO              
            ,''LIMIT''              
            ,NULL              
            ,NULL              
            ,A.TRX_CODE              
            ,A.TRX_CCY              
            ,''PNL''              
            ,''ACT'' STATUS              
            ,''N'' REVERSE              
            ,D.VALUE1 FLAG_CF          
            ,LEFT(D.VALUE2, 1)              
            ,D.VALUE3 GLNO              
            ,A.REMAINING              
            ,A.REMAINING * RATE.RATE_AMOUNT              
            ,''SME FACILITY EXP'' AS SOURCEPROCESS              
            ,NULL              
            ,CURRENT_TIMESTAMP              
            ,''SP_ACCT_JOURNAL_DATA''              
            ,E.BRANCH_CODE              
            ,''PNL'' JOURNALCODE2              
            ,D.DESCRIPTION              
            ,NULL              
            ,NULL              
            ,NULL GL_INTERNAL_CODE              
            ,NULL METHOD   
            ,MPB.ACCOUNT_TYPE  
            ,E.CUSTOMER_TYPE               
        FROM ' || ' IFRS_TRX_FACILITY' || ' A              
        LEFT JOIN ' || ' IFRS_MASTER_PARENT_LIMIT' || ' B 
            ON A.TRX_FACILITY_NO = B.LIMIT_PARENT_NO              
        AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
        LEFT JOIN ' || ' IFRS_TRANSACTION_PARAM' || ' C 
            ON A.TRX_CODE = C.TRX_CODE              
            AND (              
                A.TRX_CCY = C.CCY     
                OR C.CCY = ''ALL''              
            )        
        LEFT JOIN DBLINK(''ifrs9_stg'', ''SELECT * FROM TBL_MASTER_PRODUCT_BANKWIDE'') 
            AS MPB (
                PRODUCT_CODE VARCHAR(10)
                ,PRODUCT_DESC VARCHAR(70)
                ,ACCOUNT_TYPE VARCHAR(2)
                ,STATUS VARCHAR(15)
                ,SOURCE_DATA VARCHAR(10)
                ,LAST_UPDATE_DATE TIMESTAMP
            ) 
            ON C.PRD_CODE =  MPB.PRODUCT_CODE      
        LEFT JOIN TBLM_COMMONCODEDETAIL D 
            ON LEFT(C.IFRS_TXN_CLASS, 1) = D.VALUE1              
            AND D.COMMONCODE = ''B104''              
        LEFT JOIN ' || ' IFRS_MASTER_EXCHANGE_RATE' || ' RATE 
            ON A.TRX_CCY = RATE.CURRENCY              
            AND RATE.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
        LEFT JOIN ' || ' IFRS_IMA_LIMIT' || ' E 
            ON A.TRX_FACILITY_NO = E.MASTERID              
            AND E.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
        WHERE A.REMAINING > 0              
            AND (A.FACILITY_EXPIRED_DATE + 1)::DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
            AND A.STATUS = ''P''              
            AND A.REVID IS NULL              
            AND A.PKID NOT IN (              
                SELECT DISTINCT REVID              
                FROM ' || ' IFRS_TRX_FACILITY' || '              
                WHERE REVID IS NOT NULL              
            )              
            AND B.SME_FLAG = 1 ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT7 || ' A 
        SET REMAINING = 0              
        WHERE REMAINING > 0              
        AND FACILITY_EXPIRED_DATE < ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE               
        AND STATUS = ''P''              
        AND REVID IS NULL              
        AND PKID NOT IN (              
            SELECT DISTINCT REVID              
            FROM ' || ' IFRS_TRX_FACILITY' || '              
            WHERE REVID IS NOT NULL              
        ) ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT2 || ' A 
        SET NOREF = CASE               
            WHEN NOREF IN (              
                ''ITRCG''              
                ,''ITRCG_SL''              
                ,''ITRCG_NE''              
            )              
            THEN ''1''              
            WHEN NOREF IN (              
                ''ITRCG1''              
                ,''ITRCG_SL1''              
            )              
            THEN ''2''              
            WHEN NOREF IN (              
                ''ITRCG2''              
                ,''ITRCG_SL2''              
            )              
            THEN ''3''              
            WHEN NOREF IN (              
                ''EMPBE''              
                ,''EMPBE_SL''              
            )              
            THEN ''4''              
            WHEN NOREF IN (              
                ''EMACR''              
                ,''EMACR_SL''              
            )              
            THEN ''5''              
            WHEN NOREF = ''RLS''              
            THEN ''6''              
            ELSE ''9''              
        END || CASE               
            WHEN REVERSE = ''Y''              
            THEN ''1''              
            ELSE ''2''              
        END || CASE               
            WHEN DRCR = ''D''              
            THEN ''1''         
            ELSE ''2''              
        END              
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLENAME || ' A 
        SET FAIR_VALUE_AMOUNT = CASE 
            WHEN B.VALUE1 IS NOT NULL           
            THEN COALESCE(A.OUTSTANDING, 0) + COALESCE(A.OUTSTANDING_IDC, 0)           
            ELSE COALESCE(A.OUTSTANDING, 0) + COALESCE(A.OUTSTANDING_IDC, 0) + COALESCE(A.UNAMORT_FEE_AMT,0) + COALESCE(A.UNAMORT_COST_AMT, 0)                  
        END          
        FROM TBLM_COMMONCODEDETAIL B           
        WHERE A.DATA_SOURCE = B.VALUE1 AND A.PRODUCT_CODE = B.VALUE1           
        AND B.COMMONCODE = ''S1022'' -- PRODUCT OVERDRAFT, FAIRVALUE = OUTSTANDING           
        AND A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE                   
        AND NOT EXISTS (          
            SELECT 1           
            FROM TBLM_COMMONCODEDETAIL X           
            WHERE A.DATA_SOURCE = X.VALUE1 AND X.COMMONCODE = ''S1003'' 
            LIMIT 1
        ) ';
    EXECUTE (V_STR_QUERY);

    -- CALL SP_IFRS_ACCT_JRNL_DATA_FAC();

    IF V_CURRDATE = F_EOMONTH(V_CURRDATE, 0, 'M', 'NEXT') 
    THEN 
        -- CALL SP_IFRS_JOURNAL_GAIN_LOSS();
    END IF;

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_ACCT_JOURNAL_DATA', '');

    RAISE NOTICE 'SP_IFRS_ACCT_JOURNAL_DATA | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT2;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_ACCT_JOURNAL_DATA';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT2 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;