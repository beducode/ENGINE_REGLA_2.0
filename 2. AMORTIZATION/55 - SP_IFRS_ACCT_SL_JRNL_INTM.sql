---- DROP PROCEDURE SP_IFRS_ACCT_EIR_SWITCH;

CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_EIR_SWITCH(
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
    V_TMPTABLE1 VARCHAR(100);

    ---- VARIABLE PROCESS
    V_NUM INT;
    
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
        V_TABLEINSERT1 := 'IFRS_ACCT_JOURNAL_INTM_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_ACCT_COST_FEE_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_ACCT_SL_COST_FEE_ECF_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_ACCT_SL_COST_FEE_PREV_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_ACF_SL_MSTR_' || P_RUNID || '';
        V_TABLEINSERT6 := 'IFRS_MASTER_ACCOUNT_' || P_RUNID || '';
        V_TABLEINSERT7 := 'IFRS_ACCT_SL_ACF_' || P_RUNID || '';
        V_TABLEINSERT8 := 'IFRS_ACCT_SL_ACCRU_PREV_' || P_RUNID || '';
        V_TABLEINSERT9 := 'IFRS_ACCT_SWITCH_' || P_RUNID || '';
        V_TABLEINSERT10 := 'IFRS_ACCT_SL_STOP_REV_' || P_RUNID || '';
        V_TABLEINSERT11 := 'TMP_T5_' || P_RUNID || '';
        V_TABLEINSERT12 := 'TMP_T6_' || P_RUNID || '';
        V_TMPTABLE1 := 'TMP_LAST_EIR_CF_PREV_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_ACCT_JOURNAL_INTM';
        V_TABLEINSERT2 := 'IFRS_ACCT_COST_FEE';
        V_TABLEINSERT3 := 'IFRS_ACCT_SL_COST_FEE_ECF';
        V_TABLEINSERT4 := 'IFRS_ACCT_SL_COST_FEE_PREV';
        V_TABLEINSERT5 := 'IFRS_ACF_SL_MSTR';
        V_TABLEINSERT6 := 'IFRS_MASTER_ACCOUNT';
        V_TABLEINSERT7 := 'IFRS_ACCT_SL_ACF';
        V_TABLEINSERT8 := 'IFRS_ACCT_SL_ACCRU_PREV';
        V_TABLEINSERT9 := 'IFRS_ACCT_SWITCH';
        V_TABLEINSERT10 := 'IFRS_ACCT_SL_STOP_REV';
        V_TABLEINSERT11 := 'TMP_T5';
        V_TABLEINSERT12 := 'TMP_T6';
        V_TMPTABLE1 := 'TMP_LAST_EIR_CF_PREV';
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

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_ACCT_SL_JOURNAL_INTM', '');

    V_STR_QUERY := V_STR_QUERY || 'DELETE ' || IFRS_ACCT_JOURNAL_INTM || ' A 
       WHERE A.DOWNLOAD_DATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
       AND SOURCEPROCESS LIKE ''SL%''';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' (
        FACNO
		,CIFNO
		,DOWNLOAD_DATE
		,DATASOURCE
		,PRDCODE
		,TRXCODE
		,CCY
		,JOURNALCODE
		,STATUS
		,REVERSE
		,N_AMOUNT
		,CREATEDDATE
		,SOURCEPROCESS
		,ACCTNO
		,MASTERID
		,FLAG_CF
		,BRANCH
		,IS_PNL
		,PRDTYPE
		,JOURNALCODE2
		,CF_ID
        )
        SELECT FACNO
        ,CIFNO
        ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE
        ,DATASOURCE
        ,PRDCODE
        ,TRXCODE
        ,CCY
        ,''DEFA0''
        ,''ACT''
        ,''N''
        ,CASE
            WHEN FLAG_REVERSE = ''Y'' THEN -1 * N_AMOUNT
            ELSE N_AMOUNT
            END
        ,CREATEDDATE
        , ''SL PNL 1''
        ,ACCTNO
        ,MASTERID
        ,FLAG_CF
        ,BRCODE
        ,''Y'' IS_PNL
        ,PRDTYPE
        ,''ITRCG_SL''
        ,CF_ID
        FROM ' || IFRS_ACCT_COST_FEE || '
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND STATUS = ''PNL''
            AND METHOD = ''SL''';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (
            FACNO
            ,CIFNO
            ,DOWNLOAD_DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,STATUS
            ,REVERSE
            ,N_AMOUNT
            ,CREATEDDATE
            ,SOURCEPROCESS
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRANCH
            ,IS_PNL
            ,PRDTYPE
            ,JOURNALCODE2
            ,CF_ID 
        ) SELECT 
            FACNO
            ,CIFNO
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE
            ,DATASOURCE
            ,PRD_CODE
            ,TRX_CODE
            ,CCY
            ,''AMORT''
            ,''ACT''
            ,''N''
            ,- 1 * (
                CASE 
                    WHEN FLAG_REVERSE = ''Y''
                        THEN - 1 * AMOUNT
                    ELSE AMOUNT
                    END
                )
            ,CURRENT_TIMESTAMP
            ,''SL PNL 2''
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRCODE
            ,''Y'' IS_PNL
            ,PRD_TYPE
            ,''ACCRU_SL''
            ,CF_ID
        FROM ' || V_TABLEINSERT2 || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND STATUS = ''PNL''
            AND METHOD = ''SL''';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO
            ,CIFNO
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,STATUS
            ,REVERSE
            ,N_AMOUNT
            ,CREATEDDATE
            ,SOURCEPROCESS
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRANCH
            ,PRDTYPE
            ,JOURNALCODE2
            ,CF_ID
            ) 
        ) SELECT 
            FACNO
            ,CIFNO
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,''AMORT''
            ,''ACT''
            ,''N''
            ,- 1 * (
                CASE 
                    WHEN FLAG_REVERSE = ''Y''
                        THEN - 1 * AMOUNT
                    ELSE AMOUNT
                    END
                )
            ,CURRENT_TIMESTAMP
            ,''SL PNL 3''
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRCODE
            ,PRDTYPE
            ,''ACRRU_SL''
            ,CF_ID 
        FROM ' || V_TABLEINSERT4 || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND STATUS = ''PNL''';
    EXECUTE (V_STR_QUERY);

	-- PNL2 = AMORT OF UNAMORT BY PREVDATE
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO
            ,CIFNO
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,STATUS
            ,REVERSE
            ,N_AMOUNT
            ,CREATEDDATE
            ,SOURCEPROCESS
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRANCH
            ,PRDTYPE
            ,JOURNALCODE2
            ,CF_ID
            ) 
        ) SELECT 
            FACNO
            ,CIFNO
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,''AMORT''
            ,''ACT''
            ,''N''
            ,- 1 * (
                CASE 
                    WHEN FLAG_REVERSE = ''Y''
                        THEN - 1 * AMOUNT
                    ELSE AMOUNT
                    END
                )
            ,CURRENT_TIMESTAMP
            ,''SL PNL 3''
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRCODE
            ,PRDTYPE
            ,''ACRRU_SL''
            ,CF_ID 
        FROM ' || V_TABLEINSERT4 || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND STATUS = ''PNL2''';
    EXECUTE (V_STR_QUERY);

	--DEFA0 NORMAL AMORTIZED COST/FEE
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO
            ,CIFNO
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,STATUS
            ,REVERSE
            ,N_AMOUNT
            ,CREATEDDATE
            ,SOURCEPROCESS
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRANCH
            ,PRDTYPE
            ,JOURNALCODE2
            ,CF_ID
            ) 
        ) SELECT 
            FACNO
            ,CIFNO
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,''DEFA0''
            ,''ACT''
            ,''N''
            ,- 1 * (
                CASE 
                    WHEN FLAG_REVERSE = ''Y''
                        THEN - 1 * AMOUNT
                    ELSE AMOUNT
                    END
                )
            ,CURRENT_TIMESTAMP
            ,''SL ACT 1''
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRCODE
            ,PRDTYPE
            ,''ITRCG_SL''
            ,CF_ID 
        FROM ' || V_TABLEINSERT2 || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND STATUS = ''ACT''
            AND STATUS = ''SL''';
    EXECUTE (V_STR_QUERY);

	--REVERSE ACCRUAL
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO
            ,CIFNO
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,STATUS
            ,REVERSE
            ,N_AMOUNT
            ,CREATEDDATE
            ,SOURCEPROCESS
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRANCH
            ,PRDTYPE
            ,JOURNALCODE2
            ,CF_ID
            ) 
        ) SELECT 
            FACNO
            ,CIFNO
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,STATUS
            ,''Y''
            ,N_AMOUNT
            ,CURRENT_TIMESTAMP
            ,''SL REV ACCRU''
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRCODE
            ,PRDTYPE
            ,JOURNALCODE
            ,CF_ID 
        FROM ' || V_TABLEINSERT1 || '
        WHERE DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE
            AND STATUS = ''ACT''
            AND JOURNALCODE = ''ACCRU_SL''
            AND REVERSE = ''N''
            AND SUBSTRING(SOURCEPROCESS, 1, 2) = ''SL''';
    EXECUTE (V_STR_QUERY);

	--ACCRU FEE
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT11 || ' 
        (    
            FACNO
			,CIFNO
			,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE
			,DATASOURCE
			,PRDCODE
			,TRXCODE
			,CCY
			,N_AMOUNT
			,ACCTNO
			,MASTERID
			,BRCODE
			,PRDTYPE
			,CF_ID
            ) 
        ) SELECT 
            FACNO
            ,CIFNO
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS ECFDATE
            ,DATASOURCE
			,PRDCODE
			,TRXCODE
			,CCY
			,CASE 
				WHEN FLAG_REVERSE = ''Y''
					THEN - 1 * AMOUNT
				ELSE AMOUNT
				END AS N_AMOUNT
			,ACCTNO
			,MASTERID
			,BRCODE
			,PRDTYPE
			,CF_ID 
        FROM ' || V_TABLEINSERT3 || '
        WHERE FLAG_CS = ''F''';
    EXECUTE (V_STR_QUERY);

	--JOURNAL SL BARU
    IF SL_METHOD = ''NO_ECF''
    BEGIN
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT11 || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
            (    
                FACNO
                ,CIFNO
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE 
                ,DATASOURCE
                ,PRDCODE
                ,TRXCODE
                ,CCY
                ,N_AMOUNT
                ,ACCTNO
                ,MASTERID
                ,BRCODE
                ,PRDTYPE
                ,CF_ID 
            ) SELECT 
                FACNO
                ,CIFNO
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS EFFDATE 
                ,A.DATA_SOURCE
                ,PRD_CODE
                ,TRX_CODE
                ,CCY
                ,A.SL_AMORT_DAILY
                ,A.MASTERID
                ,A.MASTERID
                ,BRCODE
                ,B.PRODUCT_TYPE
                ,ID_SL
            FROM ' || V_TABLEINSERT5 || ' A
            JOIN ' || V_TABLEINSERT6 || ' B 
                ON A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
                AND A.EFFDATE = B.DOWNLOAD_DATE
            WHERE FLAG_CF = ''F''
                AND IFRS_STATUS = ''ACT''';
            EXECUTE (V_STR_QUERY);
    END IF;

	--JOURNAL SL BARU

    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT12 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT12 || '
        (    
            FACNO
            ,CIFNO
            ,DOWNLOAD_DATE
            ,DATASOURCE
            ,SUM_AMT
            ,ACCTNO
            ,MASTERID
            ,BRCODE
        ) SELECT 
            FACNO
            ,CIFNO
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE 
            ,DATASOURCE
    		,SUM(N_AMOUNT) AS SUM_AMT
            ,ACCTNO
            ,MASTERID
            ,BRCODE
        FROM ' || V_TABLEINSERT11 || ' D
        GROUP BY 
            FACNO
            ,CIFNO
            ,DOWNLOAD_DATE
            ,DATASOURCE
            ,ACCTNO
            ,MASTERID
            ,BRCODE';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO
            ,CIFNO
            ,DOWNLOAD_DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,STATUS
            ,REVERSE
            ,N_AMOUNT
            ,CREATEDDATE
            ,SOURCEPROCESS
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRANCH
            ,PRDTYPE
            ,JOURNALCODE2
            ,CF_ID 
        ) SELECT 
            A.FACNO
            ,A.CIFNO
            ,A.DOWNLOAD_DATE
            ,A.DATASOURCE
            ,B.PRDCODE
            ,B.TRXCODE
            ,B.CCY
            ,''ACCRU_SL''
            ,''ACT''
            ,''N''
            ,A.N_ACCRU_FEE * CAST(CAST(B.N_AMOUNT AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS DECIMAL(32, 20))
            ,CURRENT_TIMESTAMP
            ,''SL ACCRU FEE 1''
            ,A.ACCTNO
            ,A.MASTERID
            ,''F''
            ,B.BRCODE
            ,B.PRDTYPE
            ,''ACCRU_SL''
            ,B.CF_ID 
        FROM ' || V_TABLEINSERT7 || ' A 
        JOIN ' || V_TABLEINSERT11 || ' B
            ON B.MASTERID = A.PREV_MASTERID 
        JOIN ' || V_TABLEINSERT12 || ' C
            ON C.MASTERID = A.MASTERID
		    AND A.ECFDATE = C.DOWNLOAD_DATE
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND A.DO_AMORT = ''N''';
    EXECUTE (V_STR_QUERY);

	--AMORT FEE
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO
            ,CIFNO
            ,DOWNLOAD_DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,STATUS
            ,REVERSE
            ,N_AMOUNT
            ,CREATEDDATE
            ,SOURCEPROCESS
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRANCH
            ,PRDTYPE
            ,JOURNALCODE2
            ,CF_ID 
        ) SELECT 
            A.FACNO
            ,A.CIFNO
            ,A.DOWNLOAD_DATE
            ,A.DATASOURCE
            ,B.PRDCODE
            ,B.TRXCODE
            ,B.CCY
            ,''AMORT''
            ,''ACT''
            ,''N''
            ,A.N_ACCRU_FEE * CAST(CAST(B.N_AMOUNT AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS DECIMAL(32, 20))
            ,CURRENT_TIMESTAMP
            ,''SL AMORT FEE 1''
            ,A.ACCTNO
            ,A.MASTERID
            ,''F''
            ,B.BRCODE
            ,B.PRDTYPE
            ,''ACCRU_SL''
            ,B.CF_ID 
        FROM ' || V_TABLEINSERT7 || ' A 
        JOIN ' || V_TABLEINSERT11 || ' B
            ON B.DOWNLOAD_DATE = A.ECFDATE 
            AND B.MASTERID = A.MASTERID
        JOIN ' || V_TABLEINSERT12 || ' C
            ON C.MASTERID = A.MASTERID
		    AND A.ECFDATE = C.DOWNLOAD_DATE
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND A.DO_AMORT = ''Y''';
    EXECUTE (V_STR_QUERY);

	--JOURNAL SL BARU
    IF SL_METHOD = ''NO_ECF''
    BEGIN
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
            (    
                FACNO
                ,CIFNO
                ,DOWNLOAD_DATE
                ,DATASOURCE
                ,PRDCODE
                ,TRXCODE
                ,CCY
                ,JOURNALCODE
                ,STATUS
                ,REVERSE
                ,N_AMOUNT
                ,CREATEDDATE
                ,SOURCEPROCESS
                ,ACCTNO
                ,MASTERID
                ,FLAG_CF
                ,BRANCH
                ,PRDTYPE
                ,JOURNALCODE2
                ,CF_ID 
            ) SELECT 
                A.FACNO
                ,A.CIFNO
                ,A.EFFDATE
                ,A.DATA_SOURCE
                ,B.PRDCODE
                ,B.TRXCODE
                ,B.CCY
                ,''AMORT''
                ,''ACT''
                ,''N''
                ,A.SL_AMORT_DAILY
                ,CURRENT_TIMESTAMP
                ,''SL AMORT FEE 1''
                ,A.MASTERID
                ,A.MASTERID
                ,''F''
                ,B.BRCODE
                ,B.PRDTYPE
                ,''ACCRU_SL''
                ,B.CF_ID 
            FROM ' || V_TABLEINSERT5 || ' A 
            JOIN ' || V_TABLEINSERT11 || ' B
                ON B.DOWNLOAD_DATE = A.ECFDATE 
                AND B.MASTERID = A.MASTERID
            JOIN ' || V_TABLEINSERT12 || ' C
                ON C.MASTERID = A.MASTERID
                AND A.ECFDATE = C.DOWNLOAD_DATE
            WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
                AND A.IFRS_STATUS = ''ACT''';
        EXECUTE (V_STR_QUERY);
    END IF;

	--DEFA0 FEE STOP REV AT PMTDATE 20160619
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO
            ,CIFNO
            ,DOWNLOAD_DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,STATUS
            ,REVERSE
            ,N_AMOUNT
            ,CREATEDDATE
            ,SOURCEPROCESS
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRANCH
            ,PRDTYPE
            ,JOURNALCODE2
            ,CF_ID 
        ) SELECT 
            A.FACNO
            ,A.CIFNO
            ,A.DOWNLOAD_DATE
            ,A.DATASOURCE
            ,B.PRDCODE
            ,B.TRXCODE
            ,B.CCY
            ,''DEFA0''
            ,''ACT''
            ,''N''
            ,- 1 * A.N_ACCRU_FEE * CAST(CAST(B.N_AMOUNT AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS DECIMAL(32, 20))
            ,CURRENT_TIMESTAMP
            ,''SL DEFA0 FEE 1''
            ,A.ACCTNO
            ,A.MASTERID
            ,''F''
            ,B.BRCODE
            ,B.PRDTYPE
            ,''ITRCG_SL''
            ,B.CF_ID 
        FROM ' || V_TABLEINSERT7 || ' A 
        JOIN ' || V_TABLEINSERT11 || ' B
            ON B.DOWNLOAD_DATE = A.ECFDATE 
            AND B.MASTERID = A.MASTERID
        JOIN ' || V_TABLEINSERT12 || ' C
            ON C.MASTERID = A.MASTERID
		    AND A.ECFDATE = C.DOWNLOAD_DATE
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND A.DO_AMORT = ''Y''
            AND A.MASTERID IN (
                SELECT MASTERID 
                FROM ' || V_TABLEINSERT3 || '
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            )';
    EXECUTE (V_STR_QUERY);

	--ACCRU COST
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT11 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT11 || ' 
        (    
            FACNO
			,CIFNO
			,DOWNLOAD_DATE
			,DATASOURCE
			,PRDCODE
			,TRXCODE
			,CCY
			,N_AMOUNT
			,ACCTNO
			,MASTERID
			,BRCODE
			,PRDTYPE
			,CF_ID 
        ) SELECT 
            FACNO
			,CIFNO
			,ECFDATE
			,DATASOURCE
			,PRDCODE
			,TRXCODE
			,CCY
			,CASE 
				WHEN FLAG_REVERSE = ''Y''
					THEN - 1 * AMOUNT
				ELSE AMOUNT
				END AS N_AMOUNT
			,ACCTNO
			,MASTERID
			,BRCODE
			,PRDTYPE
			,CF_ID 
        FROM ' || V_TABLEINSERT3 || ' 
        WHERE FLAG_CS = ''F''';
    EXECUTE (V_STR_QUERY);

    --JOURNAL SL BARU
    IF SL_METHOD = ''NO_ECF''
    BEGIN
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT11 || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT11 || ' 
            (    
                FACNO
                ,CIFNO
                ,DOWNLOAD_DATE
                ,DATASOURCE
                ,PRDCODE
                ,TRXCODE
                ,CCY
                ,N_AMOUNT
                ,ACCTNO
                ,MASTERID
                ,BRCODE
                ,PRDTYPE
                ,CF_ID 
            ) SELECT 
                FACNO
                ,CIFNO
                ,EFFDATE
                ,A.DATA_SOURCE
                ,PRD_CODE
                ,TRX_CODE
                ,CCY
                ,SL_AMORT_DAILY
                ,A.MASTERID
                ,A.MASTERID
                ,BRCODE
                ,B.PRODUCT_TYPE
                ,ID_SL
            FROM ' || V_TABLEINSERT5 || ' A 
            JOIN ' || V_TABLEINSERT6 || ' B
                ON A.MASTERID = B.MASTERID
			    AND A.EFFDATE = B.DOWNLOAD_DATE
            WHERE FLAG_CF = ''F''';
        EXECUTE (V_STR_QUERY);
    END IF;

    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT12 || '';
    EXECUTE (V_STR_QUERY);
        
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT12 || ' 
        (    
            FACNO
            ,CIFNO
            ,DOWNLOAD_DATE
            ,DATASOURCE
            ,SUM_AMT
            ,ACCTNO
            ,MASTERID
            ,BRCODE
        ) SELECT 
            FACNO
            ,CIFNO
            ,DOWNLOAD_DATE
            ,DATASOURCE
            ,SUM(N_AMOUNT) AS SUM_AMT
            ,ACCTNO
            ,MASTERID
            ,BRCODE
        FROM ' || V_TABLEINSERT11 || ' D
        GROUP BY 
            FACNO
            ,CIFNO
            ,DOWNLOAD_DATE
            ,DATASOURCE
            ,ACCTNO
            ,MASTERID
            ,BRCODE';
    EXECUTE (V_STR_QUERY); 
        
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO
            ,CIFNO
            ,DOWNLOAD_DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,STATUS
            ,REVERSE
            ,N_AMOUNT
            ,CREATEDDATE
            ,SOURCEPROCESS
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRANCH
            ,PRDTYPE
            ,JOURNALCODE2
            ,CF_ID
        ) SELECT 
            A.FACNO
            ,A.CIFNO
            ,A.DOWNLOAD_DATE
            ,A.DATASOURCE
            ,B.PRDCODE
            ,B.TRXCODE
            ,B.CCY
            ,''ACCRU_SL''
            ,''ACT''
            ,''N''
            ,A.N_ACCRU_COST * CAST(CAST(B.N_AMOUNT AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS DECIMAL(32, 20))
            ,CURRENT_TIMESTAMP
            ,''SL ACCRU COST 1''
            ,A.ACCTNO
            ,A.MASTERID
            ,''C''
            ,B.BRCODE
            ,B.PRDTYPE
            ,''ACCRU_SL''
            ,B.CF_ID
        FROM ' || V_TABLEINSERT7 || ' A
        JOIN ' || V_TABLEINSERT11 || ' B
            ON B.DOWNLOAD_DATE = A.ECFDATE
            AND B.MASTERID = A.MASTERID
        JOIN ' || V_TABLEINSERT12 || ' C
            ON C.MASTERID = A.MASTERID
            AND A.ECFDATE = C.DOWNLOAD_DATE
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND A.DO_AMORT = ''N''';
    EXECUTE (V_STR_QUERY); 

	--AMORT COST
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO
            ,CIFNO
            ,DOWNLOAD_DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,STATUS
            ,REVERSE
            ,N_AMOUNT
            ,CREATEDDATE
            ,SOURCEPROCESS
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRANCH
            ,PRDTYPE
            ,JOURNALCODE2
            ,CF_ID
        ) SELECT 
            A.FACNO
            ,A.CIFNO
            ,A.DOWNLOAD_DATE
            ,A.DATASOURCE
            ,B.PRDCODE
            ,B.TRXCODE
            ,B.CCY
            ,''AMORT''
            ,''ACT''
            ,''N''
            ,A.N_ACCRU_COST * CAST(CAST(B.N_AMOUNT AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS DECIMAL(32, 20))
            ,CURRENT_TIMESTAMP
            ,''SL AMORT COST 1''
            ,A.ACCTNO
            ,A.MASTERID
            ,''C''
            ,B.BRCODE
            ,B.PRDTYPE
            ,''ACCRU_SL''
            ,B.CF_ID
        FROM ' || V_TABLEINSERT7 || ' A
        JOIN ' || V_TABLEINSERT11 || ' B
            ON B.DOWNLOAD_DATE = A.ECFDATE
            AND B.MASTERID = A.MASTERID
        JOIN ' || V_TABLEINSERT12 || ' C
            ON C.MASTERID = A.MASTERID
            AND A.ECFDATE = C.DOWNLOAD_DATE
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND A.DO_AMORT = ''Y''';
    EXECUTE (V_STR_QUERY);

	--STOP REV DEFA0 COST 20160619
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO
            ,CIFNO
            ,DOWNLOAD_DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,STATUS
            ,REVERSE
            ,N_AMOUNT
            ,CREATEDDATE
            ,SOURCEPROCESS
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRANCH
            ,PRDTYPE
            ,JOURNALCODE2
            ,CF_ID
        ) SELECT 
            A.FACNO
            ,A.CIFNO
            ,A.DOWNLOAD_DATE
            ,A.DATASOURCE
            ,B.PRDCODE
            ,B.TRXCODE
            ,B.CCY
            ,''DEFA0''
            ,''ACT''
            ,''N''
            ,- 1 * A.N_ACCRU_COST * CAST(CAST(B.N_AMOUNT AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS DECIMAL(32, 20))
            ,CURRENT_TIMESTAMP
            ,''SL AMORT COST 1''
            ,A.ACCTNO
            ,A.MASTERID
            ,''C''
            ,B.BRCODE
            ,B.PRDTYPE
            ,''ITRCG_SL''
            ,B.CF_ID
        FROM ' || V_TABLEINSERT7 || ' A
        JOIN ' || V_TABLEINSERT11 || ' B
            ON B.DOWNLOAD_DATE = A.ECFDATE
            AND B.MASTERID = A.MASTERID
        JOIN ' || V_TABLEINSERT12 || ' C
            ON C.MASTERID = A.MASTERID
            AND A.ECFDATE = C.DOWNLOAD_DATE
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND A.DO_AMORT = ''Y''
            -- STOP REV
            AND A.MASTERID IN (
                SELECT MASTERID 
                FROM ' || V_TABLEINSERT3 || '
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            )';
    EXECUTE (V_STR_QUERY); 

    -- 20160407 DANIEL S : SET BLK BEFORE ACCRU PREV CODE
	-- UPDATE STATUS ACCRU PREV FOR SL STOP REV
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT8 || '
        SET ' || V_TABLEINSERT8 || '.STATUS = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE + ''BLK''
        FROM ' || V_TABLEINSERT8 || ' A
        JOIN ' || V_TABLEINSERT10 || ' E
            ON E.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND E.MASTERID = A.MASTERID
        JOIN ' || V_TABLEINSERT8 || ' C
            ON C.MASTERID = A.MASTERID
            AND C.STATUS = ''ACT''
            AND C.DOWNLOAD_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE';
    EXECUTE (V_STR_QUERY);

	--SL ACCRU PREV
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO
            ,CIFNO
            ,DOWNLOAD_DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,STATUS
            ,REVERSE
            ,N_AMOUNT
            ,CREATEDDATE
            ,SOURCEPROCESS
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRANCH
            ,PRDTYPE
            ,JOURNALCODE2
            ,CF_ID
        ) SELECT 
            A.FACNO
            ,A.CIFNO
            ,A.DOWNLOAD_DATE
            ,A.DATASOURCE
            ,C.PRDCODE
            ,C.TRXCODE
            ,C.CCY
            ,''ACCRU_SL''
            ,''ACT''
            ,''N''
            ,CASE 
                WHEN C.FLAG_REVERSE = ''Y''
                    THEN - 1 * C.AMOUNT
                ELSE C.AMOUNT
                END
            ,CURRENT_TIMESTAMP
            ,''SL ACCRU PREV''
            ,A.ACCTNO
            ,A.MASTERID
            ,C.FLAG_CF
            ,A.BRANCH
            ,C.PRDTYPE
            ,''ACCRU_SL''
            ,C.CF_ID
        FROM ' || V_TABLEINSERT7 || ' A
        JOIN ' || V_TABLEINSERT8 || ' C
            ON C.MASTERID = A.MASTERID
            AND C.STATUS = ''ACT''
            AND C.DOWNLOAD_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND A.DO_AMORT = ''N''';
    EXECUTE (V_STR_QUERY); 

	--SL SWITCH AMORT OF ACCRU PREV
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO
            ,CIFNO
            ,DOWNLOAD_DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,STATUS
            ,REVERSE
            ,N_AMOUNT
            ,CREATEDDATE
            ,SOURCEPROCESS
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRANCH
            ,PRDTYPE
            ,JOURNALCODE2
            ,CF_ID
        ) SELECT 
            A.PREV_FACNO
            ,A.PREV_CIFNO
            ,A.DOWNLOAD_DATE
            ,A.PREV_DATASOURCE
            ,A.PREV_PRDCODE
            ,--20180808 USE PREV PRD CODE
            C.TRXCODE
            ,C.CCY
            ,''AMORT''
            ,''ACT''
            ,''N''
            ,CASE 
                WHEN C.FLAG_REVERSE = ''Y''
                    THEN - 1 * C.AMOUNT
                ELSE C.AMOUNT
                END
            ,CURRENT_TIMESTAMP
            ,''SL ACRU SW''
            ,A.PREV_ACCTNO
            ,A.PREV_MASTERID
            ,C.FLAG_CF
            ,A.PREV_BRCODE
            ,A.PREV_PRDTYPE
            ,--20180808 USE PREV PRD TYPE
            ''ACCRU_SL''
            ,C.CF_ID
        FROM ' || V_TABLEINSERT9 || ' A
        JOIN ' || V_TABLEINSERT8 || ' C
            ON C.MASTERID = A.PREV_MASTERID
            AND C.STATUS = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND C.DOWNLOAD_DATE <= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND A.PREV_SL_ECF = ''Y''
            ';
    EXECUTE (V_STR_QUERY); 

	-- REV = DEFA0 REV OF UNAMORT BY CURRDATE
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO
            ,CIFNO
            ,DOWNLOAD_DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,STATUS
            ,REVERSE
            ,N_AMOUNT
            ,CREATEDDATE
            ,SOURCEPROCESS
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRANCH
            ,PRDTYPE
            ,JOURNALCODE2
            ,CF_ID
        ) SELECT 
            FACNO
            ,CIFNO
            ,DOWNLOAD_DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,''DEFA0''
            ,''ACT''
            ,''Y''
            ,1 * (
                CASE 
                    WHEN FLAG_REVERSE = ''Y''
                        THEN - 1 * AMOUNT
                    ELSE AMOUNT
                    END
                )
            ,CURRENT_TIMESTAMP
            ,''SL_REV_SWITCH'' 
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRCODE
            ,PRDTYPE
            ,''ITRCG_SL''
            ,CF_ID
        FROM ' || V_TABLEINSERT4 || ' A
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND STATUS = ''REV'' AND CREATEDBY = ''SL_SWITCH''
            ';
    EXECUTE (V_STR_QUERY); 

	-- REV = DEFA0 REV OF UNAMORT BY CURRDATE
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO
            ,CIFNO
            ,DOWNLOAD_DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,STATUS
            ,REVERSE
            ,N_AMOUNT
            ,CREATEDDATE
            ,SOURCEPROCESS
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRANCH
            ,PRDTYPE
            ,JOURNALCODE2
            ,CF_ID
        ) SELECT 
            FACNO
            ,CIFNO
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,''DEFA0''
            ,''ACT''
            ,''Y''
            ,1 * (
                CASE 
                    WHEN FLAG_REVERSE = ''Y''
                        THEN - 1 * AMOUNT
                    ELSE AMOUNT
                    END
                )
            ,CURRENT_TIMESTAMP
            ,''SL_REV_SWITCH'' 
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRCODE
            ,PRDTYPE
            ,''ITRCG_SL''
            ,CF_ID
        FROM ' || V_TABLEINSERT4 || '
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE
            AND STATUS = ''REV2'' AND CREATEDBY = ''SL_SWITCH''
            ';
    EXECUTE (V_STR_QUERY); 

	-- DEFA0 FOR NEW ACCT OF SL SWITCH
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO
            ,CIFNO
            ,DOWNLOAD_DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,STATUS
            ,REVERSE
            ,N_AMOUNT
            ,CREATEDDATE
            ,SOURCEPROCESS
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRANCH
            ,PRDTYPE
            ,JOURNALCODE2
            ,CF_ID
        ) SELECT 
            FACNO
            ,CIFNO
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,''DEFA0''
            ,''ACT''
            ,''N''
            ,1 * (
                CASE 
                    WHEN FLAG_REVERSE = ''Y''
                        THEN - 1 * AMOUNT
                    ELSE AMOUNT
                    END
                )
            ,CURRENT_TIMESTAMP
            ,''SL_SWITCH'' 
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRCODE
            ,PRDTYPE
            ,''ITRCG_SL''
            ,CF_ID
        FROM ' || V_TABLEINSERT4 || '
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE
            AND STATUS = ''ACT'' AND SEQ = ''0''
            ';
    EXECUTE (V_STR_QUERY); 

	----JOURNAL SL SWITCH NO ECF
    IF SL_METHOD = 'NO_ECF'
    BEGIN

        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
            (    
                FACNO
                ,CIFNO
                ,DOWNLOAD_DATE
                ,DATASOURCE
                ,PRDCODE
                ,TRXCODE
                ,CCY
                ,JOURNALCODE
                ,STATUS
                ,REVERSE
                ,N_AMOUNT
                ,CREATEDDATE
                ,SOURCEPROCESS
                ,ACCTNO
                ,MASTERID
                ,FLAG_CF
                ,BRANCH
                ,PRDTYPE
                ,JOURNALCODE2
                ,CF_ID
            ) SELECT 
                FACNO
                ,CIFNO
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                ,A.DATA_SOURCE
                ,PRD_CODE
                ,TRX_CODE
                ,CCY
                ,''DEFA0''
                ,''ACT''
                ,''Y''
                ,UNAMORT_VALUE
                ,CURRENT_TIMESTAMP
                ,''SL_SWITCH''
                ,A.MASTERID
                ,A.MASTERID
                ,FLAG_CF
                ,BRCODE
                ,B.PRODUCT_TYPE
                ,''ITRCG_SL''
                ,ID_SL
            FROM ' || V_TABLEINSERT5 || ' A 
            JOIN ' || V_TABLEINSERT6 || ' B
                ON A.MASTERID = B.MASTERID
			    AND A.EFFDATE = B.DOWNLOAD_DATE
            WHERE EFFDATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE 
                AND A.IFRS_STATUS = ''ACT'' 
                AND A.MASTERID IN (
                    SELECT MASTERID 
                    FROM ' || V_TABLEINSERT5 || '
                    WHERE EFFDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                        AND IFRS_STATUS = ''SWC''
                )';
            WHERE FLAG_CF = ''F'';
        EXECUTE (V_STR_QUERY);
    

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
            (    
                FACNO
                ,CIFNO
                ,DOWNLOAD_DATE
                ,DATASOURCE
                ,PRDCODE
                ,TRXCODE
                ,CCY
                ,JOURNALCODE
                ,STATUS
                ,REVERSE
                ,N_AMOUNT
                ,CREATEDDATE
                ,SOURCEPROCESS
                ,ACCTNO
                ,MASTERID
                ,FLAG_CF
                ,BRANCH
                ,PRDTYPE
                ,JOURNALCODE2
                ,CF_ID
            ) SELECT 
                FACNO
                ,CIFNO
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                ,A.DATA_SOURCE
                ,PRD_CODE
                ,TRX_CODE
                ,CCY
                ,'DEFA0'
                ,'ACT'
                ,'N'
                ,UNAMORT_VALUE
                ,CURRENT_TIMESTAMP
                ,'SL_SWITCH'
                ,A.MASTERID
                ,A.MASTERID
                ,FLAG_CF
                ,B.BRANCH_CODE
                ,B.PRODUCT_TYPE
                ,'ITRCG_SL'
                ,ID_SL
            FROM ' || V_TABLEINSERT5 || '
            JOIN ' || V_TABLEINSERT6 || ' B
                ON A.MASTERID = B.MASTERID
            WHERE EFFDATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE 
                AND B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                AND A.IFRS_STATUS = ''ACT''
                AND A.MASTERID IN (
                    SELECT DISTINCT MASTERID 
                    FROM ' || V_TABLEINSERT5 || '
                    WHERE EFFDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                        AND IFRS_STATUS = ''SWC''
                )';
        EXECUTE (V_STR_QUERY);
    END IF; 

	-- 20160407 SL STOP REVERSE
	-- BEFORE SL ACF RUN
	-- REVERSE UNAMORTIZED AND AMORT ACCRU IF EXIST
	-- UNAMORTIZED MAY BE USED BY OTHER PROCESS
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' 
        (    
            FACNO
            ,CIFNO
            ,DOWNLOAD_DATE
            ,DATASOURCE
            ,PRDCODE
            ,TRXCODE
            ,CCY
            ,JOURNALCODE
            ,STATUS
            ,REVERSE
            ,N_AMOUNT
            ,CREATEDDATE
            ,SOURCEPROCESS
            ,ACCTNO
            ,MASTERID
            ,FLAG_CF
            ,BRANCH
            ,PRDTYPE
            ,JOURNALCODE2
            ,CF_ID
        ) SELECT 
            A.FACNO
            ,A.CIFNO
            ,''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE AS DOWNLOAD_DATE
            ,A.DATASOURCE
            ,A.PRDCODE
            ,A.TRXCODE
            ,A.CCY
            ,''DEFA0''
            ,''ACT''
            ,''Y''
            ,CASE 
                WHEN FLAG_REVERSE = ''Y''
                    THEN - 1 * AMOUNT
                ELSE AMOUNT
                END
            ,CURRENT_TIMESTAMP
            ,''SL STOP REV 1''
            ,A.ACCTNO
            ,A.MASTERID
            ,A.FLAG_CF
            ,A.BRCODE
            ,A.PRDTYPE
            ,''ITRCG_SL''
            ,A.CF_ID
        FROM ' || V_TABLEINSERT4 || ' A
	    JOIN VW_LAST_SL_CF_PREV_YEST|  C 
            ON C.MASTERID = A.MASTERID
            AND C.DOWNLOAD_DATE = A.DOWNLOAD_DATE
		    AND ISNULL(C.SEQ, '''') = ISNULL(A.SEQ, '''')
        JOIN ' || V_TABLEINSERT10 || ' b
            ON B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
            AND B.MASTERID = A.MASTERID
        WHERE A.DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE
            AND A.STATUS = ''ACT''
            )';
    EXECUTE (V_STR_QUERY); 

    -- 20160407 AMORT YESTERDAY ACCRU
	-- BLOCK ACCRU PREV GENERATION ON SL_ECF
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'PRINT ' || CAST(GETDATE() AS VARCHAR(50)) || ' START SP_FAC_JOURNAL_INTM SL STOP REV 19';

    IF PARAM_DISABLE_ACCRU_PREV = 0
    BEGIN 
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || '
            (
                FACNO
                ,CIFNO
                ,DOWNLOAD_DATE
                ,DATASOURCE
                ,PRDCODE
                ,TRXCODE
                ,CCY
                ,JOURNALCODE
                ,STATUS
                ,REVERSE
                ,N_AMOUNT
                ,CREATEDDATE
                ,SOURCEPROCESS
                ,ACCTNO
                ,MASTERID
                ,FLAG_CF
                ,BRANCH
                ,PRDTYPE
                ,JOURNALCODE2
                ,CF_ID
            ) SELECT 
                FACNO
                ,CIFNO
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                ,DATASOURCE
                ,PRDCODE
                ,TRXCODE
                ,CCY
                ,''AMORT''
                ,STATUS
                ,''N''
                ,N_AMOUNT
                ,CURRENT_TIMESTAMP
                ,''SL STOP REV 2''
                ,ACCTNO
                ,MASTERID
                ,FLAG_CF
                ,BRCODE
                ,PRDTYPE
                ,''ACCRU_SL''
                ,CF_ID
            FROM ' || V_TABLEINSERT1 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE
                AND STATUS = ''ACT''
                AND JOURNALCODE = ''ACCRU_SL''
                AND REVERSE = ''N''
                AND SUBSTRING(SOURCEPROCESS, 1, 2) = ''SL''
                AND MASTERID IN (
                    SELECT MASTERID 
                    FROM ' || V_TABLEINSERT10 || '
                    WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                )';
        EXECUTE (V_STR_QUERY);
    END IF
    ELSE
    BEGIN
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || '
            (
                FACNO
                ,CIFNO
                ,DOWNLOAD_DATE
                ,DATASOURCE
                ,PRDCODE
                ,TRXCODE
                ,CCY
                ,JOURNALCODE
                ,STATUS
                ,REVERSE
                ,N_AMOUNT
                ,CREATEDDATE
                ,SOURCEPROCESS
                ,ACCTNO
                ,MASTERID
                ,FLAG_CF
                ,BRANCH
                ,PRDTYPE
                ,JOURNALCODE2
                ,CF_ID
            ) SELECT 
                FACNO
                ,CIFNO
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                ,DATASOURCE
                ,PRDCODE
                ,TRXCODE
                ,CCY
                ,''DEFA0''
                ,STATUS
                ,''Y''
                ,-1 * N_AMOUNT
                ,CURRENT_TIMESTAMP
                ,''SL STOP REV 2''
                ,ACCTNO
                ,MASTERID
                ,FLAG_CF
                ,BRCODE
                ,PRDTYPE
                ,''ITRCG_SL''
                ,CF_ID
            FROM ' || V_TABLEINSERT1 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE
                AND STATUS = ''ACT''
                AND JOURNALCODE = ''ACCRU_SL''
                AND REVERSE = ''N''
                AND SUBSTRING(SOURCEPROCESS, 1, 2) = ''SL''
                AND MASTERID IN (
                    SELECT MASTERID 
                    FROM ' || V_TABLEINSERT10 || '
                    WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                )';
        EXECUTE (V_STR_QUERY);    
    END IF;
    
    
    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_ACCT_SL_JOURNAL_INTM', '');

    RAISE NOTICE 'SP_IFRS_ACCT_EIR_SWITCH | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT6;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_ACCT_SL_JOURNAL_INTM';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT6 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;