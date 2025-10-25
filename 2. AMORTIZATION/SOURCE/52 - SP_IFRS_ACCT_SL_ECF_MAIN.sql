CREATE OR REPLACE PROCEDURESP_IFRS_ACCT_SL_ECF_MAIN(IN P_RUNID CHARACTER VARYING DEFAULT 'S_00000_0000'::CHARACTER VARYING, IN P_DOWNLOAD_DATE DATE DEFAULT NULL::DATE, IN P_PRC CHARACTER VARYING DEFAULT 'S'::CHARACTER VARYING)
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

    ---- VARIABLE PROCESS
    V_ROUND INT;
    V_FUNCROUND INT;
    V_MIN_ID INT;
    V_MAX_ID INT;
    V_ID2 INT;
    V_X INT;
    V_X_INC INT;
    V_PARAM_DISABLE_ACCRU_PREV INT;
    V_PARAM_CALC_TO_LASTPAYMENT INT;
    V_EXISTS BOOLEAN;
    
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
        V_TABLEINSERT2 := 'IFRS_ACCT_EIR_STOP_REV_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_ACCT_JOURNAL_INTM_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_ACCT_SL_ACCRU_PREV_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_ACCT_SL_ACF_' || P_RUNID || '';
        V_TABLEINSERT6 := 'IFRS_ACCT_SL_CF_ECF_' || P_RUNID || '';
        V_TABLEINSERT7 := 'IFRS_ACCT_SL_COST_FEE_ECF_' || P_RUNID || '';
        V_TABLEINSERT8 := 'IFRS_ACCT_SL_COST_FEE_PREV_' || P_RUNID || '';
        V_TABLEINSERT9 := 'IFRS_ACCT_SL_ECF_' || P_RUNID || '';
        V_TABLEINSERT10 := 'IFRS_ACCT_SL_ECF_STOPPED_' || P_RUNID || '';
        V_TABLEINSERT11 := 'IFRS_ACCT_SL_ECF1_' || P_RUNID || '';
        V_TABLEINSERT12 := 'IFRS_ACCT_SL_ECF2_' || P_RUNID || '';
        V_TABLEINSERT13 := 'IFRS_ACCT_SL_STOP_REV_' || P_RUNID || '';
        V_TABLEINSERT14 := 'IFRS_ACCT_SWITCH_' || P_RUNID || '';
        V_TABLEINSERT15 := 'IFRS_IMA_AMORT_CURR_' || P_RUNID || '';
        V_TABLEINSERT16 := 'IFRS_PRODUCT_PARAM_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_ACCT_COST_FEE';
        V_TABLEINSERT2 := 'IFRS_ACCT_EIR_STOP_REV';
        V_TABLEINSERT3 := 'IFRS_ACCT_JOURNAL_INTM';
        V_TABLEINSERT4 := 'IFRS_ACCT_SL_ACCRU_PREV';
        V_TABLEINSERT5 := 'IFRS_ACCT_SL_ACF';
        V_TABLEINSERT6 := 'IFRS_ACCT_SL_CF_ECF';
        V_TABLEINSERT7 := 'IFRS_ACCT_SL_COST_FEE_ECF';
        V_TABLEINSERT8 := 'IFRS_ACCT_SL_COST_FEE_PREV';
        V_TABLEINSERT9 := 'IFRS_ACCT_SL_ECF';
        V_TABLEINSERT10 := 'IFRS_ACCT_SL_ECF_STOPPED';
        V_TABLEINSERT11 := 'IFRS_ACCT_SL_ECF1';
        V_TABLEINSERT12 := 'IFRS_ACCT_SL_ECF2';
        V_TABLEINSERT13 := 'IFRS_ACCT_SL_STOP_REV';
        V_TABLEINSERT14 := 'IFRS_ACCT_SWITCH';
        V_TABLEINSERT15 := 'IFRS_IMA_AMORT_CURR';
        V_TABLEINSERT16 := 'IFRS_PRODUCT_PARAM';
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
    V_PARAM_CALC_TO_LASTPAYMENT := 0;

    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- RECORD RUN_ID --------
    CALL SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, PG_BACKEND_PID(), CURRENT_DATE);
    -------- RECORD RUN_ID --------

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT6 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT6 || ' AS SELECT * FROM IFRS_ACCT_SL_CF_ECF WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT10 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT10 || ' AS SELECT * FROM IFRS_ACCT_SL_ECF_STOPPED WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT11 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT11 || ' AS SELECT * FROM IFRS_ACCT_SL_ECF1 WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT12 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT12 || ' AS SELECT * FROM IFRS_ACCT_SL_ECF2 WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_ACCT_SL_ECF_MAIN', '');

    --RESET ACCRU CAUSED BY NEW ECF    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT4 || ' 
        WHERE DOWNLOAD_DATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
        AND SRCPROCESS = ''ECF'' ';
    EXECUTE (V_STR_QUERY);

    --RESET DATA BEFORE PROCESSING    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET STATUS = ''ACT''    
        WHERE STATUS = ''PNL''    
        AND CREATEDBY = ''SLECF1''    
        AND DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT8 || ' A 
        SET STATUS = ''ACT''    
        WHERE STATUS = ''PNL''    
        AND CREATEDBY = ''SLECF2''    
        AND DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT8 || ' A 
        SET STATUS = ''ACT''    
        WHERE STATUS = ''PNL2''    
        AND CREATEDBY = ''SLECF2''    
        AND DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T1' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T1' || ' (MASTERID) 
        SELECT MASTERID    
        FROM ' || V_TABLEINSERT15 || ' 
        WHERE ECF_STATUS = ''Y'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT6 || '';
    EXECUTE (V_STR_QUERY);

    --20160721 GET DATA FROM EIR STOP REV    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || 'TMP_IFRS_ACCT_STOP_REV' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_IFRS_ACCT_STOP_REV' || ' AS 
        SELECT * 
        FROM (    
            SELECT MASTERID    
            FROM ' || V_TABLEINSERT2 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
                
            UNION    
                
            SELECT MASTERID    
            FROM ' || V_TABLEINSERT13 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
        ) A ';
    EXECUTE (V_STR_QUERY);
    
    --20180109 ACCT WITH REVERSAL TODAY    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || 'TMP_TODAYREV' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_TODAYREV' || ' AS 
        SELECT DISTINCT MASTERID    
        FROM ' || V_TABLEINSERT1 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
        AND FLAG_REVERSE = ''Y''    
        AND CF_ID_REV IS NOT NULL ';
    EXECUTE (V_STR_QUERY);

    -- TODAY NEW COST FEE    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT6 || ' 
        (    
            MASTERID    
            ,FEE_AMT    
            ,COST_AMT    
            ,FEE_AMT_ACRU    
            ,COST_AMT_ACRU    
        ) SELECT 
            A.MASTERID    
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
        FROM ' || 'TMP_T1' || ' A    
        LEFT JOIN ' || V_TABLEINSERT1 || ' C 
            ON C.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
            AND C.MASTERID = A.MASTERID    
            AND C.STATUS = ''ACT''    
            AND C.METHOD = ''SL''    
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
            GROUP BY A.MASTERID ';
    EXECUTE (V_STR_QUERY);

    -- SISA UNAMORT    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' A 
        SET 
            FEE_AMT = A.FEE_AMT + B.FEE_AMT    
            ,COST_AMT = A.COST_AMT + B.COST_AMT    
        FROM (    
            SELECT 
                B.MASTERID    
                ,SUM(COALESCE(CASE     
                    WHEN B.FLAG_CF = ''F''    
                    THEN CASE     
                        WHEN B.FLAG_REVERSE = ''Y''    
                        THEN - 1 * CASE     
                            WHEN CFREV.MASTERID IS NULL    
                            THEN B.AMOUNT    
                            ELSE B.AMOUNT_ORG    
                        END    
                        ELSE CASE     
                            WHEN CFREV.MASTERID IS NULL    
                            THEN B.AMOUNT    
                            ELSE B.AMOUNT_ORG    
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
                            ELSE B.AMOUNT_ORG    
                        END    
                        ELSE CASE     
                            WHEN CFREV.MASTERID IS NULL    
                            THEN B.AMOUNT    
                            ELSE B.AMOUNT_ORG    
                        END    
                    END    
                    ELSE 0    
                END, 0)) AS COST_AMT    
            FROM ' || V_TABLEINSERT8 || ' B    
            JOIN VW_LAST_SL_CF_PREV X 
                ON X.MASTERID = B.MASTERID    
                AND X.DOWNLOAD_DATE = B.DOWNLOAD_DATE    
                AND B.SEQ = X.SEQ    
            LEFT JOIN ' || 'TMP_IFRS_ACCT_STOP_REV' || ' REV 
                ON B.MASTERID = REV.MASTERID    
            --20180109 RESONA REQ    
            LEFT JOIN ' || 'TMP_TODAYREV' || ' CFREV 
                ON CFREV.MASTERID = B.MASTERID    
            WHERE B.DOWNLOAD_DATE IN (    
                ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
                ,''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE    
            )    
                AND B.STATUS = ''ACT''    
                AND REV.MASTERID IS NULL    
                --20180108 EXCLUDE CF REVERSAL AND ITS PAIR    
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
            GROUP BY B.MASTERID    
        ) B    
        WHERE B.MASTERID = A.MASTERID  ';
    EXECUTE (V_STR_QUERY);

    IF V_PARAM_DISABLE_ACCRU_PREV != 0 THEN 
        -- NO ACCRU IF TODAY IS DOING AMORT    
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T1' || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T1' || ' (MASTERID) 
            SELECT DISTINCT MASTERID    
            FROM ' || V_TABLEINSERT5 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
            AND DO_AMORT = ''Y'' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T3' || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T3' || ' (MASTERID) 
            SELECT MASTERID    
            FROM ' || V_TABLEINSERT6 || ' 
            WHERE MASTERID NOT IN (    
                SELECT MASTERID    
                FROM ' || 'TMP_T1' || ' 
            )    
            --20180109 RESONA RECALC FROM START DATE : EXCLUDE ACCT TODAY HAVE REVERSAL    
            AND MASTERID NOT IN (    
                SELECT MASTERID    
                FROM ' || 'TMP_TODAYREV' || ' 
            ) ';
        EXECUTE (V_STR_QUERY);

        -- GET LAST ACF WITH DO_AMORT=N    
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_P1' || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_P1' || ' (ID) 
            SELECT MAX(ID) AS ID    
            FROM ' || V_TABLEINSERT5 || ' 
            WHERE MASTERID IN (    
                SELECT MASTERID    
                FROM ' || 'TMP_T3' || ' 
            )    
            AND DO_AMORT = ''N''    
            AND DOWNLOAD_DATE < ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
            AND DOWNLOAD_DATE >= ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE    
            GROUP BY MASTERID ';
        EXECUTE (V_STR_QUERY);

        -- NO ACCRU IF TODAY IS DOING AMORT    
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' A 
            SET 
                FEE_AMT = FEE_AMT - B.N_ACCRU_FEE    
                ,COST_AMT = COST_AMT - B.N_ACCRU_COST    
            FROM (    
                SELECT *    
                FROM ' || V_TABLEINSERT5 || ' 
                WHERE ID IN (    
                    SELECT ID    
                    FROM ' || 'TMP_P1' || ' 
                )    
            ) B    
            WHERE (B.MASTERID = A.MASTERID)    
            --20160407 SL STOP REV    
            AND A.MASTERID NOT IN (    
                SELECT MASTERID    
                FROM ' || 'TMP_IFRS_ACCT_STOP_REV' || ' 
                )    
            ----ADD 20180924        
            AND A.MASTERID NOT IN (    
                SELECT DISTINCT MASTERID 
                FROM ' || V_TABLEINSERT14 || ' 
                WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
            ) ';
        EXECUTE (V_STR_QUERY);

        --20180108 FEE ADJ REV AMBIL DARI UNAMORT UNTUK PAIR DARI CF REV    
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' A 
            SET FEE_AMT = FEE_AMT + B.N_AMOUNT    
            FROM (    
                SELECT 
                    MASTERID
                    ,SUM(N_AMOUNT)  N_AMOUNT     
                FROM ' || V_TABLEINSERT3 || '
                WHERE CF_ID IN (    
                    SELECT CF_ID_REV    
                    FROM ' || V_TABLEINSERT1 || '
                    WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
                        AND FLAG_REVERSE = ''Y''    
                        AND CF_ID_REV IS NOT NULL    
                )    
                    AND DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE    
                    AND REVERSE = ''N''    
                    AND JOURNALCODE = ''ACCRU_SL''    
                    AND FLAG_CF = ''F''    
                GROUP BY MASTERID    
            ) B    
            WHERE (B.MASTERID = A.MASTERID)    
            --20160407 SL STOP REV    
            AND A.MASTERID NOT IN (    
                SELECT MASTERID    
                FROM ' || 'TMP_IFRS_ACCT_STOP_REV' || '
            )    
            --20180109 RESONA RECALC FROM START DATE : EXCLUDE ACCT TODAY HAVE REVERSAL    
            AND A.MASTERID NOT IN (    
                SELECT MASTERID    
                FROM ' || 'TMP_TODAYREV' || '
            ) ';
        EXECUTE (V_STR_QUERY);

        --20180108 COST ADJ REV AMBIL DARI UNAMORT UNTUK PAIR DARI CF REV    
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' A 
            SET COST_AMT = COST_AMT + B.N_AMOUNT    
            FROM (    
                SELECT 
                    
                    MASTERID
                    ,SUM(N_AMOUNT)  N_AMOUNT     
                FROM ' || V_TABLEINSERT3 || '
                WHERE CF_ID IN (    
                    SELECT CF_ID_REV    
                    FROM ' || V_TABLEINSERT1 || '
                    WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
                        AND FLAG_REVERSE = ''Y''    
                        AND CF_ID_REV IS NOT NULL    
                )    
                    AND DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE    
                    AND [REVERSE] = ''N''    
                    AND JOURNALCODE = ''ACCRU_SL''    
                    AND FLAG_CF = ''C''    
                GROUP BY MASTERID    
            ) B    
            WHERE (B.MASTERID = A.MASTERID)    
            --20160407 SL STOP REV    
            AND A.MASTERID NOT IN (    
                SELECT MASTERID    
                FROM ' || 'TMP_IFRS_ACCT_STOP_REV' || ' 
            )    
            --20180109 RESONA RECALC FROM START DATE : EXCLUDE ACCT TODAY HAVE REVERSAL    
            AND A.MASTERID NOT IN (    
                SELECT MASTERID    
                FROM ' || 'TMP_TODAYREV' || ' 
            ) ';
        EXECUTE (V_STR_QUERY);
    END IF;

    -- ACCRU    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' A 
        SET 
            FEE_AMT_ACRU = B.FEE_AMT    
            ,COST_AMT_ACRU = B.COST_AMT    
        FROM (    
            SELECT 
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
            FROM ' || V_TABLEINSERT4 || ' B    
            WHERE B.STATUS = ''ACT''    
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
            )    
            --20180109 RESONA RECALC FROM START DATE : EXCLUDE ACCT TODAY HAVE REVERSAL    
            AND B.MASTERID NOT IN (    
                SELECT MASTERID    
                FROM ' || 'TMP_TODAYREV' || ' 
            )    
            GROUP BY B.MASTERID    
        ) B    
        WHERE B.MASTERID = A.MASTERID ';
    EXECUTE (V_STR_QUERY);

    /* FD DELETE ROUNDING  27-06-2018*/    
    -- UPDATE TOTAL    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' A 
        SET 
            TOTAL_AMT = FEE_AMT + COST_AMT    
            ,TOTAL_AMT_ACRU = FEE_AMT + COST_AMT + FEE_AMT_ACRU + COST_AMT_ACRU ';
    EXECUTE (V_STR_QUERY);

    -- UPDATE DUE_DATE TO EXPECTED LIFE DATE IF AVAILABLE    20120717    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' A 
        SET DUE_DATE = CASE     
            WHEN COALESCE(P.EXPECTED_LIFE, 0) > 0    
            THEN CASE     
                WHEN (B.LOAN_START_DATE + (P.EXPECTED_LIFE * INTERVAL ''1 MONTH'')) < B.LOAN_DUE_DATE    
                THEN (B.LOAN_START_DATE + (P.EXPECTED_LIFE * INTERVAL ''1 MONTH''))    
                ELSE B.LOAN_DUE_DATE    
            END    
            ELSE B.LOAN_DUE_DATE    
        END    
        FROM ' || V_TABLEINSERT15 || ' B    
        LEFT JOIN ' || V_TABLEINSERT16 || ' P 
            ON P.PRD_CODE = B.PRODUCT_CODE    
            AND P.PRD_TYPE = B.PRODUCT_TYPE    
            AND P.DATA_SOURCE = B.DATA_SOURCE    
            AND P.CCY = B.CURRENCY    
        WHERE B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
        AND A.MASTERID = B.MASTERID ';
    EXECUTE (V_STR_QUERY);

    -- DO FULL AMORT IF SUM COST FEE ZERO AND DONT CREATE NEW ECF    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET 
            STATUS = ''PNL''    
            ,CREATEDBY = ''SLECF1''    
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
        AND MASTERID IN (    
            SELECT MASTERID    
            FROM ' || V_TABLEINSERT6 || ' 
            WHERE TOTAL_AMT = 0    
            OR TOTAL_AMT_ACRU = 0    
        )    
        AND STATUS = ''ACT''    
        --20180108 EXCLUDE CF REV AND ITS PAIR, WILL BE HANDLED BY ACF_ABN    
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

    -- IF LAST COST FEE PREV IS CURRDATE    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT8 || ' A 
        SET 
            STATUS = ''PNL''    
            ,CREATEDBY = ''SLECF2''    
        WHERE ID IN (    
            SELECT A.ID    
            FROM ' || V_TABLEINSERT8 || ' A    
            JOIN ' || 'VW_LAST_SL_CF_PREV' || ' B    
                ON A.MASTERID = B.MASTERID
                AND A.SEQ = B.SEQ
                AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
            WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
                AND A.MASTERID IN (    
                    SELECT MASTERID    
                    FROM ' || V_TABLEINSERT6 || ' 
                    WHERE TOTAL_AMT = 0    
                    OR TOTAL_AMT_ACRU = 0    
                )    
                AND A.STATUS = ''ACT''    
        )    
        --20180108 EXCLUDE CF REV AND ITS PAIR, WILL BE HANDLED BY SP ACF ABN    
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
        )    
        --20180109 RESONA RECALC FROM START DATE : EXCLUDE ACCT TODAY HAVE REVERSAL    
        AND MASTERID NOT IN (    
            SELECT MASTERID    
            FROM ' || 'TMP_TODAYREV' || ' 
        ) ';
    EXECUTE (V_STR_QUERY);

    -- IF LAST COST FEE PREV IS PREVDATE    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT8 || ' A 
        SET 
            STATUS = ''PNL2''    
            ,CREATEDBY = ''SLECF2''    
        WHERE ID IN (    
            SELECT A.ID    
            FROM ' || V_TABLEINSERT8 || ' A    
            JOIN ' || 'VW_LAST_SL_CF_PREV' || ' B    
                ON A.MASTERID = B.MASTERID
                AND A.SEQ = B.SEQ
                AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
            WHERE A.DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE    
                AND A.MASTERID IN (    
                    SELECT MASTERID    
                    FROM ' || V_TABLEINSERT6 || ' 
                    WHERE TOTAL_AMT = 0    
                    OR TOTAL_AMT_ACRU = 0    
                )    
                AND A.STATUS = ''ACT''    
                --20180426 EXCLUDE PREVDATE IF NOT FROM SP ACF ACCRU    
                AND CASE     
                    WHEN A.DOWNLOAD_DATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE    
                        AND A.SEQ <> ''2''    
                    THEN 0    
                    ELSE 1    
                END = 1    
        )    
        --20180108 EXCLUDE CF REV AND ITS PAIR, WILL BE HANDLED BY ACF_ABN    
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
        )    
        --20180109 RESONA RECALC FROM START DATE : EXCLUDE ACCT TODAY HAVE REVERSAL    
        AND MASTERID NOT IN (    
            SELECT MASTERID    
            FROM ' || 'TMP_TODAYREV' || ' 
        ) ';
    EXECUTE (V_STR_QUERY);

    IF V_PARAM_DISABLE_ACCRU_PREV != 0 THEN 
        -- INSERT ACCRU PREV ONLY FOR PNL ED    
        -- GET LAST ACF WITH DO_AMORT=N    
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_P1' || '';
        EXECUTE (V_STR_QUERY);
        
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_P1' || ' (ID) 
            SELECT MAX(ID) AS ID    
            FROM ' || V_TABLEINSERT5 || '
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
                FROM ' || V_TABLEINSERT6 || '
                WHERE TOTAL_AMT = 0    
                OR TOTAL_AMT_ACRU = 0    
            )    
            --20180109 RESONA RECALC FROM START DATE : EXCLUDE ACCT TODAY HAVE REVERSAL    
            AND MASTERID NOT IN (    
                SELECT MASTERID    
                FROM ' || 'TMP_TODAYREV' || ' 
            )    
            GROUP BY MASTERID ';
        EXECUTE (V_STR_QUERY);

        -- GET FEE SUMMARY    
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
                FROM ' || V_TABLEINSERT7 || ' A    
                WHERE A.MASTERID IN (    
                    SELECT MASTERID    
                    FROM ' || 'TMP_T3' || ' 
                )    
                AND A.STATUS = ''ACT''    
                AND A.FLAG_CF = ''F''    
                AND A.METHOD = ''SL''    
            ) A    
            GROUP BY 
                A.DOWNLOAD_DATE    
                ,A.MASTERID ';
        EXECUTE (V_STR_QUERY);

        -- GET COST SUMMARY    
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
                FROM ' || V_TABLEINSERT7 || ' A    
                WHERE A.MASTERID IN (    
                    SELECT MASTERID    
                    FROM ' || 'TMP_T3' || '    
                )    
                AND A.STATUS = ''ACT''    
                AND A.FLAG_CF = ''C''    
                AND A.METHOD = ''SL''    
            ) A    
            GROUP BY 
                A.DOWNLOAD_DATE    
                ,A.MASTERID ';
        EXECUTE (V_STR_QUERY);

        --INSERT FEE 1    
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || ' 
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
                ,CAST(CAST(CASE     
                    WHEN B.FLAG_REVERSE = ''Y''    
                    THEN - 1 * B.AMOUNT    
                    ELSE B.AMOUNT    
                END AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS DECIMAL(32, 20)) * A.N_ACCRU_FEE AS N_AMOUNT    
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
            FROM ' || V_TABLEINSERT5 || ' A    
            JOIN ' || V_TABLEINSERT7 || ' B 
                ON B.ECFDATE = A.ECFDATE    
                AND A.MASTERID = B.MASTERID    
                AND B.FLAG_CF = ''F''    
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

        --COST 1    
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || ' 
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
                ,CAST(CAST(CASE     
                    WHEN B.FLAG_REVERSE = ''Y''    
                    THEN - 1 * B.AMOUNT    
                    ELSE B.AMOUNT    
                END AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS DECIMAL(32, 20)) * A.N_ACCRU_COST AS N_AMOUNT    
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
            FROM ' || V_TABLEINSERT5 || ' A    
            JOIN ' || V_TABLEINSERT7 || ' B 
                ON B.ECFDATE = A.ECFDATE    
                AND A.MASTERID = B.MASTERID    
                AND B.FLAG_CF = ''C''    
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

    -- MARK FOR DO AMORT ACRU    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT4 || ' A 
        SET STATUS = TO_CHAR(''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE, ''YYYYMMDD'')    
        WHERE STATUS = ''ACT''    
        AND MASTERID IN (    
            SELECT MASTERID    
            FROM ' || V_TABLEINSERT6 || ' 
            WHERE TOTAL_AMT = 0    
            OR TOTAL_AMT_ACRU = 0    
        ) ';
    EXECUTE (V_STR_QUERY);
    
    -- STOP OLD ECF    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT9 || ' A 
        SET 
            AMORTSTOPDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
            ,AMORTSTOPREASON = ''SP_ACCT_SL_ECF''    
        WHERE MASTERID IN (    
            SELECT MASTERID    
            FROM ' || V_TABLEINSERT6 || ' 
        )    
        AND AMORTSTOPDATE IS NULL ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT10 || ' 
        (    
            DOWNLOAD_DATE    
            ,MASTERID    
            ,AMORTSTOPDATE    
            ,AMORTSTOPREASON    
        ) SELECT 
            DOWNLOAD_DATE    
            ,MASTERID    
            ,AMORTSTOPDATE    
            ,AMORTSTOPREASON    
        FROM ' || V_TABLEINSERT9 || ' 
        WHERE AMORTSTOPDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
            AND AMORTSTOPREASON = ''SP_ACCT_SL_ECF''    
            AND PMTDATE = PREVDATE ';
    EXECUTE (V_STR_QUERY);

    -- 20160412 UPDATE START_AMORT_DATE    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' A 
        SET START_AMORT_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    IF V_PARAM_CALC_TO_LASTPAYMENT = 1 THEN 
        -- 20160412 SET START AMORT DATE TO LAST PAYMENT DATE    
        -- BETTER GET IT FROM MASTER ACCOUNT IF AVAILABLE    
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' A 
            SET START_AMORT_DATE = DUE_DATE ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT * FROM ' || V_TABLEINSERT6 || ' 
            WHERE START_AMORT_DATE > ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
        EXECUTE (V_STR_QUERY) INTO V_EXISTS;

        WHILE V_EXISTS LOOP 
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' A 
                SET START_AMORT_DATE = (START_AMORT_DATE - INTERVAL ''1 MONTH'')    
                WHERE START_AMORT_DATE > ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
            EXECUTE (V_STR_QUERY);
        END LOOP;
    END IF;

    --NEW LOOP    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT11 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT11 || ' 
        (    
            DOWNLOAD_DATE    
            ,FACNO    
            ,CIFNO    
            ,DATASOURCE    
            ,PREVDATE    
            ,PMTDATE    
            ,I_DAYSCNT    
            ,N_UNAMORT_COST    
            ,N_UNAMORT_FEE    
            ,N_AMORT_COST    
            ,N_AMORT_FEE    
            ,CREATEDDATE    
            ,CREATEDBY    
            ,MASTERID    
            ,ACCTNO    
            ,N_DAILY_AMORT_COST    
            ,N_DAILY_AMORT_FEE    
            ,AMORTENDDATE    
            ,PERIOD    
            ,UNAMORT_COST_PREV    
            ,UNAMORT_FEE_PREV    
            ,START_AMORT_DATE    
        ) SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
            ,B.FACILITY_NUMBER    
            ,B.CUSTOMER_NUMBER    
            ,B.DATA_SOURCE    
            ,CASE     
                WHEN EXTRACT(DAY FROM ((A.DUE_DATE - INTERVAL ''1 MONTH'') - A.START_AMORT_DATE)) > 0    
                THEN (A.DUE_DATE - INTERVAL ''1 MONTH'')    
                ELSE A.START_AMORT_DATE    
            END -- AS PREVDATE    
            ,A.DUE_DATE -- AS PMTDATE    
            ,EXTRACT(DAY FROM (A.DUE_DATE - CASE     
                WHEN EXTRACT(DAY FROM ((A.DUE_DATE - INTERVAL ''1 MONTH'') - A.START_AMORT_DATE)) > 0    
                THEN (A.DUE_DATE - INTERVAL ''1 MONTH'')    
                ELSE A.START_AMORT_DATE    
            END)) --DAYSCOUNT    
            ,A.COST_AMT - (CAST(A.DUE_DATE - A.START_AMORT_DATE AS FLOAT) / CAST(A.DUE_DATE - A.START_AMORT_DATE AS FLOAT) * A.COST_AMT) --UNAMORT COST    
            ,A.FEE_AMT - (CAST(A.DUE_DATE - A.START_AMORT_DATE AS FLOAT) / CAST(A.DUE_DATE - A.START_AMORT_DATE AS FLOAT) * A.FEE_AMT) --UNAMORT FEE    
            ,(CAST(A.DUE_DATE - A.START_AMORT_DATE AS FLOAT) / CAST(A.DUE_DATE - A.START_AMORT_DATE AS FLOAT) * A.COST_AMT) * - 1 --AMORT COST    
            ,(CAST(A.DUE_DATE - A.START_AMORT_DATE AS FLOAT) / CAST(A.DUE_DATE - A.START_AMORT_DATE AS FLOAT) * A.FEE_AMT) * - 1 --AMORT FEE    
            ,CURRENT_TIMESTAMP    
            ,''SP_ACCT_SL_ECF''    
            ,B.MASTERID    
            ,B.ACCOUNT_NUMBER    
            ,(A.COST_AMT / (A.DUE_DATE - A.START_AMORT_DATE)) * - 1    
            ,(A.FEE_AMT / (A.DUE_DATE - A.START_AMORT_DATE)) * - 1    
            ,A.DUE_DATE AMORTENDATE    
            ,(A.DUE_DATE - A.START_AMORT_DATE) PERIOD    
            ,A.COST_AMT - (    
                CAST(EXTRACT(DAY FROM (CASE     
                    WHEN EXTRACT(DAY FROM ((A.DUE_DATE - INTERVAL ''1 MONTH'') - A.START_AMORT_DATE)) > 0    
                    THEN (A.DUE_DATE - INTERVAL ''1 MONTH'')    
                    ELSE A.START_AMORT_DATE    
                END - A.START_AMORT_DATE)) AS FLOAT) / CAST((A.DUE_DATE - A.START_AMORT_DATE) AS FLOAT) * CAST(A.COST_AMT AS FLOAT)    
            ) --UNAMORT COST    
            ,A.FEE_AMT - (    
                CAST(EXTRACT(DAY FROM (CASE     
                    WHEN EXTRACT(DAY FROM ((A.DUE_DATE - INTERVAL ''1 MONTH'') - A.START_AMORT_DATE)) > 0    
                    THEN (A.DUE_DATE - INTERVAL ''1 MONTH'')    
                    ELSE A.START_AMORT_DATE    
                END - A.START_AMORT_DATE)) AS FLOAT) / CAST((A.DUE_DATE - A.START_AMORT_DATE) AS FLOAT) * CAST(A.FEE_AMT AS FLOAT)    
            ) --UNAMORT FEE    
            ,A.START_AMORT_DATE    
        FROM ' || V_TABLEINSERT6 || ' A    
        JOIN ' || V_TABLEINSERT15 || ' B 
            ON B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
            AND A.MASTERID = B.MASTERID    
            AND B.LOAN_DUE_DATE > ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
        WHERE A.TOTAL_AMT <> 0    
            AND A.TOTAL_AMT_ACRU <> 0    
            AND A.DUE_DATE > ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'SELECT EXISTS(SELECT * FROM ' || V_TABLEINSERT11 || ' LIMIT 1)';
    EXECUTE (V_STR_QUERY) INTO V_EXISTS;

    WHILE V_EXISTS LOOP
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT9 || ' 
            (    
                DOWNLOAD_DATE    
                ,FACNO    
                ,CIFNO    
                ,DATASOURCE    
                ,PREVDATE    
                ,PMTDATE    
                ,I_DAYSCNT    
                ,N_UNAMORT_COST    
                ,N_UNAMORT_FEE    
                ,N_AMORT_COST    
                ,N_AMORT_FEE    
                ,CREATEDDATE    
                ,CREATEDBY    
                ,MASTERID    
                ,ACCTNO    
                ,N_DAILY_AMORT_COST    
                ,N_DAILY_AMORT_FEE    
                ,AMORTENDDATE    
                ,PERIOD    
                ,UNAMORT_COST_PREV    
                ,UNAMORT_FEE_PREV    
            ) SELECT 
                DOWNLOAD_DATE    
                ,FACNO    
                ,CIFNO    
                ,DATASOURCE    
                ,PREVDATE    
                ,PMTDATE    
                ,I_DAYSCNT    
                ,N_UNAMORT_COST    
                ,N_UNAMORT_FEE    
                ,N_AMORT_COST    
                ,N_AMORT_FEE    
                ,CREATEDDATE    
                ,CREATEDBY    
                ,MASTERID    
                ,ACCTNO    
                ,N_DAILY_AMORT_COST    
                ,N_DAILY_AMORT_FEE    
                ,AMORTENDDATE    
                ,PERIOD    
                ,UNAMORT_COST_PREV    
                ,UNAMORT_FEE_PREV    
            FROM ' || V_TABLEINSERT11 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT12 || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT12 || ' 
            (    
                DOWNLOAD_DATE    
                ,FACNO    
                ,CIFNO    
                ,DATASOURCE    
                ,PREVDATE    
                ,PMTDATE    
                ,I_DAYSCNT    
                ,N_UNAMORT_COST    
                ,N_UNAMORT_FEE    
                ,N_AMORT_COST    
                ,N_AMORT_FEE    
                ,CREATEDDATE    
                ,CREATEDBY    
                ,MASTERID    
                ,ACCTNO    
                ,N_DAILY_AMORT_COST    
                ,N_DAILY_AMORT_FEE    
                ,AMORTENDDATE    
                ,PERIOD    
                ,UNAMORT_COST_PREV    
                ,UNAMORT_FEE_PREV    
                ,START_AMORT_DATE    
            ) SELECT 
                DOWNLOAD_DATE    
                ,FACNO    
                ,CIFNO    
                ,DATASOURCE    
                ,PREVDATE    
                ,PMTDATE    
                ,I_DAYSCNT    
                ,N_UNAMORT_COST    
                ,N_UNAMORT_FEE    
                ,N_AMORT_COST    
                ,N_AMORT_FEE    
                ,CREATEDDATE    
                ,CREATEDBY    
                ,MASTERID    
                ,ACCTNO    
                ,N_DAILY_AMORT_COST    
                ,N_DAILY_AMORT_FEE    
                ,AMORTENDDATE    
                ,PERIOD    
                ,UNAMORT_COST_PREV    
                ,UNAMORT_FEE_PREV    
                ,START_AMORT_DATE    
            FROM ' || V_TABLEINSERT11 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT11 || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT11 || ' 
            (    
                DOWNLOAD_DATE    
                ,FACNO    
                ,CIFNO    
                ,DATASOURCE    
                ,PREVDATE    
                ,PMTDATE    
                ,I_DAYSCNT    
                ,N_UNAMORT_COST    
                ,N_UNAMORT_FEE    
                ,N_AMORT_COST    
                ,N_AMORT_FEE    
                ,CREATEDDATE    
                ,CREATEDBY    
                ,MASTERID    
                ,ACCTNO    
                ,N_DAILY_AMORT_COST    
                ,N_DAILY_AMORT_FEE    
                ,AMORTENDDATE    
                ,PERIOD    
                ,UNAMORT_COST_PREV    
                ,UNAMORT_FEE_PREV    
                ,START_AMORT_DATE    
            ) SELECT 
                ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
                ,A.FACNO    
                ,A.CIFNO    
                ,A.DATASOURCE    
                ,CASE     
                    WHEN EXTRACT(DAY FROM (((A.PMTDATE - INTERVAL ''1 MONTH'') - INTERVAL ''1 MONTH'') - A.START_AMORT_DATE)) > 0    
                    THEN ((A.PMTDATE - INTERVAL ''1 MONTH'') - INTERVAL ''1 MONTH'')    
                    ELSE A.START_AMORT_DATE    
                END --PREVDATE    
                ,(A.PMTDATE - INTERVAL ''1 MONTH'') --PMTDATE    
                ,EXTRACT(DAY FROM ((A.PMTDATE - INTERVAL ''1 MONTH'') - CASE     
                    WHEN EXTRACT(DAY FROM (((A.PMTDATE - INTERVAL ''1 MONTH'') - INTERVAL ''1 MONTH'') - A.START_AMORT_DATE)) > 0    
                    THEN ((A.PMTDATE - INTERVAL ''1 MONTH'') - INTERVAL ''1 MONTH'')    
                    ELSE A.START_AMORT_DATE    
                END)) --DAYSCOUNT    
                ,B.COST_AMT - (EXTRACT(DAY FROM ((A.PMTDATE - INTERVAL ''1 MONTH'') - A.START_AMORT_DATE)) / CAST(A.PERIOD AS FLOAT) * CAST(B.COST_AMT AS FLOAT)) --UNAMORT COST    
                ,B.FEE_AMT - (EXTRACT(DAY FROM ((A.PMTDATE - INTERVAL ''1 MONTH'') - A.START_AMORT_DATE)) / CAST(A.PERIOD AS FLOAT) * CAST(B.FEE_AMT AS FLOAT)) --UNAMORT FEE    
                ,((EXTRACT(DAY FROM ((A.PMTDATE - INTERVAL ''1 MONTH'') - A.START_AMORT_DATE)) / CAST(A.PERIOD AS FLOAT) * CAST(B.COST_AMT AS FLOAT))) * - 1 --AMORT COST    
                ,((EXTRACT(DAY FROM ((A.PMTDATE - INTERVAL ''1 MONTH'') - A.START_AMORT_DATE)) / CAST(A.PERIOD AS FLOAT) * CAST(B.FEE_AMT AS FLOAT))) * - 1 --AMORT FEE    
                ,CURRENT_TIMESTAMP    
                ,''SP_ACCT_SL_ECF''    
                ,A.MASTERID    
                ,A.ACCTNO    
                ,A.N_DAILY_AMORT_COST    
                ,A.N_DAILY_AMORT_FEE    
                ,A.AMORTENDDATE    
                ,A.PERIOD    
                ,B.COST_AMT - (    
                    EXTRACT(DAY FROM (CASE     
                        WHEN EXTRACT(DAY FROM (((A.PMTDATE - INTERVAL ''1 MONTH'') - INTERVAL ''1 MONTH'') - A.START_AMORT_DATE)) > 0    
                        THEN ((A.PMTDATE - INTERVAL ''1 MONTH'') - INTERVAL ''1 MONTH'')    
                        ELSE A.START_AMORT_DATE    
                    END - A.START_AMORT_DATE)) / CAST(A.PERIOD AS FLOAT) * CAST(B.COST_AMT AS FLOAT)    
                ) --UNAMORT COST    
                ,B.FEE_AMT - (    
                    EXTRACT(DAY FROM (CASE     
                        WHEN EXTRACT(DAY FROM (((A.PMTDATE - INTERVAL ''1 MONTH'') - INTERVAL ''1 MONTH'') - A.START_AMORT_DATE)) > 0    
                        THEN ((A.PMTDATE - INTERVAL ''1 MONTH'') - INTERVAL ''1 MONTH'')    
                        ELSE A.START_AMORT_DATE    
                    END - A.START_AMORT_DATE)) / CAST(A.PERIOD AS FLOAT) * CAST(B.FEE_AMT AS FLOAT)    
                ) --UNAMORT FEE    
                ,A.START_AMORT_DATE    
            FROM ' || V_TABLEINSERT12 || ' A    
            JOIN ' || V_TABLEINSERT6 || ' B 
            ON B.MASTERID = A.MASTERID ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT11 || ' WHERE PMTDATE <= START_AMORT_DATE ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT EXISTS(SELECT * FROM ' || V_TABLEINSERT11 || ' LIMIT 1)';
        EXECUTE (V_STR_QUERY) INTO V_EXISTS;
    END LOOP;

    -- CURRDATE ROW    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT9 || ' 
        (    
            DOWNLOAD_DATE    
            ,FACNO    
            ,CIFNO    
            ,DATASOURCE    
            ,PREVDATE    
            ,PMTDATE    
            ,I_DAYSCNT    
            ,N_UNAMORT_COST    
            ,N_UNAMORT_FEE    
            ,N_AMORT_COST    
            ,N_AMORT_FEE    
            ,CREATEDDATE    
            ,CREATEDBY    
            ,MASTERID    
            ,ACCTNO    
            ,N_DAILY_AMORT_COST    
            ,N_DAILY_AMORT_FEE    
            ,AMORTENDDATE    
            ,PERIOD    
            ,UNAMORT_COST_PREV    
            ,UNAMORT_FEE_PREV    
        ) SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
            ,B.FACILITY_NUMBER    
            ,B.CUSTOMER_NUMBER    
            ,B.DATA_SOURCE    
            ,A.START_AMORT_DATE    
            ,A.START_AMORT_DATE    
            ,0    
            ,A.COST_AMT    
            ,A.FEE_AMT    
            ,0    
            ,0    
            ,CURRENT_TIMESTAMP    
            ,NULL    
            ,B.MASTERID    
            ,B.ACCOUNT_NUMBER    
            ,(A.COST_AMT / (A.DUE_DATE - A.START_AMORT_DATE)) * - 1    
            ,(A.FEE_AMT / (A.DUE_DATE - A.START_AMORT_DATE)) * - 1    
            ,A.DUE_DATE AMORTENDATE    
            ,(A.DUE_DATE - A.START_AMORT_DATE) PERIOD    
            ,A.COST_AMT    
            ,A.FEE_AMT    
        FROM ' || V_TABLEINSERT6 || ' A    
        JOIN ' || V_TABLEINSERT15 || ' B 
            ON B.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
            AND A.MASTERID = B.MASTERID    
            AND B.LOAN_DUE_DATE > ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
        WHERE A.TOTAL_AMT <> 0    
            AND A.TOTAL_AMT_ACRU <> 0    
            AND A.DUE_DATE > ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    -- INSERT IFRS_ACCT_SL_COST_FEE_ECF    
    -- DELETE FROM IFRS_ACCT_SL_COST_FEE_ECF WHERE ECFDATE=''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT7 || ' 
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
        ) 
        -- INSERT CURRDATE COST/FEE    
        SELECT 
            C.DOWNLOAD_DATE    
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ECFDATE    
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
            ,''SL_ECF_MAIN'' CREATEDBY    
            ,'''' SEQ    
            ,C.AMOUNT    
            ,C.ORG_CCY    
            ,C.ORG_CCY_EXRATE    
            ,C.PRD_TYPE    
            ,C.CF_ID    
        FROM ' || V_TABLEINSERT1 || ' C    
        JOIN ' || V_TABLEINSERT6 || ' B 
            ON B.MASTERID = C.MASTERID    
            AND B.TOTAL_AMT <> 0    
            AND B.TOTAL_AMT_ACRU <> 0    
        WHERE C.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
            AND C.MASTERID = B.MASTERID    
            AND C.STATUS = ''ACT''    
            AND C.METHOD = ''SL''    
        --20180108 EXCLUDE CF REV AND ITS PAIR    
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
            
        UNION ALL       

        -- INSERT UNAMORT    
        SELECT 
            C.DOWNLOAD_DATE    
            ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ECFDATE    
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
            ,CASE     
                WHEN CFREV.MASTERID IS NULL    
                THEN C.AMOUNT    
                ELSE C.AMOUNT_ORG    
            END    
            ,CURRENT_TIMESTAMP CREATEDDATE    
            ,''SL_ECF_MAIN'' CREATEDBY    
            ,'''' SEQ    
            ,C.AMOUNT_ORG    
            ,C.ORG_CCY    
            ,C.ORG_CCY_EXRATE    
            ,C.PRDTYPE    
            ,C.CF_ID    
        FROM ' || V_TABLEINSERT8 || ' C    
        JOIN ' || 'VW_LAST_SL_CF_PREV' || ' X 
            ON X.MASTERID = C.MASTERID    
            AND X.DOWNLOAD_DATE = C.DOWNLOAD_DATE    
            AND C.SEQ = X.SEQ    
        JOIN ' || V_TABLEINSERT6 || ' B 
            ON B.MASTERID = C.MASTERID    
            AND B.TOTAL_AMT <> 0    
            AND B.TOTAL_AMT_ACRU <> 0    
        --20160721    
        LEFT JOIN ' || 'TMP_IFRS_ACCT_STOP_REV' || ' REV 
            ON C.MASTERID = REV.MASTERID    
        --20180109 RESONA RECALC FROM LOAN START DATE    
        LEFT JOIN ' || 'TMP_TODAYREV' || ' CFREV 
            ON CFREV.MASTERID = C.MASTERID    
        WHERE C.DOWNLOAD_DATE IN (    
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
            ,''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE    
        )    
        AND C.STATUS = ''ACT''    
        -- 20160407 SL STOP REV EXCLUDED    
        AND REV.MASTERID IS NULL    
        --20180108 EXCLUDE CF REV AND ITS PAIR    
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
    
    IF V_PARAM_DISABLE_ACCRU_PREV != 0 THEN 
        -- PURPOSE : TO INSERT YESTERDAY ACCRU TO SL_COST_FEE ECF    
        -- NO ACCRU IF TODAY IS DOING AMORT    
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T1' || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T1' || ' (MASTERID)
            SELECT DISTINCT MASTERID    
            FROM ' || V_TABLEINSERT5 || ' 
            WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
            AND DO_AMORT = ''Y'' ';
        EXECUTE (V_STR_QUERY);
            
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T3' || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T3' || ' (MASTERID) 
            SELECT MASTERID    
            FROM ' || V_TABLEINSERT6 || ' 
            WHERE MASTERID NOT IN (    
                SELECT MASTERID    
                FROM ' || 'TMP_T1' || ' 
            ) ';
        EXECUTE (V_STR_QUERY);
            
            
        -- GET LAST ACF WITH DO_AMORT=N    
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_P1' || '';
        EXECUTE (V_STR_QUERY);
            
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_P1' || ' (ID) 
            SELECT MAX(ID) AS ID    
            FROM ' || V_TABLEINSERT5 || ' 
            WHERE MASTERID IN (    
                SELECT MASTERID    
                FROM ' || 'TMP_T3' || ' 
            )    
            AND DO_AMORT = ''N''    
            AND DOWNLOAD_DATE < ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
            AND DOWNLOAD_DATE >= ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE    
            --20180109 RESONA RECALC FROM START DATE    
            AND MASTERID NOT IN (    
                SELECT MASTERID    
                FROM ' || 'TMP_TODAYREV' || ' 
            )    
            GROUP BY MASTERID ';
        EXECUTE (V_STR_QUERY);
            
        -- GET FEE SUMMARY    
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
                FROM ' || V_TABLEINSERT7 || ' A
                WHERE A.MASTERID IN (    
                    SELECT MASTERID    
                    FROM ' || 'TMP_T3' || ' 
                )    
                AND A.STATUS = ''ACT''    
                AND A.FLAG_CF = ''F''    
                AND A.METHOD = ''SL''    
            ) A    
            GROUP BY 
                A.DOWNLOAD_DATE    
                ,A.MASTERID ';
        EXECUTE (V_STR_QUERY);
            
        -- GET COST SUMMARY    
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
                FROM ' || V_TABLEINSERT7 || ' A    
                WHERE A.MASTERID IN (    
                    SELECT MASTERID    
                    FROM ' || 'TMP_T3' || ' 
                )    
                AND A.STATUS = ''ACT''    
                AND A.FLAG_CF = ''C''    
                AND A.METHOD = ''SL''    
            ) A    
            GROUP BY 
                A.DOWNLOAD_DATE    
                ,A.MASTERID ';
        EXECUTE (V_STR_QUERY);
            
        --INSERT FEE 1     
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT7 || ' 
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
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ECFDATE    
                ,A.DATASOURCE    
                ,B.PRDCODE    
                ,B.TRXCODE    
                ,B.CCY    
                ,CAST(CAST(CASE     
                    WHEN B.FLAG_REVERSE = ''Y''    
                    THEN - 1 * B.AMOUNT    
                    ELSE B.AMOUNT    
                END AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS DECIMAL(32, 20)) * A.N_ACCRU_FEE * - 1 AS N_AMOUNT    
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
            FROM ' || V_TABLEINSERT5 || ' A    
            JOIN ' || V_TABLEINSERT7 || ' B 
                ON B.ECFDATE = A.ECFDATE    
                AND A.MASTERID = B.MASTERID    
                AND B.FLAG_CF = ''F''    
                AND B.STATUS = ''ACT''      
                AND A.MASTERID NOT IN (    
                    SELECT DISTINCT MASTERID     
                    FROM ' || V_TABLEINSERT14 || ' 
                    WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                )     
            JOIN ' || 'TMP_TF' || ' C 
                ON C.DOWNLOAD_DATE = A.ECFDATE    
                AND C.MASTERID = A.MASTERID    
            --20160721    
            LEFT JOIN ' || 'TMP_IFRS_ACCT_STOP_REV' || ' REV 
                ON A.MASTERID = REV.MASTERID    
            WHERE A.ID IN (    
                SELECT ID    
                FROM ' || 'TMP_P1' || ' 
            )    
            --20160407 SL STOP REV    
            --AND A.MASTERID NOT IN (SELECT MASTERID FROM IFRS_ACCT_SL_STOP_REV WHERE DOWNLOAD_DATE=''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE)    
            AND REV.MASTERID IS NULL    
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
            
        --COST 1    
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT7 || ' 
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
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ECFDATE    
                ,A.DATASOURCE    
                ,B.PRDCODE    
                ,B.TRXCODE    
                ,B.CCY    
                ,CAST(CAST(CASE     
                    WHEN B.FLAG_REVERSE = ''Y''    
                    THEN - 1 * B.AMOUNT    
                    ELSE B.AMOUNT    
                END AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS DECIMAL(32, 20)) * A.N_ACCRU_COST * - 1 AS N_AMOUNT    
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
            FROM ' || V_TABLEINSERT5 || ' A    
            JOIN ' || V_TABLEINSERT7 || ' B 
                ON B.ECFDATE = A.ECFDATE    
                AND A.MASTERID = B.MASTERID    
                AND B.FLAG_CF = ''C''    
                AND B.STATUS = ''ACT''      
                AND A.MASTERID NOT IN (    
                    SELECT DISTINCT MASTERID     
                    FROM ' || V_TABLEINSERT14 || ' 
                    WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                ) 
            JOIN ' || 'TMP_TC' || ' C 
                ON C.DOWNLOAD_DATE = A.ECFDATE    
                AND C.MASTERID = A.MASTERID    
            --20160721    
            LEFT JOIN ' || 'TMP_IFRS_ACCT_STOP_REV' || ' REV 
                ON A.MASTERID = REV.MASTERID    
            WHERE A.ID IN (    
                SELECT ID    
                FROM ' || 'TMP_P1' || ' 
            )    
            --20160407 EIR STOP REV    
            AND REV.MASTERID IS NULL    
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
    
    -- 20160412 GROUP MULTIPLE ROWS BY CF_ID    
    -- CALL SP_IFRS_ACCT_SL_CF_ECF_GRP();

    -- GET ALL MASTER ID OF NEWLY GENERATED SL ECF    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || 'TMP_M0' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_M0' || ' AS 
        SELECT DISTINCT MASTERID    
        FROM ' || V_TABLEINSERT9 || '     
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    --FILTER OUT NOT TODAY STOPPED ECF    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || 'TMP_MX' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_MX' || ' AS 
        SELECT DISTINCT A.MASTERID    
        FROM ' || 'TMP_M0' || ' A    
        JOIN ' || V_TABLEINSERT9 || ' B 
            ON B.PREVDATE = B.PMTDATE    
            AND B.AMORTSTOPDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
            AND B.MASTERID = A.MASTERID 

        UNION -- 20171013 ALSO INCLUDE ACCOUNT WITH ZERO AMOUNT (FIX CHKAMORT ON DUE_DATE CHANGE WHEN END_AMORT_DT - 1)    
     
        SELECT MASTERID    
        FROM ' || V_TABLEINSERT6 || ' 
        WHERE TOTAL_AMT = 0    
        OR TOTAL_AMT_ACRU = 0 ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || 'TMP_M' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_M' || ' AS 
        SELECT DISTINCT A.MASTERID    
        FROM ' || 'TMP_MX' || ' A    
        LEFT JOIN ' || 'TMP_IFRS_ACCT_STOP_REV' || ' REV 
            ON A.MASTERID = REV.MASTERID    
        LEFT JOIN ' || V_TABLEINSERT14 || ' SWITCH 
            ON A.MASTERID = SWITCH.MASTERID    
            AND SWITCH.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
        --SWITCH EXCLUDED    
        WHERE SWITCH.MASTERID IS NULL    
        -- 20160407 SL STOP REV EXCLUDED    
        AND REV.MASTERID IS NULL ';
    EXECUTE (V_STR_QUERY);

    -- INSERT ACCRU VALUES FOR NEWLY GENERATED ECF    
    -- NO ACCRU IF TODAY IS DOING AMORT    
    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || 'TMP_M2' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_M2' || ' AS 
        SELECT DISTINCT MASTERID    
        FROM ' || V_TABLEINSERT5 || ' 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
        AND DO_AMORT = ''Y'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || 'TMP_ACRU' || ' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_ACRU' || ' AS 
        SELECT MASTERID    
        FROM ' || 'TMP_M' || ' 
        WHERE MASTERID NOT IN (    
            SELECT MASTERID    
            FROM ' || 'TMP_M2' || ' 
        ) ';
    EXECUTE (V_STR_QUERY);

    IF V_PARAM_DISABLE_ACCRU_PREV = 0 THEN 
        -- GET LAST ACF WITH DO_AMORT=N    
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || 'TMP_ACRU2' || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_ACRU2' || ' AS 
            SELECT MAX(ID) AS ID    
            FROM ' || V_TABLEINSERT5 || ' 
            WHERE MASTERID IN (    
                SELECT MASTERID    
                FROM ' || 'TMP_ACRU' || ' 
            )    
            AND DO_AMORT = ''N''    
            AND DOWNLOAD_DATE < ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
            --20180109 RESONA RECALC FROM START DATE    
            AND MASTERID NOT IN (    
                SELECT MASTERID    
                FROM ' || 'TMP_TODAYREV' || ' 
            )    
            GROUP BY MASTERID ';
        EXECUTE (V_STR_QUERY);
            
        -- GET FEE SUMMARY    
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || 'TMP_TF' || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_TF' || ' AS 
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
                FROM ' || V_TABLEINSERT7 || ' A    
                WHERE A.MASTERID IN (    
                    SELECT MASTERID    
                    FROM ' || 'TMP_ACRU' || ' 
                )    
                AND A.STATUS = ''ACT''    
                AND A.FLAG_CF = ''F''    
                AND A.METHOD = ''SL''    
            ) A    
            GROUP BY 
                A.DOWNLOAD_DATE    
                ,A.MASTERID ';
        EXECUTE (V_STR_QUERY);
            
        -- GET COST SUMMARY    
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || 'TMP_TC' || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || 'TMP_TC' || ' AS 
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
                FROM ' || V_TABLEINSERT7 || ' A    
                WHERE A.MASTERID IN (    
                    SELECT MASTERID    
                    FROM ' || 'TMP_ACRU' || ' 
                )    
                AND A.STATUS = ''ACT''    
                AND A.FLAG_CF = ''C''    
                AND A.METHOD = ''SL''    
            ) A    
            GROUP BY 
                A.DOWNLOAD_DATE    
                ,A.MASTERID ';
        EXECUTE (V_STR_QUERY);
            
        --INSERT FEE 1    
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || ' 
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
            ) SELECT 
                A.FACNO    
                ,A.CIFNO    
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
                ,A.ECFDATE    
                ,A.DATASOURCE    
                ,B.PRDCODE    
                ,B.TRXCODE    
                ,B.CCY    
                ,CAST(CAST(CASE     
                    WHEN B.FLAG_REVERSE = ''Y''    
                    THEN - 1 * B.AMOUNT    
                    ELSE B.AMOUNT    
                END AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS DECIMAL(32, 20)) * A.N_ACCRU_FEE AS N_AMOUNT    
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
            FROM ' || V_TABLEINSERT5 || ' A    
            JOIN ' || V_TABLEINSERT7 || ' B 
                ON B.ECFDATE = A.ECFDATE    
                AND A.MASTERID = B.MASTERID    
                AND B.FLAG_CF = ''F''    
            JOIN ' || 'TMP_TF' || ' C 
                ON C.DOWNLOAD_DATE = A.ECFDATE    
                AND C.MASTERID = A.MASTERID    
            WHERE A.ID IN (    
                SELECT ID    
                FROM ' || 'TMP_ACRU2' || ' 
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
            
        --COST 1    
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || ' 
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
            ) SELECT 
                A.FACNO    
                ,A.CIFNO    
                ,''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE    
                ,A.ECFDATE    
                ,A.DATASOURCE    
                ,B.PRDCODE    
                ,B.TRXCODE    
                ,B.CCY    
                ,CAST(CAST(CASE     
                    WHEN B.FLAG_REVERSE = ''Y''    
                    THEN - 1 * B.AMOUNT    
                    ELSE B.AMOUNT    
                END AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS DECIMAL(32, 20)) * A.N_ACCRU_COST AS N_AMOUNT    
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
            FROM ' || V_TABLEINSERT5 || ' A    
            JOIN ' || V_TABLEINSERT7 || ' B 
                ON B.ECFDATE = A.ECFDATE    
                AND A.MASTERID = B.MASTERID    
                AND B.FLAG_CF = ''C''    
            JOIN ' || 'TMP_TC' || ' C 
                ON C.DOWNLOAD_DATE = A.ECFDATE    
                AND C.MASTERID = A.MASTERID    
            WHERE A.ID IN (    
                SELECT ID    
                FROM ' || 'TMP_ACRU2' || ' 
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
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT4 || ' A 
        SET STATUS = TO_CHAR(''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE, ''YYYYMMDD'')    
        WHERE STATUS = ''ACT''    
        AND MASTERID IN (    
            SELECT MASTERID    
            FROM ' || V_TABLEINSERT6 || ' 
            WHERE TOTAL_AMT = 0    
            OR TOTAL_AMT_ACRU = 0    
        ) ';
    EXECUTE (V_STR_QUERY);

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_ACCT_SL_ECF_MAIN', '');

    ---------- ====== BODY ======

END;

$$
