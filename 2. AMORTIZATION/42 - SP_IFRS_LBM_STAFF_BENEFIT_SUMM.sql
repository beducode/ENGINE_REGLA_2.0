---- DROP PROCEDURE SP_IFRS_RESET_AMT_PRC;

CREATE OR REPLACE PROCEDURE SP_IFRS_RESET_AMT_PRC(
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
    V_TABLEINSERT VARCHAR(100);

    ---- TABLE LIST       
    V_TABLEUPDATE1 VARCHAR(100);
    V_TABLEUPDATE2 VARCHAR(100);
    V_TABLEUPDATE3 VARCHAR(100);
    V_TABLEUPDATE4 VARCHAR(100);
    V_TABLEUPDATE5 VARCHAR(100);
    V_TABLEUPDATE6 VARCHAR(100);
    V_TABLEUPDATE7 VARCHAR(100);
    V_TABLEUPDATE8 VARCHAR(100);
    V_TABLEUPDATE9 VARCHAR(100);
    V_TABLEUPDATE10 VARCHAR(100);
    V_TABLEUPDATE11 VARCHAR(100);
    V_TABLEUPDATE12 VARCHAR(100);

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
        V_TABLEUPDATE1 := 'IFRS_LBM_STAFF_BENEFIT_SUMM_' || P_RUNID || '';
        V_TABLEUPDATE2 := 'IFRS_PRC_DATE_AMORT_' || P_RUNID || '';
    ELSE 
        V_TABLEUPDATE1 := 'IFRS_LBM_STAFF_BENEFIT_SUMM';
        V_TABLEUPDATE2 := 'IFRS_PRC_DATE_AMORT';
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
    -------- ====== VARIABLE ======

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEUPDATE1 || ', ' || V_TABLEUPDATE2 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEUPDATE1 || ' AS SELECT * FROM IFRS_LBM_STAFF_BENEFIT_SUMM WHERE 1=0; ';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEUPDATE2 || ' AS SELECT * FROM IFRS_PRC_DATE_AMORT WHERE 1=0; ';
        
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    ---- DELETE FIRST
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_STAFF_BENEFIT_SUMM', '');

    V_STR_QUERY := 'DELETE FROM ' || V_TABLEUPDATE1 || ' WHERE DOWNLOAD_DATE >= ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEUPDATE1 || ' 
        ( 
            DOWNLOAD_DATE
            ,MASTERID
            ,BRCODE
            ,CIFNO
            ,FACNO
            ,ACCTNO
            ,DATASOURCE
            ,CCY
            ,AMOUNT_FEE
            ,AMOUNT_COST
            ,CREATEDDATE
            ,CREATEDBY
            ,AMORT_FEE
            ,AMORT_COST 
        ) SELECT 
            DOWNLOAD_DATE
            ,MASTERID
            ,BRCODE
            ,CIFNO
            ,FACNO
            ,ACCTNO
            ,DATASOURCE
            ,CCY
            ,AMOUNT_FEE
            ,AMOUNT_COST
            ,CREATEDDATE
            ,CREATEDBY
            ,AMORT_FEE
            ,AMORT_COST 
        FROM ( 
            SELECT 
                A.DOWNLOAD_DATE
                ,A.MASTERID
                ,A.BRCODE
                ,A.CIFNO
                ,A.FACNO
                ,A.ACCTNO
                ,A.DATASOURCE
                ,A.CCY
                ,SUM(A.AMOUNT_FEE) AS AMOUNT_FEE
                ,SUM(A.AMOUNT_COST) AS AMOUNT_COST
                ,CURRENT_DATE AS CREATEDDATE
                ,''SYSTEM'' AS CREATEDBY
                ,SUM(A.AMORT_FEE) AS AMORT_FEE
                ,SUM(A.AMORT_COST) AS AMORT_COST 
            FROM (
                SELECT 
                    ASTERID
                    ,BRCODE
                    ,CIFNO
                    ,FACNO
                    ,ACCTNO
                    ,DATASOURCE
                    ,CCY
                    ,AMOUNT_FEE
                    ,AMOUNT_COST 
                FROM ' || V_TABLEUPDATE1 || ' A 
                WHERE A.DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE
                UNION ALL
                SELECT
                    A.MASTERID
                    ,A.BRCODE
                    ,A.CIFNO
                    ,A.FACNO
                    ,A.ACCTNO
                    ,A.DATASOURCE
                    ,A.CCY
                    ,SUM(CASE
                            WHEN FLAG_CF = ''F''
                                THEN CASE 
                                    WHEN FLAG_REVERSE = 'Y'
									THEN - 1 * AMOUNT
								    ELSE AMOUNT
                                END
                            ELSE 0
                        END) AS AMOUNT_FEE
                    ,SUM(CASE
                            WHEN FLAG_CF = ''C''
                                THEN CASE 
                                    WHEN FLAG_REVERSE = 'Y'
                                        THEN - 1 * AMOUNT
                                    ELSE AMOUNT
                                END
                            ELSE 0 
                        END) AS AMOUNT_COST
                FROM ' || V_TABLEUPDATE1 || ' A 
                WHERE ECFDATE = ''' || CAST(V_PREVDATE AS VARCHAR(10)) || '''::DATE
                AND STATUS IN (''ACT'', ''PNL'', ''REV'')
                AND SRCPROCESS NOT IN (''SL_TO_EIR'')
                AND TRXCODE = ''BENEFIT''
            GROUP BY 
                A.MASTERID, A.BRCODE, A.CIFNO, A.FACNO, A.ACCTNO, A.DATASOURCE, A.CCY
            ) AS A
        GROUP BY 
            A.DOWNLOAD_DATE, A.MASTERID, A.BRCODE, A.CIFNO, A.FACNO, A.ACCTNO, A.DATASOURCE, A.CCY
        ) Z';
        
    EXECUTE (V_STR_QUERY);

    ---- END DELETE FIRST
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_STAFF_BENEFIT_SUMM', '');
    ---------- ====== BODY ======
    ---- START
    V_STR_QUERY := 'UPDATE ' || V_TABLEUPDATE1 || ' 
        SET AMORT_FEE = 0, AMORT_COST = 0 
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE';
    EXECUTE (V_STR_QUERY);
    V_STR_QUERY := 'UPDATE ' || V_TABLEUPDATE1 || ' 
        SET AMORT_FEE = B.AMORT_FEE
	        ,AMORT_COST = B.AMORT_COST
        FROM (
            SELECT X.*
                ,Y.*
            FROM ' || V_TABLEUPDATE1 || ' X
            JOIN ' || V_TABLEUPDATE2 || ' Y ON Y.PREVDATE = X.DOWNLOAD_DATE
        ) B
        WHERE DOWNLOAD_DATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE';
            AND B.MASTERID = ' || V_TABLEUPDATE1 || '.MASTERID

    EXECUTE (V_STR_QUERY);

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_STAFF_BENEFIT_SUMM', '');
    ---------- ====== BODY ======

END;

$$;