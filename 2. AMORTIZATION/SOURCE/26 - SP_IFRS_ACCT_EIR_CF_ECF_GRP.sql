CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_EIR_CF_ECF_GRP(IN P_RUNID CHARACTER VARYING DEFAULT 'S_00000_0000'::CHARACTER VARYING, IN P_DOWNLOAD_DATE DATE DEFAULT NULL::DATE, IN P_PRC CHARACTER VARYING DEFAULT 'S'::CHARACTER VARYING)
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
        V_TABLEINSERT1 := 'IFRS_ACCT_EIR_COST_FEE_ECF_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_ACCT_COST_FEE_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_ACCT_EIR_COST_FEE_ECF';
        V_TABLEINSERT2 := 'IFRS_ACCT_COST_FEE';
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
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_ACCT_EIR_CF_ECF_GRP', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT1 || ' A 
        SET 
            AMOUNT = CASE 
                WHEN B.FLAG_REV_ORI = ''N'' 
                THEN CASE 
                    WHEN B.ID1 = A.ID 
                    THEN B.AMT 
                    ELSE 0 
                END 
                ELSE CASE 
                    WHEN B.ID1 = A.ID 
                    THEN -1 * B.AMT 
                    ELSE 0 
                END 
            END 
            ,AMOUNT_ORG = CASE 
                WHEN B.FLAG_REV_ORI = ''N'' 
                THEN CASE 
                    WHEN B.ID1 = A.ID 
                    THEN B.AMT 
                    ELSE 0 
                END 
                ELSE CASE 
                    WHEN B.ID1 = A.ID 
                    THEN -1 * B.AMT 
                    ELSE 0 
                END 
            END 
            ,FLAG_REVERSE = B.FLAG_REV_ORI 
        FROM ( 
            SELECT 
                MIN(A.ID) AS ID1 
                ,A.CF_ID 
                ,B.FLAG_REVERSE AS FLAG_REV_ORI 
                ,SUM(CASE 
                    WHEN A.FLAG_REVERSE = ''N'' 
                    THEN A.AMOUNT 
                    ELSE -1 * A.AMOUNT 
                END) AS AMT 
            FROM ' || V_TABLEINSERT1 || ' A 
            JOIN ' || V_TABLEINSERT2 || ' B 
            ON B.ID = A.CF_ID 
            AND B.MASTERID = A.MASTERID 
            WHERE A.ECFDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            AND A.STATUS = ''ACT'' 
            AND A.CREATEDBY <> ''EIR_SWITCH'' 
            GROUP BY A.CF_ID, B.FLAG_REVERSE 
        ) B 
        WHERE A.ECFDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND A.CF_ID = B.CF_ID 
        AND A.STATUS = ''ACT'' ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ' || V_TABLEINSERT1 || ' 
        WHERE ECFDATE = ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
        AND AMOUNT = 0 ';
    EXECUTE (V_STR_QUERY);

    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_ACCT_EIR_CF_ECF_GRP', '');
    -------- ====== BODY ======

END;

$$
