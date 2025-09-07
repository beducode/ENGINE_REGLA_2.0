---- DROP PROCEDURE SP_IFRS_ACCT_EIR_GS_PROC3;

CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_EIR_GS_PROC3(
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
    V_TABLEINSERT3 VARCHAR(100);
    V_TABLEINSERT4 VARCHAR(100);
    V_TABLEINSERT5 VARCHAR(100);
    V_TABLEINSERT6 VARCHAR(100);
    V_TABLEINSERT7 VARCHAR(100);
    V_TABLEINSERT8 VARCHAR(100);

    ---- VARIABLE PROCESS
    V_LOOP2 INT;
    V_CNT INT;
    V_CNT2 INT;
    
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
        V_TABLEINSERT3 := 'IFRS_ACCT_EIR_FAILED_GS3_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_ACCT_EIR_GS_RESULT3_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_ACCT_EIR_PAYM_GS_' || P_RUNID || '';
        V_TABLEINSERT6 := 'IFRS_ACCT_EIR_PAYM_GS_DATE_' || P_RUNID || '';
        V_TABLEINSERT7 := 'IFRS_GS_DATE1_' || P_RUNID || '';
        V_TABLEINSERT8 := 'IFRS_GS_DATE2_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT3 := 'IFRS_ACCT_EIR_FAILED_GS3';
        V_TABLEINSERT4 := 'IFRS_ACCT_EIR_GS_RESULT3';
        V_TABLEINSERT5 := 'IFRS_ACCT_EIR_PAYM_GS';
        V_TABLEINSERT6 := 'IFRS_ACCT_EIR_PAYM_GS_DATE';
        V_TABLEINSERT7 := 'IFRS_GS_DATE1';
        V_TABLEINSERT8 := 'IFRS_GS_DATE2';
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

    V_LOOP2 := 0;
    V_CNT := 0;
    V_CNT2 := 0;
    
    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- RECORD RUN_ID --------
    CALL SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, PG_BACKEND_PID(), CURRENT_DATE);
    -------- RECORD RUN_ID --------

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
       V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT3 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT3 || ' AS SELECT * FROM IFRS_ACCT_EIR_FAILED_GS3 WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT4 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT4 || ' AS SELECT * FROM IFRS_ACCT_EIR_GS_RESULT3 WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT7 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT7 || ' AS SELECT * FROM IFRS_GS_DATE1 WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEINSERT8 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEINSERT8 || ' AS SELECT * FROM IFRS_GS_DATE2 WHERE 1=0 ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======

    -------- ====== BODY ======
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'START', 'SP_IFRS_ACCT_EIR_GS_PROC3', '');

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' A 
        SET 
            EIR = NULL 
		    ,NEXT_EIR = NULL 
		    ,FINAL_EIR = NULL 
		    ,FINAL_UNAMORT = NULL 
		    ,NEXT_UNAMORT = NULL 
		    ,UNAMORT_GLOSS = NULL ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_GS2' || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_GS2' || ' 
        (
            MASTERID 
		    ,DTMIN 
		    ,BENEFIT 
		    ,STAFFLOAN 
		    ,COST_AMT 
		    ,FEE_AMT 
		    ,PREV_EIR 
        ) SELECT 
            B.MASTERID 
		    ,C.DTMIN 
		    ,B.BENEFIT 
		    ,B.STAFFLOAN 
		    ,B.COST_AMT 
		    ,B.FEE_AMT 
		    ,CASE 
			    WHEN D.GAIN_LOSS_CALC = ''Y'' 
    			THEN D.PREV_EIR 
			    ELSE D.NPV_RATE 
			END 
        FROM ' || 'IFRS_ACCT_EIR_CF_ECF1' || ' B 
        JOIN ' || V_TABLEINSERT6 || ' C 
        ON C.MASTERID = B.MASTERID 
        JOIN ' || 'IFRS_ACCT_EIR_CF_ECF' || ' D
        ON D.MASTERID = C.MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT5 || ' A 
        SET 
            PREV_UNAMORT1 = CASE 
                WHEN B.STAFFLOAN = 1
                    AND B.BENEFIT < 0
                THEN B.BENEFIT
                WHEN B.STAFFLOAN = 1
                    AND B.BENEFIT >= 0
                THEN 0
                ELSE B.FEE_AMT
            END + CASE 
                WHEN B.STAFFLOAN = 1
                    AND B.BENEFIT <= 0
                THEN 0
                WHEN B.STAFFLOAN = 1
                    AND B.BENEFIT > 0
                THEN B.BENEFIT
                ELSE B.COST_AMT
            END
            ,PREV_UNAMORT2 = 1.001 * (
                CASE 
                    WHEN B.STAFFLOAN = 1
                        AND B.BENEFIT < 0
                THEN B.BENEFIT
                    WHEN B.STAFFLOAN = 1
                        AND B.BENEFIT >= 0
                THEN 0
                    ELSE case when B.FEE_AMT=0 then -1000 else B.FEE_AMT end --btpn handle zero prev unamort
                    END + CASE 
                    WHEN B.STAFFLOAN = 1
                        AND B.BENEFIT <= 0
                THEN 0
                    WHEN B.STAFFLOAN = 1
                        AND B.BENEFIT > 0
                THEN B.BENEFIT
                    ELSE case when B.COST_AMT=0 then 700 else B.COST_AMT end --btpn handle zero prev unamort
            END
            )
            ,EIR1 = B.PREV_EIR
            ,EIR2 = B.PREV_EIR 
        FROM ' || 'TMP_GS2' || ' B 
        WHERE B.MASTERID = A.MASTERID 
        AND A.PMT_DATE = B.DTMIN ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' A 
        SET UNAMORT = B.PREV_UNAMORT1 
        FROM ' || V_TABLEINSERT5 || ' B 
        WHERE A.MASTERID = B.MASTERID
        AND B.PMT_DATE = A.DTMIN ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT5 || ' A 
        SET 
            PREV_CRYAMT1 = N_OSPRN_PREV + PREV_UNAMORT1
            ,PREV_CRYAMT2 = N_OSPRN_PREV + PREV_UNAMORT2
            ,EIRAMT1 = CASE 
                --WHEN INTCALCCODE IN (''2'', ''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                WHEN INTCALCCODE IN (''1'', ''6'')
                THEN CAST(I_DAYS AS NUMERIC(18, 10)) / CAST(360 AS NUMERIC(18, 10)) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                --WHEN INTCALCCODE = ''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                WHEN INTCALCCODE IN (''2'', ''3'')
                THEN CAST(I_DAYS AS NUMERIC(18, 10)) / CAST(365 AS NUMERIC(18, 10)) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                ELSE (CAST(M AS NUMERIC(18, 10)) / CAST(1200 AS NUMERIC(18, 10)) * EIR1 * (N_OSPRN_PREV + PREV_UNAMORT1))
            END
            ,EIRAMT2 = CASE 
                --WHEN INTCALCCODE IN (''2'', ''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                WHEN INTCALCCODE IN (''1'', ''6'')
                THEN CAST(I_DAYS AS NUMERIC(18, 10)) / CAST(360 AS NUMERIC(18, 10)) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT2)
                --WHEN INTCALCCODE = ''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                WHEN INTCALCCODE IN (''2'', ''3'')
                THEN CAST(I_DAYS AS NUMERIC(18, 10)) / CAST(365 AS NUMERIC(18, 10)) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT2)
                ELSE (CAST(M AS NUMERIC(18, 10)) / CAST(1200 AS NUMERIC(18, 10)) * EIR2 * (N_OSPRN_PREV + PREV_UNAMORT2))
            END
            ,AMORT1 = CASE 
                --WHEN INTCALCCODE IN (''2'', ''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                WHEN INTCALCCODE IN (''1'', ''6'')
                THEN CAST(I_DAYS AS NUMERIC(18, 10)) / CAST(360 AS NUMERIC(18, 10)) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                --WHEN INTCALCCODE = ''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                WHEN INTCALCCODE IN (''2'', ''3'')
                THEN CAST(I_DAYS AS NUMERIC(18, 10)) / CAST(365 AS NUMERIC(18, 10)) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                ELSE (CAST(M AS NUMERIC(18, 10)) / CAST(1200 AS NUMERIC(18, 10)) * EIR1 * (N_OSPRN_PREV + PREV_UNAMORT1))
            END - N_INT_PAYMENT
            ,AMORT2 = CASE 
                --WHEN INTCALCCODE IN (''2'', ''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                WHEN INTCALCCODE IN (''1'', ''6'')
                THEN CAST(I_DAYS AS NUMERIC(18, 10)) / CAST(360 AS NUMERIC(18, 10)) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT2)
                --WHEN INTCALCCODE = ''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                WHEN INTCALCCODE IN (''2'', ''3'')
                THEN CAST(I_DAYS AS NUMERIC(18, 10)) / CAST(365 AS NUMERIC(18, 10)) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT2)
                ELSE (CAST(M AS NUMERIC(18, 10)) / CAST(1200 AS NUMERIC(18, 10)) * EIR2 * (N_OSPRN_PREV + PREV_UNAMORT2))
            END - N_INT_PAYMENT
            ,UNAMORT1 = CASE 
                --WHEN INTCALCCODE IN (''2'', ''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                WHEN INTCALCCODE IN (''1'', ''6'')
                THEN CAST(I_DAYS AS NUMERIC(18, 10)) / CAST(360 AS NUMERIC(18, 10)) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                --WHEN INTCALCCODE = ''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                WHEN INTCALCCODE IN (''2'', ''3'')
                THEN CAST(I_DAYS AS NUMERIC(18, 10)) / CAST(365 AS NUMERIC(18, 10)) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                ELSE (CAST(M AS NUMERIC(18, 10)) / CAST(1200 AS NUMERIC(18, 10)) * EIR1 * (N_OSPRN_PREV + PREV_UNAMORT1))
            END - N_INT_PAYMENT + PREV_UNAMORT1
            ,UNAMORT2 = CASE 
                --WHEN INTCALCCODE IN (''2'', ''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                WHEN INTCALCCODE IN (''1'', ''6'')
                THEN CAST(I_DAYS AS NUMERIC(18, 10)) / CAST(360 AS NUMERIC(18, 10)) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT2)
                --WHEN INTCALCCODE = ''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                WHEN INTCALCCODE IN (''2'', ''3'')
                THEN CAST(I_DAYS AS NUMERIC(18, 10)) / CAST(365 AS NUMERIC(18, 10)) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT2)
                ELSE (CAST(M AS NUMERIC(18, 10)) / CAST(1200 AS NUMERIC(18, 10)) * EIR2 * (N_OSPRN_PREV + PREV_UNAMORT2))
            END - N_INT_PAYMENT + PREV_UNAMORT2
            ,CRYAMT1 = (N_OSPRN_PREV + PREV_UNAMORT1) - N_PRN_PAYMENT + (
                CASE 
                    --WHEN INTCALCCODE IN (''2'', ''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                    WHEN INTCALCCODE IN (''1'', ''6'')
                    THEN CAST(I_DAYS AS NUMERIC(18, 10)) / CAST(360 AS NUMERIC(18, 10)) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                    --WHEN INTCALCCODE = ''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                    WHEN INTCALCCODE IN (''2'', ''3'')
                    THEN CAST(I_DAYS AS NUMERIC(18, 10)) / CAST(365 AS NUMERIC(18, 10)) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                    ELSE (CAST(M AS NUMERIC(18, 10)) / CAST(1200 AS NUMERIC(18, 10)) * EIR1 * (N_OSPRN_PREV + PREV_UNAMORT1))
                END - N_INT_PAYMENT
            )
            ,CRYAMT2 = (N_OSPRN_PREV + PREV_UNAMORT2) - N_PRN_PAYMENT + (
                CASE 
                    --WHEN INTCALCCODE IN (''2'', ''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                    WHEN INTCALCCODE IN (''1'', ''6'')
                    THEN CAST(I_DAYS AS NUMERIC(18, 10)) / CAST(360 AS NUMERIC(18, 10)) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT2)
                    --WHEN INTCALCCODE = ''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                    WHEN INTCALCCODE IN (''2'', ''3'')
                    THEN CAST(I_DAYS AS NUMERIC(18, 10)) / CAST(365 AS NUMERIC(18, 10)) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT2)
                    ELSE (CAST(M AS NUMERIC(18, 10)) / CAST(1200 AS NUMERIC(18, 10)) * EIR2 * (N_OSPRN_PREV + PREV_UNAMORT2))
                END - N_INT_PAYMENT
            )
        FROM ' || V_TABLEINSERT6 || ' C 
        WHERE C.MASTERID = A.MASTERID 
        AND A.PMT_DATE = C.DTMIN ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT7 || '';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT7 || ' 
        (
            MASTERID
		    ,PMT_DATE
        ) SELECT 
            A.MASTERID
            ,MIN(A.PMT_DATE) DT
        FROM ' || V_TABLEINSERT5 || ' A
        JOIN ' || V_TABLEINSERT6 || ' B 
        ON A.PMT_DATE > B.DTMIN
        AND A.MASTERID = B.MASTERID
        GROUP BY A.MASTERID ';
    EXECUTE (V_STR_QUERY);

    V_LOOP2 := 0;
    WHILE 1 = 1 
    LOOP 
        V_LOOP2 := V_LOOP2 + 1;

        IF V_LOOP2 > 30 THEN EXIT; END IF;

        WHILE 1 = 1
        LOOP 
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'SELECT COUNT(*) FROM ' || V_TABLEINSERT7 || '';
            EXECUTE (V_STR_QUERY) INTO V_CNT;

            IF V_CNT <= 0 THEN EXIT; END IF;

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_GS3' || '';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_GS3' || ' 
                (
                    MASTERID
				    ,PMT_DATE
				    ,EIR1
				    ,CRYAMT1
				    ,EIR2
				    ,CRYAMT2
				    ,UNAMORT1
				    ,UNAMORT2
                ) SELECT 
                    B.MASTERID
				    ,B.PMT_DATE
				    ,C.EIR1
				    ,C.CRYAMT1
				    ,C.EIR2
				    ,C.CRYAMT2
				    ,C.UNAMORT1
				    ,C.UNAMORT2
			    FROM ' || V_TABLEINSERT7 || ' B
			    JOIN ' || V_TABLEINSERT5 || ' D 
                    ON D.MASTERID = B.MASTERID
				    AND D.PMT_DATE = B.PMT_DATE
			    JOIN ' || V_TABLEINSERT5 || ' C 
                    ON C.MASTERID = B.MASTERID
				    AND C.PMT_DATE = D.PREV_PMT_DATE ';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT5 || ' A 
                SET 
                    PREV_UNAMORT1 = C.UNAMORT1
				    ,PREV_UNAMORT2 = C.UNAMORT2
				    ,PREV_CRYAMT1 = C.CRYAMT1
				    ,PREV_CRYAMT2 = C.CRYAMT2
				    ,EIR1 = C.EIR1
				    ,EIR2 = C.EIR2
				    ,EIRAMT1 = CASE 
					    --WHEN INTCALCCODE IN (''2'', ''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
					    WHEN INTCALCCODE IN (''1'', ''6'')
    					THEN CAST(I_DAYS AS DECIMAL(18, 10)) / CAST(360 AS DECIMAL(18, 10)) * C.EIR1 / 100 * C.CRYAMT1
						--WHEN INTCALCCODE = ''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
					    WHEN INTCALCCODE IN (''2'', ''3'')
    					THEN CAST(I_DAYS AS DECIMAL(18, 10)) / CAST(365 AS DECIMAL(18, 10)) * C.EIR1 / 100 * C.CRYAMT1
					    ELSE (CAST(M AS DECIMAL(18, 10)) / CAST(1200 AS DECIMAL(18, 10)) * C.EIR1 * C.CRYAMT1)
					END
				    ,EIRAMT2 = CASE 
					    --WHEN INTCALCCODE IN (''2'', ''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
					    WHEN INTCALCCODE IN (''1'', ''6'')
    					THEN CAST(I_DAYS AS DECIMAL(18, 10)) / CAST(360 AS DECIMAL(18, 10)) * C.EIR2 / 100 * C.CRYAMT2
						--WHEN INTCALCCODE = ''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
					    WHEN INTCALCCODE IN (''2'', ''3'')
    					THEN CAST(I_DAYS AS DECIMAL(18, 10)) / CAST(365 AS DECIMAL(18, 10)) * C.EIR2 / 100 * C.CRYAMT2
					    ELSE (CAST(M AS DECIMAL(18, 10)) / CAST(1200 AS DECIMAL(18, 10)) * C.EIR2 * C.CRYAMT2)
					END
                FROM ' || 'TMP_GS3' || ' C 
                WHERE A.MASTERID = C.MASTERID
                AND A.PMT_DATE = C.PMT_DATE ';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT5 || ' A 
                SET 
                    AMORT1 = EIRAMT1 - N_INT_PAYMENT
                    ,AMORT2 = EIRAMT2 - N_INT_PAYMENT
                    ,UNAMORT1 = (EIRAMT1 - N_INT_PAYMENT) + PREV_UNAMORT1
                    ,UNAMORT2 = (EIRAMT2 - N_INT_PAYMENT) + PREV_UNAMORT2
                    ,CRYAMT1 = PREV_CRYAMT1 + (EIRAMT1 - N_INT_PAYMENT) - N_PRN_PAYMENT
                    ,CRYAMT2 = PREV_CRYAMT2 + (EIRAMT2 - N_INT_PAYMENT) - N_PRN_PAYMENT
                FROM ' || V_TABLEINSERT7 || ' B 
                WHERE A.MASTERID = B.MASTERID 
                AND A.PMT_DATE = B.PMT_DATE ';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT8 || '';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT8 || ' 
                (
                    MASTERID 
                    ,PMT_DATE 
                ) SELECT 
                    A.MASTERID 
                    ,MIN(A.PMT_DATE) DT
                FROM ' || V_TABLEINSERT5 || ' A 
                JOIN ' || V_TABLEINSERT7 || ' B 
                    ON A.PMT_DATE > B.PMT_DATE
                    AND B.MASTERID = A.MASTERID
                GROUP BY A.MASTERID ';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT7 || '';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT7 || ' 
                (
                    MASTERID 
                    ,PMT_DATE 
                ) SELECT 
                    MASTERID 
                    ,PMT_DATE 
                FROM ' || V_TABLEINSERT8 || '';
            EXECUTE (V_STR_QUERY);
        END LOOP;

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_T14' || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_T14' || ' 
            (
                MASTERID
                ,U1
                ,E1
            ) SELECT 
                B.MASTERID
                ,U.PREV_UNAMORT1
                ,U.EIR1
            FROM ' || V_TABLEINSERT6 || ' B
            JOIN ' || V_TABLEINSERT5 || ' U 
                ON U.PMT_DATE = B.DTMIN
                AND B.MASTERID = U.MASTERID
            JOIN ' || V_TABLEINSERT5 || ' C 
                ON C.PMT_DATE = B.DTMAX
                AND B.MASTERID = C.MASTERID
                AND ABS(C.UNAMORT1) < 0.01 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' A 
            SET 
                FINAL_UNAMORT = C.U1
                ,EIR = C.E1
            FROM ' || 'TMP_T14' || ' C
            WHERE A.MASTERID = C.MASTERID
            AND A.UNAMORT_GLOSS IS NULL ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' A 
            SET UNAMORT_GLOSS = FINAL_UNAMORT 
            WHERE FINAL_UNAMORT IS NOT NULL ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_GS1' || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_GS1' || ' 
            (
                MASTERID
                ,UNAMORT1
                ,UNAMORT2
                ,UNAMORT_GLOSS1
                ,UNAMORT_GLOSS2
			) SELECT 
                B.MASTERID
                ,C.UNAMORT1
                ,C.UNAMORT2
                ,U.PREV_UNAMORT1
                ,U.PREV_UNAMORT2
            FROM ' || V_TABLEINSERT6 || ' B
            JOIN ' || V_TABLEINSERT5 || ' U 
                ON U.PMT_DATE = B.DTMIN
                AND B.MASTERID = U.MASTERID
            JOIN ' || V_TABLEINSERT5 || ' C 
                ON C.PMT_DATE = B.DTMAX
                AND B.MASTERID = C.MASTERID ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' A 
            SET NEXT_UNAMORT = (((C.UNAMORT2 - C.UNAMORT1) / NULLIF((C.UNAMORT_GLOSS2 - C.UNAMORT_GLOSS1), 0)) * C.UNAMORT_GLOSS1 - C.UNAMORT1) / NULLIF(((C.UNAMORT2 - C.UNAMORT1) / NULLIF((C.UNAMORT_GLOSS2 - C.UNAMORT_GLOSS1), 0)), 0)
            FROM ' || 'TMP_GS1' || ' C
            WHERE A.MASTERID = C.MASTERID
            AND A.UNAMORT_GLOSS IS NULL ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' A 
            SET FINAL_UNAMORT = NEXT_UNAMORT
            FROM ' || 'TMP_GS1' || ' C
            WHERE A.MASTERID = C.MASTERID
            AND A.UNAMORT_GLOSS IS NULL
            AND C.UNAMORT_GLOSS1 = A.NEXT_UNAMORT
            AND ABS(C.UNAMORT1) < 1 ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' A 
            SET UNAMORT_GLOSS = FINAL_UNAMORT 
            WHERE FINAL_UNAMORT IS NOT NULL ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || 'TMP_GS2' || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || 'TMP_GS2' || ' 
            (
                MASTERID
                ,DTMIN
                ,NEXT_EIR
                ,STAFFLOAN
                ,BENEFIT
                ,FEE_AMT
                ,COST_AMT
                ,PREV_EIR
                ,NEXT_UNAMORT
			) SELECT 
                B.MASTERID
                ,C.DTMIN
                ,C.NEXT_EIR
                ,B.STAFFLOAN
                ,B.BENEFIT
                ,B.FEE_AMT
                ,B.COST_AMT
                ,B.PREV_EIR
                ,C.NEXT_UNAMORT
            FROM ' || 'IFRS_ACCT_EIR_CF_ECF1' || ' B
            JOIN ' || V_TABLEINSERT6 || ' C 
                ON C.MASTERID = B.MASTERID
                AND C.UNAMORT_GLOSS IS NULL ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT COUNT(*) FROM ' || 'TMP_GS2' || '';
        EXECUTE (V_STR_QUERY) INTO V_CNT2;

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT5 || ' A 
            SET 
                PREV_UNAMORT1 = B.NEXT_UNAMORT
                ,PREV_UNAMORT2 = B.NEXT_UNAMORT + (0.001 * B.NEXT_UNAMORT)
            FROM ' || 'TMP_GS2' || ' B
            WHERE B.MASTERID = A.MASTERID
            AND A.PMT_DATE = B.DTMIN ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT5 || ' A 
            SET 
                PREV_CRYAMT1 = N_OSPRN_PREV + PREV_UNAMORT1
                ,PREV_CRYAMT2 = N_OSPRN_PREV + PREV_UNAMORT2
                ,EIRAMT1 = CASE 
                    --WHEN INTCALCCODE IN (''2'', ''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                    WHEN INTCALCCODE IN (''1'', ''6'')
                    THEN CAST(I_DAYS AS DECIMAL(18, 10)) / CAST(360 AS DECIMAL(18, 10)) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                    --WHEN INTCALCCODE = ''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                    WHEN INTCALCCODE IN (''2'', ''3'')
                    THEN CAST(I_DAYS AS DECIMAL(18, 10)) / CAST(365 AS DECIMAL(18, 10)) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                    ELSE (CAST(M AS DECIMAL(18, 10)) / CAST(1200 AS DECIMAL(18, 10)) * EIR1 * (N_OSPRN_PREV + PREV_UNAMORT1))
                END
                ,EIRAMT2 = CASE 
                    --WHEN INTCALCCODE IN (''2'', ''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                    WHEN INTCALCCODE IN (''1'', ''6'')
                    THEN CAST(I_DAYS AS DECIMAL(18, 10)) / CAST(360 AS DECIMAL(18, 10)) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT2)
                    --WHEN INTCALCCODE = ''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                    WHEN INTCALCCODE IN (''2'', ''3'')
                    THEN CAST(I_DAYS AS DECIMAL(18, 10)) / CAST(365 AS DECIMAL(18, 10)) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT2)
                    ELSE (CAST(M AS DECIMAL(18, 10)) / CAST(1200 AS DECIMAL(18, 10)) * EIR2 * (N_OSPRN_PREV + PREV_UNAMORT2))
                END
                ,AMORT1 = CASE 
                    --WHEN INTCALCCODE IN (''2'', ''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                    WHEN INTCALCCODE IN (''1'', ''6'')
                    THEN CAST(I_DAYS AS DECIMAL(18, 10)) / CAST(360 AS DECIMAL(18, 10)) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                    --WHEN INTCALCCODE = ''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                    WHEN INTCALCCODE IN (''2'', ''3'')
                    THEN CAST(I_DAYS AS DECIMAL(18, 10)) / CAST(365 AS DECIMAL(18, 10)) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                    ELSE (CAST(M AS DECIMAL(18, 10)) / CAST(1200 AS DECIMAL(18, 10)) * EIR1 * (N_OSPRN_PREV + PREV_UNAMORT1))
                END - N_INT_PAYMENT
                ,AMORT2 = CASE 
                    --WHEN INTCALCCODE IN (''2'', ''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                    WHEN INTCALCCODE IN (''1'', ''6'')
                    THEN CAST(I_DAYS AS DECIMAL(18, 10)) / CAST(360 AS DECIMAL(18, 10)) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT2)
                    --WHEN INTCALCCODE = ''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                    WHEN INTCALCCODE IN (''2'', ''3'')
                    THEN CAST(I_DAYS AS DECIMAL(18, 10)) / CAST(365 AS DECIMAL(18, 10)) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT2)
                    ELSE (CAST(M AS DECIMAL(18, 10)) / CAST(1200 AS DECIMAL(18, 10)) * EIR2 * (N_OSPRN_PREV + PREV_UNAMORT2))
                END - N_INT_PAYMENT
                ,UNAMORT1 = CASE 
                    --WHEN INTCALCCODE IN (''2'', ''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                    WHEN INTCALCCODE IN (''1'', ''6'')
                    THEN CAST(I_DAYS AS DECIMAL(18, 10)) / CAST(360 AS DECIMAL(18, 10)) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                    --WHEN INTCALCCODE = ''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                    WHEN INTCALCCODE IN (''2'', ''3'')
                    THEN CAST(I_DAYS AS DECIMAL(18, 10)) / CAST(365 AS DECIMAL(18, 10)) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                    ELSE (CAST(M AS DECIMAL(18, 10)) / CAST(1200 AS DECIMAL(18, 10)) * EIR1 * (N_OSPRN_PREV + PREV_UNAMORT1))
                END - N_INT_PAYMENT + PREV_UNAMORT1
                ,UNAMORT2 = CASE 
                    --WHEN INTCALCCODE IN (''2'', ''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                    WHEN INTCALCCODE IN (''1'', ''6'')
                    THEN CAST(I_DAYS AS DECIMAL(18, 10)) / CAST(360 AS DECIMAL(18, 10)) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT2)
                    --WHEN INTCALCCODE = ''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                    WHEN INTCALCCODE IN (''2'', ''3'')
                    THEN CAST(I_DAYS AS DECIMAL(18, 10)) / CAST(365 AS DECIMAL(18, 10)) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT2)
                    ELSE (CAST(M AS DECIMAL(18, 10)) / CAST(1200 AS DECIMAL(18, 10)) * EIR2 * (N_OSPRN_PREV + PREV_UNAMORT2))
                END - N_INT_PAYMENT + PREV_UNAMORT2
                ,CRYAMT1 = (N_OSPRN_PREV + PREV_UNAMORT1) - N_PRN_PAYMENT + (
                    CASE 
                        --WHEN INTCALCCODE IN (''2'', ''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                        WHEN INTCALCCODE IN (''1'', ''6'')
                        THEN CAST(I_DAYS AS DECIMAL(18, 10)) / CAST(360 AS DECIMAL(18, 10)) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                        --WHEN INTCALCCODE = ''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                        WHEN INTCALCCODE IN (''2'', ''3'')
                        THEN CAST(I_DAYS AS DECIMAL(18, 10)) / CAST(365 AS DECIMAL(18, 10)) * EIR1 / 100 * (N_OSPRN_PREV + PREV_UNAMORT1)
                        ELSE (CAST(M AS DECIMAL(18, 10)) / CAST(1200 AS DECIMAL(18, 10)) * EIR1 * (N_OSPRN_PREV + PREV_UNAMORT1))
                    END - N_INT_PAYMENT
                )
                ,CRYAMT2 = (N_OSPRN_PREV + PREV_UNAMORT2) - N_PRN_PAYMENT + (
                    CASE 
                        --WHEN INTCALCCODE IN (''2'', ''6'') REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                        WHEN INTCALCCODE IN (''1'', ''6'')
                        THEN CAST(I_DAYS AS DECIMAL(18, 10)) / CAST(360 AS DECIMAL(18, 10)) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT2)
                        --WHEN INTCALCCODE = ''3'' REMARKS FOR ALIGN WITH ICC PAYMENT SCHEDULE 20160428  
                        WHEN INTCALCCODE IN (''2'', ''3'')
                        THEN CAST(I_DAYS AS DECIMAL(18, 10)) / CAST(365 AS DECIMAL(18, 10)) * EIR2 / 100 * (N_OSPRN_PREV + PREV_UNAMORT2)
                        ELSE (CAST(M AS DECIMAL(18, 10)) / CAST(1200 AS DECIMAL(18, 10)) * EIR2 * (N_OSPRN_PREV + PREV_UNAMORT2))
                    END - N_INT_PAYMENT
                )
            FROM ' || V_TABLEINSERT6 || ' C
            WHERE C.MASTERID = A.MASTERID
            AND A.PMT_DATE = C.DTMIN
            AND C.UNAMORT_GLOSS IS NULL ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'TRUNCATE TABLE ' || V_TABLEINSERT7 || '';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT7 || ' 
            (
                MASTERID
                ,PMT_DATE
			) SELECT 
                A.MASTERID
                ,MIN(A.PMT_DATE) DT
            FROM ' || V_TABLEINSERT5 || ' A
            JOIN ' || V_TABLEINSERT6 || ' B 
                ON A.PMT_DATE > B.DTMIN
                AND A.MASTERID = B.MASTERID
                AND B.UNAMORT_GLOSS IS NULL
            GROUP BY A.MASTERID ';
        EXECUTE (V_STR_QUERY);
    END LOOP;

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' A 
        SET 
            FINAL_UNAMORT = B.PREV_UNAMORT1
            ,EIR = B.EIR1
        FROM (
            SELECT 
                B.MASTERID
                ,U.PREV_UNAMORT1
                ,U.EIR1
            FROM ' || V_TABLEINSERT6 || ' B
            JOIN ' || V_TABLEINSERT5 || ' U 
                ON U.PMT_DATE = B.DTMIN
                AND B.MASTERID = U.MASTERID
            JOIN ' || V_TABLEINSERT5 || ' C 
                ON C.PMT_DATE = B.DTMAX
                AND B.MASTERID = C.MASTERID
                AND ABS(C.UNAMORT1) < 1
        ) B
        WHERE A.MASTERID = B.MASTERID
        AND A.UNAMORT_GLOSS IS NULL ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'UPDATE ' || V_TABLEINSERT6 || ' A 
        SET UNAMORT_GLOSS = FINAL_UNAMORT 
        WHERE FINAL_UNAMORT IS NOT NULL ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT3 || ' 
        (
            DOWNLOAD_DATE
            ,MASTERID
            ,CREATEDBY
            ,CREATEDDATE
        ) SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ,MASTERID
            ,''EIR_GS''
            ,CURRENT_TIMESTAMP
        FROM ' || V_TABLEINSERT6 || ' 
        WHERE UNAMORT_GLOSS IS NULL ';
    EXECUTE (V_STR_QUERY);

    V_STR_QUERY := '';
    V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || ' 
        (
            DOWNLOAD_DATE
            ,MASTERID
            ,CREATEDBY
            ,CREATEDDATE
            ,EIR
            ,GLOSS
            ,UNAMORT
        ) SELECT 
            ''' || CAST(V_CURRDATE AS VARCHAR(10)) || '''::DATE 
            ,MASTERID
            ,''EIR_GS''
            ,CURRENT_TIMESTAMP
            ,EIR
            ,UNAMORT - UNAMORT_GLOSS
            ,UNAMORT
        FROM ' || V_TABLEINSERT6 || '
        WHERE UNAMORT_GLOSS IS NOT NULL ';
    EXECUTE (V_STR_QUERY);

    GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
    V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
    V_RETURNROWS := 0;
    
    ---- END
    CALL SP_IFRS_LOG_AMORT(V_CURRDATE, 'END', 'SP_IFRS_ACCT_EIR_GS_PROC3', '');

    RAISE NOTICE 'SP_IFRS_ACCT_EIR_GS_PROC3 | AFFECTED RECORD : %', V_RETURNROWS2;
    ---------- ====== BODY ======

    -------- ====== LOG ======
    V_TABLEDEST = V_TABLEINSERT4;
    V_COLUMNDEST = '-';
    V_SPNAME = 'SP_IFRS_ACCT_EIR_GS_PROC3';
    V_OPERATION = 'INSERT';
    
    CALL SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SPNAME, V_OPERATION, V_RETURNROWS2, P_RUNID);
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS = 'SELECT * FROM ' || V_TABLEINSERT4 || '';
    CALL SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SPNAME, V_RETURNROWS2, P_RUNID);
    -------- ====== RESULT ======

END;

$$;