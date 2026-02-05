CREATE OR REPLACE PROCEDURE SP_IFRS_LBM_ACCT_EIR_ECF_MAIN
AS
  V_CURRDATE DATE;
  V_PREVDATE DATE;
  V_VMIN_ID NUMBER(19);
  V_VMAX_ID NUMBER(19);
  V_VX NUMBER(19);
  V_ID2 NUMBER(19);
  V_VX_INC NUMBER(19);
  V_PARAM_DISABLE_ACCRU_PREV NUMBER(19);
  V_ROUND NUMBER(10);
  V_FUNCROUND NUMBER(10);

  BEGIN
    SELECT MAX(CURRDATE)
        , MAX(PREVDATE) INTO V_CURRDATE, V_PREVDATE
    FROM IFRS_PRC_DATE_AMORT;

    BEGIN
      SELECT CAST(VALUE1 AS NUMBER(10))
           , CAST(VALUE2 AS NUMBER(10))
      INTO V_ROUND, V_FUNCROUND
      FROM TBLM_COMMONCODEDETAIL
      WHERE COMMONCODE = 'SCM003';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        V_ROUND := 2;
        V_FUNCROUND:=0;
    END;

    --DISABLE ACCRU PREV CREATE ON NEW ECF AND RETURN ACCRUAL TO UNAMORT
    --ADD YAHYA
     BEGIN
      SELECT  CASE WHEN COMMONUSAGE = 'Y' THEN 1  ELSE 0  END
    INTO V_PARAM_DISABLE_ACCRU_PREV
    FROM    TBLM_COMMONCODEHEADER
    WHERE   COMMONCODE = 'SCM005';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        V_PARAM_DISABLE_ACCRU_PREV := 0;
    END;

    --SET @PARAM_DISABLE_ACCRU_PREV = 1
    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'START','SP_IFRS_LBM_ACCT_EIR_ECF_MAIN','');

    COMMIT;

    --RESET DATA BEFORE PROCESSING
    DELETE /*+ PARALLEL(12) */
    FROM IFRS_LBM_ACCT_EIR_ACCRU_PREV
    WHERE DOWNLOAD_DATE >= V_CURRDATE
        AND SRCPROCESS = 'ECF';
    COMMIT;

    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_COST_FEE
    SET STATUS = 'ACT'
    WHERE STATUS = 'PNL'
        AND CREATEDBY = 'EIRECF1'
        AND DOWNLOAD_DATE = V_CURRDATE;
    COMMIT;

    UPDATE /*+ PARALLEL(12) */ IFRS_LBM_ACCT_EIR_CF_PREV
    SET STATUS = 'ACT'
    WHERE STATUS = 'PNL'
        AND CREATEDBY = 'EIRECF2'
        AND DOWNLOAD_DATE = V_CURRDATE;
    COMMIT;

    UPDATE /*+ PARALLEL(12) */ IFRS_LBM_ACCT_EIR_CF_PREV
    SET STATUS = 'ACT'
    WHERE STATUS = 'PNL2'
        AND CREATEDBY = 'EIRECF2'
        AND DOWNLOAD_DATE = V_PREVDATE;

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T7';



    INSERT /*+ PARALLEL(12) */ INTO TMP_T7 (
        MASTERID
        ,STAFFLOAN
        ,PKID
        ,NPVRATE
        )
    SELECT /*+ PARALLEL(12) */ A.MASTERID
        ,CASE
            WHEN COALESCE(STAFF_LOAN_FLAG, 'N') IN (
                    'N'
                    ,''
                    )
                THEN 0
            ELSE 1
            END
        ,A.ID
        ,CASE
            WHEN STAFF_LOAN_FLAG = 'Y'
                THEN COALESCE(P.MARKET_RATE, 0)
            ELSE 0
            END MARKET_RATE
    FROM IFRS_IMA_AMORT_CURR A
    LEFT JOIN IFRS_PRODUCT_PARAM P ON P.DATA_SOURCE = A.DATA_SOURCE
        AND P.PRD_TYPE = A.PRODUCT_TYPE
        AND P.PRD_CODE = A.PRODUCT_CODE
        AND (
            P.CCY = A.CURRENCY
            OR NVL(P.CCY, 'ALL') = 'ALL'
            )
    WHERE A.EIR_STATUS = 'Y'
        AND A.AMORT_TYPE <> 'SL';


    COMMIT;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_LBM_ACCT_EIR_CF_ECF';

    --20180116 ACCT WITH REVERSAL TODAY
    INSERT /*+ PARALLEL(12) */ INTO TMP_TODAYREV(MASTERID)
    SELECT /*+ PARALLEL(12) */ DISTINCT MASTERID
    FROM IFRS_ACCT_COST_FEE
    WHERE DOWNLOAD_DATE = V_CURRDATE
        AND FLAG_REVERSE = 'Y'
        AND CF_ID_REV IS NOT NULL;

    COMMIT;

    -- TODAY NEW COST FEE
    INSERT /*+ PARALLEL(12) */ INTO IFRS_LBM_ACCT_EIR_CF_ECF (
        MASTERID
        ,FEE_AMT
        ,COST_AMT
        ,FEE_AMT_ACRU
        ,COST_AMT_ACRU
        ,STAFFLOAN
        ,PKID
        ,NPV_RATE
        ,GAIN_LOSS_CALC --20180226 SET N
        )
    SELECT /*+ PARALLEL(12) */ A.MASTERID
        ,SUM(COALESCE(CASE
                    WHEN C.FLAG_CF = 'F'
                        THEN CASE
                                WHEN C.FLAG_REVERSE = 'Y'
                                    THEN - 1 * C.AMOUNT
                                ELSE C.AMOUNT
                                END
                    ELSE 0
                    END, 0))
        ,SUM(COALESCE(CASE
                    WHEN C.FLAG_CF = 'C'
                        THEN CASE
                                WHEN C.FLAG_REVERSE = 'Y'
                                    THEN - 1 * C.AMOUNT
                                ELSE C.AMOUNT
                                END
                    ELSE 0
                    END, 0))
        ,0
        ,0
        ,A.STAFFLOAN
        ,A.PKID
        ,A.NPVRATE
        ,'N' --20180226
    FROM TMP_T7 A
    LEFT JOIN IFRS_ACCT_COST_FEE C ON C.DOWNLOAD_DATE = V_CURRDATE
        AND C.MASTERID = A.MASTERID
        AND C.STATUS = 'ACT'
        AND C.METHOD = 'EIR'
        --20180108 EXCLUDE CF REVERSAL AND ITS PAIR
        AND C.CF_ID NOT IN (
            SELECT CF_ID
            FROM IFRS_ACCT_COST_FEE
            WHERE DOWNLOAD_DATE = V_CURRDATE
                AND FLAG_REVERSE = 'Y'
                AND CF_ID_REV IS NOT NULL

            UNION ALL

            SELECT CF_ID_REV
            FROM IFRS_ACCT_COST_FEE
            WHERE DOWNLOAD_DATE = V_CURRDATE
                AND FLAG_REVERSE = 'Y'
                AND CF_ID_REV IS NOT NULL
            )
    --WHERE C.METHOD = 'EIR'
    GROUP BY A.MASTERID
        ,A.STAFFLOAN
        ,A.PKID
        ,A.NPVRATE;

    COMMIT;

    --20180226 FILL TO COLUMN FOR NEW COST/FEE
    UPDATE /*+ PARALLEL(12) */ IFRS_LBM_ACCT_EIR_CF_ECF
      SET NEW_FEE_AMT = NVL(FEE_AMT, 0)
        ,NEW_COST_AMT = NVL(COST_AMT, 0)
        ,NEW_TOTAL_AMT = NVL(NEW_FEE_AMT, 0) + NVL(NEW_COST_AMT, 0);

    COMMIT;

    -- SISA UNAMORT
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T10';

    INSERT /*+ PARALLEL(12) */ INTO TMP_T10 (
        MASTERID
        ,FEE_AMT
        ,COST_AMT
        )
    SELECT /*+ PARALLEL(12) */ B.MASTERID
        ,SUM(COALESCE(CASE
                    WHEN B.FLAG_CF = 'F'
                        THEN CASE
                                WHEN B.FLAG_REVERSE = 'Y'
                                    THEN - 1 * CASE
                                            WHEN CFREV.MASTERID IS NULL
                                                THEN B.AMOUNT
                                            ELSE B.AMOUNT
                                            END
                                ELSE CASE
                                        WHEN CFREV.MASTERID IS NULL
                                            THEN B.AMOUNT
                                        ELSE B.AMOUNT
                                        END
                                END
                    ELSE 0
                    END, 0)) AS FEE_AMT
        ,SUM(COALESCE(CASE
                    WHEN B.FLAG_CF = 'C'
                        THEN CASE
                                WHEN B.FLAG_REVERSE = 'Y'
                                    THEN - 1 * CASE
                                            WHEN CFREV.MASTERID IS NULL
                                                THEN B.AMOUNT
                                            ELSE B.AMOUNT
                                            END
                                ELSE CASE
                                        WHEN CFREV.MASTERID IS NULL
                                            THEN B.AMOUNT
                                        ELSE B.AMOUNT
                                        END
                                END
                    ELSE 0
                    END, 0)) AS COST_AMT
    FROM IFRS_LBM_ACCT_EIR_CF_PREV B
    JOIN VW_LBM_LAST_EIR_CF_PREV X ON X.MASTERID = B.MASTERID
        AND X.DOWNLOAD_DATE = B.DOWNLOAD_DATE
        AND B.SEQ = X.SEQ
    --20160407 EIR STOP REV
    LEFT JOIN (
        SELECT DISTINCT MASTERID
        FROM IFRS_LBM_ACCT_EIR_STOP_REV
        WHERE DOWNLOAD_DATE = V_CURRDATE
        ) A ON A.MASTERID = B.MASTERID
    --20180116 RESONA REQ
    LEFT JOIN TMP_TODAYREV CFREV ON CFREV.MASTERID = B.MASTERID
    WHERE B.DOWNLOAD_DATE IN (V_CURRDATE,V_PREVDATE)
        AND B.STATUS = 'ACT'
        AND A.MASTERID IS NULL
        --20180116 EXCLUDE CF REVERSAL AND ITS PAIR
        AND B.CF_ID NOT IN (
            SELECT CF_ID
            FROM IFRS_ACCT_COST_FEE
            WHERE DOWNLOAD_DATE = V_CURRDATE
                AND FLAG_REVERSE = 'Y'
                AND CF_ID_REV IS NOT NULL
            UNION ALL
            SELECT CF_ID_REV
            FROM IFRS_ACCT_COST_FEE
            WHERE DOWNLOAD_DATE = V_CURRDATE
                AND FLAG_REVERSE = 'Y'
                AND CF_ID_REV IS NOT NULL
            )
        --20180426 EXCLUDE PREVDATE IF NOT FROM SP ACF ACCRU
        AND CASE WHEN B.DOWNLOAD_DATE = V_PREVDATE AND B.SEQ <> '2' THEN 0 ELSE 1 END = 1
    GROUP BY B.MASTERID;

    COMMIT;

    MERGE INTO IFRS_LBM_ACCT_EIR_CF_ECF A
    USING  TMP_T10 B
    ON (B.MASTERID = A.MASTERID
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.FEE_AMT = A.FEE_AMT + B.FEE_AMT
        ,A.COST_AMT = A.COST_AMT + B.COST_AMT;

    COMMIT;

    IF V_PARAM_DISABLE_ACCRU_PREV != 0
    THEN
        -- NO ACCRU IF TODAY IS DOING AMORT
        EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T1  ';

        INSERT INTO TMP_T1 (
            MASTERID
            ,ACCTNO
            )
        SELECT DISTINCT MASTERID
            ,ACCTNO
        FROM IFRS_LBM_ACCT_EIR_ACF
        WHERE DOWNLOAD_DATE = V_CURRDATE
            AND DO_AMORT = 'Y';

        COMMIT;

	/* REMARKS 20180824
        INSERT INTO TMP_T1 (MASTERID)
        SELECT DISTINCT A.MASTERID
        FROM IFRS_ACCT_SWITCH A
        JOIN IFRS_LBM_ACCT_EIR_ECF B ON A.MASTERID = B.MASTERID
            AND A.DOWNLOAD_DATE = B.PMT_DATE
            AND B.AMORTSTOPDATE IS NULL
        WHERE A.DOWNLOAD_DATE = V_CURRDATE;

        COMMIT;

        INSERT INTO TMP_T1   (MASTERID)
        SELECT DISTINCT A.MASTERID
        FROM IFRS_ACCT_SWITCH A
        WHERE A.DOWNLOAD_DATE = V_CURRDATE
            AND A.MASTERID IN (
                SELECT DISTINCT MASTERID
                FROM IFRS_LBM_EVENT_CHANGES
                WHERE EVENT_ID = 3 --STAFF LOAN EVENT
                    AND DOWNLOAD_DATE = V_CURRDATE
                );

        COMMIT;
        REMARKS 20180824*/

        EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T3';

        INSERT /*+ PARALLEL(12) */ INTO TMP_T3 (MASTERID)
        SELECT /*+ PARALLEL(12) */ MASTERID
        FROM IFRS_LBM_ACCT_EIR_CF_ECF
        WHERE MASTERID NOT IN (
                SELECT MASTERID
                FROM TMP_T1
                );
        COMMIT;

        -- GET LAST ACF WITH DO_AMORT=N
        EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_P1';

        INSERT /*+ PARALLEL(12) */ INTO TMP_P1 (ID)
        SELECT /*+ PARALLEL(12) */ MAX(ID) AS ID
        FROM IFRS_LBM_ACCT_EIR_ACF
        WHERE MASTERID IN (SELECT MASTERID FROM TMP_T3)
            AND DO_AMORT = 'N'
            AND DOWNLOAD_DATE < V_CURRDATE
            AND DOWNLOAD_DATE >= V_PREVDATE
        GROUP BY MASTERID;

        COMMIT;


        MERGE INTO IFRS_LBM_ACCT_EIR_CF_ECF A
        USING (SELECT *
                FROM IFRS_LBM_ACCT_EIR_ACF
                WHERE ID IN (SELECT ID FROM TMP_P1)
              ) B
        ON (B.MASTERID = A.MASTERID
            --20160407 EIR STOP REV
            AND A.MASTERID NOT IN (SELECT MASTERID
                                    FROM IFRS_LBM_ACCT_EIR_STOP_REV
                                    WHERE DOWNLOAD_DATE = V_CURRDATE
                                    )
	    AND A.MASTERID NOT IN (SELECT DISTINCT MASTERID
	    			   FROM IFRS_ACCT_SWITCH
	                           WHERE DOWNLOAD_DATE = V_CURRDATE
	                           ))
        WHEN MATCHED THEN
        UPDATE
        SET A.FEE_AMT = A.FEE_AMT - B.N_ACCRU_FEE
            ,A.COST_AMT = A.COST_AMT - B.N_ACCRU_COST;

        COMMIT;

       /*TIDAK TERPAKAI DI LBM 20180824
  --20180116 FEE ADJ REV AMBIL DARI UNAMORT UNTUK PAIR DARI CF REV
        MERGE INTO IFRS_LBM_ACCT_EIR_CF_ECF A
        USING (SELECT *
              FROM IFRS_ACCT_JOURNAL_INTM
              WHERE CF_ID IN (
                      SELECT CF_ID_REV
                      FROM IFRS_ACCT_COST_FEE
                      WHERE DOWNLOAD_DATE = V_CURRDATE
                          AND FLAG_REVERSE = 'Y'
                          AND CF_ID_REV IS NOT NULL
                      )
                  AND DOWNLOAD_DATE = V_PREVDATE
                  AND REVERSE = 'N'
                  AND JOURNALCODE = 'ACCRU'
                  AND FLAG_CF = 'F'
              ) B
        ON (B.MASTERID = A.MASTERID
            --20180404 ADD FILTER
            AND A.MASTERID IN (SELECT MASTERID FROM TMP_T3)
            --20160407 SL STOP REV
            AND A.MASTERID NOT IN (SELECT MASTERID FROM IFRS_LBM_ACCT_EIR_STOP_REV WHERE DOWNLOAD_DATE = V_CURRDATE)
           )
        WHEN MATCHED THEN
        UPDATE
        SET A.FEE_AMT = A.FEE_AMT + B.N_AMOUNT;

        COMMIT;


        --20180116 COST ADJ REV AMBIL DARI UNAMORT UNTUK PAIR DARI CF REV

        MERGE INTO IFRS_LBM_ACCT_EIR_CF_ECF A
        USING (SELECT *
              FROM IFRS_ACCT_JOURNAL_INTM
              WHERE CF_ID IN (
                      SELECT CF_ID_REV
                      FROM IFRS_ACCT_COST_FEE
                      WHERE DOWNLOAD_DATE = V_CURRDATE
                          AND FLAG_REVERSE = 'Y'
                          AND CF_ID_REV IS NOT NULL
                      )
                  AND DOWNLOAD_DATE = V_PREVDATE
                  AND REVERSE = 'N'
                  AND JOURNALCODE = 'ACCRU'
                  AND FLAG_CF = 'C'
              ) B
        ON (B.MASTERID = A.MASTERID
            --20180404 ADD FILTER
            AND A.MASTERID IN (SELECT MASTERID FROM TMP_T3)
            --20160407 SL STOP REV
            AND A.MASTERID NOT IN (SELECT MASTERID FROM IFRS_LBM_ACCT_EIR_STOP_REV WHERE DOWNLOAD_DATE = V_CURRDATE)
           )
        WHEN MATCHED THEN
        UPDATE
        SET A.COST_AMT = A.COST_AMT + B.N_AMOUNT;

        COMMIT;
          20180824*/
    END IF; --IF @PARAM_DISABLE_ACCRU_PREV != 0

    COMMIT;

    -- ACCRU
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T10';


    INSERT /*+ PARALLEL(12) */ INTO TMP_T10 (
        MASTERID
        ,FEE_AMT
        ,COST_AMT
        )
    SELECT /*+ PARALLEL(12) */ B.MASTERID
        ,SUM(COALESCE(CASE
                    WHEN B.FLAG_CF = 'F'
                        THEN CASE
                                WHEN B.FLAG_REVERSE = 'Y'
                                    THEN - 1 * B.AMOUNT
                                ELSE B.AMOUNT
                                END
                    ELSE 0
                    END, 0)) AS FEE_AMT
        ,SUM(COALESCE(CASE
                    WHEN B.FLAG_CF = 'C'
                        THEN CASE
                                WHEN B.FLAG_REVERSE = 'Y'
                                    THEN - 1 * B.AMOUNT
                                ELSE B.AMOUNT
                                END
                    ELSE 0
                    END, 0)) AS COST_AMT
    FROM IFRS_LBM_ACCT_EIR_ACCRU_PREV B
    WHERE B.STATUS = 'ACT'
        --20180116 EXCLUDE CF REV AND ITS PAIR
        AND B.CF_ID NOT IN (
            SELECT CF_ID
            FROM IFRS_ACCT_COST_FEE
            WHERE DOWNLOAD_DATE = V_CURRDATE
                AND FLAG_REVERSE = 'Y'
                AND CF_ID_REV IS NOT NULL

            UNION ALL

            SELECT CF_ID_REV
            FROM IFRS_ACCT_COST_FEE
            WHERE DOWNLOAD_DATE = V_CURRDATE
                AND FLAG_REVERSE = 'Y'
                AND CF_ID_REV IS NOT NULL
            )
    GROUP BY B.MASTERID;

    COMMIT;

    MERGE INTO IFRS_LBM_ACCT_EIR_CF_ECF A
    USING TMP_T10 B
    ON (B.MASTERID = A.MASTERID
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.FEE_AMT_ACRU = B.FEE_AMT
        ,A.COST_AMT_ACRU = B.COST_AMT;

    COMMIT;

    -- UPDATE TOTAL
    UPDATE /*+ PARALLEL(12) */ IFRS_LBM_ACCT_EIR_CF_ECF
    SET TOTAL_AMT = ROUND(FEE_AMT + COST_AMT, 0)
        ,TOTAL_AMT_ACRU = ROUND(FEE_AMT + COST_AMT + FEE_AMT_ACRU + COST_AMT_ACRU, 0);

    COMMIT;
    -- UPDATE PREV EIR
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T13';

    INSERT /*+ PARALLEL(12) */ INTO TMP_T13 (
        MASTERID
        ,N_EFF_INT_RATE
        ,ENDAMORTDATE
        )
    SELECT /*+ PARALLEL(12) */ B.MASTERID
        ,B.N_EFF_INT_RATE
        ,B.ENDAMORTDATE
    FROM IFRS_LBM_ACCT_EIR_ECF B
    WHERE B.AMORTSTOPDATE IS NULL
        AND B.PMT_DATE = B.PREV_PMT_DATE;

    COMMIT;


    MERGE INTO IFRS_LBM_ACCT_EIR_CF_ECF A
    USING TMP_T13 B
    ON (B.MASTERID = A.MASTERID
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.PREV_EIR = N_EFF_INT_RATE
        ,A.PREV_ENDAMORTDATE = B.ENDAMORTDATE;

    COMMIT;

    --20180226 SET GAIN_LOSS_CALC TO Y IF PREPAYMENT EVENT DETECTED WITHOUT OTHER EVENT (SIMPLIFY FOR NOW)
    --PARTIAL PAYMENT EVENTID IS 6
    UPDATE /*+ PARALLEL(12) */ IFRS_LBM_ACCT_EIR_CF_ECF
    SET GAIN_LOSS_CALC = 'Y'
    WHERE MASTERID IN (
            SELECT MASTERID
            FROM IFRS_LBM_EVENT_CHANGES
            WHERE EVENT_ID = 6
                AND EFFECTIVE_DATE = V_CURRDATE
            )
        AND MASTERID NOT IN (
            SELECT MASTERID
            FROM IFRS_LBM_EVENT_CHANGES
            WHERE EVENT_ID IN (0,1,2,3)
                AND EFFECTIVE_DATE = V_CURRDATE
            );

    COMMIT;

    --20180226 IF DONT HAVE PREV EIR THEN SET BACK TO N
    UPDATE /*+ PARALLEL(12) */ IFRS_LBM_ACCT_EIR_CF_ECF
    SET GAIN_LOSS_CALC = 'N'
    WHERE PREV_EIR IS NULL
        AND GAIN_LOSS_CALC = 'Y';


    COMMIT;


    -- DO FULL AMORT IF SUM COST FEE ZERO AND DONT CREATE NEW ECF
    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_COST_FEE
    SET STATUS = 'PNL'
        ,CREATEDBY = 'EIRECF1'
    WHERE DOWNLOAD_DATE = V_CURRDATE
        AND MASTERID IN (
            SELECT MASTERID
            FROM IFRS_LBM_ACCT_EIR_CF_ECF
            WHERE TOTAL_AMT = 0
                OR TOTAL_AMT_ACRU = 0
            )
        AND STATUS = 'ACT'
        --20180116 EXCLUDE CF REV AND ITS PAIR, WILL BE HANDLED BY ACF_ABN
        AND CF_ID NOT IN (
            SELECT CF_ID
            FROM IFRS_ACCT_COST_FEE
            WHERE DOWNLOAD_DATE = V_CURRDATE
                AND FLAG_REVERSE = 'Y'
                AND CF_ID_REV IS NOT NULL

            UNION ALL

            SELECT CF_ID_REV
            FROM IFRS_ACCT_COST_FEE
            WHERE DOWNLOAD_DATE = V_CURRDATE
                AND FLAG_REVERSE = 'Y'
                AND CF_ID_REV IS NOT NULL
            );
    COMMIT;

    -- IF LAST COST FEE PREV IS CURRDATE
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T11';

    INSERT /*+ PARALLEL(12) */ INTO TMP_T11 (
        MASTERID
        ,DOWNLOAD_DATE
        ,SEQ
        ,CURRDATE
        )
    SELECT /*+ PARALLEL(12) */ B.MASTERID
        ,B.DOWNLOAD_DATE
        ,B.SEQ
        ,V_CURRDATE CURRDATE
    FROM VW_LBM_LAST_EIR_CF_PREV B
    WHERE B.MASTERID IN (
            SELECT MASTERID
            FROM IFRS_LBM_ACCT_EIR_CF_ECF
            WHERE TOTAL_AMT = 0
                OR TOTAL_AMT_ACRU = 0
            );

    COMMIT;


    MERGE INTO IFRS_LBM_ACCT_EIR_CF_PREV A
    USING TMP_T11 B
    ON (A.DOWNLOAD_DATE = B.CURRDATE
        AND A.MASTERID = B.MASTERID
        AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
        AND A.SEQ = B.SEQ
        --20180116 EXCLUDE CF REV AND ITS PAIR, WILL BE HANDLED BY SP ACF ABN
        AND A.CF_ID NOT IN (
            SELECT CF_ID
            FROM IFRS_ACCT_COST_FEE
            WHERE DOWNLOAD_DATE = V_CURRDATE
                AND FLAG_REVERSE = 'Y'
                AND CF_ID_REV IS NOT NULL
            UNION ALL
            SELECT CF_ID_REV
            FROM IFRS_ACCT_COST_FEE
            WHERE DOWNLOAD_DATE = V_CURRDATE
                AND FLAG_REVERSE = 'Y'
                AND CF_ID_REV IS NOT NULL
            )
       )
    WHEN MATCHED THEN
    UPDATE
    SET STATUS = CASE WHEN STATUS = 'ACT' THEN 'PNL' ELSE STATUS END
        ,CREATEDBY = 'EIRECF2';

    COMMIT;

    -- IF LAST COST FEE PREV IS PREVDATE
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T12';

    INSERT /*+ PARALLEL(12) */ INTO TMP_T12 (
        MASTERID
        ,DOWNLOAD_DATE
        ,SEQ
        ,PREVDATE
        )
    SELECT /*+ PARALLEL(12) */ B.MASTERID
        ,B.DOWNLOAD_DATE
        ,B.SEQ
        ,V_PREVDATE PREVDATE
    FROM VW_LBM_LAST_EIR_CF_PREV B
    WHERE B.MASTERID IN (
            SELECT MASTERID
            FROM IFRS_LBM_ACCT_EIR_CF_ECF
            WHERE TOTAL_AMT = 0
                OR TOTAL_AMT_ACRU = 0
            );
    COMMIT;



    MERGE INTO IFRS_LBM_ACCT_EIR_CF_PREV A
    USING TMP_T12 B
    ON (A.DOWNLOAD_DATE = B.PREVDATE
        AND A.MASTERID = B.MASTERID
        AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
        AND A.SEQ = B.SEQ
        --20180116 EXCLUDE CF REV AND ITS PAIR, WILL BE HANDLED BY SP ACF ABN
        AND A.CF_ID NOT IN (
            SELECT CF_ID
            FROM IFRS_ACCT_COST_FEE
            WHERE DOWNLOAD_DATE = V_CURRDATE
                AND FLAG_REVERSE = 'Y'
                AND CF_ID_REV IS NOT NULL
            UNION ALL
            SELECT CF_ID_REV
            FROM IFRS_ACCT_COST_FEE
            WHERE DOWNLOAD_DATE = V_CURRDATE
                AND FLAG_REVERSE = 'Y'
                AND CF_ID_REV IS NOT NULL
            )
        --20180426 EXCLUDE PREVDATE IF NOT FROM SP ACF ACCRU
        AND CASE WHEN A.DOWNLOAD_DATE = V_PREVDATE AND A.SEQ <> '2' THEN 0 ELSE 1 END = 1
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.STATUS = CASE WHEN STATUS = 'ACT' THEN 'PNL2' ELSE STATUS END
        ,CREATEDBY = 'EIRECF2';

    COMMIT;

    IF V_PARAM_DISABLE_ACCRU_PREV != 0
    THEN
        -- INSERT ACCRU PREV ONLY FOR PNL ED
        -- GET LAST ACF WITH DO_AMORT=N
        EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_P1';

        INSERT INTO TMP_P1 (ID)
        SELECT MAX(ID) AS ID
        FROM IFRS_LBM_ACCT_EIR_ACF
        WHERE MASTERID IN (
                SELECT MASTERID
                FROM TMP_T3
                )
            AND DO_AMORT = 'N'
            AND DOWNLOAD_DATE < V_CURRDATE
            AND DOWNLOAD_DATE >= V_PREVDATE
            -- ADD FILTER PNL ED ACCTNO
            AND MASTERID IN (
                SELECT MASTERID
                FROM IFRS_LBM_ACCT_EIR_CF_ECF
                WHERE TOTAL_AMT = 0
                    OR TOTAL_AMT_ACRU = 0
                )
        GROUP BY MASTERID;

        COMMIT;

        -- GET FEE SUMMARY
        EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_TF';

        INSERT /*+ PARALLEL(12) */ INTO TMP_TF (
            SUM_AMT
            ,DOWNLOAD_DATE
            ,MASTERID
            )
        SELECT /*+ PARALLEL(12) */ SUM(A.N_AMOUNT) AS SUM_AMT
            ,A.DOWNLOAD_DATE
            ,A.MASTERID
        FROM (
            SELECT CASE
                    WHEN A.FLAG_REVERSE = 'Y'
                        THEN - 1 * A.AMOUNT
                    ELSE A.AMOUNT
                    END AS N_AMOUNT
                ,A.ECFDATE DOWNLOAD_DATE
                ,A.MASTERID
            FROM IFRS_LBM_ACCT_EIR_COST_FEE_ECF A
            WHERE A.MASTERID IN (
                    SELECT MASTERID
                    FROM TMP_T3
                    )
                AND A.STATUS = 'ACT'
                AND A.FLAG_CF = 'F'
                AND A.METHOD = 'EIR'
            ) A
        GROUP BY A.DOWNLOAD_DATE
            ,A.MASTERID;

        COMMIT;

        -- GET COST SUMMARY
        EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_TC';

        INSERT /*+ PARALLEL(12) */ INTO TMP_TC (
            SUM_AMT
            ,DOWNLOAD_DATE
            ,MASTERID
            )
        SELECT /*+ PARALLEL(12) */ SUM(A.N_AMOUNT) AS SUM_AMT
            ,A.DOWNLOAD_DATE
            ,A.MASTERID
        FROM (
            SELECT CASE
                    WHEN A.FLAG_REVERSE = 'Y'
                        THEN - 1 * A.AMOUNT
                    ELSE A.AMOUNT
                    END AS N_AMOUNT
                ,A.ECFDATE DOWNLOAD_DATE
                ,A.MASTERID
            FROM IFRS_LBM_ACCT_EIR_COST_FEE_ECF A
            WHERE A.MASTERID IN (
                    SELECT MASTERID
                    FROM TMP_T3
                    )
                AND A.STATUS = 'ACT'
                AND A.FLAG_CF = 'C'
                AND A.METHOD = 'EIR'
            ) A
        GROUP BY A.DOWNLOAD_DATE
            ,A.MASTERID;

        COMMIT;

        --INSERT FEE 1
        INSERT /*+ PARALLEL(12) */ INTO IFRS_LBM_ACCT_EIR_ACCRU_PREV (
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
            )
        SELECT /*+ PARALLEL(12) */ A.FACNO
            ,A.CIFNO
            ,V_CURRDATE
            ,A.ECFDATE
            ,A.DATASOURCE
            ,B.PRDCODE
            ,B.TRXCODE
            ,B.CCY
            ,ROUND(CAST(CAST(CASE
                            WHEN B.FLAG_REVERSE = 'Y'
                                THEN - 1 * B.AMOUNT
                            ELSE B.AMOUNT
                            END AS BINARY_DOUBLE) / NVL(CAST(C.SUM_AMT AS BINARY_DOUBLE), 0) AS NUMBER(32, 20)) * A.N_ACCRU_FEE, V_ROUND)  AS N_AMOUNT
            ,B.STATUS
            ,SYSTIMESTAMP
            ,A.ACCTNO
            ,A.MASTERID
            ,B.FLAG_CF
            ,'N'
            ,NULL AS AMORTDATE
            ,'ECF'
            ,B.ORG_CCY
            ,B.ORG_CCY_EXRATE
            ,B.PRDTYPE
            ,B.CF_ID
            ,B.METHOD
        FROM IFRS_LBM_ACCT_EIR_ACF A
        JOIN IFRS_LBM_ACCT_EIR_COST_FEE_ECF B ON B.ECFDATE = A.ECFDATE
            AND A.MASTERID = B.MASTERID
            AND B.FLAG_CF = 'F' AND B.STATUS = 'ACT'
        JOIN TMP_TF C ON C.DOWNLOAD_DATE = A.ECFDATE
            AND C.MASTERID = A.MASTERID
        WHERE A.ID IN (
                SELECT ID
                FROM TMP_P1
                )
            --20180108 EXCLUDE CF REV AND ITS PAIR
            AND B.CF_ID NOT IN (SELECT CF_ID FROM IFRS_ACCT_COST_FEE WHERE DOWNLOAD_DATE = V_CURRDATE AND FLAG_REVERSE = 'Y' AND CF_ID_REV IS NOT NULL
                                UNION ALL
                                SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE WHERE DOWNLOAD_DATE = V_CURRDATE AND FLAG_REVERSE = 'Y' AND CF_ID_REV IS NOT NULL
                );

        COMMIT;

        --COST 1
        INSERT /*+ PARALLEL(12) */ INTO IFRS_LBM_ACCT_EIR_ACCRU_PREV (
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
            )
        SELECT /*+ PARALLEL(12) */ A.FACNO
            ,A.CIFNO
            ,V_CURRDATE
            ,A.ECFDATE
            ,A.DATASOURCE
            ,B.PRDCODE
            ,B.TRXCODE
            ,B.CCY
            ,ROUND(CAST(CAST(CASE WHEN B.FLAG_REVERSE = 'Y' THEN - 1 * B.AMOUNT ELSE B.AMOUNT
                            END AS BINARY_DOUBLE) / NVL(CAST(C.SUM_AMT AS BINARY_DOUBLE), 0) AS NUMBER(32, 20)) * A.N_ACCRU_COST, V_ROUND) AS N_AMOUNT
            ,B.STATUS
            ,SYSTIMESTAMP
            ,A.ACCTNO
            ,A.MASTERID
            ,B.FLAG_CF
            ,'N'
            ,NULL AS AMORTDATE
            ,'ECF'
            ,B.ORG_CCY
            ,B.ORG_CCY_EXRATE
            ,B.PRDTYPE
            ,B.CF_ID
            ,B.METHOD
        FROM IFRS_LBM_ACCT_EIR_ACF A
        JOIN IFRS_LBM_ACCT_EIR_COST_FEE_ECF B ON B.ECFDATE = A.ECFDATE
            AND A.MASTERID = B.MASTERID
            AND B.FLAG_CF = 'C' AND B.STATUS = 'ACT'
        JOIN TMP_TC C ON C.DOWNLOAD_DATE = A.ECFDATE
            AND C.MASTERID = A.MASTERID
        WHERE A.ID IN (
                SELECT ID
                FROM TMP_P1
                )
            --20180108 EXCLUDE CF REV AND ITS PAIR
            AND B.CF_ID NOT IN (SELECT CF_ID FROM IFRS_ACCT_COST_FEE WHERE DOWNLOAD_DATE = V_CURRDATE AND FLAG_REVERSE = 'Y' AND CF_ID_REV IS NOT NULL
                                UNION ALL
                                SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE WHERE DOWNLOAD_DATE = V_CURRDATE AND FLAG_REVERSE = 'Y' AND CF_ID_REV IS NOT NULL
                );

    END IF; --IF @PARAM_DISABLE_ACCRU_PREV != 0

    COMMIT;


    -- AMORT ACRU
    UPDATE /*+ PARALLEL(12) */ IFRS_LBM_ACCT_EIR_ACCRU_PREV
    SET STATUS = TO_CHAR (V_CURRDATE, 'YYYYMMDD')
    WHERE STATUS = 'ACT'
        AND MASTERID IN (SELECT MASTERID FROM IFRS_LBM_ACCT_EIR_CF_ECF
                         WHERE TOTAL_AMT = 0 OR TOTAL_AMT_ACRU = 0);

    COMMIT;

    -- STOP OLD ECF
    UPDATE /*+ PARALLEL(12) */ IFRS_LBM_ACCT_EIR_ECF
    SET AMORTSTOPDATE = V_CURRDATE
        ,AMORTSTOPMSG = 'SP_ACCT_EIR_ECF'
    WHERE MASTERID IN (SELECT MASTERID FROM IFRS_LBM_ACCT_EIR_CF_ECF)
        AND AMORTSTOPDATE IS NULL;


    COMMIT;

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_ECF_MAIN','2');

    COMMIT;

    -- INSERT FROM IFRS_LBM_PAYM_SCHD FILTERED
    /* REMARKS DAHULU KARENA SUDAH DI INSERT DI SP_IFRS_LBM_PAYMENT_SCHEDULE 20160524
    TRUNCATE TABLE IFRS_LBM_PAYM_CORE_SRC
    TRUNCATE TABLE TMP_T1
    INSERT  INTO TMP_T1
            ( MASTERID ,
              ICC ,
              INT_RATE
            )
            SELECT  MASTERID ,
                    INTEREST_CALCULATION_CODE ,
                    INTEREST_RATE
            FROM    IFRS_IMA_AMORT_CURR
            WHERE   EIRECF = 'Y'


    INSERT  INTO IFRS_LBM_PAYM_CORE_SRC
            ( MASTERID ,
              ACCTNO ,
              PMT_DATE ,
              INTEREST_RATE ,
              PRN_AMT ,
              INT_AMT ,
              DISB_PERCENTAGE ,
              DISB_AMOUNT ,
              PLAFOND ,
              ICC ,
              GRACE_DATE
            )
            SELECT  A.MASTERID ,
                    A.MASTERID ,
                    A.PMTDATE ,
                    A.INTEREST_RATE ,
                    A.PRINCIPAL ,
                    A.INTEREST ,
                    A.DISB_PERCENTAGE ,
                    A.DISB_AMOUNT ,
                    A.PLAFOND ,
                    B.ICC ,
                    A.GRACE_DATE
            FROM    PSAK_PAYM_SCHD A ,
                    TMP_T1   B
            WHERE   B.MASTERID = A.MASTERID
                    AND A.PMTDATE > @V_CURRDATE

-- CALC EFF RATE FROM TABLE IFRS_LBM_PAYM_CORE

    TRUNCATE TABLE IFRS_LBM_GS_MASTERID
    TRUNCATE TABLE IFRS_LBM_ACCT_EIR_PAYM

-- END GET LAST OR START DATE FOR ASSIGN FIRST PAYM DATE  --RIDWAN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_PSAK_LAST_PAYM_DATE';

    INSERT  INTO TMP_PSAK_LAST_PAYM_DATE
            ( MASTERID ,
              CURRDATE ,
              LAST_PAYMENT_DATE_SCHD ,
              LOAN_START_DATE
            )
            SELECT  A.MASTERID ,
                    V_CURRDATE ,
                    B.PMTDATE ,
                    A.LOAN_START_DATE
            FROM    IFRS_IMA_AMORT_CURR A
                    LEFT JOIN ( SELECT  MAX(PMTDATE) AS PMTDATE ,
                                        MASTERID
                                FROM    PSAK_PAYM_SCHD
                                WHERE   PMTDATE <= V_CURRDATE
                       GROUP BY MASTERID
                              ) B ON A.MASTERID = B.MASTERID
            WHERE   A.EIRECF = 'Y'
                    AND A.FLAG_AL IN ( 'A' );

    COMMIT;


    UPDATE  TMP_PSAK_LAST_PAYM_DATE
    SET     LAST_PAYMENT_DATE_ASSIGN = CASE WHEN LAST_PAYMENT_DATE_SCHD IS NOT NULL
                                            THEN LAST_PAYMENT_DATE_SCHD
                                            ELSE LOAN_START_DATE
                                       END;
    COMMIT;
-- END GET LAST OR START DATE FOR ASSIGN FIRST PAYM DATE  --RIDWAN

    INSERT  INTO IFRS_LBM_PAYM_CORE_SRC
            ( MASTERID ,
              ACCTNO ,
              PREV_PMT_DATE ,
              PMT_DATE ,
              INTEREST_RATE ,
              PRN_AMT ,
              INT_AMT ,
              ICC ,
              GRACE_DATE
            )
            SELECT DISTINCT
                    A.MASTERID ,
                    A.ACCTNO ,
                    V_CURRDATE ,
                    V_CURRDATE ,
                    B.INT_RATE ,
                    0 ,
                    0 ,
                    A.ICC ,
                    A.GRACE_DATE
            FROM    IFRS_LBM_PAYM_CORE_SRC A ,
                 TMP_T1   B
            WHERE   B.MASTERID = A.MASTERID;
    COMMIT;
--UPDATE DISB AMOUNT 20160428
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T2' ;

    INSERT  INTO TMP_T2( MASTERID ,DOWNLOAD_DATE)
    SELECT  A.MASTERID ,
            MAX(A.PMTDATE) DOWNLOAD_DATE
    FROM    PSAK_PAYM_SCHD A ,TMP_T1   B
    WHERE   A.MASTERID = B.MASTERID
    AND A.PMTDATE <= V_CURRDATE
    GROUP BY A.MASTERID;

    COMMIT;

    MERGE INTO IFRS_LBM_PAYM_CORE_SRC A
    USING ( SELECT A.MASTERID MASTERID ,
                   A.DISB_PERCENTAGE ,
                   A.DISB_AMOUNT ,
                   A.PLAFOND
            FROM PSAK_PAYM_SCHD A ,TMP_T2 B
            WHERE A.MASTERID = B.MASTERID
            AND A.PMTDATE = B.DOWNLOAD_DATE
          ) B
        ON ( A.MASTERID = B.MASTERID
             AND A.PMT_DATE = A.PREV_PMT_DATE
           )
        WHEN MATCHED THEN
        UPDATE
        SET
        A.DISB_PERCENTAGE = B.DISB_PERCENTAGE ,
        A.DISB_AMOUNT = B.DISB_AMOUNT ,
        A.PLAFOND = B.PLAFOND ;

        COMMIT;

      --UPDATE DISB AMOUNT 20160428
      REMARKS DAHULU KARENA SUDAH DI INSERT DI SP_IFRS_LBM_PAYMENT_SCHEDULE 20160524 */
          /*REMARKS DULU 20160524
      -- GENERATE SCHEDULE FOR FUNDING PRODUCT
          EXEC SP_PSAK_FUNDING_PAYM_SCHD
      REMARKS DULU 20160524 */


    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T1';

    INSERT /*+ PARALLEL(12) */ INTO TMP_T1 (MASTERID)
    SELECT /*+ PARALLEL(12) */ MASTERID
    FROM IFRS_LBM_PAYM_CORE_SRC
    WHERE PREV_PMT_DATE = PMT_DATE
        AND MASTERID IN (
            SELECT B.MASTERID
            FROM IFRS_LBM_ACCT_EIR_CF_ECF B
            WHERE (
                    (B.TOTAL_AMT <> 0 AND B.TOTAL_AMT_ACRU <> 0)
                    OR B.STAFFLOAN = 1
                    --20170927, ANYINK
                    OR (B.MASTERID IN (SELECT DISTINCT MASTERID FROM IFRS_LBM_EVENT_CHANGES WHERE DOWNLOAD_DATE = V_CURRDATE AND EVENT_ID = 4)
                        )
                    )
            );

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_LBM_GS_MASTERID';

    INSERT /*+ PARALLEL(12) */ INTO IFRS_LBM_GS_MASTERID (MASTERID)
    SELECT /*+ PARALLEL(12) */ A.MASTERID
    FROM TMP_T1 A;


    COMMIT;


    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_LBM_ACCT_EIR_PAYM';

    SELECT MIN(ID)
        , MAX(ID) INTO V_VMIN_ID, V_VMAX_ID
    FROM IFRS_LBM_GS_MASTERID;

    COMMIT;

    V_VX := V_VMIN_ID;
    V_VX_INC := 500000;

    WHILE V_VX <= V_VMAX_ID
    LOOP --LOOP
        INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
        VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_PAYM_CORE_PROCESS',TO_CHAR(V_VX));

        EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_LBM_PAYM_CORE';

        INSERT /*+ PARALLEL(12) */ INTO IFRS_LBM_PAYM_CORE (
            MASTERID
            ,ACCTNO
            ,PREV_PMT_DATE
            ,PMT_DATE
            ,INT_RATE
            ,I_DAYS
            ,COUNTER
            ,OS_PRN_PREV
            ,PRN_AMT
            ,INT_AMT
            ,OS_PRN
            ,DISB_PERCENTAGE
            ,DISB_AMOUNT
            ,PLAFOND
            ,ICC
            ,GRACE_DATE
            )
        /*  BCA DISABLE BPI ,SPECIAL_FLAG --- BPI FLAG ONLY CTBC */
        SELECT /*+ PARALLEL(12) */ MASTERID
            ,ACCTNO
            ,PREV_PMT_DATE
            ,PMT_DATE
            ,INTEREST_RATE
            ,I_DAYS
            ,COUNTER
            ,OS_PRN_PREV
            ,PRN_AMT
            ,INT_AMT
            ,OS_PRN
            ,DISB_PERCENTAGE
            ,DISB_AMOUNT
            ,PLAFOND
            ,ICC
            ,GRACE_DATE
        /*  BCA DISABLE BPI ,SPECIAL_FLAG --- BPI FLAG ONLY CTBC */
        FROM IFRS_LBM_PAYM_CORE_SRC
        WHERE MASTERID IN (SELECT MASTERID FROM IFRS_LBM_GS_MASTERID WHERE ID >= V_VX AND ID < (V_VX + V_VX_INC));

        SP_IFRS_EXEC_AND_LOG_PROCESS ('SP_IFRS_LBM_PAYM_CORE_PROC_NOP'); -- TANPA EFEKTIFISASI
            --EXEC SP_IFRS_LBM_PAYM_CORE_PROCESS;    -- DENGAN EFEKTIFISASI

        V_VX := V_VX + V_VX_INC;
    END LOOP; --LOOP;

    COMMIT;

    -- INSERT PAYMENT SCHEDULE
    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_ECF_MAIN','3');

    -- UPDATE NPV RATE FOR STAFF LOAN
    MERGE INTO IFRS_LBM_ACCT_EIR_PAYM A
    USING IFRS_LBM_ACCT_EIR_CF_ECF B
    ON (B.STAFFLOAN = 1
        AND A.MASTERID = B.MASTERID
        AND COALESCE(B.NPV_RATE, 0) > 0
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.NPV_RATE = B.NPV_RATE;

    COMMIT;

    -- UPDATE NPV_INSTALLMENT FOR STAFF LOAN
    UPDATE /*+ PARALLEL(12) */ IFRS_LBM_ACCT_EIR_PAYM
    SET NPV_INSTALLMENT = CASE
            WHEN ROUND(FN_CNT_DAYS_30_360(STARTAMORTDATE, PMT_DATE) / 30, 0) = 0
                THEN N_INSTALLMENT / (POWER(1 + NVL(NPV_RATE, 0) / 360 / 100, FN_CNT_DAYS_30_360(STARTAMORTDATE, PMT_DATE)))
            ELSE N_INSTALLMENT / NULLIF((POWER(1 + NVL(NPV_RATE, 0) / 12 / 100, ROUND(FN_CNT_DAYS_30_360(STARTAMORTDATE, PMT_DATE) / 30, 0))), 0)
            END
    WHERE NPV_RATE > 0;

    COMMIT;

    -- CALC STAFF LOAN BENEFIT
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_B1';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_B2';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_B3';

    COMMIT;

    -- GET OS
    INSERT /*+ PARALLEL(12) */ INTO TMP_B1 (
        MASTERID
        ,N_OSPRN
        )
    SELECT /*+ PARALLEL(12) */ MASTERID
        ,N_OSPRN
    FROM IFRS_LBM_ACCT_EIR_PAYM
    WHERE DOWNLOAD_DATE = V_CURRDATE
        AND PREV_PMT_DATE = PMT_DATE
        AND NPV_RATE > 0;

    COMMIT;

    --GET NPV SUM
    INSERT /*+ PARALLEL(12) */ INTO TMP_B2 (
        MASTERID
        ,NPV_SUM
        )
    SELECT /*+ PARALLEL(12) */ MASTERID
        ,SUM(COALESCE(NPV_INSTALLMENT, 0)) AS NPV_SUM
    FROM IFRS_LBM_ACCT_EIR_PAYM
    WHERE DOWNLOAD_DATE = V_CURRDATE
        AND NPV_RATE > 0
    GROUP BY MASTERID;

    COMMIT;

    -- GET BENEFIT
    INSERT /*+ PARALLEL(12) */ INTO TMP_B3 (
        MASTERID
        ,N_OSPRN
        ,NPV_SUM
        ,BENEFIT
        )
    SELECT /*+ PARALLEL(12) */ A.MASTERID
        ,A.N_OSPRN
        ,B.NPV_SUM
        ,B.NPV_SUM - A.N_OSPRN AS BENEFIT
    FROM TMP_B1 A
    JOIN TMP_B2 B ON B.MASTERID = A.MASTERID;

    COMMIT;

    -- UPDATE BACK
    MERGE INTO IFRS_LBM_ACCT_EIR_CF_ECF A
    USING TMP_B3 B
    ON (A.MASTERID = B.MASTERID
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.BENEFIT = B.BENEFIT;

    COMMIT;

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_ECF_MAIN','3A');

    /*
    -- INSERT TODAY COST FEE
    INSERT INTO IFRS_LBM_ACCT_EIR_COST_FEE_ECF (
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
    SELECT C.DOWNLOAD_DATE
        ,V_CURRDATE ECFDATE
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
        ,SYSTIMESTAMP CREATEDDATE
        ,'EIR_ECF_MAIN' CREATEDBY
        ,'' SEQ
        ,C.AMOUNT
        ,C.ORG_CCY
        ,C.ORG_CCY_EXRATE
        ,C.PRD_TYPE
        ,C.CF_ID
    FROM IFRS_ACCT_COST_FEE C
    JOIN IFRS_LBM_ACCT_EIR_CF_ECF B ON B.MASTERID = C.MASTERID
        AND B.TOTAL_AMT <> 0
        AND B.TOTAL_AMT_ACRU <> 0
    WHERE C.DOWNLOAD_DATE = V_CURRDATE
        AND C.MASTERID = B.MASTERID
        AND C.STATUS = 'ACT'
        AND C.METHOD = 'EIR'
        --20180116 EXCLUDE CF REV AND ITS PAIR
        AND C.CF_ID NOT IN (SELECT CF_ID FROM IFRS_ACCT_COST_FEE WHERE DOWNLOAD_DATE = V_CURRDATE AND FLAG_REVERSE = 'Y' AND CF_ID_REV IS NOT NULL
                            UNION ALL
                            SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE WHERE DOWNLOAD_DATE = V_CURRDATE AND FLAG_REVERSE = 'Y' AND CF_ID_REV IS NOT NULL
            );
    */

    --INSERT UNAMORT
    INSERT /*+ PARALLEL(12) */ INTO IFRS_LBM_ACCT_EIR_COST_FEE_ECF (
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
    SELECT /*+ PARALLEL(12) */ C.DOWNLOAD_DATE
        ,V_CURRDATE ECFDATE
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
        ,C.AMOUNT
        ,SYSTIMESTAMP CREATEDDATE
        ,'EIR_ECF_MAIN' CREATEDBY
        ,'' SEQ
        ,C.AMOUNT_ORG
        ,C.ORG_CCY
        ,C.ORG_CCY_EXRATE
        ,C.PRDTYPE
        ,C.CF_ID
    FROM IFRS_LBM_ACCT_EIR_CF_PREV C
    JOIN VW_LBM_LAST_EIR_CF_PREV X ON X.MASTERID = C.MASTERID
        AND X.DOWNLOAD_DATE = C.DOWNLOAD_DATE
        AND C.SEQ = X.SEQ
    JOIN IFRS_LBM_ACCT_EIR_CF_ECF B ON B.MASTERID = C.MASTERID
        AND B.TOTAL_AMT <> 0
        AND B.TOTAL_AMT_ACRU <> 0
    --20160407 EIR STOP REV
    LEFT JOIN (
        SELECT DISTINCT MASTERID
        FROM IFRS_LBM_ACCT_EIR_STOP_REV
        WHERE DOWNLOAD_DATE = V_CURRDATE
        ) A ON A.MASTERID = C.MASTERID
    WHERE C.DOWNLOAD_DATE IN (
            V_CURRDATE
            ,V_PREVDATE
            )
        AND C.STATUS = 'ACT'
	AND C.TRXCODE = 'BENEFIT'
        --20160407 EIR STOP REV
        AND A.MASTERID IS NULL
        --20180116 EXCLUDE CF REV AND ITS PAIR
        AND C.CF_ID NOT IN (SELECT CF_ID FROM IFRS_ACCT_COST_FEE WHERE DOWNLOAD_DATE = V_CURRDATE AND FLAG_REVERSE = 'Y' AND CF_ID_REV IS NOT NULL
                            UNION ALL
                            SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE WHERE DOWNLOAD_DATE = V_CURRDATE AND FLAG_REVERSE = 'Y' AND CF_ID_REV IS NOT NULL
            )
        --20180426 EXCLUDE PREVDATE IF NOT FROM SP ACF ACCRU
        AND CASE  WHEN C.DOWNLOAD_DATE = V_PREVDATE AND C.SEQ <> '2' THEN 0 ELSE 1 END = 1;
    COMMIT;


    IF V_PARAM_DISABLE_ACCRU_PREV != 0
    THEN
        --MASUKKAN KEMBALI ACCRU PREVDATE KE COST_FEE_ECF
        -- NO ACCRU IF TODAY IS DOING AMORT
        EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T1';

        INSERT /*+ PARALLEL(12) */ INTO TMP_T1 (MASTERID)
        SELECT /*+ PARALLEL(12) */ DISTINCT MASTERID
        FROM IFRS_LBM_ACCT_EIR_ACF
        WHERE DOWNLOAD_DATE = V_CURRDATE
            AND DO_AMORT = 'Y';

        EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T3';

        INSERT /*+ PARALLEL(12) */ INTO TMP_T3 (MASTERID)
        SELECT /*+ PARALLEL(12) */ MASTERID
        FROM IFRS_LBM_ACCT_EIR_CF_ECF
        WHERE MASTERID NOT IN (
                SELECT MASTERID
                FROM TMP_T1
                );

        -- GET LAST ACF WITH DO_AMORT=N
        EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_P1';

        INSERT /*+ PARALLEL(12) */ INTO TMP_P1 (ID)
        SELECT /*+ PARALLEL(12) */ MAX(ID) AS ID
        FROM IFRS_LBM_ACCT_EIR_ACF
        WHERE MASTERID IN (
                SELECT MASTERID
                FROM TMP_T3
                )
            AND DO_AMORT = 'N'
            AND DOWNLOAD_DATE < V_CURRDATE
            AND DOWNLOAD_DATE >= V_PREVDATE
        GROUP BY MASTERID;

        -- GET FEE SUMMARY
        EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_TF';

        INSERT /*+ PARALLEL(12) */ INTO TMP_TF (
            SUM_AMT
            ,DOWNLOAD_DATE
            ,MASTERID
            )
        SELECT /*+ PARALLEL(12) */ SUM(A.N_AMOUNT) AS SUM_AMT
            ,A.DOWNLOAD_DATE
            ,A.MASTERID
        FROM (
            SELECT CASE
                    WHEN A.FLAG_REVERSE = 'Y'
                        THEN - 1 * A.AMOUNT
                    ELSE A.AMOUNT
                    END AS N_AMOUNT
                ,A.ECFDATE DOWNLOAD_DATE
                ,A.MASTERID
            FROM IFRS_LBM_ACCT_EIR_COST_FEE_ECF A
            WHERE A.MASTERID IN (
                    SELECT MASTERID
                    FROM TMP_T3
                    )
                AND A.STATUS = 'ACT'
                AND A.FLAG_CF = 'F'
                AND A.METHOD = 'EIR'
            ) A
        GROUP BY A.DOWNLOAD_DATE
            ,A.MASTERID;

        -- GET COST SUMMARY
        EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_TC';

        INSERT /*+ PARALLEL(12) */ INTO TMP_TC (
            SUM_AMT
            ,DOWNLOAD_DATE
            ,MASTERID
            )
        SELECT /*+ PARALLEL(12) */ SUM(A.N_AMOUNT) AS SUM_AMT
            ,A.DOWNLOAD_DATE
            ,A.MASTERID
        FROM (
            SELECT CASE
                    WHEN A.FLAG_REVERSE = 'Y'
                        THEN - 1 * A.AMOUNT
                    ELSE A.AMOUNT
                    END AS N_AMOUNT
                ,A.ECFDATE DOWNLOAD_DATE
                ,A.MASTERID
            FROM IFRS_LBM_ACCT_EIR_COST_FEE_ECF A
            WHERE A.MASTERID IN (
                    SELECT MASTERID
                    FROM TMP_T3
                    )
                AND A.STATUS = 'ACT'
                AND A.FLAG_CF = 'C'
                AND A.METHOD = 'EIR'
            ) A
        GROUP BY A.DOWNLOAD_DATE
            ,A.MASTERID;

        INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
        VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_ECF_MAIN','3B');

        --INSERT FEE 1
        INSERT /*+ PARALLEL(12) */ INTO IFRS_LBM_ACCT_EIR_COST_FEE_ECF (
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
            )
        SELECT /*+ PARALLEL(12) */ A.FACNO
            ,A.CIFNO
            ,V_CURRDATE
            ,V_CURRDATE ECFDATE
            ,A.DATASOURCE
            ,B.PRDCODE
            ,B.TRXCODE
            ,B.CCY
            ,ROUND(CAST(CAST(CASE WHEN B.FLAG_REVERSE = 'Y'THEN - 1 * B.AMOUNT ELSE B.AMOUNT
                            END AS BINARY_DOUBLE) / NVL(CAST(C.SUM_AMT AS BINARY_DOUBLE), 0) AS NUMBER(32, 20)) * A.N_ACCRU_FEE * - 1, V_ROUND) AS N_AMOUNT
            ,B.STATUS
            ,SYSTIMESTAMP
            ,A.ACCTNO
            ,A.MASTERID
            ,B.FLAG_CF
            ,'N'
            ,'ECFACCRU'
            ,B.ORG_CCY
            ,B.ORG_CCY_EXRATE
            ,B.PRDTYPE
            ,B.CF_ID
            ,B.BRCODE
            ,B.METHOD
        FROM IFRS_LBM_ACCT_EIR_ACF A
        JOIN IFRS_LBM_ACCT_EIR_COST_FEE_ECF B ON B.ECFDATE = A.ECFDATE
            AND A.MASTERID = B.MASTERID
            AND B.FLAG_CF = 'F'
	    AND B.STATUS = 'ACT' --TAMBAHIN ISSUE STAFFLOAN 20180326
	    AND A.MASTERID NOT IN
		(
		SELECT DISTINCT MASTERID
		FROM IFRS_ACCT_SWITCH
		WHERE DOWNLOAD_DATE = V_CURRDATE
		)
        JOIN TMP_TF C ON C.DOWNLOAD_DATE = A.ECFDATE
            AND C.MASTERID = A.MASTERID
        --20160407 EIR STOP REV
        LEFT JOIN (
            SELECT DISTINCT MASTERID
            FROM IFRS_LBM_ACCT_EIR_STOP_REV
            WHERE DOWNLOAD_DATE = V_CURRDATE
            ) D ON A.MASTERID = D.MASTERID
        WHERE A.ID IN (
                SELECT ID
                FROM TMP_P1
                )
            --20160407 EIR STOP REV
            AND D.MASTERID IS NULL
            --20180116 EXCLUDE CF REV AND ITS PAIR
            AND B.CF_ID NOT IN (SELECT CF_ID FROM IFRS_ACCT_COST_FEE WHERE DOWNLOAD_DATE = V_CURRDATE AND FLAG_REVERSE = 'Y' AND CF_ID_REV IS NOT NULL
                                UNION ALL
                                SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE WHERE DOWNLOAD_DATE = V_CURRDATE AND FLAG_REVERSE = 'Y' AND CF_ID_REV IS NOT NULL
                                );

        INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
        VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_ECF_MAIN','3C');

        --COST 1
        INSERT /*+ PARALLEL(12) */ INTO IFRS_LBM_ACCT_EIR_COST_FEE_ECF (
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
            )
        SELECT /*+ PARALLEL(12) */ A.FACNO
            ,A.CIFNO
            ,V_CURRDATE
            ,V_CURRDATE ECFDATE
            ,A.DATASOURCE
            ,B.PRDCODE
            ,B.TRXCODE
            ,B.CCY
            ,ROUND(CAST(CAST(CASE
                            WHEN B.FLAG_REVERSE = 'Y'
                                THEN - 1 * B.AMOUNT
                            ELSE B.AMOUNT
                            END AS BINARY_DOUBLE) / NVL(CAST(C.SUM_AMT AS BINARY_DOUBLE), 0) AS NUMBER(32, 20)) * A.N_ACCRU_COST * - 1, V_ROUND)  AS N_AMOUNT
            ,B.STATUS
            ,SYSTIMESTAMP
            ,A.ACCTNO
            ,A.MASTERID
            ,B.FLAG_CF
            ,'N'
            ,'ECFACCRU'
            ,B.ORG_CCY
            ,B.ORG_CCY_EXRATE
            ,B.PRDTYPE
            ,B.CF_ID
            ,B.BRCODE
            ,B.METHOD
        FROM IFRS_LBM_ACCT_EIR_ACF A
        JOIN IFRS_LBM_ACCT_EIR_COST_FEE_ECF B ON B.ECFDATE = A.ECFDATE
            AND A.MASTERID = B.MASTERID
            AND B.FLAG_CF = 'C'
	    AND B.STATUS = 'ACT' --TAMBAHIN ISSUE STAFFLOAN 20180326
	    AND A.MASTERID NOT IN
	    (
		SELECT DISTINCT MASTERID
		FROM IFRS_ACCT_SWITCH
		WHERE DOWNLOAD_DATE = V_CURRDATE
	    )
        JOIN TMP_TC C ON C.DOWNLOAD_DATE = A.ECFDATE
            AND C.MASTERID = A.MASTERID
        --20160407 EIR STOP REV
        LEFT JOIN (
            SELECT DISTINCT MASTERID
            FROM IFRS_LBM_ACCT_EIR_STOP_REV
            WHERE DOWNLOAD_DATE = V_CURRDATE
            ) D ON A.MASTERID = D.MASTERID
        WHERE A.ID IN (
                SELECT ID
                FROM TMP_P1
                )
            --20160407 EIR STOP REV
            AND D.MASTERID IS NULL
            --20180108 EXCLUDE CF REV AND ITS PAIR
            AND B.CF_ID NOT IN (SELECT CF_ID FROM IFRS_ACCT_COST_FEE WHERE DOWNLOAD_DATE = V_CURRDATE AND FLAG_REVERSE = 'Y' AND CF_ID_REV IS NOT NULL
                                UNION ALL
                                SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE WHERE DOWNLOAD_DATE = V_CURRDATE AND FLAG_REVERSE = 'Y' AND CF_ID_REV IS NOT NULL
                                );
    END IF; --MASUKKAN KEMBALI ACCRU PREVDATE KE COST_FEE_ECF

    COMMIT;


    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_ECF_MAIN','3D');

    COMMIT;

     -- 20160412 GROUP MULTIPLE ROWS BY CF_ID
     --UPDATE COST FEE ECF DARI SWITCH APABILA TERDAPAT EVENT 20180824
     EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T1';
     EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T2';

     INSERT /*+ PARALLEL(12) */ INTO TMP_T1(MASTERID)
     SELECT /*+ PARALLEL(12) */ DISTINCT MASTERID FROM IFRS_LBM_ACCT_EIR_COST_FEE_ECF WHERE ECFDATE = V_CURRDATE AND CREATEDBY = 'EIR_SWITCH'  ;COMMIT;

     INSERT /*+ PARALLEL(12) */ INTO TMP_T2(MASTERID)
     SELECT /*+ PARALLEL(12) */ DISTINCT MASTERID FROM IFRS_LBM_ACCT_EIR_COST_FEE_ECF WHERE ECFDATE = V_CURRDATE AND CREATEDBY != 'EIR_SWITCH' ; COMMIT;

     UPDATE /*+ PARALLEL(12) */ IFRS_LBM_ACCT_EIR_COST_FEE_ECF
     SET STATUS = TO_CHAR(V_CURRDATE,'YYYYMM')
     WHERE ECFDATE = V_CURRDATE
     AND MASTERID IN (SELECT MASTERID FROM TMP_T1)
     AND MASTERID IN (SELECT MASTERID FROM TMP_T2)
     AND CREATEDBY = 'EIR_SWITCH'
     AND STATUS = 'ACT'  ;COMMIT;
     --UPDATE COST FEE ECF DARI SWITCH APABILA TERDAPAT EVENT 20180824

    -- 20160412 GROUP MULTIPLE ROWS BY CF_ID
    SP_IFRS_EXEC_AND_LOG_PROCESS( 'SP_IFRS_LBM_ACCT_EIR_CFECF_GRP');

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_ECF_MAIN','4 GS START');

    COMMIT;

    DELETE /*+ PARALLEL(12) */
    FROM IFRS_LBM_ACCT_EIR_FAILED_GS
    WHERE DOWNLOAD_DATE = V_CURRDATE;COMMIT;

    DELETE /*+ PARALLEL(12) */
    FROM IFRS_LBM_ACCT_EIR_GS_RESULT
    WHERE DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_LBM_ACCT_EIR_CF_ECF1';

    INSERT /*+ PARALLEL(12) */ INTO IFRS_LBM_ACCT_EIR_CF_ECF1 (
        MASTERID
        ,FEE_AMT
        ,COST_AMT
        ,BENEFIT
        ,STAFFLOAN
        ,PREV_EIR
        --20180226 COPY DATA
        ,TOTAL_AMT --20180517  ADD YACOP
        ,NEW_FEE_AMT
        ,NEW_COST_AMT
        ,NEW_TOTAL_AMT
        ,GAIN_LOSS_CALC
        )
    SELECT /*+ PARALLEL(12) */ B.MASTERID
        ,B.FEE_AMT
        ,B.COST_AMT
        ,B.BENEFIT
        ,B.STAFFLOAN
        ,B.PREV_EIR
        ,B.TOTAL_AMT --20180517  ADD YACOP
        ,NEW_FEE_AMT
        ,NEW_COST_AMT
        ,NEW_TOTAL_AMT
        ,GAIN_LOSS_CALC
    FROM IFRS_LBM_ACCT_EIR_CF_ECF B
    WHERE (B.TOTAL_AMT <> 0 AND B.TOTAL_AMT_ACRU <> 0)
        OR (B.STAFFLOAN = 1 --AND B.PREV_EIR IS NULL
	    )
        --20170927, IVAN NOCF
        OR (B.MASTERID IN (SELECT DISTINCT MASTERID FROM IFRS_LBM_EVENT_CHANGES WHERE DOWNLOAD_DATE = V_CURRDATE AND EVENT_ID = 4 )
        );

    COMMIT;

    --START: GOAL SEEK PREPARE STAFFLOAN BENEFIT
    -- PUT BEFORE REMARK -- GOAL SEEK PREPARE SP_IFRS_LBM_ACCT_EIR_ECF_MAIN
    -- RESULT BENEFIT=UNAMORT-GLOSS GET FROM TABLE IFRS_LBM_ACCT_EIR_GS_RESULT3
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_LBM_GS_MASTERID';

    --CLEAN UP
    DELETE /*+ PARALLEL(12) */
    FROM IFRS_LBM_ACCT_EIR_GS_RESULT3
    WHERE DOWNLOAD_DATE = V_CURRDATE;COMMIT;

    DELETE /*+ PARALLEL(12) */
    FROM IFRS_LBM_ACCT_EIR_FAILED_GS3
    WHERE DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    --ONLY PROCESS STAFFLOAN WITH NO RUNNING AMORTIZATION
    INSERT /*+ PARALLEL(12) */ INTO IFRS_LBM_GS_MASTERID (MASTERID)
    SELECT /*+ PARALLEL(12) */ A.MASTERID
    FROM (
        SELECT MASTERID
            ,PERIOD
        FROM IFRS_LBM_ACCT_EIR_PAYM
        WHERE PREV_PMT_DATE = PMT_DATE
            AND MASTERID IN (
                SELECT MASTERID
                FROM IFRS_LBM_ACCT_EIR_CF_ECF1
                WHERE (
                        STAFFLOAN = 1
                        --AND PREV_EIR IS NULL
                        )
                    OR GAIN_LOSS_CALC = 'Y' --20180226 PREPAYMENT
                )
        ) A
    ORDER BY PERIOD;

    COMMIT;

    SELECT MIN(ID) INTO V_VMIN_ID
    FROM IFRS_LBM_GS_MASTERID;

    SELECT MAX(ID) INTO V_VMAX_ID
    FROM IFRS_LBM_GS_MASTERID;

    V_VX := V_VMIN_ID;
    V_VX_INC := 500000;

    WHILE V_VX <= V_VMAX_ID
    LOOP --LOOP
        INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
        VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_GS_RANGE',TO_CHAR(V_VX));

        V_ID2 := V_VX + V_VX_INC - 1;

        SP_IFRS_LBM_ACCT_EIR_GS_RANGE( V_VX,V_ID2);

        INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
        VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_GS_RANGE','DONE');

        SP_IFRS_EXEC_AND_LOG_PROCESS ('SP_IFRS_LBM_ACCT_EIR_GS_PROC3');

        V_VX := V_VX + V_VX_INC;
    END LOOP; --LOOP;

    COMMIT;

    -- UPDATE BACK RESULT TO IFRS_LBM_ACCT_EIR_CF_ECF1
    MERGE INTO IFRS_LBM_ACCT_EIR_CF_ECF1 A
    USING IFRS_LBM_ACCT_EIR_GS_RESULT3 B
    ON ( B.MASTERID = A.MASTERID
            AND B.DOWNLOAD_DATE = V_CURRDATE
            --20180226 ONLY FOR STAFF LOAN
            AND A.STAFFLOAN = 1
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.BENEFIT = B.UNAMORT - B.GLOSS;

    COMMIT;

    MERGE INTO IFRS_LBM_ACCT_EIR_CF_ECF A
    USING IFRS_LBM_ACCT_EIR_GS_RESULT3 B
    ON (  B.MASTERID = A.MASTERID
            AND B.DOWNLOAD_DATE = V_CURRDATE
            --20180226 ONLY FOR STAFF LOAN
            AND A.STAFFLOAN = 1
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.BENEFIT = B.UNAMORT - B.GLOSS;

    COMMIT;

    --20180226 UPDATE FOR PARTIAL PAYMENT

    MERGE INTO IFRS_LBM_ACCT_EIR_CF_ECF1 A
    USING IFRS_LBM_ACCT_EIR_GS_RESULT3 B
    ON (A.MASTERID = B.MASTERID
            AND B.DOWNLOAD_DATE = V_CURRDATE
            AND A.STAFFLOAN = 0
            AND A.GAIN_LOSS_CALC = 'Y'
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.GAIN_LOSS_AMT = ROUND(B.GLOSS, V_ROUND)
        ,A.GAIN_LOSS_FEE_AMT = CASE WHEN FEE_AMT <> 0 AND COST_AMT = 0 THEN ROUND(B.GLOSS, V_ROUND)
                                  WHEN FEE_AMT = 0 AND COST_AMT <> 0 THEN 0
                                  ELSE ROUND(B.GLOSS * FEE_AMT / TOTAL_AMT, V_ROUND)
                                  END
        ,A.GAIN_LOSS_COST_AMT = CASE WHEN FEE_AMT = 0 AND COST_AMT <> 0 THEN ROUND(B.GLOSS, V_ROUND)
                                  WHEN FEE_AMT <> 0 AND COST_AMT = 0 THEN 0
                                  ELSE ROUND(B.GLOSS, V_ROUND) - ROUND(B.GLOSS * FEE_AMT / TOTAL_AMT, V_ROUND)
            END;

    COMMIT;

    MERGE INTO IFRS_LBM_ACCT_EIR_CF_ECF A
    USING IFRS_LBM_ACCT_EIR_GS_RESULT3 B
    ON ( B.MASTERID = A.MASTERID
            AND B.DOWNLOAD_DATE = V_CURRDATE
            AND A.STAFFLOAN = 0
            AND A.GAIN_LOSS_CALC = 'Y'
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.GAIN_LOSS_AMT = ROUND(B.GLOSS, V_ROUND)
        ,A.GAIN_LOSS_FEE_AMT = CASE
            WHEN FEE_AMT <> 0 AND COST_AMT = 0 THEN ROUND(B.GLOSS, V_ROUND)
            WHEN FEE_AMT = 0 AND COST_AMT <> 0 THEN 0
            ELSE ROUND(B.GLOSS * FEE_AMT / TOTAL_AMT, V_ROUND)
            END
        ,A.GAIN_LOSS_COST_AMT = CASE
            WHEN FEE_AMT = 0 AND COST_AMT <> 0 THEN ROUND(B.GLOSS, V_ROUND)
            WHEN FEE_AMT <> 0 AND COST_AMT = 0 THEN 0
            ELSE ROUND(B.GLOSS, V_ROUND) - ROUND(B.GLOSS * FEE_AMT / TOTAL_AMT, V_ROUND)
            END;
    COMMIT;

    --RIDWAN  20 AUG 2015  INSERT BENEFIT AFTER GET BENEFIT
    --INSERT BENEFIT
    -- GET OS
    -- CALC STAFF LOAN BENEFIT
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_B1';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_B2';

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_B3';

    COMMIT;

    INSERT /*+ PARALLEL(12) */ INTO TMP_B1 (
        MASTERID
        ,N_OSPRN
        )
    SELECT /*+ PARALLEL(12) */ MASTERID
        ,N_OSPRN
    FROM IFRS_LBM_ACCT_EIR_PAYM
    WHERE DOWNLOAD_DATE = V_CURRDATE
        AND PREV_PMT_DATE = PMT_DATE
        AND NPV_RATE > 0;

    COMMIT;

    --GET NPV SUM
    INSERT /*+ PARALLEL(12) */ INTO TMP_B2 (
        MASTERID
        ,NPV_SUM
        )
    SELECT /*+ PARALLEL(12) */ A.MASTERID
        ,(COALESCE(A.N_OSPRN, 0) + COALESCE(BENEFIT, 0)) AS NPV
    FROM TMP_B1 A
    JOIN IFRS_LBM_ACCT_EIR_CF_ECF B ON A.MASTERID = B.MASTERID
    JOIN IFRS_LBM_ACCT_EIR_GS_RESULT3 C ON A.MASTERID = C.MASTERID
    WHERE C.DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    -- GET BENEFIT
    INSERT /*+ PARALLEL(12) */ INTO TMP_B3 (
        MASTERID
        ,N_OSPRN
        ,NPV_SUM
        ,BENEFIT
        )
    SELECT /*+ PARALLEL(12) */ A.MASTERID
        ,A.N_OSPRN
        ,B.NPV_SUM
        ,B.NPV_SUM - A.N_OSPRN AS BENEFIT
    FROM TMP_B1 A
    JOIN TMP_B2 B ON B.MASTERID = A.MASTERID;

    COMMIT;

    -- UPDATE BACK

    MERGE INTO IFRS_LBM_ACCT_EIR_CF_ECF B
    USING TMP_B3 A
    ON (A.MASTERID = B.MASTERID
       )
    WHEN MATCHED THEN
    UPDATE SET  B.BENEFIT = A.BENEFIT;

    COMMIT;

     /*ADD TO REVERSE TRIGER ITRCG STAFFLOAN 20170403*/
      MERGE INTO (SELECT * FROM IFRS_LBM_ACCT_EIR_COST_FEE_ECF WHERE STATUS='ACT')A
      USING IFRS_LBM_ACCT_EIR_CF_ECF B
      ON (B.MASTERID = A.MASTERID  --ADD JOIN TO IFRS_LBM_ACCT_EIR_CF_ECF
      AND A.ECFDATE = V_CURRDATE
         )
      WHEN MATCHED THEN
      UPDATE
      SET A.STATUS = 'REV'
      ,A.FLAG_REVERSE = 'Y';COMMIT;


    INSERT /*+ PARALLEL(12) */ INTO IFRS_LBM_ACCT_EIR_COST_FEE_ECF (
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
    SELECT /*+ PARALLEL(12) */ V_CURRDATE
        ,V_CURRDATE
        ,A.MASTERID
        ,M.BRANCH_CODE
        ,M.CUSTOMER_NUMBER
        ,M.FACILITY_NUMBER
        ,M.ACCOUNT_NUMBER
        ,M.DATA_SOURCE
        ,M.CURRENCY
        ,M.PRODUCT_CODE
        ,'BENEFIT'
        ,CASE
            WHEN A.BENEFIT < 0
                THEN 'F'
            ELSE 'C'
            END
        ,'N'
        ,'EIR'
        ,'ACT'
        ,'STAFFLOAN'
        ,A.BENEFIT
        ,SYSTIMESTAMP CREATEDDATE
        ,'EIR_ECF_MAIN_TT' CREATEDBY
        ,'' SEQ
        ,A.BENEFIT
        ,M.CURRENCY
        ,1
        ,M.PRODUCT_TYPE
        ,0 AS CF_ID
    FROM TMP_B3 A
    JOIN IFRS_IMA_AMORT_CURR M ON M.MASTERID = A.MASTERID
        AND M.DOWNLOAD_DATE = V_CURRDATE
    JOIN IFRS_LBM_ACCT_EIR_CF_ECF C ON C.MASTERID = A.MASTERID;COMMIT;
        --AND C.PREV_EIR IS NULL; -- NO PREV ECF THEN INSERT

    UPDATE /*+ PARALLEL(12) */ IFRS_LBM_ACCT_EIR_COST_FEE_ECF
    SET CF_ID = ID
    WHERE CF_ID = 0
        AND SRCPROCESS = 'STAFFLOAN'
        AND DOWNLOAD_DATE = V_CURRDATE;
    COMMIT;

    --END: GOAL SEEK PREPARE STAFFLOAN BENEFIT
    --START GOALSEEK CF  CF
    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_ECF_MAIN','4 GS START');

    COMMIT;

    DELETE /*+ PARALLEL(12) */ IFRS_LBM_ACCT_EIR_ECF_NOCF
    WHERE DOWNLOAD_DATE >= V_CURRDATE; -- CLEAN UP
    COMMIT;
    DELETE /*+ PARALLEL(12) */ IFRS_LBM_ACCT_EIR_GS_RESULT4
    WHERE DOWNLOAD_DATE = V_CURRDATE;
    COMMIT;
    DELETE /*+ PARALLEL(12) */ IFRS_LBM_ACCT_EIR_FAILED_GS4
    WHERE DOWNLOAD_DATE = V_CURRDATE;
    COMMIT;
    DELETE /*+ PARALLEL(12) */ IFRS_LBM_ACCT_EIR_GS_RESULT
    WHERE DOWNLOAD_DATE = V_CURRDATE;
    COMMIT;
    DELETE /*+ PARALLEL(12) */ IFRS_LBM_ACCT_EIR_FAILED_GS
    WHERE DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_LBM_GS_MASTERID';

    INSERT /*+ PARALLEL(12) */ INTO IFRS_LBM_GS_MASTERID (MASTERID)
    SELECT /*+ PARALLEL(12) */ A.MASTERID
    FROM (
        SELECT MASTERID
            ,PERIOD
        FROM IFRS_LBM_ACCT_EIR_PAYM
        WHERE PREV_PMT_DATE = PMT_DATE
        ) A
    ORDER BY PERIOD;

    COMMIT;

    SELECT MIN(ID) INTO V_VMIN_ID
    FROM IFRS_LBM_GS_MASTERID;

    SELECT MAX(ID) INTO V_VMAX_ID
    FROM IFRS_LBM_GS_MASTERID;

    V_VX := V_VMIN_ID;
    V_VX_INC := 500000;

    WHILE V_VX <= V_VMAX_ID
    LOOP --LOOP
        INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
        VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_GS_RANGE',TO_CHAR(V_VX));

        V_ID2 := V_VX + V_VX_INC - 1;

        SP_IFRS_LBM_ACCT_EIR_GS_RANGE (V_VX,V_ID2);

        INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
        VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_GS_RANGE','DONE');

        SP_IFRS_EXEC_AND_LOG_PROCESS ('SP_IFRS_LBM_ACCT_EIR_GS_ALL');

        V_VX := V_VX + V_VX_INC;
    END LOOP; --LOOP;

    COMMIT;

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_ECF_MAIN','5 GS END');

    COMMIT;

    SP_IFRS_EXEC_AND_LOG_PROCESS ('SP_IFRS_LBM_ACCT_EIR_GS_INSER4');

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_ECF_MAIN','6 ECFNOCF INSERT');

    COMMIT;

    SP_IFRS_EXEC_AND_LOG_PROCESS ('SP_IFRS_LBM_ACCT_EIR_ECF_ALGN4');

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_ECF_MAIN','7 ECFNOCF ALIGNED');

    COMMIT;

    /* REMARKS
--UPDATE PNL IF FAILED GOAL SEEK 20160524
    UPDATE  IFRS_ACCT_COST_FEE
    SET     STATUS = 'PNL' ,
            CREATEDBY = 'EIRECF3'
    WHERE   DOWNLOAD_DATE = @V_CURRDATE
            AND MASTERID IN ( SELECT    MASTERID
                              FROM      IFRS_LBM_ACCT_EIR_FAILED_GS
                              WHERE     DOWNLOAD_DATE = @V_CURRDATE )
            AND STATUS = 'ACT'
*/
    SP_IFRS_EXEC_AND_LOG_PROCESS ('SP_IFRS_LBM_ACCT_EIR_GS_INSERT');

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_ECF_MAIN','8 ECF INSERTED');

    COMMIT;

    SP_IFRS_EXEC_AND_LOG_PROCESS( 'SP_IFRS_LBM_ACCT_EIR_ECF_ALIGN');

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_ECF_MAIN','9 ECF ALIGNED');

    COMMIT;

    -- MERGE ECF FOR MASTERID WITH DIFFERENT INTEREST STRUCTURE
    SP_IFRS_EXEC_AND_LOG_PROCESS ('SP_IFRS_LBM_ACCT_EIR_ECF_MERGE');

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LBM_ACCT_EIR_ECF_MAIN','10 ECF MERGED');

    COMMIT;

    -- GET ALL MASTER ID OF NEWLY GENERATED EIR ECF
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T1';

    INSERT /*+ PARALLEL(12) */ INTO TMP_T1 (MASTERID)
    SELECT /*+ PARALLEL(12) */ DISTINCT MASTERID
    FROM IFRS_LBM_ACCT_EIR_ECF
    WHERE DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    --FILTER OUT NOT TODAY STOPPED ECF
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T2';

    INSERT /*+ PARALLEL(12) */ INTO TMP_T2 (MASTERID)
    SELECT /*+ PARALLEL(12) */ DISTINCT A.MASTERID
    FROM TMP_T1   A
    JOIN IFRS_LBM_ACCT_EIR_ECF B ON B.PREV_PMT_DATE = B.PMT_DATE
        AND B.AMORTSTOPDATE = V_CURRDATE
        AND B.MASTERID = A.MASTERID

    UNION -- 20171016 ALSO INCLUDE ACCOUNT WITH ZERO AMOUNT (FIX CHKAMORT ON DUE_DATE CHANGE WHEN END_AMORT_DT - 1)

    SELECT /*+ PARALLEL(12) */ MASTERID
    FROM /*+ PARALLEL(12) */ IFRS_LBM_ACCT_EIR_CF_ECF
    WHERE TOTAL_AMT = 0
        OR TOTAL_AMT_ACRU = 0;

    COMMIT;

    -- INSERT ACCRU VALUES FOR NEWLY GENERATED ECF
    -- NO ACCRU IF TODAY IS DOING AMORT
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T1';

    INSERT /*+ PARALLEL(12) */ INTO TMP_T1   (MASTERID)
    SELECT /*+ PARALLEL(12) */ DISTINCT MASTERID
    FROM IFRS_LBM_ACCT_EIR_ACF
    WHERE DOWNLOAD_DATE = V_CURRDATE
        AND DO_AMORT = 'Y';

    COMMIT;


    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T3';

    INSERT /*+ PARALLEL(12) */ INTO TMP_T3 (MASTERID)
    SELECT /*+ PARALLEL(12) */ MASTERID
    FROM TMP_T2
    WHERE MASTERID NOT IN (
            SELECT MASTERID
            FROM TMP_T1
            );
    COMMIT;

    IF V_PARAM_DISABLE_ACCRU_PREV = 0
    THEN
        -- GET LAST ACF WITH DO_AMORT=N
        EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_P1';

        INSERT /*+ PARALLEL(12) */ INTO TMP_P1 (ID)
        SELECT /*+ PARALLEL(12) */ MAX(ID) AS ID
        FROM IFRS_LBM_ACCT_EIR_ACF
        WHERE MASTERID IN (
                SELECT MASTERID
                FROM TMP_T3
                )
            AND DO_AMORT = 'N'
            AND DOWNLOAD_DATE < V_CURRDATE
            AND DOWNLOAD_DATE >= V_PREVDATE
        GROUP BY MASTERID;COMMIT;

        -- GET FEE SUMMARY
        EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_TF';

        INSERT /*+ PARALLEL(12) */ INTO TMP_TF (
            SUM_AMT
            ,DOWNLOAD_DATE
            ,MASTERID
            )
        SELECT /*+ PARALLEL(12) */ SUM(A.N_AMOUNT) AS SUM_AMT
            ,A.DOWNLOAD_DATE
            ,A.MASTERID
        FROM (
            SELECT CASE
                    WHEN A.FLAG_REVERSE = 'Y'
                        THEN - 1 * A.AMOUNT
                    ELSE A.AMOUNT
                    END AS N_AMOUNT
                ,A.ECFDATE DOWNLOAD_DATE
                ,A.MASTERID
            FROM IFRS_LBM_ACCT_EIR_COST_FEE_ECF A
            WHERE A.MASTERID IN (
                    SELECT MASTERID
                    FROM TMP_T3
                    )
                AND A.STATUS = 'ACT'
                AND A.FLAG_CF = 'F'
                AND A.METHOD = 'EIR'
            ) A
        GROUP BY A.DOWNLOAD_DATE
            ,A.MASTERID;COMMIT;

        -- GET COST SUMMARY
        EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_TC';

        INSERT /*+ PARALLEL(12) */ INTO TMP_TC (
            SUM_AMT
            ,DOWNLOAD_DATE
            ,MASTERID
            )
        SELECT /*+ PARALLEL(12) */ SUM(A.N_AMOUNT) AS SUM_AMT
            ,A.DOWNLOAD_DATE
            ,A.MASTERID
        FROM (
            SELECT CASE
                    WHEN A.FLAG_REVERSE = 'Y'
                        THEN - 1 * A.AMOUNT
                    ELSE A.AMOUNT
                    END AS N_AMOUNT
                ,A.ECFDATE DOWNLOAD_DATE
                ,A.MASTERID
            FROM IFRS_LBM_ACCT_EIR_COST_FEE_ECF A
            WHERE A.MASTERID IN (
                    SELECT MASTERID
                    FROM TMP_T3
                    )
                AND A.STATUS = 'ACT'
                AND A.FLAG_CF = 'C'
                AND A.METHOD = 'EIR'
            ) A
        GROUP BY A.DOWNLOAD_DATE
            ,A.MASTERID;COMMIT;

        --INSERT FEE 1
        INSERT /*+ PARALLEL(12) */ INTO IFRS_LBM_ACCT_EIR_ACCRU_PREV (
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
            )
        SELECT /*+ PARALLEL(12) */ A.FACNO
            ,A.CIFNO
            ,V_CURRDATE
            ,A.ECFDATE
            ,A.DATASOURCE
            ,B.PRDCODE
            ,B.TRXCODE
            ,B.CCY
            ,ROUND(CAST(CAST(CASE
                            WHEN B.FLAG_REVERSE = 'Y'
                                THEN - 1 * B.AMOUNT
                            ELSE B.AMOUNT
                            END AS BINARY_DOUBLE) / NVL(CAST(C.SUM_AMT AS BINARY_DOUBLE), 0) AS NUMBER(32, 20)) * A.N_ACCRU_FEE, V_ROUND) AS N_AMOUNT
            ,B.STATUS
            ,SYSTIMESTAMP
            ,A.ACCTNO
            ,A.MASTERID
            ,B.FLAG_CF
            ,'N'
            ,NULL AS AMORTDATE
            ,'ECF'
            ,B.ORG_CCY
            ,B.ORG_CCY_EXRATE
            ,B.PRDTYPE
            ,B.CF_ID
            ,B.METHOD
        FROM IFRS_LBM_ACCT_EIR_ACF A
        JOIN IFRS_LBM_ACCT_EIR_COST_FEE_ECF B ON B.ECFDATE = A.ECFDATE
            AND A.MASTERID = B.MASTERID
            AND B.FLAG_CF = 'F'
	    AND B.STATUS = 'ACT'
        JOIN TMP_TF C ON C.DOWNLOAD_DATE = A.ECFDATE
            AND C.MASTERID = A.MASTERID
        --20160407 EIR STOP REV
        LEFT JOIN (
            SELECT DISTINCT MASTERID
            FROM IFRS_LBM_ACCT_EIR_STOP_REV
            WHERE DOWNLOAD_DATE = V_CURRDATE
            ) D ON A.MASTERID = D.MASTERID
        WHERE A.ID IN (
                SELECT ID
                FROM TMP_P1
                )
            --20160407 EIR STOP REV
            AND D.MASTERID IS NULL
            --20180108 EXCLUDE CF REV AND ITS PAIR
            AND B.CF_ID NOT IN (SELECT CF_ID FROM IFRS_ACCT_COST_FEE WHERE DOWNLOAD_DATE = V_CURRDATE AND FLAG_REVERSE = 'Y' AND CF_ID_REV IS NOT NULL
                                UNION ALL
                                SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE WHERE DOWNLOAD_DATE = V_CURRDATE AND FLAG_REVERSE = 'Y' AND CF_ID_REV IS NOT NULL
                                );COMMIT;*
        --COST 1
        INSERT INTO IFRS_LBM_ACCT_EIR_ACCRU_PREV (
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
            )
        SELECT A.FACNO
            ,A.CIFNO
            ,V_CURRDATE
            ,A.ECFDATE
            ,A.DATASOURCE
            ,B.PRDCODE
            ,B.TRXCODE
            ,B.CCY
            ,ROUND(CAST(CAST(CASE
                            WHEN B.FLAG_REVERSE = 'Y'
                                THEN - 1 * B.AMOUNT
                            ELSE B.AMOUNT
                            END AS BINARY_DOUBLE) / NVL(CAST(C.SUM_AMT AS BINARY_DOUBLE), 0) AS NUMBER(32, 20)) * A.N_ACCRU_COST, V_ROUND) AS N_AMOUNT
            ,B.STATUS
            ,SYSTIMESTAMP
            ,A.ACCTNO
            ,A.MASTERID
            ,B.FLAG_CF
            ,'N'
            ,NULL AS AMORTDATE
            ,'ECF'
            ,B.ORG_CCY
            ,B.ORG_CCY_EXRATE
            ,B.PRDTYPE
            ,B.CF_ID
            ,B.METHOD
        FROM IFRS_LBM_ACCT_EIR_ACF A
        JOIN IFRS_LBM_ACCT_EIR_COST_FEE_ECF B ON B.ECFDATE = A.ECFDATE
            AND A.MASTERID = B.MASTERID
            AND B.FLAG_CF = 'C'
	    AND B.STATUS = 'ACT'
        JOIN TMP_TC C ON C.DOWNLOAD_DATE = A.ECFDATE
            AND C.MASTERID = A.MASTERID
        --20160407 EIR STOP REV
        LEFT JOIN (
            SELECT DISTINCT MASTERID
            FROM IFRS_LBM_ACCT_EIR_STOP_REV
            WHERE DOWNLOAD_DATE = V_CURRDATE
            ) D ON A.MASTERID = D.MASTERID
        WHERE A.ID IN (
                SELECT ID
                FROM TMP_P1
                )
            --20160407 EIR STOP REV
            AND D.MASTERID IS NULL
            --20180108 EXCLUDE CF REV AND ITS PAIR
            AND B.CF_ID NOT IN (SELECT CF_ID FROM IFRS_ACCT_COST_FEE WHERE DOWNLOAD_DATE = V_CURRDATE AND FLAG_REVERSE = 'Y' AND CF_ID_REV IS NOT NULL
                                UNION ALL
                                SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE WHERE DOWNLOAD_DATE = V_CURRDATE AND FLAG_REVERSE = 'Y' AND CF_ID_REV IS NOT NULL
                                );*/
    END IF; --IF;

    COMMIT;

    -- 20171016 MARK FOR DO AMORT ACRU (FIX CHKAMORT ON DUE_DATE CHANGE WHEN END_AMORT_DT - 1)
    UPDATE /*+ PARALLEL(12) */ IFRS_LBM_ACCT_EIR_ACCRU_PREV
    SET STATUS = TO_CHAR (V_CURRDATE, 'YYYYMMDD')
    WHERE STATUS = 'ACT'
        AND MASTERID IN (SELECT MASTERID FROM IFRS_LBM_ACCT_EIR_CF_ECF WHERE TOTAL_AMT = 0 OR TOTAL_AMT_ACRU = 0);

    COMMIT;

    --20180226 INSERT GAIN LOSS
    -- GET FEE SUMMARY WITH ECFDATE=@CURRDATE
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_TF';

    INSERT /*+ PARALLEL(12) */ INTO TMP_TF (
        SUM_AMT
        ,DOWNLOAD_DATE
        ,MASTERID
        )
    SELECT /*+ PARALLEL(12) */ SUM(A.N_AMOUNT) AS SUM_AMT
        ,A.DOWNLOAD_DATE
        ,A.MASTERID
    FROM (
        SELECT CASE
                WHEN A.FLAG_REVERSE = 'Y'
                    THEN - 1 * A.AMOUNT
                ELSE A.AMOUNT
                END AS N_AMOUNT
            ,A.ECFDATE DOWNLOAD_DATE
            ,A.MASTERID
        FROM IFRS_LBM_ACCT_EIR_COST_FEE_ECF A
        WHERE A.ECFDATE = V_CURRDATE
            AND A.STATUS = 'ACT'
            AND A.FLAG_CF = 'F'
            AND A.METHOD = 'EIR'
        ) A
    GROUP BY A.DOWNLOAD_DATE
        ,A.MASTERID;

    COMMIT;

    -- GET COST SUMMARY WITH ECFDATE=@CURRDATE
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_TC';

    INSERT /*+ PARALLEL(12) */ INTO TMP_TC (
        SUM_AMT
        ,DOWNLOAD_DATE
        ,MASTERID
        )
    SELECT /*+ PARALLEL(12) */ SUM(A.N_AMOUNT) AS SUM_AMT
        ,A.DOWNLOAD_DATE
        ,A.MASTERID
    FROM (
        SELECT CASE
                WHEN A.FLAG_REVERSE = 'Y'
                    THEN - 1 * A.AMOUNT
                ELSE A.AMOUNT
                END AS N_AMOUNT
            ,A.ECFDATE DOWNLOAD_DATE
            ,A.MASTERID
        FROM IFRS_LBM_ACCT_EIR_COST_FEE_ECF A
        WHERE A.ECFDATE = V_CURRDATE
            AND A.STATUS = 'ACT'
            AND A.FLAG_CF = 'C'
            AND A.METHOD = 'EIR'
        ) A
    GROUP BY A.DOWNLOAD_DATE
        ,A.MASTERID;

    COMMIT;

    --201801417 CLEAN UP GAIN LOSS
    DELETE /*+ PARALLEL(12) */
    FROM IFRS_LBM_ACCT_EIR_GAIN_LOSS
    WHERE DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    --INSERT FEE GAIN LOSS
    INSERT  INTO IFRS_LBM_ACCT_EIR_GAIN_LOSS (
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
        )
    SELECT  IMA.FACILITY_NUMBER
        ,IMA.CUSTOMER_NUMBER
        ,V_CURRDATE
        ,V_CURRDATE
        ,IMA.DATA_SOURCE
        ,B.PRDCODE
        ,B.TRXCODE
        ,B.CCY
        ,- 1 * --20180417 GAIN LOSS DIBALIK
        ROUND(CAST(CAST(CASE WHEN B.FLAG_REVERSE = 'Y'THEN - 1 * B.AMOUNT ELSE B.AMOUNT
                        END AS BINARY_DOUBLE) / CAST(C.SUM_AMT AS BINARY_DOUBLE) AS NUMBER(32, 20)) * A.GAIN_LOSS_FEE_AMT, V_ROUND) AS N_AMOUNT
        ,B.STATUS
        ,SYSTIMESTAMP
        ,IMA.ACCOUNT_NUMBER
        ,A.MASTERID
        ,B.FLAG_CF
        ,'N'
        ,NULL AS AMORTDATE
        ,'ECF'
        ,B.ORG_CCY
        ,B.ORG_CCY_EXRATE
        ,B.PRDTYPE
        ,B.CF_ID
        ,B.METHOD
    FROM IFRS_LBM_ACCT_EIR_CF_ECF A
    JOIN IFRS_IMA_AMORT_CURR IMA ON IMA.MASTERID = A.MASTERID
    JOIN IFRS_LBM_ACCT_EIR_COST_FEE_ECF B ON B.ECFDATE = V_CURRDATE
        AND A.MASTERID = B.MASTERID
        AND B.FLAG_CF = 'F'
    JOIN TMP_TF C ON C.MASTERID = A.MASTERID
    WHERE COALESCE(A.GAIN_LOSS_AMT, 0) <> 0;COMMIT;

    --INSERT COST GAIN LOSS
    INSERT INTO IFRS_LBM_ACCT_EIR_GAIN_LOSS (
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
        )
    SELECT  IMA.FACILITY_NUMBER
        ,IMA.CUSTOMER_NUMBER
        ,V_CURRDATE
        ,V_CURRDATE
        ,IMA.DATA_SOURCE
        ,B.PRDCODE
        ,B.TRXCODE
        ,B.CCY
        ,- 1 * --20180417 GAIN LOSS DIBALIK
        ROUND(CAST(CAST(CASE WHEN B.FLAG_REVERSE = 'Y' THEN - 1 * B.AMOUNT ELSE B.AMOUNT
                        END AS BINARY_DOUBLE) / CAST(C.SUM_AMT AS BINARY_DOUBLE) AS NUMBER(32, 20)) * A.GAIN_LOSS_COST_AMT, V_ROUND) AS N_AMOUNT
        ,B.STATUS
        ,SYSTIMESTAMP
        ,IMA.ACCOUNT_NUMBER
        ,A.MASTERID
        ,B.FLAG_CF
        ,'N'
        ,NULL AS AMORTDATE
        ,'ECF'
        ,B.ORG_CCY
        ,B.ORG_CCY_EXRATE
        ,B.PRDTYPE
        ,B.CF_ID
        ,B.METHOD
    FROM IFRS_LBM_ACCT_EIR_CF_ECF A
    JOIN IFRS_IMA_AMORT_CURR IMA ON IMA.MASTERID = A.MASTERID
    JOIN IFRS_LBM_ACCT_EIR_COST_FEE_ECF B ON B.ECFDATE = V_CURRDATE
        AND A.MASTERID = B.MASTERID
        AND B.FLAG_CF = 'C'
    JOIN TMP_TC C ON C.MASTERID = A.MASTERID
    WHERE COALESCE(A.GAIN_LOSS_AMT, 0) <> 0;COMMIT;

    --20180226 ADJUST GAIN LOSS BACK TO IFRS_LBM_ACCT_EIR_COST_FEE_ECF
    MERGE INTO IFRS_LBM_ACCT_EIR_COST_FEE_ECF B
    USING IFRS_LBM_ACCT_EIR_CF_ECF A
    ON (A.MASTERID = B.MASTERID
        AND B.ECFDATE = V_CURRDATE
        AND COALESCE(A.GAIN_LOSS_FEE_AMT, 0) <> 0
        AND B.FLAG_CF = 'F'
       )
    WHEN MATCHED THEN
    UPDATE
    SET B.AMOUNT = ((A.FEE_AMT + A.GAIN_LOSS_FEE_AMT) / A.FEE_AMT) * AMOUNT;

    COMMIT;

    MERGE INTO IFRS_LBM_ACCT_EIR_COST_FEE_ECF B
    USING IFRS_LBM_ACCT_EIR_CF_ECF A
    ON (A.MASTERID = B.MASTERID
        AND B.ECFDATE = V_CURRDATE
        AND COALESCE(A.GAIN_LOSS_FEE_AMT, 0) <> 0
        AND B.FLAG_CF = 'C'
       )
    WHEN MATCHED THEN
    UPDATE
    SET B.AMOUNT = ((A.COST_AMT + A.GAIN_LOSS_COST_AMT) / A.COST_AMT) * AMOUNT;

    COMMIT;

    INSERT INTO IFRS_AMORT_LOG (DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES (V_CURRDATE,SYSTIMESTAMP,'END','SP_IFRS_LBM_ACCT_EIR_ECF_MAIN','');

    COMMIT;
END;