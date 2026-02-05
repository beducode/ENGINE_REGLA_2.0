CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_EIR_ECF_MAIN_K
AS
  V_CURRDATE DATE ;
  V_PREVDATE DATE ;
  V_VMIN_ID NUMBER(19) ;
  V_VMAX_ID NUMBER(19) ;
  V_VX NUMBER(19) ;
  V_ID2 NUMBER(19) ;
  V_VX_INC NUMBER(19) ;
  V_PARAM_DISABLE_ACCRU_PREV NUMBER(19);
  V_ROUND NUMBER(10);
  V_FUNCROUND NUMBER(10);
BEGIN
    SELECT  MAX(CURRDATE), MAX(PREVDATE)
    INTO V_CURRDATE, V_PREVDATE
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

    --disable accru prev create on new ecf and return accrual to unamort
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

    --SET @param_disable_accru_prev = 1

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_IFRS_ACCT_EIR_ECF_MAIN' ,'');

    --reset data before processing
    DELETE  FROM IFRS_ACCT_EIR_ACCRU_PREV
    WHERE   DOWNLOAD_DATE >= V_CURRDATE
    AND SRCPROCESS = 'ECF';

    COMMIT;

    UPDATE  IFRS_ACCT_COST_FEE
    SET     STATUS = 'ACT'
    WHERE   STATUS = 'PNL'
    AND CREATEDBY = 'EIRECF1'
    AND DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    UPDATE  IFRS_ACCT_EIR_COST_FEE_PREV
    SET     STATUS = 'ACT'
    WHERE   STATUS = 'PNL'
    AND CREATEDBY = 'EIRECF2'
    AND DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    UPDATE  IFRS_ACCT_EIR_COST_FEE_PREV
    SET     STATUS = 'ACT'
    WHERE   STATUS = 'PNL2'
    AND CREATEDBY = 'EIRECF2'
    AND DOWNLOAD_DATE = V_PREVDATE;

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T7';

    INSERT  INTO TMP_T7
    ( MASTERID
     ,STAFFLOAN
     ,PKID
     ,NPVRATE
    )
    SELECT  A.MASTERID ,
            CASE WHEN COALESCE(STAFF_LOAN_FLAG, 'N') IN ( 'N', '' ) THEN 0 ELSE 1 END ,
            A.ID ,
            CASE WHEN STAFF_LOAN_FLAG = 'Y' THEN COALESCE(P.MARKET_RATE, 0) ELSE 0 END MARKET_RATE
    FROM IFRS_IMA_AMORT_CURR A
    LEFT JOIN IFRS_PRODUCT_PARAM P
      ON P.DATA_SOURCE = A.DATA_SOURCE
      AND P.PRD_TYPE = A.PRODUCT_TYPE
      AND P.PRD_CODE = A.PRODUCT_CODE
      AND (P.CCY = A.CURRENCY OR NVL(P.CCY, 'ALL') = 'ALL')
    WHERE   A.EIR_STATUS = 'Y' AND A.AMORT_TYPE <> 'SL';

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_ACCT_EIR_CF_ECF' ;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_TODAYREV' ;

    --20180116 acct with reversal today
    INSERT INTO TMP_TODAYREV(MASTERID)
    SELECT MASTERID
    FROM IFRS_ACCT_COST_FEE
    WHERE DOWNLOAD_DATE=V_CURRDATE
    AND FLAG_REVERSE='Y'
    AND CF_ID_REV IS NOT NULL;

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_IFRS_ACCT_COST_FEE';

    INSERT INTO TMP_IFRS_ACCT_COST_FEE
    SELECT * FROM IFRS_ACCT_COST_FEE
    WHERE DOWNLOAD_DATE = V_CURRDATE
      AND STATUS = 'ACT'
      AND METHOD = 'EIR'
      --20180108 exclude cf reversal and its pair
      AND CF_ID NOT IN (SELECT CF_ID FROM IFRS_ACCT_COST_FEE
                          WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                          UNION ALL
                          SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE
                          WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                         );

    COMMIT;

    -- today new cost fee

    INSERT  INTO IFRS_ACCT_EIR_CF_ECF
    ( MASTERID ,
      FEE_AMT ,
      COST_AMT ,
      FEE_AMT_ACRU ,
      COST_AMT_ACRU ,
      STAFFLOAN ,
      PKID ,
      NPV_RATE,
      GAIN_LOSS_CALC --20180226 set N
    )
    SELECT  A.MASTERID ,
            SUM(COALESCE(CASE WHEN C.FLAG_CF = 'F' THEN CASE WHEN C.FLAG_REVERSE = 'Y' THEN -1 * C.AMOUNT ELSE C.AMOUNT END
                              ELSE 0
                         END, 0)) ,
            SUM(COALESCE(CASE WHEN C.FLAG_CF = 'C' THEN CASE WHEN C.FLAG_REVERSE = 'Y' THEN -1 * C.AMOUNT ELSE C.AMOUNT END
                              ELSE 0
                         END, 0)) ,
            0 ,
            0 ,
            A.STAFFLOAN ,
            A.PKID ,
            A.NPVRATE ,
            'N' --20180226
    FROM TMP_T7 A
    LEFT JOIN TMP_IFRS_ACCT_COST_FEE C
      ON C.DOWNLOAD_DATE = V_CURRDATE
      AND C.MASTERID = A.MASTERID
      --WHERE C.METHOD = 'EIR'
    GROUP BY A.MASTERID ,
    A.STAFFLOAN ,
    A.PKID ,
    A.NPVRATE;

    COMMIT;

    --20180226 fill to column for new cost/fee
    UPDATE IFRS_ACCT_EIR_CF_ECF
    SET NEW_FEE_AMT=NVL(FEE_AMT,0)
     ,NEW_COST_AMT=NVL(COST_AMT,0)
     ,NEW_TOTAL_AMT=NVL(NEW_FEE_AMT,0)+NVL(NEW_COST_AMT,0);

     COMMIT;
    -- sisa unamort

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_ACCT_EIR_COST_FEE_PREV';

    INSERT INTO TMP_ACCT_EIR_COST_FEE_PREV
    (
        ID,
        DOWNLOAD_DATE,
        ECFDATE,
        MASTERID,
        BRCODE,
        CIFNO,
        FACNO,
        ACCTNO,
        DATASOURCE,
        PRDTYPE,
        PRDCODE,
        TRXCODE,
        CCY,
        FLAG_CF,
        FLAG_REVERSE,
        METHOD,
        STATUS,
        SRCPROCESS,
        AMOUNT,
        AMOUNT_ORG,
        CREATEDDATE,
        CREATEDBY,
        ISUSED,
        SEQ,
        ORG_CCY,
        ORG_CCY_EXRATE,
        CF_ID
    )
    SELECT ID,
        DOWNLOAD_DATE,
        ECFDATE,
        MASTERID,
        BRCODE,
        CIFNO,
        FACNO,
        ACCTNO,
        DATASOURCE,
        PRDTYPE,
        PRDCODE,
        TRXCODE,
        CCY,
        FLAG_CF,
        FLAG_REVERSE,
        METHOD,
        STATUS,
        SRCPROCESS,
        AMOUNT,
        AMOUNT_ORG,
        CREATEDDATE,
        CREATEDBY,
        ISUSED,
        SEQ,
        ORG_CCY,
        ORG_CCY_EXRATE,
        CF_ID
    FROM IFRS_ACCT_EIR_COST_FEE_PREV B
    WHERE B.DOWNLOAD_DATE IN ( V_CURRDATE, V_PREVDATE )
    AND B.STATUS = 'ACT'
    --20180116 exclude cf reversal and its pair
    AND B.CF_ID NOT IN (SELECT CF_ID FROM IFRS_ACCT_COST_FEE
                        WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                        UNION ALL
                        SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE
                        WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                        )
    --20180426 exclude prevdate if not from sp acf accru
    AND CASE WHEN B.DOWNLOAD_DATE= V_PREVDATE AND B.SEQ<>'2' THEN 0 ELSE 1 END  = 1;

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T10';

    INSERT  INTO TMP_T10
    ( MASTERID ,
      FEE_AMT ,
      COST_AMT
    )
    SELECT  B.MASTERID ,
            SUM(COALESCE(CASE WHEN B.FLAG_CF = 'F' THEN CASE WHEN B.FLAG_REVERSE = 'Y' THEN -1 * CASE WHEN CFREV.MASTERID IS NULL THEN B.AMOUNT ELSE B.AMOUNT END
                                                             ELSE CASE WHEN CFREV.MASTERID IS NULL THEN B.AMOUNT ELSE B.AMOUNT END
                                                        END
                              ELSE 0
                         END, 0)) AS FEE_AMT ,
            SUM(COALESCE(CASE WHEN B.FLAG_CF = 'C' THEN CASE WHEN B.FLAG_REVERSE = 'Y' THEN -1 * CASE WHEN CFREV.MASTERID IS NULL THEN B.AMOUNT ELSE B.AMOUNT END
                                                             ELSE CASE WHEN CFREV.MASTERID IS NULL THEN B.AMOUNT ELSE B.AMOUNT END
                                                        END
                              ELSE 0
                         END, 0)) AS COST_AMT
    FROM    TMP_ACCT_EIR_COST_FEE_PREV B
    JOIN VW_LAST_EIR_COST_FEE_PREV X
      ON B.DOWNLOAD_DATE = X.DOWNLOAD_DATE
      AND B.MASTERID = X.MASTERID
      AND B.SEQ = X.SEQ
    --20160407 eir stop rev
    LEFT JOIN (SELECT DISTINCT MASTERID FROM IFRS_ACCT_EIR_STOP_REV WHERE DOWNLOAD_DATE = V_CURRDATE) A
      ON A.MASTERID = B.MASTERID
    --20180116 resona req
    LEFT JOIN TMP_TODAYREV CFREV ON CFREV.MASTERID=B.MASTERID
    WHERE A.MASTERID IS NULL
    GROUP BY B.MASTERID;

    COMMIT;

    MERGE INTO IFRS_ACCT_EIR_CF_ECF A
    USING TMP_T10 B
    ON (B.MASTERID = A.MASTERID)
    WHEN MATCHED THEN
    UPDATE
    SET A.FEE_AMT=A.FEE_AMT+B.FEE_AMT
       ,A.COST_AMT=A.COST_AMT+B.COST_AMT;

    COMMIT;


    IF V_PARAM_DISABLE_ACCRU_PREV != 0
    THEN
        -- no accru if today is doing amort
          EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T1';

          INSERT  INTO TMP_T1( MASTERID ,ACCTNO )
          SELECT DISTINCT MASTERID,ACCTNO
          FROM    IFRS_ACCT_EIR_ACF
          WHERE   DOWNLOAD_DATE = V_CURRDATE
          AND DO_AMORT = 'Y';

          COMMIT;

          EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T3'  ;

          INSERT  INTO TMP_T3 ( MASTERID )
          SELECT  MASTERID
          FROM    IFRS_ACCT_EIR_CF_ECF
          WHERE   MASTERID NOT IN ( SELECT MASTERID FROM TMP_T1 );

          COMMIT;

          -- get last acf with do_amort=N
          EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_P1';

          INSERT  INTO TMP_P1 ( ID )
          SELECT  MAX(ID) AS ID
          FROM    IFRS_ACCT_EIR_ACF
          WHERE   MASTERID IN ( SELECT MASTERID FROM TMP_T3 )
          AND DO_AMORT = 'N'
          AND DOWNLOAD_DATE < V_CURRDATE
          AND DOWNLOAD_DATE >= V_PREVDATE
          GROUP BY MASTERID;

          COMMIT;


          MERGE INTO IFRS_ACCT_EIR_CF_ECF A
          USING ( SELECT * FROM IFRS_ACCT_EIR_ACF WHERE ID IN (SELECT  ID FROM TMP_P1)) B
          ON (A.MASTERID=B.MASTERID
              AND A.MASTERID NOT IN ( SELECT MASTERID
                                      FROM IFRS_ACCT_EIR_STOP_REV
                                      WHERE DOWNLOAD_DATE = V_CURRDATE
                                    )
             )
          WHEN MATCHED THEN
          UPDATE
          SET  A.FEE_AMT=A.FEE_AMT-B.N_ACCRU_FEE
          , A.COST_AMT=A.COST_AMT-B.N_ACCRU_COST
	  WHERE A.MASTERID NOT IN (
	     SELECT DISTINCT MASTERID FROM IFRS_ACCT_SWITCH
	      WHERE DOWNLOAD_DATE = V_CURRDATE
	  );

	  COMMIT;

          --20180116 fee adj rev ambil dari unamort untuk pair dari cf rev
          MERGE INTO IFRS_ACCT_EIR_CF_ECF A
          USING (SELECT * FROM IFRS_ACCT_JOURNAL_INTM
                 WHERE CF_ID IN (SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE
                                 WHERE DOWNLOAD_DATE=V_CURRDATE
                                 AND FLAG_REVERSE='Y'
                                 AND CF_ID_REV IS NOT NULL
                                 )
                 AND DOWNLOAD_DATE=V_PREVDATE
                 AND REVERSE='N'
                 AND JOURNALCODE='ACCRU'
                 AND FLAG_CF='F'
                 ) B
          ON (A.MASTERID=B.MASTERID
              AND A.MASTERID IN ( SELECT MASTERID FROM TMP_T3 )
                        --20160407 sl stop rev
              AND A.MASTERID NOT IN (SELECT  MASTERID FROM IFRS_ACCT_EIR_STOP_REV
                                         WHERE   DOWNLOAD_DATE = V_CURRDATE)
             )
          WHEN MATCHED THEN
          UPDATE
          SET A.FEE_AMT=A.FEE_AMT+B.N_AMOUNT;

          COMMIT;

          --20180116 cost adj rev ambil dari unamort untuk pair dari cf rev
          MERGE INTO IFRS_ACCT_EIR_CF_ECF A
          USING (SELECT * FROM IFRS_ACCT_JOURNAL_INTM
                 WHERE CF_ID IN (SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE
                                 WHERE DOWNLOAD_DATE=V_CURRDATE
                                 AND FLAG_REVERSE='Y'
                                 AND CF_ID_REV IS NOT NULL
                                 )
                        AND DOWNLOAD_DATE=V_PREVDATE
                        AND REVERSE='N'
                        AND JOURNALCODE='ACCRU'
                        AND FLAG_CF='C'
                       ) B
          ON (A.MASTERID=B.MASTERID
              AND A.MASTERID IN ( SELECT MASTERID FROM TMP_T3 )
              --20160407 sl stop rev
              AND A.MASTERID
              NOT IN (SELECT  MASTERID FROM    IFRS_ACCT_EIR_STOP_REV
                      WHERE   DOWNLOAD_DATE = V_CURRDATE)
             )
          WHEN MATCHED THEN
          UPDATE
          SET A.COST_AMT=A.COST_AMT+B.N_AMOUNT;

          COMMIT;

    END IF; --IF @param_disable_accru_prev != 0


    -- accru
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T10'  ;

    INSERT  INTO TMP_T10
    ( MASTERID ,
      FEE_AMT ,
      COST_AMT
    )
    SELECT  B.MASTERID ,
            SUM(COALESCE(CASE WHEN B.FLAG_CF = 'F' THEN CASE WHEN B.FLAG_REVERSE = 'Y' THEN -1 * B.AMOUNT ELSE B.AMOUNT END
                              ELSE 0 END, 0)) AS FEE_AMT ,
            SUM(COALESCE(CASE WHEN B.FLAG_CF = 'C' THEN CASE WHEN B.FLAG_REVERSE = 'Y' THEN -1 * B.AMOUNT ELSE B.AMOUNT END
                              ELSE 0 END, 0)) AS COST_AMT
    FROM    IFRS_ACCT_EIR_ACCRU_PREV B
    WHERE   B.STATUS = 'ACT'
    --20180116 exclude cf rev and its pair
    AND B.CF_ID NOT IN (SELECT CF_ID FROM IFRS_ACCT_COST_FEE
                        WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                        UNION ALL
                        SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE
                        WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                       )

    GROUP BY B.MASTERID;

    COMMIT;


    MERGE INTO IFRS_ACCT_EIR_CF_ECF A
    USING TMP_T10 B
    ON (A.MASTERID=B.MASTERID)
    WHEN MATCHED THEN
    UPDATE
    SET A.FEE_AMT_ACRU=B.FEE_AMT
    , A.COST_AMT_ACRU=B.COST_AMT;

    COMMIT;


    -- update total
    UPDATE  IFRS_ACCT_EIR_CF_ECF
    SET     TOTAL_AMT = ROUND(FEE_AMT + COST_AMT, 0) ,
            TOTAL_AMT_ACRU = ROUND(FEE_AMT + COST_AMT + FEE_AMT_ACRU
                                   + COST_AMT_ACRU, 0);
    COMMIT;

    -- update prev eir
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T13'  ;

    INSERT  INTO TMP_T13
    ( MASTERID ,
      N_EFF_INT_RATE ,
      ENDAMORTDATE
    )
    SELECT  B.MASTERID ,
            B.N_EFF_INT_RATE ,
            B.ENDAMORTDATE
    FROM    IFRS_ACCT_EIR_ECF B
    WHERE   B.AMORTSTOPDATE IS NULL
    AND B.PMT_DATE = B.PREV_PMT_DATE;

    COMMIT;


    MERGE INTO IFRS_ACCT_EIR_CF_ECF A
    USING TMP_T13 B
    ON (A.MASTERID=B.MASTERID)
    WHEN MATCHED THEN
    UPDATE
    SET A.PREV_EIR=B.N_EFF_INT_RATE
      , A.PREV_ENDAMORTDATE=B.ENDAMORTDATE;

    COMMIT;
    --20180226 set gain_loss_calc to Y if prepayment event detected without other event (simplify for now)
    --partial payment eventid is 6
    UPDATE IFRS_ACCT_EIR_CF_ECF
    SET GAIN_LOSS_CALC='Y'
    WHERE MASTERID IN (SELECT MASTERID FROM IFRS_EVENT_CHANGES WHERE EVENT_ID=6 AND EFFECTIVE_DATE=V_CURRDATE)
    AND MASTERID NOT IN (SELECT MASTERID FROM IFRS_EVENT_CHANGES WHERE EVENT_ID IN (0,1,2,3,7,8) AND EFFECTIVE_DATE=V_CURRDATE);--FOR EVENT 7,8 BY VIVI 11 FEB 2019

    --20180226 if dont have prev eir then set back to N
    UPDATE IFRS_ACCT_EIR_CF_ECF
    SET GAIN_LOSS_CALC='N'
    WHERE PREV_EIR IS NULL
     AND GAIN_LOSS_CALC='Y';

    COMMIT;

    -- do full amort if sum cost fee zero and dont create new ecf
    UPDATE  IFRS_ACCT_COST_FEE
    SET     STATUS = 'PNL' ,
            CREATEDBY = 'EIRECF1'
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND MASTERID IN ( SELECT MASTERID
                      FROM IFRS_ACCT_EIR_CF_ECF
                      WHERE TOTAL_AMT = 0 OR TOTAL_AMT_ACRU = 0 )
    AND STATUS = 'ACT'
    --20180116 exclude cf rev and its pair, will be handled by acf_abn
    AND CF_ID NOT IN (SELECT CF_ID FROM IFRS_ACCT_COST_FEE
                      WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                      UNION ALL
                      SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE
                      WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                     );
    COMMIT;

    -- if last cost fee prev is currdate
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T11' ;


    INSERT  INTO TMP_T11
    ( MASTERID ,
      DOWNLOAD_DATE ,
      SEQ ,
      CURRDATE
    )
    SELECT  B.MASTERID ,
            B.DOWNLOAD_DATE ,
            B.SEQ ,
            V_CURRDATE
    FROM VW_LAST_EIR_COST_FEE_PREV B
    WHERE   B.MASTERID IN ( SELECT  MASTERID
                            FROM    IFRS_ACCT_EIR_CF_ECF
                            WHERE   TOTAL_AMT = 0 OR TOTAL_AMT_ACRU = 0 ) ;
    COMMIT;


    MERGE INTO IFRS_ACCT_EIR_COST_FEE_PREV A
    USING TMP_T11 B
    ON (A.DOWNLOAD_DATE = B.CURRDATE
        AND A.MASTERID = B.MASTERID
        AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
        AND A.SEQ = B.SEQ
        AND A.CF_ID NOT IN (SELECT CF_ID FROM IFRS_ACCT_COST_FEE
                                  WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                                  UNION ALL
                                  SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE
                                  WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                                 )
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.STATUS = CASE WHEN A.STATUS = 'ACT' THEN 'PNL' ELSE A.STATUS END
      , A.CREATEDBY = 'EIRECF2';

    COMMIT;

    -- if last cost fee prev is prevdate
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T12' ;

    INSERT  INTO TMP_T12
    ( MASTERID ,
      DOWNLOAD_DATE ,
      SEQ ,
      PREVDATE
    )
    SELECT  B.MASTERID ,
            B.DOWNLOAD_DATE ,
            B.SEQ ,
            V_PREVDATE
    FROM    VW_LAST_EIR_COST_FEE_PREV B
    WHERE   B.MASTERID IN ( SELECT MASTERID FROM IFRS_ACCT_EIR_CF_ECF
                            WHERE TOTAL_AMT = 0 OR TOTAL_AMT_ACRU = 0 );


    COMMIT;

    MERGE INTO IFRS_ACCT_EIR_COST_FEE_PREV A
    USING TMP_T12 B
    ON (A.DOWNLOAD_DATE = B.PREVDATE
        AND A.MASTERID = B.MASTERID
        AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
        AND A.SEQ = B.SEQ
        AND A.CF_ID NOT IN ( SELECT CF_ID FROM IFRS_ACCT_COST_FEE
                             WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                             UNION ALL
                             SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE
                             WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                            )
            --20180426 exclude prevdate if not from sp acf accru
        AND CASE WHEN A.DOWNLOAD_DATE=V_PREVDATE AND A.SEQ<>'2' THEN 0 ELSE 1 END  = 1
       )
    WHEN MATCHED THEN
    UPDATE
    SET  A.STATUS = CASE WHEN A.STATUS = 'ACT' THEN 'PNL2' ELSE A.STATUS END
        ,A.CREATEDBY = 'EIRECF2';

    COMMIT;


    IF V_PARAM_DISABLE_ACCRU_PREV != 0
    THEN
    -- insert accru prev only for pnl ed
    -- get last acf with do_amort=N

          EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_P1' ;

          INSERT  INTO TMP_P1 ( ID )
          SELECT  MAX(ID) AS ID
          FROM    IFRS_ACCT_EIR_ACF
          WHERE   MASTERID IN ( SELECT MASTERID FROM TMP_T3 )
          AND DO_AMORT = 'N'
          AND DOWNLOAD_DATE < V_CURRDATE
          AND DOWNLOAD_DATE >= V_PREVDATE
          -- add filter pnl ed acctno
          AND MASTERID IN ( SELECT MASTERID FROM IFRS_ACCT_EIR_CF_ECF WHERE TOTAL_AMT = 0 OR TOTAL_AMT_ACRU = 0 )
          GROUP BY MASTERID;


          COMMIT;

          -- get fee summary
          EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_TF' ;

          INSERT  INTO TMP_TF
          ( SUM_AMT ,
            DOWNLOAD_DATE ,
            MASTERID
          )
          SELECT  SUM(A.N_AMOUNT) AS SUM_AMT ,
                  A.DOWNLOAD_DATE ,
                  A.MASTERID
          FROM    ( SELECT CASE WHEN A.FLAG_REVERSE = 'Y' THEN -1 * A.AMOUNT ELSE A.AMOUNT END AS N_AMOUNT ,
                           A.ECFDATE DOWNLOAD_DATE ,
                           A.MASTERID
                    FROM IFRS_ACCT_EIR_COST_FEE_ECF A
                    WHERE    A.MASTERID IN ( SELECT MASTERID FROM  TMP_T3 )
                    AND A.STATUS = 'ACT'
                    AND A.FLAG_CF = 'F'
                    AND A.METHOD = 'EIR'
                  ) A
          GROUP BY A.DOWNLOAD_DATE ,
                   A.MASTERID;


          COMMIT;



          -- get cost summary

          EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_TC';

          INSERT  INTO TMP_TC
          ( SUM_AMT ,
            DOWNLOAD_DATE ,
            MASTERID
          )
          SELECT  SUM(A.N_AMOUNT) AS SUM_AMT ,
                  A.DOWNLOAD_DATE ,
                  A.MASTERID
          FROM    ( SELECT CASE WHEN A.FLAG_REVERSE = 'Y' THEN -1 * A.AMOUNT ELSE A.AMOUNT END AS N_AMOUNT ,
                           A.ECFDATE DOWNLOAD_DATE ,
                           A.MASTERID
                    FROM IFRS_ACCT_EIR_COST_FEE_ECF A
                    WHERE A.MASTERID IN ( SELECT MASTERID FROM  TMP_T3 )
                    AND A.STATUS = 'ACT'
                    AND A.FLAG_CF = 'C'
                    AND A.METHOD = 'EIR'
                  ) A
          GROUP BY A.DOWNLOAD_DATE ,
                   A.MASTERID;

          COMMIT;


          --insert fee 1
          INSERT  INTO IFRS_ACCT_EIR_ACCRU_PREV
          ( FACNO ,
            CIFNO ,
            DOWNLOAD_DATE ,
            ECFDATE ,
            DATASOURCE ,
            PRDCODE ,
            TRXCODE ,
            CCY ,
            AMOUNT ,
            STATUS ,
            CREATEDDATE ,
            ACCTNO ,
            MASTERID ,
            FLAG_CF ,
            FLAG_REVERSE ,
            AMORTDATE ,
            SRCPROCESS ,
            ORG_CCY ,
            ORG_CCY_EXRATE ,
            PRDTYPE ,
            CF_ID ,
            METHOD
          )
          SELECT  A.FACNO ,
                  A.CIFNO ,
                  V_CURRDATE ,
                  A.ECFDATE ,
                  A.DATASOURCE ,
                  B.PRDCODE ,
                  B.TRXCODE ,
                  B.CCY ,
                  ROUND(CAST(CAST(CASE WHEN B.FLAG_REVERSE = 'Y' THEN -1 * B.AMOUNT ELSE B.AMOUNT END AS BINARY_DOUBLE)
                        / NVL(CAST(C.SUM_AMT AS BINARY_DOUBLE),0) AS NUMBER(32,20)) * A.N_ACCRU_FEE,V_ROUND) AS N_AMOUNT ,
                  B.STATUS ,
                  SYSTIMESTAMP ,
                  A.ACCTNO ,
                  A.MASTERID ,
                  B.FLAG_CF ,
                  'N' ,
                  NULL AS AMORTDATE ,
                  'ECF' ,
                  B.ORG_CCY ,
                  B.ORG_CCY_EXRATE ,
                  B.PRDTYPE ,
                  B.CF_ID ,
                  B.METHOD
          FROM IFRS_ACCT_EIR_ACF A
          JOIN IFRS_ACCT_EIR_COST_FEE_ECF B
            ON B.ECFDATE = A.ECFDATE
            AND A.MASTERID = B.MASTERID
            AND B.FLAG_CF = 'F'
          JOIN TMP_TF C
            ON C.DOWNLOAD_DATE = A.ECFDATE
            AND C.MASTERID = A.MASTERID
          WHERE   A.ID IN ( SELECT ID FROM TMP_P1 )
          --20180108 exclude cf rev and its pair
          AND B.CF_ID NOT IN(SELECT CF_ID FROM IFRS_ACCT_COST_FEE
                             WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                             UNION ALL
                             SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE
                             WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                            );
          COMMIT;

          --cost 1
          INSERT  INTO IFRS_ACCT_EIR_ACCRU_PREV
          ( FACNO ,
            CIFNO ,
            DOWNLOAD_DATE ,
            ECFDATE ,
            DATASOURCE ,
            PRDCODE ,
            TRXCODE ,
            CCY ,
            AMOUNT ,
            STATUS ,
            CREATEDDATE ,
            ACCTNO ,
            MASTERID ,
            FLAG_CF ,
            FLAG_REVERSE ,
            AMORTDATE ,
            SRCPROCESS ,
            ORG_CCY ,
            ORG_CCY_EXRATE ,
            PRDTYPE ,
            CF_ID ,
            METHOD
          )
          SELECT  A.FACNO ,
                  A.CIFNO ,
                  V_CURRDATE ,
                  A.ECFDATE ,
                  A.DATASOURCE ,
                  B.PRDCODE ,
                  B.TRXCODE ,
                  B.CCY ,
                  ROUND(CAST(CAST(CASE WHEN B.FLAG_REVERSE = 'Y' THEN -1 * B.AMOUNT ELSE B.AMOUNT END AS BINARY_DOUBLE)
                        / NVL(CAST(C.SUM_AMT AS BINARY_DOUBLE),0) AS NUMBER(32,20)) * A.N_ACCRU_COST,V_ROUND) AS N_AMOUNT ,
                  B.STATUS ,
                  SYSTIMESTAMP ,
                  A.ACCTNO ,
                  A.MASTERID ,
                  B.FLAG_CF ,
                  'N' ,
                  NULL AS AMORTDATE ,
                  'ECF' ,
                  B.ORG_CCY ,
                  B.ORG_CCY_EXRATE ,
                  B.PRDTYPE ,
                  B.CF_ID ,
                  B.METHOD
          FROM IFRS_ACCT_EIR_ACF A
          JOIN IFRS_ACCT_EIR_COST_FEE_ECF B
            ON B.ECFDATE = A.ECFDATE
            AND A.MASTERID = B.MASTERID
            AND B.FLAG_CF = 'C'
          JOIN TMP_TC C
            ON C.DOWNLOAD_DATE = A.ECFDATE
            AND C.MASTERID = A.MASTERID
          WHERE   A.ID IN ( SELECT ID FROM TMP_P1 )
          --20180108 exclude cf rev and its pair
          AND B.CF_ID NOT IN (SELECT CF_ID FROM IFRS_ACCT_COST_FEE
                              WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                              UNION ALL
                              SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE
                              WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                             );


          COMMIT;


    END IF; --IF @param_disable_accru_prev != 0

    -- amort acru
    UPDATE  IFRS_ACCT_EIR_ACCRU_PREV
    SET     STATUS = TO_CHAR (V_CURRDATE, 'YYYYMMDD')
    WHERE   STATUS = 'ACT'
          AND MASTERID IN ( SELECT    MASTERID
                            FROM      IFRS_ACCT_EIR_CF_ECF
                            WHERE     TOTAL_AMT = 0  OR TOTAL_AMT_ACRU = 0 );

    -- stop old ecf
    UPDATE  IFRS_ACCT_EIR_ECF
    SET     AMORTSTOPDATE = V_CURRDATE ,
            AMORTSTOPMSG = 'SP_ACCT_EIR_ECF'
    WHERE   MASTERID IN ( SELECT MASTERID FROM IFRS_ACCT_EIR_CF_ECF )
    AND AMORTSTOPDATE IS NULL;


    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_ECF_MAIN' ,'2') ;

    COMMIT;

    -- insert from ifrs_paym_schd filtered

    /* Remarks dahulu karena sudah di insert di SP_IFRS_PAYMENT_SCHEDULE 20160524
    TRUNCATE TABLE IFRS_PAYM_CORE_SRC
    TRUNCATE TABLE TMP_T1
    INSERT  INTO TMP_T1
            ( masterid ,
              icc ,
              int_rate
            )
            SELECT  masterid ,
                    interest_calculation_code ,
                    INTEREST_RATE
            FROM    IMA_AMORT_CURR
            WHERE   eirecf = 'Y'


    INSERT  INTO IFRS_PAYM_CORE_SRC
            ( masterid ,
              acctno ,
              pmt_date ,
              interest_rate ,
              prn_amt ,
              int_amt ,
              disb_percentage ,
              disb_amount ,
              plafond ,
              icc ,
              grace_date
            )
            SELECT  a.acc_mstr_id ,
                    a.acc_mstr_id ,
                    a.pmtdate ,
                    a.INTEREST_RATE ,
                    a.principal ,
                    a.interest ,
                    a.DISB_PERCENTAGE ,
                    a.DISB_AMOUNT ,
                    a.PLAFOND ,
                    b.icc ,
                    a.GRACE_DATE
            FROM    psak_paym_schd a ,
                    TMP_T1 b
            WHERE   b.masterid = a.ACC_MSTR_ID
                    AND a.PMTDATE > @v_currdate

    -- calc eff rate from table IFRS_PAYM_CORE

    TRUNCATE TABLE IFRS_GS_MASTERID
    TRUNCATE TABLE IFRS_ACCT_EIR_PAYM

     end get last or start date for assign first paym date  --ridwan

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TT_PSAK_LAST_PAYM_DATE' ;

    INSERT  INTO TMP_PSAK_LAST_PAYM_DATE
    ( MASTER_ACCOUNT_ID ,
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
                        ACC_MSTR_ID
                FROM    IFRS_PAYM_SCHD
                WHERE   PMTDATE <= V_CURRDATE
                GROUP BY ACC_MSTR_ID
              ) B ON A.MASTERID = B.ACC_MSTR_ID
    WHERE   A.EIRECF = 'Y'
    AND A.FLAG_AL IN ( 'A' );


    UPDATE  TMP_PSAK_LAST_PAYM_DATE
    SET     LAST_PAYMENT_DATE_ASSIGN = CASE WHEN LAST_PAYMENT_DATE_SCHD IS NOT NULL
                                            THEN LAST_PAYMENT_DATE_SCHD
                                            ELSE LOAN_START_DATE
                                       END;
     end get last or start date for assign first paym date  --ridwan

    INSERT  INTO IFRS_PAYM_CORE_SRC
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
    SELECT DISTINCT A.MASTERID ,
                    A.ACCTNO ,
                    V_CURRDATE ,
                    V_CURRDATE ,
                    B.INT_RATE ,
                    0 ,
                    0 ,
                    A.ICC ,
                    A.GRACE_DATE
    FROM    IFRS_PAYM_CORE_SRC A , TMP_T1 B
    WHERE   B.MASTERID = A.MASTERID;

    --update disb amount 20160428
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T2' ;

    INSERT  INTO TMP_T2
    ( MASTERID ,
      DOWNLOAD_DATE
    )
    SELECT  A.ACC_MSTR_ID ,
            MAX(A.PMTDATE) DOWNLOAD_DATE
    FROM    IFRS_PAYM_SCHD A , TMP_T1 B
    WHERE   A.ACC_MSTR_ID = B.MASTERID
    AND A.PMTDATE <= V_CURRDATE
    GROUP BY A.ACC_MSTR_ID;


    MERGE INTO IFRS_PAYM_CORE_SRC A
    USING
    ( SELECT  A.ACC_MSTR_ID MASTERID ,
              A.DISB_PERCENTAGE ,
              A.DISB_AMOUNT ,
              A.PLAFOND
      FROM IFRS_PAYM_SCHD A , TMP_T2 B
      WHERE A.ACC_MSTR_ID = B.MASTERID
      AND A.PMTDATE = B.DOWNLOAD_DATE
    ) B
    ON ( A.MASTERID = B.MASTERID
         AND A.PMT_DATE = A.PREV_PMT_DATE
        )
    WHEN MATCHED
      THEN UPDATE SET
        A.DISB_PERCENTAGE = B.DISB_PERCENTAGE ,
        A.DISB_AMOUNT = B.DISB_AMOUNT ,
        A.PLAFOND = B.PLAFOND ;

    --update disb amount 20160428
    --REMARKS DAHULU KARENA SUDAH DI INSERT DI SP_IFRS_PAYMENT_SCHEDULE 20160524

    --Remarks dulu 20160524
    -- generate schedule for funding product
        EXEC SP_PSAK_FUNDING_PAYM_SCHD
    Remarks dulu 20160524 */

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T1' ;

    INSERT  INTO TMP_T1 ( MASTERID)
    SELECT  MASTERID
    FROM    IFRS_PAYM_CORE_SRC
    WHERE   PREV_PMT_DATE = PMT_DATE
    AND MASTERID IN ( SELECT    B.MASTERID
                      FROM IFRS_ACCT_EIR_CF_ECF B
                      WHERE ((B.TOTAL_AMT <> 0 AND B.TOTAL_AMT_ACRU <> 0 )
                            --  OR B.STAFFLOAN = 1
                              OR (EXISTS (SELECT DISTINCT MASTERID
                                                 FROM IFRS_EVENT_CHANGES C
                                                 WHERE DOWNLOAD_DATE = V_CURRDATE
                                                 AND EVENT_ID = 4
                                                 AND B.MASTERID = C.MASTERID)
                                 )
                              ));


    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_GS_MASTERID' ;

    COMMIT;

    INSERT  INTO IFRS_GS_MASTERID ( MASTERID)
    SELECT  A.MASTERID
    FROM    TMP_T1 A;

    COMMIT;


    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_ACCT_EIR_PAYM';

    COMMIT;

    SELECT  MIN(ID) , MAX(ID)
    INTO V_VMIN_ID, V_VMAX_ID
    FROM IFRS_GS_MASTERID;

    V_VX := V_VMIN_ID ;
    V_VX_INC := 500000;

    WHILE V_VX <= V_VMAX_ID
    LOOP --loop
      INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
      VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_PAYM_CORE_PROCESS' ,TO_CHAR(V_VX));

      EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_PAYM_CORE';

      COMMIT;

      INSERT  INTO IFRS_PAYM_CORE
      ( MASTERID ,
        ACCTNO ,
        PREV_PMT_DATE ,
        PMT_DATE ,
        INT_RATE ,
        I_DAYS ,
        COUNTER ,
        OS_PRN_PREV ,
        PRN_AMT ,
        INT_AMT ,
        OS_PRN ,
        DISB_PERCENTAGE ,
        DISB_AMOUNT ,
        PLAFOND ,
        ICC ,
        GRACE_DATE
      )
      SELECT  MASTERID ,
              ACCTNO ,
              PREV_PMT_DATE ,
              PMT_DATE ,
              INTEREST_RATE,
              I_DAYS ,
              COUNTER ,
              OS_PRN_PREV ,
              PRN_AMT ,
              INT_AMT ,
              OS_PRN ,
              DISB_PERCENTAGE ,
              DISB_AMOUNT ,
              PLAFOND ,
              ICC ,
              GRACE_DATE
      FROM    IFRS_PAYM_CORE_SRC
      WHERE   MASTERID IN ( SELECT    MASTERID
                            FROM      IFRS_GS_MASTERID
                            WHERE     ID >= V_VX
                            AND ID < ( V_VX + V_VX_INC ) );

      SP_IFRS_EXEC_AND_LOG_PROCESS('SP_IFRS_PAYM_CORE_PROCESS_NOP');    -- tanpa efektifisasi

      COMMIT;

      --exec SP_IFRS_PAYM_CORE_PROCESS;    -- dengan efektifisasi
      V_VX := V_VX + V_VX_INC;

    END LOOP; --loop;

    -- insert payment schedule

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_ECF_MAIN' ,'3');


    -- update npv rate for staff loan
    MERGE INTO IFRS_ACCT_EIR_PAYM A
    USING IFRS_ACCT_EIR_CF_ECF B
    ON (A.MASTERID = B.MASTERID
        AND B.TOTAL_AMT = 0
        AND B.TOTAL_AMT_ACRU = 0
        AND B.STAFFLOAN = 1
        AND COALESCE(B.NPV_RATE, 0) > 0
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.NPV_RATE = B.NPV_RATE ;

    COMMIT;



    -- update npv_installment for staff loan
    UPDATE IFRS_ACCT_EIR_PAYM
    SET     NPV_INSTALLMENT = CASE WHEN ROUND(FN_CNT_DAYS_30_360(STARTAMORTDATE,PMT_DATE) / 30,0) = 0
                                   THEN N_INSTALLMENT / ( POWER(1 + NVL(NPV_RATE,0)/ 360 / 100,FN_CNT_DAYS_30_360(STARTAMORTDATE,PMT_DATE)))
                                   ELSE N_INSTALLMENT / NVL(( POWER(1 + NVL(NPV_RATE,0)/ 12 / 100,ROUND(FN_CNT_DAYS_30_360(STARTAMORTDATE,PMT_DATE) / 30,0)) ),0)
                              END
    WHERE   NPV_RATE > 0;


    -- calc staff loan benefit
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_B1' ;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_B2' ;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_B3' ;

    COMMIT;

    -- get os
    INSERT  INTO TMP_B1( MASTERID ,N_OSPRN)
    SELECT  MASTERID ,N_OSPRN
    FROM    IFRS_ACCT_EIR_PAYM
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND PREV_PMT_DATE = PMT_DATE
    AND NPV_RATE > 0;

    COMMIT;


    --get npv sum
    INSERT  INTO TMP_B2
    ( MASTERID ,
      NPV_SUM
    )
    SELECT  MASTERID ,
            SUM(COALESCE(NPV_INSTALLMENT, 0)) AS NPV_SUM
    FROM    IFRS_ACCT_EIR_PAYM
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND NPV_RATE > 0
    GROUP BY MASTERID;

    COMMIT;


    -- get benefit
    INSERT  INTO TMP_B3 ( MASTERID ,N_OSPRN ,NPV_SUM ,BENEFIT)
    SELECT  A.MASTERID ,
            A.N_OSPRN ,
            B.NPV_SUM ,
            B.NPV_SUM - A.N_OSPRN AS BENEFIT
    FROM    TMP_B1 A
    JOIN TMP_B2 B ON B.MASTERID = A.MASTERID;

    COMMIT;


    -- update back
    MERGE INTO IFRS_ACCT_EIR_CF_ECF A
    USING TMP_B3 B
    ON (A.MASTERID = B.MASTERID)
    WHEN MATCHED THEN
    UPDATE
    SET A.BENEFIT=B.BENEFIT;



    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_ECF_MAIN' ,'3a');

    -- insert today cost fee
    INSERT  INTO IFRS_ACCT_EIR_COST_FEE_ECF
    ( DOWNLOAD_DATE ,
      ECFDATE ,
      MASTERID ,
      BRCODE ,
      CIFNO ,
      FACNO ,
      ACCTNO ,
      DATASOURCE ,
      CCY ,
      PRDCODE ,
      TRXCODE ,
      FLAG_CF ,
      FLAG_REVERSE ,
      METHOD ,
      STATUS ,
      SRCPROCESS ,
      AMOUNT ,
      CREATEDDATE ,
      CREATEDBY ,
      SEQ ,
      AMOUNT_ORG ,
      ORG_CCY ,
      ORG_CCY_EXRATE ,
      PRDTYPE ,
      CF_ID
    )
    SELECT  C.DOWNLOAD_DATE ,
            V_CURRDATE ECFDATE ,
            C.MASTERID ,
            C.BRCODE ,
            C.CIFNO ,
            C.FACNO ,
            C.ACCTNO ,
            C.DATASOURCE ,
            C.CCY ,
            C.PRD_CODE ,
            C.TRX_CODE ,
            C.FLAG_CF ,
            C.FLAG_REVERSE ,
            C.METHOD ,
            C.STATUS ,
            C.SRCPROCESS ,
            C.AMOUNT ,
            SYSTIMESTAMP CREATEDDATE ,
            'EIR_ECF_MAIN' CREATEDBY ,
            '' SEQ ,
            C.AMOUNT ,
            C.ORG_CCY ,
            C.ORG_CCY_EXRATE ,
            C.PRD_TYPE ,
            C.CF_ID
    FROM    IFRS_ACCT_COST_FEE C
    JOIN IFRS_ACCT_EIR_CF_ECF B
      ON B.MASTERID = C.MASTERID
      AND B.TOTAL_AMT <> 0
      AND B.TOTAL_AMT_ACRU <> 0
    WHERE   C.DOWNLOAD_DATE = V_CURRDATE
    AND C.MASTERID = B.MASTERID
    AND C.STATUS = 'ACT'
    AND C.METHOD = 'EIR'
    --20180116 exclude cf rev and its pair
    AND C.CF_ID NOT IN (SELECT CF_ID FROM IFRS_ACCT_COST_FEE
                        WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                        UNION ALL
                        SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE
                        WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                       );

    COMMIT;
    --insert unamort
    INSERT  INTO IFRS_ACCT_EIR_COST_FEE_ECF
    ( DOWNLOAD_DATE ,
      ECFDATE ,
      MASTERID ,
      BRCODE ,
      CIFNO ,
      FACNO ,
      ACCTNO ,
      DATASOURCE ,
      CCY ,
      PRDCODE ,
      TRXCODE ,
      FLAG_CF ,
      FLAG_REVERSE ,
      METHOD ,
      STATUS ,
      SRCPROCESS ,
      AMOUNT ,
      CREATEDDATE ,
      CREATEDBY ,
      SEQ ,
      AMOUNT_ORG ,
      ORG_CCY ,
      ORG_CCY_EXRATE ,
      PRDTYPE ,
      CF_ID
    )
    SELECT  C.DOWNLOAD_DATE ,
            V_CURRDATE ECFDATE ,
            C.MASTERID ,
            C.BRCODE ,
            C.CIFNO ,
            C.FACNO ,
            C.ACCTNO ,
            C.DATASOURCE ,
            C.CCY ,
            C.PRDCODE ,
            C.TRXCODE ,
            C.FLAG_CF ,
            C.FLAG_REVERSE ,
            C.METHOD ,
            C.STATUS ,
            C.SRCPROCESS ,
            C.AMOUNT,
            SYSTIMESTAMP CREATEDDATE ,
            'EIR_ECF_MAIN' CREATEDBY ,
            '' SEQ ,
            C.AMOUNT_ORG ,
            C.ORG_CCY ,
            C.ORG_CCY_EXRATE ,
            C.PRDTYPE ,
            C.CF_ID
    FROM    IFRS_ACCT_EIR_COST_FEE_PREV C
    JOIN VW_LAST_EIR_COST_FEE_PREV X
      ON X.MASTERID = C.MASTERID
      AND X.DOWNLOAD_DATE = C.DOWNLOAD_DATE
      AND C.SEQ = X.SEQ
    JOIN IFRS_ACCT_EIR_CF_ECF B
      ON B.MASTERID = C.MASTERID
      AND B.TOTAL_AMT <> 0
      AND B.TOTAL_AMT_ACRU <> 0
    --20160407 eir stop rev
    LEFT JOIN (SELECT DISTINCT MASTERID FROM IFRS_ACCT_EIR_STOP_REV WHERE DOWNLOAD_DATE = V_CURRDATE) A
      ON A.MASTERID = C.MASTERID
    WHERE   C.DOWNLOAD_DATE IN ( V_CURRDATE, V_PREVDATE )
    AND C.STATUS = 'ACT'
    --20160407 eir stop rev
    AND A.MASTERID IS NULL
    --20180116 exclude cf rev and its pair
    AND C.CF_ID NOT IN (SELECT CF_ID FROM IFRS_ACCT_COST_FEE
                        WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                        UNION ALL
                        SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE
                        WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                        )
    --20180426 exclude prevdate if not from sp acf accru
    AND CASE WHEN C.DOWNLOAD_DATE=V_PREVDATE AND C.SEQ<>'2' THEN 0 ELSE 1 END  = 1;

    COMMIT;


    IF V_PARAM_DISABLE_ACCRU_PREV != 0
    THEN
    --masukkan kembali accru prevdate ke cost_fee_ecf
    -- no accru if today is doing amort


      EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T1';

      INSERT  INTO TMP_T1( MASTERID)
      SELECT DISTINCT MASTERID
      FROM    IFRS_ACCT_EIR_ACF
      WHERE   DOWNLOAD_DATE = V_CURRDATE
      AND DO_AMORT = 'Y';

      COMMIT;


      EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T3';

      INSERT  INTO TMP_T3( MASTERID)
      SELECT  MASTERID
      FROM    IFRS_ACCT_EIR_CF_ECF
      WHERE   MASTERID NOT IN ( SELECT MASTERID FROM TMP_T1 );

      COMMIT;
      -- get last acf with do_amort=N
      EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_P1';

      INSERT  INTO TMP_P1( ID)
      SELECT  MAX(ID) AS ID
      FROM    IFRS_ACCT_EIR_ACF
      WHERE   MASTERID IN ( SELECT MASTERID FROM TMP_T3 )
      AND DO_AMORT = 'N'
      AND DOWNLOAD_DATE < V_CURRDATE
      AND DOWNLOAD_DATE >= V_PREVDATE
      GROUP BY MASTERID;

      COMMIT;

      -- get fee summary
      EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_TF' ;

      INSERT  INTO TMP_TF
      ( SUM_AMT ,
        DOWNLOAD_DATE ,
        MASTERID
      )
      SELECT  SUM(A.N_AMOUNT) AS SUM_AMT ,
              A.DOWNLOAD_DATE ,
              A.MASTERID
      FROM    ( SELECT CASE WHEN A.FLAG_REVERSE = 'Y' THEN -1 * A.AMOUNT ELSE A.AMOUNT END AS N_AMOUNT ,
                       A.ECFDATE DOWNLOAD_DATE ,
                       A.MASTERID
                FROM IFRS_ACCT_EIR_COST_FEE_ECF A
                WHERE A.MASTERID IN ( SELECT MASTERID FROM TMP_T3 )
                AND A.STATUS = 'ACT'
                AND A.FLAG_CF = 'F'
                AND A.METHOD = 'EIR'
              ) A
      GROUP BY A.DOWNLOAD_DATE ,
               A.MASTERID;

      COMMIT;
      -- get cost summary
      EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_TC' ;

      INSERT  INTO TMP_TC
      ( SUM_AMT ,
        DOWNLOAD_DATE ,
        MASTERID
      )
      SELECT  SUM(A.N_AMOUNT) AS SUM_AMT ,
              A.DOWNLOAD_DATE ,
              A.MASTERID
      FROM    ( SELECT CASE WHEN A.FLAG_REVERSE = 'Y' THEN -1 * A.AMOUNT ELSE A.AMOUNT END AS N_AMOUNT ,
                       A.ECFDATE DOWNLOAD_DATE ,
                       A.MASTERID
                FROM IFRS_ACCT_EIR_COST_FEE_ECF A
                WHERE A.MASTERID IN ( SELECT MASTERID FROM  TMP_T3 )
                AND A.STATUS = 'ACT'
                AND A.FLAG_CF = 'C'
                AND A.METHOD = 'EIR'
              ) A
      GROUP BY A.DOWNLOAD_DATE ,
               A.MASTERID;


      INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
      VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_ECF_MAIN' ,'3b');

      COMMIT;

      --insert fee 1
      INSERT  INTO IFRS_ACCT_EIR_COST_FEE_ECF
      ( FACNO ,
        CIFNO ,
        DOWNLOAD_DATE ,
        ECFDATE ,
        DATASOURCE ,
        PRDCODE ,
        TRXCODE ,
        CCY ,
        AMOUNT ,
        STATUS ,
        CREATEDDATE ,
        ACCTNO ,
        MASTERID ,
        FLAG_CF ,
        FLAG_REVERSE ,
        SRCPROCESS ,
        ORG_CCY ,
        ORG_CCY_EXRATE ,
        PRDTYPE ,
        CF_ID ,
        BRCODE ,
        METHOD
      )
      SELECT  A.FACNO ,
              A.CIFNO ,
              V_CURRDATE ,
              V_CURRDATE ECFDATE ,
              A.DATASOURCE ,
              B.PRDCODE ,
              B.TRXCODE ,
              B.CCY ,
              ROUND(CAST(CAST(CASE WHEN B.FLAG_REVERSE = 'Y' THEN -1 * B.AMOUNT ELSE B.AMOUNT END AS BINARY_DOUBLE)
                    / NVL(CAST(C.SUM_AMT AS BINARY_DOUBLE),0) AS NUMBER(32,20)) * A.N_ACCRU_FEE * -1,V_ROUND) AS N_AMOUNT ,
              B.STATUS ,
              SYSTIMESTAMP ,
              A.ACCTNO ,
              A.MASTERID ,
              B.FLAG_CF ,
              'N' ,
              'ECFACCRU' ,
              B.ORG_CCY ,
              B.ORG_CCY_EXRATE ,
              B.PRDTYPE ,
              B.CF_ID ,
              B.BRCODE ,
              B.METHOD
      FROM    IFRS_ACCT_EIR_ACF A
      JOIN IFRS_ACCT_EIR_COST_FEE_ECF B
        ON B.ECFDATE = A.ECFDATE
        AND A.MASTERID = B.MASTERID
        AND B.FLAG_CF = 'F'
        AND B.STATUS = 'ACT'
        AND A.MASTERID NOT IN (
            SELECT DISTINCT MASTERID
            FROM IFRS_ACCT_SWITCH
            WHERE DOWNLOAD_DATE = V_CURRDATE
        )
      JOIN TMP_TF C
        ON C.DOWNLOAD_DATE = A.ECFDATE
        AND C.MASTERID = A.MASTERID
      --20160407 eir stop rev
      LEFT JOIN (SELECT DISTINCT MASTERID FROM IFRS_ACCT_EIR_STOP_REV WHERE DOWNLOAD_DATE = V_CURRDATE) D
        ON A.MASTERID = D.MASTERID
      WHERE   A.ID IN ( SELECT ID FROM TMP_P1 )
      --20160407 eir stop rev
      AND D.MASTERID IS NULL
      --20180116 exclude cf rev and its pair
      AND B.CF_ID NOT IN (SELECT CF_ID FROM IFRS_ACCT_COST_FEE
                          WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                          UNION ALL
                          SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE
                          WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                          ) ;
      COMMIT;

      INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
      VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_ECF_MAIN' ,'3c');

      COMMIT;

      --cost 1
      INSERT  INTO IFRS_ACCT_EIR_COST_FEE_ECF
      ( FACNO ,
        CIFNO ,
        DOWNLOAD_DATE ,
        ECFDATE ,
        DATASOURCE ,
        PRDCODE ,
        TRXCODE ,
        CCY ,
        AMOUNT ,
        STATUS ,
        CREATEDDATE ,
        ACCTNO ,
        MASTERID ,
        FLAG_CF ,
        FLAG_REVERSE ,
        SRCPROCESS ,
        ORG_CCY ,
        ORG_CCY_EXRATE ,
        PRDTYPE ,
        CF_ID ,
        BRCODE ,
        METHOD
      )
      SELECT  A.FACNO ,
              A.CIFNO ,
              V_CURRDATE ,
              V_CURRDATE ECFDATE ,
              A.DATASOURCE ,
              B.PRDCODE ,
              B.TRXCODE ,
              B.CCY ,
              ROUND(CAST(CAST(CASE WHEN B.FLAG_REVERSE = 'Y' THEN -1 * B.AMOUNT ELSE B.AMOUNT END AS BINARY_DOUBLE)
                    / NVL(CAST(C.SUM_AMT AS BINARY_DOUBLE),0) AS NUMBER(32,20))* A.N_ACCRU_COST * -1,V_ROUND) AS N_AMOUNT ,
              B.STATUS ,
              SYSTIMESTAMP ,
              A.ACCTNO ,
              A.MASTERID ,
              B.FLAG_CF ,
              'N' ,
              'ECFACCRU' ,
              B.ORG_CCY ,
              B.ORG_CCY_EXRATE ,
              B.PRDTYPE ,
              B.CF_ID ,
              B.BRCODE ,
              B.METHOD
      FROM    IFRS_ACCT_EIR_ACF A
      JOIN IFRS_ACCT_EIR_COST_FEE_ECF B
        ON B.ECFDATE = A.ECFDATE
        AND A.MASTERID = B.MASTERID
        AND B.FLAG_CF = 'C'
        AND B.STATUS = 'ACT'
        AND A.MASTERID NOT IN
        (
            SELECT DISTINCT MASTERID
            FROM IFRS_ACCT_SWITCH
            WHERE DOWNLOAD_DATE = V_CURRDATE
        )
      JOIN TMP_TC C
        ON C.DOWNLOAD_DATE = A.ECFDATE
        AND C.MASTERID = A.MASTERID
      --20160407 eir stop rev
      LEFT JOIN (SELECT DISTINCT MASTERID FROM IFRS_ACCT_EIR_STOP_REV WHERE DOWNLOAD_DATE = V_CURRDATE) D
        ON A.MASTERID = D.MASTERID
      WHERE   A.ID IN ( SELECT ID FROM TMP_P1 )
      --20160407 eir stop rev
      AND D.MASTERID IS NULL
      --20180108 exclude cf rev and its pair
      AND B.CF_ID NOT IN (SELECT CF_ID FROM IFRS_ACCT_COST_FEE
                          WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                          UNION ALL
                          SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE
                          WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                          ) ;
      COMMIT;

    END IF; --masukkan kembali accru prevdate ke cost_fee_ecf


    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_ECF_MAIN' ,'3d');

    -- 20160412 group multiple rows by cf_id
 --   EXECUTE IMMEDIATE  'SP_IFRS_EXEC_AND_LOG_PROCESS''SP_IFRS_ACCT_EIR_CF_ECF_GRP''';

    SP_IFRS_EXEC_AND_LOG_PROCESS('SP_IFRS_ACCT_EIR_CF_ECF_GRP');


    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_ECF_MAIN' ,'4 GS START');

    COMMIT;

    DELETE  FROM IFRS_ACCT_EIR_FAILED_GOAL_SEEK
    WHERE   DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    DELETE  FROM IFRS_ACCT_EIR_GOAL_SEEK_RESULT
    WHERE   DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_ACCT_EIR_CF_ECF1';

    INSERT  INTO IFRS_ACCT_EIR_CF_ECF1
    ( MASTERID ,
      FEE_AMT ,
      COST_AMT ,
      BENEFIT ,
      STAFFLOAN ,
      PREV_EIR
      --20180226 copy data
      , TOTAL_AMT    --20180517  add Yacop
      ,NEW_FEE_AMT,NEW_COST_AMT,NEW_TOTAL_AMT,GAIN_LOSS_CALC
    )
    SELECT  B.MASTERID ,
            B.FEE_AMT ,
            B.COST_AMT ,
            B.BENEFIT ,
            B.STAFFLOAN ,
            B.PREV_EIR,
            B.TOTAL_AMT   --20180517  add Yacop
            ,NEW_FEE_AMT,NEW_COST_AMT,NEW_TOTAL_AMT,GAIN_LOSS_CALC
    FROM    IFRS_ACCT_EIR_CF_ECF B
    WHERE   ( B.TOTAL_AMT <> 0AND B.TOTAL_AMT_ACRU <> 0)
               OR ( B.STAFFLOAN = 1 AND B.PREV_EIR IS NULL)
              --20170927, ivan nocf
              OR (B.MASTERID IN (SELECT DISTINCT MASTERID FROM IFRS_EVENT_CHANGES
                                 WHERE DOWNLOAD_DATE = V_CURRDATE
                                 AND EVENT_ID = 4)
                 );
    COMMIT;

    -- 20180821 DISABLE FOR BCA, PINDAH KE SP SP_IFRS_LBM_....

    --START: goal seek prepare staffloan benefit
    -- put before remark -- goal seek prepare SP_IFRS_ACCT_EIR_ECF_MAIN
    -- result benefit=unamort-gloss get from table IFRS_ACCT_EIR_GOALSEEK_RESULT3
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_GS_MASTERID' ;
    --clean up

    DELETE  FROM IFRS_ACCT_EIR_GS_RESULT3
    WHERE   DOWNLOAD_DATE = V_CURRDATE;

    DELETE  FROM IFRS_ACCT_EIR_FAILED_GS3
    WHERE   DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    --only process staffloan with no running amortization
    INSERT  INTO IFRS_GS_MASTERID( MASTERID)
    SELECT  A.MASTERID
    FROM    ( SELECT    MASTERID ,
                        PERIOD
              FROM      IFRS_ACCT_EIR_PAYM
              WHERE     PREV_PMT_DATE = PMT_DATE
              AND MASTERID IN (SELECT  MASTERID
                               FROM    IFRS_ACCT_EIR_CF_ECF1
                               WHERE   /* --20190122 VIVI ( STAFFLOAN = 1 AND PREV_EIR IS NULL )
                                        OR */
                                        GAIN_LOSS_CALC='Y' --20180226 prepayment
                              )
            ) A
    ORDER BY PERIOD;

    COMMIT;


    SELECT  MIN(ID) INTO V_VMIN_ID
    FROM    IFRS_GS_MASTERID;

    SELECT  MAX(ID) INTO V_VMAX_ID
    FROM    IFRS_GS_MASTERID;

    V_VX := V_VMIN_ID;

    V_VX_INC := 500000;

    COMMIT;

    WHILE V_VX <= V_VMAX_ID
    LOOP --loop

          INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
          VALUES  (V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_PAYM_GS_RANGE' ,TO_CHAR(V_VX));

          V_ID2 := V_VX + V_VX_INC - 1;

          SP_IFRS_ACCT_EIR_PAYM_GS_RANGE (V_VX, V_ID2);

          INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
          VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_PAYM_GS_RANGE' ,'DONE');

          SP_IFRS_EXEC_AND_LOG_PROCESS('SP_IFRS_ACCT_EIR_GS_PROC3');

          V_VX := V_VX + V_VX_INC;

    END LOOP; --loop;
    /*20190122 VIVI
    -- update back result to IFRS_ACCT_EIR_CF_ECF1
    --20180226 only for staff loan


    MERGE INTO IFRS_ACCT_EIR_CF_ECF1 A
    USING  IFRS_ACCT_EIR_GOALSEEK_RESULT3 B
    ON (A.MASTERID=B.MASTERID
        AND B.DOWNLOAD_DATE=V_CURRDATE
        AND A.STAFFLOAN=1
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.BENEFIT=B.UNAMORT - B.GLOSS;


    MERGE INTO IFRS_ACCT_EIR_CF_ECF A
    USING IFRS_ACCT_EIR_GOALSEEK_RESULT3 B
    ON (A.MASTERID=B.MASTERID
        AND B.DOWNLOAD_DATE=V_CURRDATE
        AND A.STAFFLOAN=1
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.BENEFIT=B.UNAMORT - B.GLOSS;

    COMMIT;
    */
    --20180226 update for partial payment
    MERGE INTO IFRS_ACCT_EIR_CF_ECF1 A
    USING IFRS_ACCT_EIR_GS_RESULT3 B
    ON (B.MASTERID = A.MASTERID
            AND B.DOWNLOAD_DATE = V_CURRDATE
            AND A.STAFFLOAN=0
            AND A.GAIN_LOSS_CALC='Y'
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.GAIN_LOSS_AMT=ROUND(B.GLOSS,V_ROUND)
       ,A.GAIN_LOSS_FEE_AMT= CASE WHEN A.FEE_AMT<>0 AND A.COST_AMT=0 THEN ROUND(B.GLOSS,V_ROUND)
                                  WHEN A.FEE_AMT=0 AND A.COST_AMT<>0 THEN 0
                                  ELSE ROUND(B.GLOSS * A.FEE_AMT / A.TOTAL_AMT,V_ROUND)
                             END
       ,A.GAIN_LOSS_COST_AMT= CASE WHEN A.FEE_AMT=0 AND A.COST_AMT<>0 THEN ROUND(B.GLOSS,V_ROUND)
                                   WHEN A.FEE_AMT<>0 AND A.COST_AMT=0 THEN 0
                                   ELSE ROUND(B.GLOSS,V_ROUND) - ROUND(B.GLOSS * A.FEE_AMT / A.TOTAL_AMT,V_ROUND)
                              END;


    MERGE INTO IFRS_ACCT_EIR_CF_ECF A
    USING IFRS_ACCT_EIR_GS_RESULT3 B
    ON (B.MASTERID = A.MASTERID
        AND B.DOWNLOAD_DATE = V_CURRDATE
            AND A.STAFFLOAN=0
            AND A.GAIN_LOSS_CALC='Y'
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.GAIN_LOSS_AMT=ROUND(B.GLOSS,V_ROUND)
       ,A.GAIN_LOSS_FEE_AMT= CASE WHEN A.FEE_AMT<>0 AND A.COST_AMT=0 THEN ROUND(B.GLOSS,V_ROUND)
                                  WHEN A.FEE_AMT=0 AND A.COST_AMT<>0 THEN 0
                                  ELSE ROUND(B.GLOSS * A.FEE_AMT / A.TOTAL_AMT,V_ROUND)
                             END
       ,A.GAIN_LOSS_COST_AMT= CASE WHEN A.FEE_AMT=0 AND A.COST_AMT<>0 THEN ROUND(B.GLOSS,V_ROUND)
                                   WHEN A.FEE_AMT<>0 AND A.COST_AMT=0 THEN 0
                                   ELSE ROUND(B.GLOSS,V_ROUND) - ROUND(B.GLOSS * A.FEE_AMT / A.TOTAL_AMT,V_ROUND)
                              END;
    /* 20190122 VIVI
    --ridwan  20 aug 2015  insert benefit after get benefit
    --insert benefit
    -- get os
    -- calc staff loan benefit

    COMMIT;


    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_B1';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_B2';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_B3';


    INSERT  INTO TMP_B1( MASTERID ,N_OSPRN)
    SELECT  MASTERID ,N_OSPRN
    FROM    IFRS_ACCT_EIR_PAYM
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND PREV_PMT_DATE = PMT_DATE
    AND NPV_RATE > 0;


    COMMIT;

    --get npv sum
    INSERT  INTO TMP_B2( MASTERID ,NPV_SUM)
    SELECT  A.MASTERID ,
            ( COALESCE(A.N_OSPRN, 0) + COALESCE(BENEFIT, 0) ) AS NPV
    FROM    TMP_B1 A
    JOIN IFRS_ACCT_EIR_CF_ECF B ON A.MASTERID = B.MASTERID
    JOIN IFRS_ACCT_EIR_GOALSEEK_RESULT3 C ON A.MASTERID = C.MASTERID
    WHERE   C.DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;


    -- get benefit
    INSERT  INTO TMP_B3
    ( MASTERID ,
      N_OSPRN ,
      NPV_SUM ,
      BENEFIT
    )
    SELECT  A.MASTERID ,
            A.N_OSPRN ,
            B.NPV_SUM ,
            B.NPV_SUM - A.N_OSPRN AS BENEFIT
    FROM    TMP_B1 A
    JOIN TMP_B2 B ON B.MASTERID = A.MASTERID;

    COMMIT;


    -- update back
    MERGE INTO IFRS_ACCT_EIR_CF_ECF A
    USING  TMP_B3 B
    ON ( A.MASTERID=B.MASTERID
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.BENEFIT=B.BENEFIT;

    COMMIT;


    INSERT  INTO IFRS_ACCT_EIR_COST_FEE_ECF
    ( DOWNLOAD_DATE ,
      ECFDATE ,
      MASTERID ,
      BRCODE ,
      CIFNO ,
      FACNO ,
      ACCTNO ,
      DATASOURCE ,
      CCY ,
      PRDCODE ,
      TRXCODE ,
      FLAG_CF ,
      FLAG_REVERSE ,
      METHOD ,
      STATUS ,
      SRCPROCESS ,
      AMOUNT ,
      CREATEDDATE ,
      CREATEDBY ,
      SEQ ,
      AMOUNT_ORG ,
      ORG_CCY ,
      ORG_CCY_EXRATE ,
      PRDTYPE ,
      CF_ID
    )
    SELECT  V_CURRDATE ,
            V_CURRDATE ,
            A.MASTERID ,
            M.BRANCH_CODE ,
            M.CUSTOMER_NUMBER ,
            M.FACILITY_NUMBER ,
            M.ACCOUNT_NUMBER ,
            M.DATA_SOURCE ,
            M.CURRENCY ,
            M.PRODUCT_CODE ,
            'BENEFIT' ,
            CASE WHEN A.BENEFIT < 0 THEN 'F' ELSE 'C' END ,
            'N' ,
            'EIR' ,
            'ACT' ,
            'STAFFLOAN' ,
            A.BENEFIT ,
            SYSTIMESTAMP CREATEDDATE ,
            'EIR_ECF_MAIN' CREATEDBY ,
            '' SEQ ,
            A.BENEFIT ,
            M.CURRENCY ,
            1 ,
            M.PRODUCT_TYPE ,
            0 AS CF_ID
    FROM    TMP_B3 A
    JOIN IFRS_IMA_AMORT_CURR M ON M.MASTERID = A.MASTERID
    JOIN IFRS_ACCT_EIR_CF_ECF C
      ON C.MASTERID = A.MASTERID
      AND C.PREV_EIR IS NULL;  -- no prev ecf then insert

    COMMIT;


    UPDATE  IFRS_ACCT_EIR_COST_FEE_ECF
    SET     CF_ID = ID
    WHERE   CF_ID = 0
      AND SRCPROCESS = 'STAFFLOAN'
      AND DOWNLOAD_DATE = V_CURRDATE;
    --END: goal seek prepare staffloan benefit

    COMMIT;
    --20180821 DISABLE BCA, PINDAH KE SP_IFRS_LBM...
    */
    --start goalseek cf AND no cf
    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_ECF_MAIN' ,'4 GS START');
     COMMIT;
    DELETE IFRS_ACCT_EIR_ECF_NOCF WHERE DOWNLOAD_DATE >= V_CURRDATE ;  -- clean up
    COMMIT;
    DELETE IFRS_ACCT_EIR_GOALSEEK_RESULT4 WHERE DOWNLOAD_DATE = V_CURRDATE ;
    COMMIT;
    DELETE IFRS_ACCT_EIR_FAILED_GOALSEEK4 WHERE DOWNLOAD_DATE = V_CURRDATE ;
    COMMIT;
    DELETE IFRS_ACCT_EIR_GOAL_SEEK_RESULT WHERE DOWNLOAD_DATE = V_CURRDATE;
    COMMIT;
    DELETE IFRS_ACCT_EIR_FAILED_GOAL_SEEK WHERE DOWNLOAD_DATE = V_CURRDATE;
    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_GS_MASTERID';

    COMMIT;

    INSERT  INTO IFRS_GS_MASTERID ( MASTERID )
    SELECT  A.MASTERID
    FROM    ( SELECT    MASTERID ,
                        PERIOD
              FROM      IFRS_ACCT_EIR_PAYM
              WHERE     PREV_PMT_DATE = PMT_DATE
              ) A
    ORDER BY PERIOD;

    COMMIT;

    SELECT  MIN(ID) INTO V_VMIN_ID
    FROM    IFRS_GS_MASTERID;

    SELECT  MAX(ID) INTO V_VMAX_ID
    FROM    IFRS_GS_MASTERID ;

    V_VX := V_VMIN_ID;
    V_VX_INC := 500000;

    COMMIT;


    WHILE V_VX <= V_VMAX_ID
    LOOP --loop

      INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
      VALUES  (V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_PAYM_GS_RANGE' ,TO_CHAR(V_VX));

      COMMIT;

      V_ID2 := V_VX + V_VX_INC - 1;

      SP_IFRS_ACCT_EIR_PAYM_GS_RANGE(V_VX, V_ID2);

      COMMIT;

      INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
      VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_PAYM_GS_RANGE' ,'DONE');

      COMMIT;

      SP_IFRS_EXEC_AND_LOG_PROCESS('SP_IFRS_ACCT_EIR_GS_PRCS_ALL');

      V_VX := V_VX + V_VX_INC;

      COMMIT;

    END LOOP; --loop;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_ECF_MAIN' ,'5 GS END');

    COMMIT;

    SP_IFRS_EXEC_AND_LOG_PROCESS('SP_IFRS_ACCT_EIR_GS_ECF_INSER4');

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_ECF_MAIN' ,'6 ECFNOCF INSERT');

    COMMIT;

    SP_IFRS_EXEC_AND_LOG_PROCESS('SP_IFRS_ACCT_EIR_ECF_ALIGN4') ;


    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_ECF_MAIN' ,'7 ECFNOCF ALIGNED');

    COMMIT;

    /* remarks
    --UPDATE PNL IF FAILED GOAL SEEK 20160524
    UPDATE  IFRS_ACCT_COST_FEE
    SET     status = 'PNL' ,
            createdby = 'EIRECF3'
    WHERE   DOWNLOAD_DATE = @v_currdate
            AND masterid IN ( SELECT    masterid
                              FROM      IFRS_ACCT_EIR_FAILED_GOAL_SEEK
                              WHERE     DOWNLOAD_DATE = @v_currdate )
            AND status = 'ACT'
    */

    SP_IFRS_EXEC_AND_LOG_PROCESS('SP_IFRS_ACCT_EIR_GS_ECF_INSERT');

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_ECF_MAIN' ,'8 ECF INSERTED');

    COMMIT;

    SP_IFRS_EXEC_AND_LOG_PROCESS('SP_IFRS_ACCT_EIR_ECF_ALIGN');

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_ECF_MAIN' ,'9 ECF ALIGNED');

    COMMIT;

    -- merge ecf for masterid with different interest structure

    SP_IFRS_EXEC_AND_LOG_PROCESS('SP_IFRS_ACCT_EIR_ECF_MERGE');

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_EIR_ECF_MAIN' ,'10 ECF MERGED');

    COMMIT;

    -- get all master id of newly generated eir ecf
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T1';

    INSERT  INTO TMP_T1( MASTERID)
    SELECT DISTINCT MASTERID
    FROM    IFRS_ACCT_EIR_ECF
    WHERE   DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;


    --filter out not today stopped ecf
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T2';

    INSERT  INTO TMP_T2( MASTERID)
    SELECT DISTINCT A.MASTERID
    FROM    TMP_T1 A
    JOIN IFRS_ACCT_EIR_ECF B
      ON B.PREV_PMT_DATE = B.PMT_DATE
      AND B.AMORTSTOPDATE = V_CURRDATE
      AND B.MASTERID = A.MASTERID
    UNION        -- 20171016 also include account with zero amount (fix chkamort on due_date change when end_amort_dt - 1)
    SELECT MASTERID
    FROM IFRS_ACCT_EIR_CF_ECF
    WHERE TOTAL_AMT = 0 OR TOTAL_AMT_ACRU = 0;

    COMMIT;

    -- insert accru values for newly generated ecf
    -- no accru if today is doing amort
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T1';

    INSERT  INTO TMP_T1( MASTERID)
    SELECT DISTINCT MASTERID
    FROM    IFRS_ACCT_EIR_ACF
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND DO_AMORT = 'Y';

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T3';

    INSERT  INTO TMP_T3( MASTERID)
    SELECT  MASTERID
    FROM    TMP_T2
    WHERE   MASTERID NOT IN ( SELECT MASTERID FROM TMP_T1
                              UNION /*15 FEB 2019 NOT INCLUDE FOR PINDAH CABANG*/
                              SELECT MASTERID FROM IFRS_ACCT_SWITCH WHERE DOWNLOAD_DATE=V_CURRDATE AND PREV_BRCODE<>BRCODE AND PREV_EIR_ECF='Y');

    COMMIT;

    IF V_PARAM_DISABLE_ACCRU_PREV = 0
    THEN
    -- get last acf with do_amort=N
      EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_P1';

      INSERT  INTO TMP_P1 ( ID )
      SELECT  MAX(ID) AS ID
      FROM    IFRS_ACCT_EIR_ACF
      WHERE   MASTERID IN ( SELECT MASTERID FROM TMP_T3 )
      AND DO_AMORT = 'N'
      AND DOWNLOAD_DATE < V_CURRDATE
      AND DOWNLOAD_DATE >= V_PREVDATE
      GROUP BY MASTERID;
      COMMIT;


      -- get fee summary
      EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_TF' ;

      INSERT  INTO TMP_TF
      ( SUM_AMT ,
        DOWNLOAD_DATE ,
        MASTERID
      )
      SELECT  SUM(A.N_AMOUNT) AS SUM_AMT ,
              A.DOWNLOAD_DATE ,
              A.MASTERID
      FROM ( SELECT CASE WHEN A.FLAG_REVERSE = 'Y' THEN -1 * A.AMOUNT ELSE A.AMOUNT END AS N_AMOUNT ,
                    A.ECFDATE DOWNLOAD_DATE ,
                    A.MASTERID
             FROM IFRS_ACCT_EIR_COST_FEE_ECF A
             WHERE A.MASTERID IN ( SELECT MASTERID FROM  TMP_T3 )
             AND A.STATUS = 'ACT'
             AND A.FLAG_CF = 'F'
             AND A.METHOD = 'EIR'
          ) A
      GROUP BY A.DOWNLOAD_DATE ,
               A.MASTERID;

      COMMIT;


      -- get cost summary
      EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_TC';

      INSERT  INTO TMP_TC
      ( SUM_AMT ,
        DOWNLOAD_DATE ,
        MASTERID
      )
      SELECT  SUM(A.N_AMOUNT) AS SUM_AMT ,
              A.DOWNLOAD_DATE ,
              A.MASTERID
      FROM ( SELECT CASE WHEN A.FLAG_REVERSE = 'Y' THEN -1 * A.AMOUNT ELSE A.AMOUNT END AS N_AMOUNT ,
                    A.ECFDATE DOWNLOAD_DATE ,
                    A.MASTERID
             FROM IFRS_ACCT_EIR_COST_FEE_ECF A
             WHERE A.MASTERID IN ( SELECT MASTERID FROM  TMP_T3 )
             AND A.STATUS = 'ACT'
             AND A.FLAG_CF = 'C'
             AND A.METHOD = 'EIR'
           ) A
      GROUP BY A.DOWNLOAD_DATE ,
               A.MASTERID;
      COMMIT;

      --insert fee 1
      INSERT  INTO IFRS_ACCT_EIR_ACCRU_PREV
      ( FACNO ,
        CIFNO ,
        DOWNLOAD_DATE ,
        ECFDATE ,
        DATASOURCE ,
        PRDCODE ,
        TRXCODE ,
        CCY ,
        AMOUNT ,
        STATUS ,
        CREATEDDATE ,
        ACCTNO ,
        MASTERID ,
        FLAG_CF ,
        FLAG_REVERSE ,
        AMORTDATE ,
        SRCPROCESS ,
        ORG_CCY ,
        ORG_CCY_EXRATE ,
        PRDTYPE ,
        CF_ID ,
        METHOD
      )
      SELECT  A.FACNO ,
              A.CIFNO ,
              V_CURRDATE ,
              A.ECFDATE ,
              A.DATASOURCE ,
              B.PRDCODE ,
              B.TRXCODE ,
              B.CCY ,
              ROUND(CAST(CAST(CASE WHEN B.FLAG_REVERSE = 'Y' THEN -1 * B.AMOUNT ELSE B.AMOUNT END AS BINARY_DOUBLE)
                    / NVL(CAST(C.SUM_AMT AS BINARY_DOUBLE),0) AS NUMBER(32,20)) * A.N_ACCRU_FEE,V_ROUND) AS N_AMOUNT ,
              B.STATUS ,
              SYSTIMESTAMP ,
              A.ACCTNO ,
              A.MASTERID ,
              B.FLAG_CF ,
              'N' ,
              NULL AS AMORTDATE ,
              'ECF' ,
              B.ORG_CCY ,
              B.ORG_CCY_EXRATE ,
              B.PRDTYPE ,
              B.CF_ID ,
              B.METHOD
      FROM IFRS_ACCT_EIR_ACF A
      JOIN IFRS_ACCT_EIR_COST_FEE_ECF B
        ON B.ECFDATE = A.ECFDATE
        AND A.MASTERID = B.MASTERID
        AND B.FLAG_CF = 'F'
      JOIN TMP_TF C
        ON C.DOWNLOAD_DATE = A.ECFDATE
        AND C.MASTERID = A.MASTERID
      --20160407 eir stop rev
      LEFT JOIN (SELECT DISTINCT MASTERID FROM IFRS_ACCT_EIR_STOP_REV WHERE DOWNLOAD_DATE = V_CURRDATE) D
        ON A.MASTERID = D.MASTERID
      WHERE   A.ID IN ( SELECT ID FROM  TMP_P1 )
      --20160407 eir stop rev
      AND D.MASTERID IS NULL
      --20180108 exclude cf rev and its pair
      AND B.CF_ID NOT IN (SELECT CF_ID FROM IFRS_ACCT_COST_FEE
                          WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                          UNION ALL
                          SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE
                          WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                          ) ;

      COMMIT;

      --cost 1
      INSERT  INTO IFRS_ACCT_EIR_ACCRU_PREV
      ( FACNO ,
        CIFNO ,
        DOWNLOAD_DATE ,
        ECFDATE ,
        DATASOURCE ,
        PRDCODE ,
        TRXCODE ,
        CCY ,
        AMOUNT ,
        STATUS ,
        CREATEDDATE ,
        ACCTNO ,
        MASTERID ,
        FLAG_CF ,
        FLAG_REVERSE ,
        AMORTDATE ,
        SRCPROCESS ,
        ORG_CCY ,
        ORG_CCY_EXRATE ,
        PRDTYPE ,
        CF_ID ,
        METHOD
      )
      SELECT  A.FACNO ,
              A.CIFNO ,
              V_CURRDATE ,
              A.ECFDATE ,
              A.DATASOURCE ,
              B.PRDCODE ,
              B.TRXCODE ,
              B.CCY ,
              ROUND(CAST(CAST(CASE WHEN B.FLAG_REVERSE = 'Y' THEN -1 * B.AMOUNT ELSE B.AMOUNT END AS BINARY_DOUBLE)
                    / NVL(CAST(C.SUM_AMT AS BINARY_DOUBLE),0) AS NUMBER(32,20)) * A.N_ACCRU_COST,V_ROUND) AS N_AMOUNT ,
              B.STATUS ,
              SYSTIMESTAMP ,
              A.ACCTNO ,
              A.MASTERID ,
              B.FLAG_CF ,
              'N' ,
              NULL AS AMORTDATE ,
              'ECF' ,
              B.ORG_CCY ,
              B.ORG_CCY_EXRATE ,
              B.PRDTYPE ,
              B.CF_ID ,
              B.METHOD
      FROM    IFRS_ACCT_EIR_ACF A
      JOIN IFRS_ACCT_EIR_COST_FEE_ECF B
        ON B.ECFDATE = A.ECFDATE
        AND A.MASTERID = B.MASTERID
        AND B.FLAG_CF = 'C'
      JOIN TMP_TC C
        ON C.DOWNLOAD_DATE = A.ECFDATE
        AND C.MASTERID = A.MASTERID
      --20160407 eir stop rev
      LEFT JOIN (SELECT DISTINCT MASTERID FROM IFRS_ACCT_EIR_STOP_REV WHERE DOWNLOAD_DATE = V_CURRDATE) D
        ON A.MASTERID = D.MASTERID
      WHERE   A.ID IN ( SELECT ID FROM TMP_P1 )
      --20160407 eir stop rev
      AND D.MASTERID IS NULL
      --20180108 exclude cf rev and its pair
      AND B.CF_ID NOT IN (SELECT CF_ID FROM IFRS_ACCT_COST_FEE
                          WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                          UNION ALL
                          SELECT CF_ID_REV FROM IFRS_ACCT_COST_FEE
                          WHERE DOWNLOAD_DATE=V_CURRDATE AND FLAG_REVERSE='Y' AND CF_ID_REV IS NOT NULL
                          ) ;
      COMMIT;

    END IF; --if;

    -- 20171016 mark for do amort acru (fix chkamort on due_date change when end_amort_dt - 1)
    UPDATE IFRS_ACCT_EIR_ACCRU_PREV
    SET STATUS = TO_CHAR (V_CURRDATE,'YYYYMMDD')
    WHERE STATUS = 'ACT'
    AND MASTERID IN (SELECT MASTERID FROM IFRS_ACCT_EIR_CF_ECF WHERE TOTAL_AMT = 0 OR TOTAL_AMT_ACRU = 0);

    COMMIT;


    --20180226 insert gain loss
    -- get fee summary with ecfdate=@currdate
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_TF';

    INSERT  INTO TMP_TF
    ( SUM_AMT ,
      DOWNLOAD_DATE ,
      MASTERID
    )
    SELECT  SUM(A.N_AMOUNT) AS SUM_AMT ,
            A.DOWNLOAD_DATE ,
            A.MASTERID
    FROM    ( SELECT CASE WHEN A.FLAG_REVERSE = 'Y' THEN -1 * A.AMOUNT ELSE A.AMOUNT END AS N_AMOUNT ,
                     A.ECFDATE DOWNLOAD_DATE ,
                     A.MASTERID
              FROM      IFRS_ACCT_EIR_COST_FEE_ECF A
              WHERE     A.ECFDATE=V_CURRDATE
              AND A.STATUS = 'ACT'
              AND A.FLAG_CF = 'F'
              AND A.METHOD = 'EIR'
    ) A
    GROUP BY A.DOWNLOAD_DATE ,
             A.MASTERID;
    COMMIT;


    -- get cost summary with ecfdate=@currdate
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_TC';

    INSERT  INTO TMP_TC
    ( SUM_AMT ,
      DOWNLOAD_DATE ,
      MASTERID
    )
    SELECT  SUM(A.N_AMOUNT) AS SUM_AMT ,
            A.DOWNLOAD_DATE ,
            A.MASTERID
    FROM ( SELECT CASE WHEN A.FLAG_REVERSE = 'Y' THEN -1 * A.AMOUNT ELSE A.AMOUNT END AS N_AMOUNT ,
                  A.ECFDATE DOWNLOAD_DATE ,
                  A.MASTERID
           FROM      IFRS_ACCT_EIR_COST_FEE_ECF A
           WHERE     A.ECFDATE=V_CURRDATE
           AND A.STATUS = 'ACT'
           AND A.FLAG_CF = 'C'
           AND A.METHOD = 'EIR'
            ) A
    GROUP BY A.DOWNLOAD_DATE ,
             A.MASTERID;

    COMMIT;


    --201801417 clean up gain loss
    DELETE  FROM IFRS_ACCT_EIR_GAIN_LOSS
    WHERE DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    --insert fee gain loss
    INSERT  INTO IFRS_ACCT_EIR_GAIN_LOSS
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      ECFDATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      AMOUNT ,
      STATUS ,
      CREATEDDATE ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      FLAG_REVERSE ,
      AMORTDATE ,
      SRCPROCESS ,
      ORG_CCY ,
      ORG_CCY_EXRATE ,
      PRDTYPE ,
      CF_ID ,
      METHOD
    )
    SELECT  IMA.FACILITY_NUMBER ,
            IMA.CUSTOMER_NUMBER ,
            V_CURRDATE ,
            V_CURRDATE ,
            IMA.DATA_SOURCE ,
            B.PRDCODE ,
            B.TRXCODE ,
            B.CCY ,
            -1 * --20180417 gain loss dibalik
              ROUND(CASE WHEN B.FLAG_REVERSE = 'Y' THEN -1 * B.AMOUNT ELSE B.AMOUNT END / IMA.SUM_AMT
              * A.GAIN_LOSS_FEE_AMT,V_ROUND)  AS N_AMOUNT ,
            B.STATUS ,
            SYSTIMESTAMP ,
            IMA.ACCOUNT_NUMBER ,
            A.MASTERID ,
            B.FLAG_CF ,
            'N' ,
            NULL AS AMORTDATE ,
            'ECF' ,
            B.ORG_CCY ,
            B.ORG_CCY_EXRATE ,
            B.PRDTYPE ,
            B.CF_ID ,
            B.METHOD
    FROM    IFRS_ACCT_EIR_CF_ECF A
    JOIN (SELECT A2.MASTERID,
            A2.FACILITY_NUMBER,
            A2.CUSTOMER_NUMBER,
            A2.ACCOUNT_NUMBER,
            A2.DATA_SOURCE,
            B2.SUM_AMT FROM IFRS_IMA_AMORT_CURR A2 JOIN TMP_TF B2 ON A2.MASTERID = B2.MASTERID) IMA ON IMA.MASTERID = A.MASTERID
--    JOIN IFRS_IMA_AMORT_CURR IMA ON IMA.MASTERID=A.MASTERID
    JOIN IFRS_ACCT_EIR_COST_FEE_ECF B
      ON B.ECFDATE = V_CURRDATE
      AND A.MASTERID = B.MASTERID
      AND B.FLAG_CF = 'F'
--    JOIN TMP_TF C ON C.MASTERID = A.MASTERID
    WHERE   COALESCE(A.GAIN_LOSS_AMT,0)<>0;

    COMMIT;


    --insert cost gain loss
    INSERT  INTO IFRS_ACCT_EIR_GAIN_LOSS
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      ECFDATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      AMOUNT ,
      STATUS ,
      CREATEDDATE ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      FLAG_REVERSE ,
      AMORTDATE ,
      SRCPROCESS ,
      ORG_CCY ,
      ORG_CCY_EXRATE ,
      PRDTYPE ,
      CF_ID ,
      METHOD
    )
    SELECT  IMA.FACILITY_NUMBER ,
            IMA.CUSTOMER_NUMBER ,
            V_CURRDATE ,
            V_CURRDATE ,
            IMA.DATA_SOURCE ,
            B.PRDCODE ,
            B.TRXCODE ,
            B.CCY ,
            -1 * --20180417 gain loss dibalik
              ROUND(CASE WHEN B.FLAG_REVERSE = 'Y' THEN -1 * B.AMOUNT ELSE B.AMOUNT END
              / C.SUM_AMT * A.GAIN_LOSS_COST_AMT,V_ROUND) AS N_AMOUNT ,
            B.STATUS ,
            SYSTIMESTAMP ,
            IMA.ACCOUNT_NUMBER ,
            A.MASTERID ,
            B.FLAG_CF ,
            'N' ,
            NULL AS AMORTDATE ,
            'ECF' ,
            B.ORG_CCY ,
            B.ORG_CCY_EXRATE ,
            B.PRDTYPE ,
            B.CF_ID ,
            B.METHOD
    FROM    IFRS_ACCT_EIR_CF_ECF A
    JOIN IFRS_IMA_AMORT_CURR IMA ON IMA.MASTERID=A.MASTERID
    JOIN IFRS_ACCT_EIR_COST_FEE_ECF B
      ON B.ECFDATE = V_CURRDATE
      AND A.MASTERID = B.MASTERID
      AND B.FLAG_CF = 'C'
    JOIN TMP_TC C ON C.MASTERID = A.MASTERID
    WHERE   COALESCE(A.GAIN_LOSS_AMT,0)<>0 ;

    COMMIT;


    --20180226 adjust gain loss back to IFRS_ACCT_EIR_COST_FEE_ECF
    MERGE INTO IFRS_ACCT_EIR_COST_FEE_ECF A
    USING IFRS_ACCT_EIR_CF_ECF B
    ON (A.MASTERID=B.MASTERID
            AND A.ECFDATE=V_CURRDATE
            AND COALESCE(B.GAIN_LOSS_FEE_AMT,0)<>0
            AND A.FLAG_CF='F'
       )
    WHEN MATCHED THEN
    UPDATE
    SET  A.AMOUNT=((B.FEE_AMT + B.GAIN_LOSS_FEE_AMT)/B.FEE_AMT)*A.AMOUNT;

    COMMIT;


    MERGE INTO IFRS_ACCT_EIR_COST_FEE_ECF A
    USING IFRS_ACCT_EIR_CF_ECF B
    ON (A.MASTERID=B.MASTERID
            AND A.ECFDATE=V_CURRDATE
            AND COALESCE(B.GAIN_LOSS_COST_AMT,0)<>0
            AND A.FLAG_CF='C'
       )
    WHEN MATCHED THEN
    UPDATE
    SET  A.AMOUNT=((B.COST_AMT + B.GAIN_LOSS_COST_AMT)/B.COST_AMT)*A.AMOUNT;

    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_IFRS_ACCT_EIR_ECF_MAIN' ,'');


    COMMIT;

END;