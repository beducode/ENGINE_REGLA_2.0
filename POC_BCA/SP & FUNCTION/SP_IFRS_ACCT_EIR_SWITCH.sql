CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_EIR_SWITCH
AS
    V_CURRDATE	DATE;
    V_PREVDATE	DATE;
	V_NUM NUMBER(10);
    V_PARAM_DISABLE_ACCRU_PREV NUMBER(19);
BEGIN

    SELECT MAX(CURRDATE),MAX(PREVDATE)
    INTO V_CURRDATE, V_PREVDATE
    FROM IFRS_PRC_DATE_AMORT ;


    INSERT INTO IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'START','SP_IFRS_ACCT_EIR_SWITCH','');

    COMMIT;

    --reset
    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_EIR_COST_FEE_PREV
    SET STATUS='ACT'
    WHERE STATUS='REV'
    AND CREATEDBY='EIR_SWITCH'
    AND DOWNLOAD_DATE=V_CURRDATE;

    COMMIT;

    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_EIR_COST_FEE_PREV
    SET STATUS='ACT'
    WHERE STATUS='REV2'
    AND CREATEDBY='EIR_SWITCH'
    AND DOWNLOAD_DATE=V_PREVDATE;

    COMMIT;

    /*20180809 UPDATED BY VIVI*/
    BEGIN
      SELECT  CASE WHEN COMMONUSAGE = 'Y' THEN 1  ELSE 0  END
    INTO V_PARAM_DISABLE_ACCRU_PREV
    FROM    TBLM_COMMONCODEHEADER
    WHERE   COMMONCODE = 'CALC_FROM_LASTPAYMDATE';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        V_PARAM_DISABLE_ACCRU_PREV := 0;
    END;

    -- exist proc if no need to process EIR switch
    SELECT /*+ PARALLEL(12) */ COUNT(*)
    INTO V_NUM
    FROM IFRS_ACCT_SWITCH
    WHERE DOWNLOAD_DATE=V_CURRDATE
    AND PREV_EIR_ECF='Y';

    COMMIT;

    IF V_NUM<=0
    THEN
      INSERT INTO IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
      VALUES(V_CURRDATE,SYSTIMESTAMP,'END','SP_IFRS_ACCT_EIR_SWITCH','');
      RETURN;
    END IF;


    -- copy ecf from old to new acctno
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_EIR_ECF
    ( DOWNLOAD_DATE,
      MASTERID,
	    N_LOAN_AMT,
        N_INT_RATE,
    	N_EFF_INT_RATE,
    	STARTAMORTDATE,
    	ENDAMORTDATE,
    	GRACEDATE,
    	PAYMENTCODE,
    	INTCALCCODE,
    	PAYMENTTERM,
    	ISGRACE,
    	PREV_PMT_DATE,
    	PMT_DATE,
    	I_DAYS,
    	I_DAYS2,
    	N_OSPRN_PREV,
    	N_INSTALLMENT,
    	N_PRN_PAYMENT,
    	N_INT_PAYMENT,
    	N_OSPRN,
    	N_FAIRVALUE_PREV,
    	N_EFF_INT_AMT,
	    N_FAIRVALUE,
	    N_UNAMORT_AMT_PREV,
    	N_AMORT_AMT ,
	    N_UNAMORT_AMT ,
    	N_COST_UNAMORT_AMT_PREV ,
    	N_COST_AMORT_AMT ,
	    N_COST_UNAMORT_AMT ,
	    N_FEE_UNAMORT_AMT_PREV ,
	    N_FEE_AMORT_AMT ,
	    N_FEE_UNAMORT_AMT ,
	    AMORTSTOPDATE ,
	    AMORTSTOPMSG ,
	    N_DAILY_AMORT_COST,
	    N_DAILY_AMORT_FEE ,
	    N_EFF_INT_AMT0 ,
	    N_EFF_INT_RATE0 ,
	    N_DAILY_INT_ADJ_AMT ,
	    N_INT_ADJ_AMT
	    -- switch adjust carry forward
	    ,SW_ADJ_COST
	    ,SW_ADJ_FEE
	    ,NOCF_OSPRN
	    ,NOCF_OSPRN_PREV
	    ,NOCF_INT_RATE
	    ,NOCF_PRN_PAYMENT
	    ,NOCF_EFF_INT_AMT
	    ,NOCF_UNAMORT_AMT_PREV
	    ,NOCF_AMORT_AMT
	    ,NOCF_UNAMORT_AMT
    )
    SELECT  /*+ PARALLEL(12) */ V_CURRDATE,
            A.MASTERID,
          	B.N_LOAN_AMT,
          	B.N_INT_RATE,
          	B.N_EFF_INT_RATE,
          	B.STARTAMORTDATE,
          	B.ENDAMORTDATE,
	          B.GRACEDATE,
          	B.PAYMENTCODE,
          	B.INTCALCCODE,
	          B.PAYMENTTERM,
          	B.ISGRACE,
          	B.PREV_PMT_DATE,
          	B.PMT_DATE,
          	B.I_DAYS,
          	B.I_DAYS2,
          	B.N_OSPRN_PREV,
          	B.N_INSTALLMENT,
          	B.N_PRN_PAYMENT,
          	B.N_INT_PAYMENT,
          	B.N_OSPRN,
          	B.N_FAIRVALUE_PREV,
          	B.N_EFF_INT_AMT,
          	B.N_FAIRVALUE,
          	B.N_UNAMORT_AMT_PREV,
	          B.N_AMORT_AMT ,
          	B.N_UNAMORT_AMT ,
          	B.N_COST_UNAMORT_AMT_PREV ,
          	B.N_COST_AMORT_AMT ,
          	B.N_COST_UNAMORT_AMT ,
          	B.N_FEE_UNAMORT_AMT_PREV ,
          	B.N_FEE_AMORT_AMT ,
          	B.N_FEE_UNAMORT_AMT ,
          	B.AMORTSTOPDATE ,
          	'EIR_SWITCH_2' ,
          	B.N_DAILY_AMORT_COST,
          	B.N_DAILY_AMORT_FEE ,
          	B.N_EFF_INT_AMT0 ,
          	B.N_EFF_INT_RATE0 ,
	          B.N_DAILY_INT_ADJ_AMT ,
          	B.N_INT_ADJ_AMT
          	-- switch adjust carry forward
          	,B.SW_ADJ_COST
          	,B.SW_ADJ_FEE
          	,B.NOCF_OSPRN
          	,B.NOCF_OSPRN_PREV
          	,B.NOCF_INT_RATE
          	,B.NOCF_PRN_PAYMENT
          	,B.NOCF_EFF_INT_AMT
          	,B.NOCF_UNAMORT_AMT_PREV
          	,B.NOCF_AMORT_AMT
          	,B.NOCF_UNAMORT_AMT
    FROM IFRS_ACCT_SWITCH A
    JOIN IFRS_ACCT_EIR_ECF B
      ON B.AMORTSTOPDATE IS NULL
      AND B.MASTERID=A.PREV_MASTERID
    WHERE A.DOWNLOAD_DATE=V_CURRDATE
    AND A.PREV_EIR_ECF='Y';

    COMMIT;


    -- copy old cost fee ecf to new acct
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_EIR_COST_FEE_ECF
    ( DOWNLOAD_DATE ,
      ECFDATE ,
    	MASTERID ,
    	BRCODE ,
    	CIFNO ,
    	FACNO ,
    	ACCTNO ,
    	DATASOURCE ,
	    CCY,
	    PRDCODE,
	    TRXCODE,
	    FLAG_CF,
	    FLAG_REVERSE,
    	METHOD,
    	STATUS,
    	SRCPROCESS,
    	AMOUNT,
    	CREATEDDATE,
    	CREATEDBY,
    	SEQ,
    	AMOUNT_ORG,
    	ORG_CCY,
    	ORG_CCY_EXRATE,
	    PRDTYPE
	    ,CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ C.DOWNLOAD_DATE ,
            V_CURRDATE ,
	          A.MASTERID ,
	          A.BRCODE ,
	          A.CIFNO ,
	          A.FACNO ,
	          A.ACCTNO ,
	          A.DATASOURCE ,
          	C.CCY,
          	A.PRDCODE,
          	C.TRXCODE,
          	C.FLAG_CF,
          	C.FLAG_REVERSE,
          	C.METHOD,
          	C.STATUS,
          	C.SRCPROCESS,
          	C.AMOUNT,
          	SYSTIMESTAMP,
          	'EIR_SWITCH',
          	C.SEQ,
          	C.AMOUNT_ORG,
          	C.ORG_CCY,
          	C.ORG_CCY_EXRATE,
	          A.PRDTYPE
          	,C.CF_ID
    FROM IFRS_ACCT_SWITCH A
    JOIN IFRS_ACCT_EIR_ECF B
      ON B.AMORTSTOPDATE IS NULL
      AND B.MASTERID=A.PREV_MASTERID
      AND B.PREV_PMT_DATE=B.PMT_DATE
      AND B.DOWNLOAD_DATE<V_CURRDATE -- add filter for branch change
    JOIN IFRS_ACCT_EIR_COST_FEE_ECF C
      ON C.ECFDATE=B.DOWNLOAD_DATE
      AND C.MASTERID=B.MASTERID AND C.STATUS ='ACT'
    WHERE A.DOWNLOAD_DATE=V_CURRDATE
    AND A.PREV_EIR_ECF='Y';

    COMMIT;

    --rev old cost fee prev
    IF V_PARAM_DISABLE_ACCRU_PREV=0
    THEN
    MERGE INTO IFRS_ACCT_EIR_COST_FEE_PREV A
    USING ( SELECT C.MASTERID,C.DOWNLOAD_DATE,C.SEQ
             FROM IFRS_ACCT_SWITCH A
             JOIN IFRS_PRC_DATE_AMORT P ON P.CURRDATE=A.DOWNLOAD_DATE
             JOIN VW_LAST_EIR_COST_FEE_PREV C ON C.MASTERID=A.PREV_MASTERID
             WHERE A.PREV_EIR_ECF='Y'
           ) C
    ON (A.DOWNLOAD_DATE=C.DOWNLOAD_DATE
        AND A.MASTERID=C.MASTERID
        AND A.SEQ=C.SEQ
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.STATUS=CASE WHEN A.DOWNLOAD_DATE=V_CURRDATE THEN 'REV' ELSE 'REV2' END
      , A.CREATEDBY='EIR_SWITCH';


    --copy old cost fee prev to new acct
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_EIR_COST_FEE_PREV
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
	  	ISUSED ,
	  	SEQ ,
	  	AMOUNT_ORG ,
	  	ORG_CCY ,
	  	ORG_CCY_EXRATE ,
	  	PRDTYPE,CF_ID
  	)
  	SELECT /*+ PARALLEL(12) */ V_CURRDATE ,
           V_CURRDATE ,
	         A.MASTERID ,
	         A.BRCODE ,
	         A.CIFNO ,
	         A.FACNO ,
	         A.ACCTNO ,
	         A.DATASOURCE ,
	         D.CCY ,
	         A.PRDCODE ,
	         D.TRXCODE ,
	         D.FLAG_CF ,
	         D.FLAG_REVERSE ,
	         D.METHOD ,
	         'ACT' STATUS ,
	         D.SRCPROCESS ,
	         D.AMOUNT ,
	         SYSTIMESTAMP ,
	         'EIR_SWITCH' ,
	         D.ISUSED ,
	         '0' SEQ_NEW ,
	         D.AMOUNT_ORG ,
	         D.ORG_CCY ,
	         D.ORG_CCY_EXRATE ,
	         A.PRDTYPE
	         ,D.CF_ID
    FROM IFRS_ACCT_SWITCH A
    JOIN VW_LAST_EIR_COST_FEE_PREV C
      ON C.MASTERID=A.PREV_MASTERID
    JOIN IFRS_ACCT_EIR_COST_FEE_PREV D
      ON D.DOWNLOAD_DATE=C.DOWNLOAD_DATE
      AND D.MASTERID=C.MASTERID AND D.SEQ=C.SEQ
    WHERE A.DOWNLOAD_DATE=V_CURRDATE AND A.PREV_EIR_ECF='Y';
    COMMIT;
    END IF;


    IF V_PARAM_DISABLE_ACCRU_PREV!=0
    THEN
          /*NOT INCLUDE ACC SWITCH AT PMTDATE*/
          MERGE INTO IFRS_ACCT_EIR_COST_FEE_PREV A
          USING (SELECT A.MASTERID,B.PMT_DATE
                FROM IFRS_ACCT_SWITCH A
                JOIN (SELECT DOWNLOAD_DATE, MASTERID ,MAX(PMT_DATE) AS PMT_DATE
                      FROM IFRS_ACCT_EIR_ECF
                            WHERE PMT_DATE<=V_CURRDATE
                            AND AMORTSTOPDATE IS NULL
                            AND DOWNLOAD_DATE<V_CURRDATE
                            GROUP BY DOWNLOAD_DATE,MASTERID
                     )B
                ON A.MASTERID=B.MASTERID
                WHERE A.DOWNLOAD_DATE=V_CURRDATE
               ) B
          ON (A.MASTERID=B.MASTERID
          AND A.DOWNLOAD_DATE=B.PMT_DATE
          AND A.SEQ=1
          )
          WHEN MATCHED THEN
          UPDATE
          SET A.STATUS=CASE WHEN A.DOWNLOAD_DATE=V_CURRDATE THEN 'REV' ELSE 'REV2' END
            , A.CREATEDBY='EIR_SWITCH';


          /*UPDATE STATUS FOR ACCOUNT SWITCH AT PMTDATE */
          MERGE INTO IFRS_ACCT_EIR_COST_FEE_PREV A
          USING (SELECT A.MASTERID,B.PMT_DATE
                FROM IFRS_ACCT_SWITCH A
                JOIN IFRS_ACCT_EIR_ECF B
                ON A.MASTERID=B.MASTERID
                AND A.DOWNLOAD_DATE=B.PMT_DATE
                AND B.AMORTSTOPDATE IS NULL
                AND B.DOWNLOAD_DATE<V_CURRDATE
                WHERE A.DOWNLOAD_DATE=V_CURRDATE
               ) B
          ON (A.MASTERID=B.MASTERID
          AND A.DOWNLOAD_DATE=B.PMT_DATE-1
          )
          WHEN MATCHED THEN
          UPDATE
          SET A.STATUS=CASE WHEN A.DOWNLOAD_DATE=V_CURRDATE THEN 'REV' ELSE 'REV2' END
            , A.CREATEDBY='EIR_SWITCH';


          --copy old cost fee prev to new acct
          /*INSERT FOR ACCOUNT SWITCH EVENT NOT AT PMT_DATE*/
          INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_EIR_COST_FEE_PREV
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
            ISUSED ,
            SEQ ,
            AMOUNT_ORG ,
            ORG_CCY ,
            ORG_CCY_EXRATE ,
            PRDTYPE,CF_ID
          )
          SELECT /*+ PARALLEL(12) */ V_CURRDATE ,
                 V_CURRDATE ,
                 A.MASTERID ,
                 A.BRCODE ,
                 A.CIFNO ,
                 A.FACNO ,
                 A.ACCTNO ,
                 A.DATASOURCE ,
                 D.CCY ,
                 A.PRDCODE ,
                 D.TRXCODE ,
                 D.FLAG_CF ,
                 D.FLAG_REVERSE ,
                 D.METHOD ,
                 'ACT' STATUS ,
                 D.SRCPROCESS ,
                 D.AMOUNT ,
                 SYSTIMESTAMP ,
                 'EIR_SWITCH' ,
                 D.ISUSED ,
                 '0' SEQ_NEW ,
                 D.AMOUNT_ORG ,
                 D.ORG_CCY ,
                 D.ORG_CCY_EXRATE ,
                 A.PRDTYPE
                 ,D.CF_ID
          FROM IFRS_ACCT_SWITCH A
          JOIN (SELECT A.DOWNLOAD_DATE,A.MASTERID,A.SEQ FROM IFRS_ACCT_EIR_COST_FEE_PREV A
                JOIN (SELECT DOWNLOAD_DATE, MASTERID ,MAX(PMT_DATE) AS PMT_DATE
                            FROM IFRS_ACCT_EIR_ECF
                                  WHERE PMT_DATE<=V_CURRDATE
                                  AND AMORTSTOPDATE IS NULL
                                  AND DOWNLOAD_DATE<V_CURRDATE
                                  GROUP BY DOWNLOAD_DATE,MASTERID
                     ) B
                ON A.MASTERID=B.MASTERID
                AND A.DOWNLOAD_DATE=B.PMT_DATE
                AND A.SEQ=1
                     )C
                ON A.MASTERID=C.MASTERID
          JOIN IFRS_ACCT_EIR_COST_FEE_PREV D
            ON D.DOWNLOAD_DATE=C.DOWNLOAD_DATE
            AND D.MASTERID=C.MASTERID AND D.SEQ=C.SEQ
          WHERE A.DOWNLOAD_DATE=V_CURRDATE AND A.PREV_EIR_ECF='Y';
          COMMIT;


          /*INSERT FOR ACCOUNT SWITCH EVENT AT PMT_DATE*/
          INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_EIR_COST_FEE_PREV
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
            ISUSED ,
            SEQ ,
            AMOUNT_ORG ,
            ORG_CCY ,
            ORG_CCY_EXRATE ,
            PRDTYPE,CF_ID
          )
          SELECT /*+ PARALLEL(12) */ V_CURRDATE ,
                 V_CURRDATE ,
                 A.MASTERID ,
                 A.BRCODE ,
                 A.CIFNO ,
                 A.FACNO ,
                 A.ACCTNO ,
                 A.DATASOURCE ,
                 D.CCY ,
                 A.PRDCODE ,
                 D.TRXCODE ,
                 D.FLAG_CF ,
                 D.FLAG_REVERSE ,
                 D.METHOD ,
                 'ACT' STATUS ,
                 D.SRCPROCESS ,
                 D.AMOUNT ,
                 SYSTIMESTAMP ,
                 'EIR_SWITCH' ,
                 D.ISUSED ,
                 '0' SEQ_NEW ,
                 D.AMOUNT_ORG ,
                 D.ORG_CCY ,
                 D.ORG_CCY_EXRATE ,
                 A.PRDTYPE
                 ,D.CF_ID
          FROM IFRS_ACCT_SWITCH A
          JOIN (SELECT A.DOWNLOAD_DATE,A.MASTERID,A.SEQ FROM IFRS_ACCT_EIR_COST_FEE_PREV A
                JOIN (SELECT MASTERID,PMT_DATE FROM IFRS_ACCT_EIR_ECF
                      WHERE AMORTSTOPDATE IS NULL
                      AND DOWNLOAD_DATE<V_CURRDATE
                      AND PMT_DATE=V_CURRDATE
                     ) B
                ON A.MASTERID=B.MASTERID
                AND A.DOWNLOAD_DATE=B.PMT_DATE-1
                )C
                ON A.MASTERID=C.MASTERID
          JOIN IFRS_ACCT_EIR_COST_FEE_PREV D
            ON D.DOWNLOAD_DATE=C.DOWNLOAD_DATE
            AND D.MASTERID=C.MASTERID AND D.SEQ=C.SEQ
          WHERE A.DOWNLOAD_DATE=V_CURRDATE AND A.PREV_EIR_ECF='Y';

        COMMIT;
    END IF;

    COMMIT;

    -- stop old ecf
    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_EIR_ECF
    SET AMORTSTOPDATE=V_CURRDATE
    , AMORTSTOPMSG='EIR_SWITCH'
    WHERE AMORTSTOPDATE IS NULL
      AND DOWNLOAD_DATE<V_CURRDATE -- add filter for branch change
      AND MASTERID IN (SELECT PREV_MASTERID FROM IFRS_ACCT_SWITCH WHERE DOWNLOAD_DATE=V_CURRDATE AND PREV_EIR_ECF='Y');

      COMMIT;


    -- handle acf that is doing accru yesterday
    EXECUTE IMMEDIATE 'truncate table TMP_T1';
    COMMIT;

    INSERT /*+ PARALLEL(12) */ INTO TMP_T1(MASTERID)
    SELECT /*+ PARALLEL(12) */ PREV_MASTERID AS MASTERID
    FROM IFRS_ACCT_SWITCH WHERE DOWNLOAD_DATE=V_CURRDATE AND PREV_EIR_ECF='Y';

    COMMIT;


    -- no accru if yesterday is doing amort
    EXECUTE IMMEDIATE 'truncate table TMP_T2';

    INSERT /*+ PARALLEL(12) */ INTO TMP_T2(MASTERID)
    SELECT /*+ PARALLEL(12) */ DISTINCT MASTERID FROM IFRS_ACCT_EIR_ACF WHERE DOWNLOAD_DATE=V_PREVDATE AND DO_AMORT='Y';

    COMMIT;

    EXECUTE IMMEDIATE 'truncate table TMP_T3';

    INSERT /*+ PARALLEL(12) */ INTO TMP_T3(MASTERID)
    SELECT /*+ PARALLEL(12) */ A.MASTERID
    FROM TMP_T1 A;
    --left join TMP_T2 b on b.masterid=a.masterid
    --where b.masterid is null

    COMMIT;


    -- get last acf with do_amort=N
    EXECUTE IMMEDIATE 'truncate table TMP_P1';

    INSERT /*+ PARALLEL(12) */ INTO TMP_P1(ID)
    SELECT /*+ PARALLEL(12) */ MAX(ID) AS ID FROM IFRS_ACCT_EIR_ACF
    WHERE MASTERID IN (SELECT MASTERID FROM TMP_T3) AND DO_AMORT='N'
    GROUP BY MASTERID;

    COMMIT;

    IF V_PARAM_DISABLE_ACCRU_PREV=0
    THEN
        --REMARKS BY VIVI 20180809
        --DISABLE BCA, BECAUSE SWITCH USING BACK TO CALC_FROM_LASTPAYMDATE
        -- update sw adj cost/fee
        MERGE INTO IFRS_ACCT_SWITCH A
        USING (SELECT B.MASTERID,B.ACCTNO, B.N_ACCRU_COST,B.N_ACCRU_FEE, V_CURRDATE CURRDATE
               FROM IFRS_ACCT_EIR_ACF B
               WHERE B.ID IN (SELECT ID FROM TMP_P1)
              ) B
        ON (A.DOWNLOAD_DATE=B.CURRDATE
                AND A.PREV_EIR_ECF='Y'
                AND A.PREV_MASTERID=B.MASTERID
                AND A.PREV_ACCTNO=B.ACCTNO
           )
        WHEN MATCHED THEN
        UPDATE
        SET A.SW_ADJ_COST=B.N_ACCRU_COST
          ,A.SW_ADJ_FEE=B.N_ACCRU_FEE;
    END IF;
    COMMIT;


    -- get fee summary
    EXECUTE IMMEDIATE 'truncate table TMP_TF';

    INSERT /*+ PARALLEL(12) */ INTO TMP_TF(SUM_AMT,DOWNLOAD_DATE,MASTERID)
    SELECT /*+ PARALLEL(12) */ SUM(A.N_AMOUNT) AS SUM_AMT, A.DOWNLOAD_DATE,A.MASTERID
    FROM( SELECT CASE WHEN A.FLAG_REVERSE='Y' THEN -1 * A.AMOUNT ELSE A.AMOUNT END AS N_AMOUNT
              ,A.ECFDATE DOWNLOAD_DATE,A.MASTERID
          FROM IFRS_ACCT_EIR_COST_FEE_ECF A
          WHERE A.MASTERID IN (SELECT MASTERID FROM TMP_T3)
          AND A.STATUS='ACT' AND A.FLAG_CF='F' AND A.METHOD='EIR'
        ) A
    GROUP BY A.DOWNLOAD_DATE,A.MASTERID;

    COMMIT;


    -- get cost summary
    EXECUTE IMMEDIATE 'truncate table TMP_TC';

    INSERT /*+ PARALLEL(12) */ INTO TMP_TC(SUM_AMT,DOWNLOAD_DATE,MASTERID)
    SELECT /*+ PARALLEL(12) */ SUM(A.N_AMOUNT) AS SUM_AMT, A.DOWNLOAD_DATE,A.MASTERID
    FROM(SELECT CASE WHEN A.FLAG_REVERSE='Y' THEN -1 * A.AMOUNT ELSE A.AMOUNT END AS N_AMOUNT
                ,A.ECFDATE DOWNLOAD_DATE,A.MASTERID
        FROM IFRS_ACCT_EIR_COST_FEE_ECF A
        WHERE A.MASTERID IN (SELECT MASTERID FROM TMP_T3)
        AND A.STATUS='ACT' AND A.FLAG_CF='C' AND A.METHOD='EIR'
        ) A
    GROUP BY A.DOWNLOAD_DATE,A.MASTERID;

    COMMIT;

    --insert fee 1
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_EIR_ACCRU_PREV
    (FACNO
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
    )
    SELECT /*+ PARALLEL(12) */ A.FACNO
          ,A.CIFNO
          ,V_CURRDATE
          ,A.ECFDATE
          ,A.DATASOURCE
          ,B.PRDCODE
          ,B.TRXCODE
          ,B.CCY
          ,CAST(CAST(CASE WHEN B.FLAG_REVERSE='Y' THEN -1 * B.AMOUNT ELSE B.AMOUNT END AS BINARY_DOUBLE)/CAST(C.SUM_AMT AS BINARY_DOUBLE) AS NUMBER(32,20)) * A.N_ACCRU_FEE AS N_AMOUNT
          ,B.STATUS
          ,SYSTIMESTAMP
          ,A.ACCTNO
          ,A.MASTERID
          ,B.FLAG_CF
          ,'N'
          ,NULL AS AMORTDATE
          ,'SW'
          ,B.ORG_CCY
          ,B.ORG_CCY_EXRATE
          ,B.PRDTYPE
          ,B.CF_ID
    FROM IFRS_ACCT_EIR_ACF A
    JOIN IFRS_ACCT_EIR_COST_FEE_ECF B ON B.ECFDATE=A.ECFDATE
      AND A.MASTERID=B.MASTERID
      AND B.FLAG_CF='F'
    JOIN TMP_TF C ON C.DOWNLOAD_DATE=A.ECFDATE AND C.MASTERID=A.MASTERID
    WHERE A.ID IN (SELECT ID FROM TMP_P1);

    COMMIT;
    /*
    and a.masterid not in (
        select masterid from IFRS_ACCT_EIR_ECF
        where pmt_date = @v_currdate
        and amortstopdate is null
      )

    */
    --cost 1
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_EIR_ACCRU_PREV
    ( FACNO
     ,CIFNO
     ,DOWNLOAD_DATE
     ,ECFDATE
     ,DATASOURCE
     ,PRDCODE
     ,TRXCODE
     ,CCY,AMOUNT
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
    )
    SELECT /*+ PARALLEL(12) */ A.FACNO
          ,A.CIFNO
          ,V_CURRDATE
          ,A.ECFDATE
          ,A.DATASOURCE
          ,B.PRDCODE
          ,B.TRXCODE
          ,B.CCY
          ,CAST(CAST(CASE WHEN B.FLAG_REVERSE='Y' THEN -1 * B.AMOUNT ELSE B.AMOUNT END AS BINARY_DOUBLE)/CAST(C.SUM_AMT AS BINARY_DOUBLE) AS NUMBER(32,20)) * A.N_ACCRU_COST AS N_AMOUNT
          ,B.STATUS
          ,SYSTIMESTAMP
          ,A.ACCTNO
          ,A.MASTERID
          ,B.FLAG_CF
          ,'N'
          , NULL AS AMORTDATE
          ,'SW'
          ,B.ORG_CCY
          ,B.ORG_CCY_EXRATE
          ,B.PRDTYPE
          ,B.CF_ID
    FROM IFRS_ACCT_EIR_ACF A
    JOIN IFRS_ACCT_EIR_COST_FEE_ECF B ON B.ECFDATE=A.ECFDATE
      AND A.MASTERID=B.MASTERID
      AND B.FLAG_CF='C'
    JOIN TMP_TC C ON C.DOWNLOAD_DATE=A.ECFDATE AND C.MASTERID=A.MASTERID
    WHERE A.ID IN (SELECT ID FROM TMP_P1);

    COMMIT;

    /*20180809 UPDATED BY VIVI */
    IF V_PARAM_DISABLE_ACCRU_PREV!=0
    THEN
    MERGE INTO IFRS_ACCT_EIR_ACCRU_PREV A
            USING (SELECT C.DOWNLOAD_DATE,A.MASTERID, A.N_ACCRU_FEE AS AMOUNT FROM IFRS_ACCT_EIR_ACF A
                  JOIN IFRS_ACCT_EIR_ECF B
                  ON A.MASTERID=B.MASTERID
                  AND A.DOWNLOAD_DATE=(B.PMT_DATE-1)
                  JOIN (SELECT DOWNLOAD_DATE, MASTERID ,MAX(PMT_DATE) AS PMT_DATE FROM IFRS_ACCT_EIR_ECF
                        WHERE PMT_DATE<=V_CURRDATE
                        AND AMORTSTOPDATE=V_CURRDATE
                        GROUP BY DOWNLOAD_DATE,MASTERID
                        )C
                  ON C.MASTERID=B.MASTERID
                     AND C.PMT_DATE=B.PMT_DATE
                     AND B.AMORTSTOPDATE=V_CURRDATE
                  JOIN IFRS_ACCT_SWITCH D ON A.MASTERID=D.MASTERID
                  WHERE D.DOWNLOAD_DATE=V_CURRDATE
                  )B
            ON (A.MASTERID=B.MASTERID
                AND A.ECFDATE=B.DOWNLOAD_DATE
                AND A.SRCPROCESS='SW'
                )
            WHEN MATCHED THEN
            UPDATE
            SET A.AMOUNT=B.AMOUNT;
    END IF;
    COMMIT;

    --stop old accru before currdate
    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_EIR_ACCRU_PREV
    SET STATUS=TO_CHAR(V_CURRDATE,'YYYYMMDD')
    WHERE STATUS='ACT'
    --and DOWNLOAD_DATE<v_currdate
    AND MASTERID IN (SELECT PREV_MASTERID FROM IFRS_ACCT_SWITCH WHERE DOWNLOAD_DATE=V_CURRDATE AND PREV_EIR_ECF='Y');

    COMMIT;

    -- sw adj cost fee to new ecf
    EXECUTE IMMEDIATE 'truncate table TMP_SW1';

    INSERT /*+ PARALLEL(12) */ INTO TMP_SW1(MASTERID,PMTDATE)
    SELECT /*+ PARALLEL(12) */ A.MASTERID,MIN(A.PMT_DATE) AS PMTDATE
    FROM IFRS_ACCT_EIR_ECF A
    JOIN IFRS_ACCT_SWITCH B
      ON B.MASTERID=A.MASTERID
      AND B.DOWNLOAD_DATE=V_CURRDATE
      AND B.PREV_EIR_ECF='Y'
    WHERE A.DOWNLOAD_DATE=V_CURRDATE
    AND A.AMORTSTOPDATE IS NULL
    AND A.PMT_DATE>=V_CURRDATE
    AND A.PMT_DATE<>A.PREV_PMT_DATE
    GROUP BY A.MASTERID;

    COMMIT;



    MERGE INTO IFRS_ACCT_EIR_ECF A
    USING (SELECT A.*,B.SW_ADJ_COST,B.SW_ADJ_FEE, V_CURRDATE CURRDATE
                  FROM TMP_SW1 A
                  JOIN IFRS_ACCT_SWITCH B
                    ON B.MASTERID=A.MASTERID
                    AND B.DOWNLOAD_DATE=V_CURRDATE
                    AND B.PREV_EIR_ECF='Y'
           )X
    ON (A.DOWNLOAD_DATE=X.CURRDATE
              AND A.MASTERID=X.MASTERID
              AND A.PMT_DATE=X.PMTDATE
       )
    WHEN MATCHED THEN
    UPDATE
    SET  A.SW_ADJ_COST=COALESCE(A.SW_ADJ_COST,0)+X.SW_ADJ_COST
      ,A.SW_ADJ_FEE=COALESCE(A.SW_ADJ_FEE,0)+X.SW_ADJ_FEE;

    COMMIT;


	-- cr change branch 13 agustus 2021
    -- update cost fee summ
    -- MERGE INTO IFRS_ACCT_COST_FEE_SUMM A
    -- USING (SELECT B.DOWNLOAD_DATE,B.MASTERID
    --               FROM IFRS_ACCT_SWITCH B
    --               WHERE B.DOWNLOAD_DATE=V_CURRDATE
    --               AND B.MASTERID IN (SELECT MASTERID FROM IFRS_ACCT_SWITCH WHERE DOWNLOAD_DATE=V_CURRDATE AND PREV_EIR_ECF='Y')
    --               ) X
    -- ON (A.DOWNLOAD_DATE=X.DOWNLOAD_DATE
    --           AND A.MASTERID=X.MASTERID
    --    )
    -- WHEN MATCHED THEN
    -- UPDATE
    -- SET A.AMOUNT_FEE = COALESCE(A.AMOUNT_FEE,0) + COALESCE(A.AMORT_FEE,0)
    -- ,A.AMOUNT_COST = COALESCE(A.AMOUNT_COST,0) + COALESCE(A.AMORT_COST,0)
    -- ,A.CREATEDBY='EIR_SWITCH';

    -- COMMIT;

	-- end of cr change branch 13 agustus 2021


    INSERT INTO IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'END','SP_IFRS_ACCT_EIR_SWITCH','');

    COMMIT;

END;