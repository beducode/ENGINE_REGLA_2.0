CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_SL_JRNL_INTM
AS
    V_CURRDATE DATE ;
    V_PREVDATE DATE ;
    V_PARAM_DISABLE_ACCRU_PREV NUMBER(19);
    V_SL_METHOD VARCHAR2(40);

BEGIN

    SELECT  MAX(CURRDATE), MAX(PREVDATE)
    INTO V_CURRDATE, V_PREVDATE
    FROM    IFRS_PRC_DATE_AMORT;

    BEGIN
      SELECT VALUE1
      INTO V_SL_METHOD
      FROM TBLM_COMMONCODEDETAIL
      WHERE COMMONCODE = 'SCM009';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        V_SL_METHOD:= 'ECF';
    END;

    --disable accru prev create on new ecf and return accrual to unamort
    V_PARAM_DISABLE_ACCRU_PREV := 0;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_IFRS_ACCT_SL_JOURNAL_INTM' ,'');

    COMMIT;


    --delete first
    DELETE /*+ PARALLEL(12) */ FROM IFRS_ACCT_JOURNAL_INTM
    WHERE   DOWNLOAD_DATE >= V_CURRDATE
    AND SOURCEPROCESS LIKE 'SL%';

    COMMIT;

    -- PNL = defa0 + amort of new cost fee today
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      IS_PNL ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ FACNO ,
            CIFNO ,
            DOWNLOAD_DATE ,
            DATASOURCE ,
            PRD_CODE ,
            TRX_CODE ,
            CCY ,
            'DEFA0' ,
            'ACT' ,
            'N' ,
            CASE WHEN FLAG_REVERSE = 'Y' THEN -1 * AMOUNT ELSE AMOUNT END ,
            SYSTIMESTAMP ,
            'SL PNL 1' ,
            ACCTNO ,
            MASTERID ,
            FLAG_CF ,
            BRCODE ,
            'Y' IS_PNL ,
            PRD_TYPE ,
            'ITRCG_SL' ,
            CF_ID
    FROM    IFRS_ACCT_COST_FEE
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND STATUS = 'PNL'
    AND METHOD = 'SL';

    COMMIT;

    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      IS_PNL ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ FACNO ,
            CIFNO ,
            DOWNLOAD_DATE ,
            DATASOURCE ,
            PRD_CODE ,
            TRX_CODE ,
            CCY ,
            'AMORT' ,
            'ACT' ,
            'N' ,
            -1 * ( CASE WHEN FLAG_REVERSE = 'Y' THEN -1 * AMOUNT ELSE AMOUNT END ) ,
            SYSTIMESTAMP ,
            'SL PNL 2' ,
            ACCTNO ,
            MASTERID ,
            FLAG_CF ,
            BRCODE ,
            'Y' IS_PNL ,
            PRD_TYPE ,
            'ACCRU_SL' ,
            CF_ID
    FROM    IFRS_ACCT_COST_FEE
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND STATUS = 'PNL'
    AND METHOD = 'SL';

    COMMIT;

    -- PNL = amort of unamort by currdate
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ FACNO ,
            CIFNO ,
            DOWNLOAD_DATE ,
            DATASOURCE ,
            PRDCODE ,
            TRXCODE ,
            CCY ,
            'AMORT' ,
            'ACT' ,
            'N' ,
            -1 * ( CASE WHEN FLAG_REVERSE = 'Y' THEN -1 * AMOUNT ELSE AMOUNT END ) ,
            SYSTIMESTAMP ,
            'SL PNL 3' ,
            ACCTNO ,
            MASTERID ,
            FLAG_CF ,
            BRCODE ,
            PRDTYPE ,
            'ACRRU_SL' ,
            CF_ID
    FROM    IFRS_ACCT_SL_COST_FEE_PREV
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND STATUS = 'PNL';

    COMMIT;


   -- PNL2 = amort of unamort by prevdate
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ FACNO ,
            CIFNO ,
            DOWNLOAD_DATE ,
            DATASOURCE ,
            PRDCODE ,
            TRXCODE ,
            CCY ,
            'AMORT' ,
            'ACT' ,
            'N' ,
            -1 * ( CASE WHEN FLAG_REVERSE = 'Y' THEN -1 * AMOUNT ELSE AMOUNT END ) ,
            SYSTIMESTAMP ,
            'SL PNL 3' ,
            ACCTNO ,
            MASTERID ,
            FLAG_CF ,
            BRCODE ,
            PRDTYPE ,
            'ACCRU_SL' ,
            CF_ID
    FROM    IFRS_ACCT_SL_COST_FEE_PREV
    WHERE   DOWNLOAD_DATE = V_PREVDATE
    AND STATUS = 'PNL2';

    COMMIT;

   --DEFA0 normal amortized cost/fee
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ FACNO ,
            CIFNO ,
            DOWNLOAD_DATE ,
            DATASOURCE ,
            PRD_CODE ,
            TRX_CODE ,
            CCY ,
            'DEFA0' ,
            'ACT' ,
            'N' ,
            CASE WHEN FLAG_REVERSE = 'Y' THEN -1 * AMOUNT ELSE AMOUNT END ,
            SYSTIMESTAMP ,
            'SL ACT 1' ,
            ACCTNO ,
            MASTERID ,
            FLAG_CF ,
            BRCODE ,
            PRD_TYPE ,
            --'ITRCG',
            'ITRCG_SL' ,
            CF_ID
    FROM    IFRS_ACCT_COST_FEE
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND STATUS = 'ACT'
    AND METHOD = 'SL';

    COMMIT;

    --reverse ACCRUAL
     INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ FACNO ,
            CIFNO ,
            V_CURRDATE ,
            DATASOURCE ,
            PRDCODE ,
            TRXCODE ,
            CCY ,
            JOURNALCODE ,
            STATUS ,
            'Y' ,
            N_AMOUNT ,
            SYSTIMESTAMP ,
            'SL REV ACCRU' ,
            ACCTNO ,
            MASTERID ,
            FLAG_CF ,
            BRANCH ,
            PRDTYPE ,
            JOURNALCODE ,
            CF_ID
    FROM    IFRS_ACCT_JOURNAL_INTM
    WHERE   DOWNLOAD_DATE = V_PREVDATE
    AND STATUS = 'ACT'
    AND JOURNALCODE = 'ACCRU_SL'
    AND REVERSE = 'N'
    AND SUBSTR(SOURCEPROCESS, 1, 2) = 'SL';

    COMMIT;


   --ACCRU FEE
	EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T5';

	IF V_SL_METHOD = 'ECF'
    THEN
		INSERT /*+ PARALLEL(12) */ INTO TMP_T5
		( FACNO ,
		  CIFNO ,
		  DOWNLOAD_DATE ,
		  DATASOURCE ,
		  PRDCODE ,
		  TRXCODE ,
		  CCY ,
		  N_AMOUNT ,
		  ACCTNO ,
		  MASTERID ,
		  BRCODE ,
		  PRDTYPE ,
		  CF_ID
		)
		SELECT  /*+ PARALLEL(12) */ FACNO ,
				CIFNO ,
				ECFDATE ,
				DATASOURCE ,
				PRDCODE ,
				TRXCODE ,
				CCY ,
				CASE WHEN FLAG_REVERSE = 'Y' THEN -1 * AMOUNT ELSE AMOUNT END AS N_AMOUNT ,
				ACCTNO ,
				MASTERID ,
				BRCODE ,
				PRDTYPE ,
				CF_ID
		FROM    IFRS_ACCT_SL_COST_FEE_ECF
		WHERE   FLAG_CF = 'F';

		COMMIT;
    ELSE
        INSERT /*+ PARALLEL(12) */ INTO TMP_T5
        ( FACNO ,
          CIFNO ,
          DOWNLOAD_DATE ,
          DATASOURCE ,
          PRDCODE ,
          TRXCODE ,
          CCY ,
          N_AMOUNT ,
          ACCTNO ,
          MASTERID ,
          BRCODE ,
          PRDTYPE ,
          CF_ID
        )
        SELECT  /*+ PARALLEL(12) */ FACNO ,
                CIFNO ,
                EFFDATE ,
                A.DATA_SOURCE ,
                PRD_CODE ,
                TRX_CODE ,
                CCY ,
                A.SL_AMORT_DAILY ,
                A.MASTERID ,
                A.MASTERID ,
                BRCODE ,
                B.PRODUCT_TYPE ,
                ID_SL
        FROM    IFRS_ACF_SL_MSTR A
				JOIN	IFRS_MASTER_ACCOUNT B
          ON A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
          AND A.EFFDATE = B.DOWNLOAD_DATE
        WHERE   FLAG_CF = 'F'
				AND	IFRS_STATUS = 'ACT';

        COMMIT;
    END IF;

    --journal sl baru
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T6';

    INSERT /*+ PARALLEL(12) */ INTO TMP_T6
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      SUM_AMT ,
      ACCTNO ,
      MASTERID ,
      BRCODE
    )
    SELECT  /*+ PARALLEL(12) */ FACNO ,
            CIFNO ,
            DOWNLOAD_DATE ,
            DATASOURCE ,
            SUM(N_AMOUNT) AS SUM_AMT ,
            ACCTNO ,
            MASTERID ,
            BRCODE
    FROM    TMP_T5 D
    GROUP BY FACNO ,
             CIFNO ,
             DOWNLOAD_DATE ,
             DATASOURCE ,
             ACCTNO ,
             MASTERID ,
             BRCODE;
    COMMIT;


    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ A.FACNO ,
            A.CIFNO ,
            A.DOWNLOAD_DATE ,
            A.DATASOURCE ,
            B.PRDCODE ,
            B.TRXCODE ,
            B.CCY ,
            'ACCRU_SL' ,
            'ACT' ,
            'N' ,
            A.N_ACCRU_FEE* CAST (CAST (B.N_AMOUNT AS BINARY_DOUBLE)/ CAST (C.SUM_AMT AS BINARY_DOUBLE) AS NUMBER(32, 20)) ,
            SYSTIMESTAMP ,
            'SL ACCRU FEE 1' ,
            A.ACCTNO ,
            A.MASTERID ,
            'F' ,
            B.BRCODE ,
            B.PRDTYPE ,
            'ACCRU_SL' ,
            B.CF_ID
    FROM    IFRS_ACCT_SL_ACF A
    JOIN TMP_T5 B
      ON B.DOWNLOAD_DATE = A.ECFDATE
      AND B.MASTERID = A.MASTERID
    JOIN TMP_T6 C
    ON C.MASTERID = A.MASTERID
    AND A.ECFDATE = C.DOWNLOAD_DATE
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.DO_AMORT = 'N';

    COMMIT;

    --AMORT FEE
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ A.FACNO ,
            A.CIFNO ,
            A.DOWNLOAD_DATE ,
            A.DATASOURCE ,
            B.PRDCODE ,
            B.TRXCODE ,
            B.CCY ,
            'AMORT' ,
            'ACT' ,
            'N' ,
            A.N_ACCRU_FEE* CAST (CAST (B.N_AMOUNT AS BINARY_DOUBLE)/ CAST (C.SUM_AMT AS BINARY_DOUBLE) AS NUMBER(32, 20)) ,
            SYSTIMESTAMP ,
            'SL AMORT FEE 1' ,
            A.ACCTNO ,
            A.MASTERID ,
            'F' ,
            B.BRCODE ,
            B.PRDTYPE ,
            'ACCRU_SL' ,
            B.CF_ID
    FROM    IFRS_ACCT_SL_ACF A
    JOIN TMP_T5 B
      ON B.DOWNLOAD_DATE = A.ECFDATE
      AND B.MASTERID = A.MASTERID
    JOIN TMP_T6 C
      ON C.MASTERID = A.MASTERID
      AND A.ECFDATE = C.DOWNLOAD_DATE
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.DO_AMORT = 'Y';

    COMMIT;

    --journal sl baru
	IF V_SL_METHOD = 'NO_ECF'
	THEN
		INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
		( FACNO ,
		  CIFNO ,
		  DOWNLOAD_DATE ,
		  DATASOURCE ,
		  PRDCODE ,
		  TRXCODE ,
		  CCY ,
		  JOURNALCODE ,
		  STATUS ,
		  REVERSE ,
		  N_AMOUNT ,
		  CREATEDDATE ,
		  SOURCEPROCESS ,
		  ACCTNO ,
		  MASTERID ,
		  FLAG_CF ,
		  BRANCH ,
		  PRDTYPE ,
		  JOURNALCODE2 ,
		  CF_ID
		)
		SELECT  /*+ PARALLEL(12) */ A.FACNO ,
				A.CIFNO ,
				A.EFFDATE ,
				A.DATA_SOURCE ,
				B.PRDCODE ,
				B.TRXCODE ,
				B.CCY ,
				'AMORT' ,
				'ACT' ,
				'N' ,
				A.SL_AMORT_DAILY,
				SYSTIMESTAMP ,
				'SL AMORT FEE 1' ,
				A.MASTERID ,
				A.MASTERID ,
				'F' ,
				B.BRCODE ,
				B.PRDTYPE ,
				'ACCRU_SL' ,
				B.CF_ID
		FROM    IFRS_ACF_SL_MSTR A
		JOIN TMP_T5 B
		  ON B.DOWNLOAD_DATE = A.EFFDATE
		  AND B.MASTERID = A.MASTERID
		JOIN TMP_T6 C
		  ON C.MASTERID = A.MASTERID
		  AND A.EFFDATE = C.DOWNLOAD_DATE
		WHERE   A.EFFDATE = V_CURRDATE
		AND A.IFRS_STATUS = 'ACT';

		COMMIT;
	END IF;

    --DEFA0 FEE stop rev at pmtdate 20160619
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO,
      CIFNO,
      DOWNLOAD_DATE,
      DATASOURCE,
      PRDCODE,
      TRXCODE,
      CCY,
      JOURNALCODE,
      STATUS,
      REVERSE,
      N_AMOUNT,
      CREATEDDATE,
      SOURCEPROCESS,
      ACCTNO,
      MASTERID,
      FLAG_CF,
      BRANCH,
      PRDTYPE,
      JOURNALCODE2,
      CF_ID
    )
    SELECT /*+ PARALLEL(12) */ A.FACNO,
           A.CIFNO,
           A.DOWNLOAD_DATE,
           A.DATASOURCE,
           B.PRDCODE,
           B.TRXCODE,
           B.CCY,
           'DEFA0',
           'ACT',
           'N',
           -1 * A.N_ACCRU_FEE* CAST (CAST (B.N_AMOUNT AS BINARY_DOUBLE) / CAST (C.SUM_AMT AS BINARY_DOUBLE) AS NUMBER (32, 20)),
           SYSTIMESTAMP,
           'SL DEFA0 FEE 1',
           A.ACCTNO,
           A.MASTERID,
           'F',
           B.BRCODE,
           B.PRDTYPE,
           'ITRCG_SL',
           B.CF_ID
    FROM IFRS_ACCT_SL_ACF A
    JOIN TMP_T5 B
      ON B.DOWNLOAD_DATE = A.ECFDATE
      AND B.MASTERID = A.MASTERID
    JOIN TMP_T6 C
      ON C.MASTERID = A.MASTERID
      AND A.ECFDATE = C.DOWNLOAD_DATE
    WHERE A.DOWNLOAD_DATE = V_CURRDATE AND A.DO_AMORT = 'Y'
    -- only for stop rev
    AND A.MASTERID IN (SELECT MASTERID FROM IFRS_ACCT_SL_STOP_REV WHERE DOWNLOAD_DATE=V_CURRDATE);

    COMMIT;

	EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T5';

    --ACCRU COST
    IF V_SL_METHOD = 'ECF'
    THEN
        INSERT /*+ PARALLEL(12) */ INTO TMP_T5
        ( FACNO ,
          CIFNO ,
          DOWNLOAD_DATE ,
          DATASOURCE ,
          PRDCODE ,
          TRXCODE ,
          CCY ,
          N_AMOUNT ,
          ACCTNO ,
          MASTERID ,
          BRCODE ,
          PRDTYPE ,
          CF_ID
        )
        SELECT  /*+ PARALLEL(12) */ FACNO ,
                CIFNO ,
                ECFDATE ,
                DATASOURCE ,
                PRDCODE ,
                TRXCODE ,
                CCY ,
                CASE WHEN FLAG_REVERSE = 'Y' THEN -1 * AMOUNT ELSE AMOUNT END AS N_AMOUNT ,
                ACCTNO ,
                MASTERID ,
                BRCODE ,
                PRDTYPE ,
                CF_ID
        FROM    IFRS_ACCT_SL_COST_FEE_ECF
        WHERE   FLAG_CF = 'C';

        COMMIT;
	ELSE
		INSERT /*+ PARALLEL(12) */ INTO TMP_T5
        ( FACNO ,
          CIFNO ,
          DOWNLOAD_DATE ,
          DATASOURCE ,
          PRDCODE ,
          TRXCODE ,
          CCY ,
          N_AMOUNT ,
          ACCTNO ,
          MASTERID ,
          BRCODE ,
          PRDTYPE ,
          CF_ID
        )
        SELECT  /*+ PARALLEL(12) */ FACNO ,
                CIFNO ,
                EFFDATE ,
                A.DATA_SOURCE ,
                PRD_CODE ,
                TRX_CODE ,
                CCY ,
                SL_AMORT_DAILY ,
                A.MASTERID ,
                A.MASTERID ,
                BRCODE ,
                B.PRODUCT_TYPE ,
                ID_SL
        FROM    IFRS_ACF_SL_MSTR A
				JOIN	IFRS_MASTER_ACCOUNT B
          ON A.MASTERID = B.MASTERID
          AND A.EFFDATE = B.DOWNLOAD_DATE
        WHERE   FLAG_CF = 'C';

        COMMIT;
    END IF;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T6';

    INSERT /*+ PARALLEL(12) */ INTO TMP_T6
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      SUM_AMT ,
      ACCTNO ,
      MASTERID ,
      BRCODE
    )
    SELECT  /*+ PARALLEL(12) */ FACNO ,
            CIFNO ,
            DOWNLOAD_DATE ,
            DATASOURCE ,
            SUM(N_AMOUNT) AS SUM_AMT ,
            ACCTNO ,
            MASTERID ,
            BRCODE
    FROM    TMP_T5 D
    GROUP BY FACNO ,
             CIFNO ,
             DOWNLOAD_DATE ,
             DATASOURCE ,
             ACCTNO ,
             MASTERID ,
             BRCODE;
    COMMIT;


    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ A.FACNO ,
            A.CIFNO ,
            A.DOWNLOAD_DATE ,
            A.DATASOURCE ,
            B.PRDCODE ,
            B.TRXCODE ,
            B.CCY ,
            'ACCRU_SL' ,
            'ACT' ,
            'N' ,
            A.N_ACCRU_COST* CAST (CAST (B.N_AMOUNT AS BINARY_DOUBLE)/ CAST (C.SUM_AMT AS BINARY_DOUBLE) AS NUMBER(32, 20)) ,
            SYSTIMESTAMP ,
            'SL ACCRU COST 1' ,
            A.ACCTNO ,
            A.MASTERID ,
            'C' ,
            B.BRCODE ,
            B.PRDTYPE ,
            'ACCRU_SL' ,
            B.CF_ID
    FROM    IFRS_ACCT_SL_ACF A
    JOIN TMP_T5 B
      ON B.DOWNLOAD_DATE = A.ECFDATE
      AND B.MASTERID = A.MASTERID
    JOIN TMP_T6 C
      ON C.MASTERID = A.MASTERID
      AND A.ECFDATE = C.DOWNLOAD_DATE
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.DO_AMORT = 'N';

    COMMIT;

    --AMORT COST
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ A.FACNO ,
            A.CIFNO ,
            A.DOWNLOAD_DATE ,
            A.DATASOURCE ,
            B.PRDCODE ,
            B.TRXCODE ,
            B.CCY ,
            'AMORT' ,
            'ACT' ,
            'N' ,
            A.N_ACCRU_COST* CAST (CAST (B.N_AMOUNT AS BINARY_DOUBLE)/ CAST (C.SUM_AMT AS BINARY_DOUBLE) AS NUMBER(32, 20)) ,
            SYSTIMESTAMP ,
            'SL AMORT COST 1' ,
            A.ACCTNO ,
            A.MASTERID ,
            'C' ,
            B.BRCODE ,
            B.PRDTYPE ,
            'ACCRU_SL' ,
            B.CF_ID
    FROM    IFRS_ACCT_SL_ACF A
    JOIN TMP_T5 B
      ON B.DOWNLOAD_DATE = A.ECFDATE
      AND B.MASTERID = A.MASTERID
    JOIN TMP_T6 C
      ON C.MASTERID = A.MASTERID
      AND A.ECFDATE = C.DOWNLOAD_DATE
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.DO_AMORT = 'Y';

    COMMIT;


    --stop rev defa0 COST 20160619
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT /*+ PARALLEL(12) */ A.FACNO,
           A.CIFNO,
           A.DOWNLOAD_DATE,
           A.DATASOURCE,
           B.PRDCODE,
           B.TRXCODE,
           B.CCY,
           'DEFA0',
           'ACT',
           'N',
           -1 * A.N_ACCRU_COST* CAST (CAST (B.N_AMOUNT AS BINARY_DOUBLE) / CAST (C.SUM_AMT AS BINARY_DOUBLE) AS NUMBER (32, 20)),
           SYSTIMESTAMP,
           'SL AMORT COST 1',
           A.ACCTNO,
           A.MASTERID,
           'C',
           B.BRCODE,
           B.PRDTYPE,
           'ITRCG_SL',
           B.CF_ID
    FROM IFRS_ACCT_SL_ACF A
    JOIN TMP_T5 B
      ON B.DOWNLOAD_DATE = A.ECFDATE
      AND B.MASTERID = A.MASTERID
    JOIN TMP_T6 C
      ON C.MASTERID = A.MASTERID
      AND A.ECFDATE = C.DOWNLOAD_DATE
      WHERE A.DOWNLOAD_DATE = V_CURRDATE AND A.DO_AMORT = 'Y'
    -- stop rev
    AND A.MASTERID IN (SELECT MASTERID FROM IFRS_ACCT_SL_STOP_REV WHERE DOWNLOAD_DATE=V_CURRDATE);

    COMMIT;


    -- 20160407 daniel s : set BLK before accru prev code
    -- update status accru prev for sl stop rev
	MERGE INTO IFRS_ACCT_SL_ACCRU_PREV A
	USING
	(
		SELECT C.ID
		FROM IFRS_ACCT_SL_ACF A
		JOIN IFRS_ACCT_SL_STOP_REV E ON E.DOWNLOAD_DATE = V_CURRDATE
		  AND E.MASTERID = A.MASTERID
		JOIN IFRS_ACCT_SL_ACCRU_PREV C ON C.MASTERID = A.MASTERID
		  AND C.STATUS = 'ACT'
		  AND C.DOWNLOAD_DATE <= V_CURRDATE
		WHERE A.DOWNLOAD_DATE = V_PREVDATE
	) B ON (A.ID = B.ID)
	WHEN MATCHED THEN
	UPDATE SET A.STATUS = TO_CHAR (V_CURRDATE, 'YYYYMMDD') || 'BLK';

	COMMIT;


    --SL ACCRU PREV
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ A.FACNO ,
            A.CIFNO ,
            A.DOWNLOAD_DATE ,
            A.DATASOURCE ,
            C.PRDCODE ,
            C.TRXCODE ,
            C.CCY ,
            'ACCRU_SL' ,
            'ACT' ,
            'N' ,
            CASE WHEN C.FLAG_REVERSE = 'Y' THEN -1 * C.AMOUNT ELSE C.AMOUNT END ,
            SYSTIMESTAMP ,
            'SL ACCRU PREV' ,
            A.ACCTNO ,
            A.MASTERID ,
            C.FLAG_CF ,
            A.BRANCH ,
            C.PRDTYPE ,
            'ACCRU_SL' ,
            C.CF_ID
    FROM    IFRS_ACCT_SL_ACF A
    JOIN IFRS_ACCT_SL_ACCRU_PREV C
    ON C.MASTERID = A.MASTERID
    AND C.STATUS = 'ACT'
    AND C.DOWNLOAD_DATE <= V_CURRDATE
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.DO_AMORT = 'N';

    COMMIT;


    --SL AMORT PREV
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ A.FACNO ,
            A.CIFNO ,
            A.DOWNLOAD_DATE ,
            A.DATASOURCE ,
            C.PRDCODE ,
            C.TRXCODE ,
            C.CCY ,
            'AMORT' ,
            'ACT' ,
            'N' ,
            CASE WHEN C.FLAG_REVERSE = 'Y' THEN -1 * C.AMOUNT ELSE C.AMOUNT END ,
            SYSTIMESTAMP ,
            'SL AMORT PREV' ,
            A.ACCTNO ,
            A.MASTERID ,
            C.FLAG_CF ,
            A.BRANCH ,
            C.PRDTYPE ,
            'ACCRU_SL' ,
            C.CF_ID
    FROM    IFRS_ACCT_SL_ACF A
    JOIN IFRS_ACCT_SL_ACCRU_PREV C
      ON C.MASTERID = A.MASTERID
      AND C.STATUS = TO_CHAR  (V_CURRDATE, 'YYYYMMDD')
      AND C.DOWNLOAD_DATE <= V_CURRDATE
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.DO_AMORT = 'Y'
    --20180808 must not include switch acct
    AND A.MASTERID NOT IN ( SELECT PREV_MASTERID FROM IFRS_ACCT_SWITCH WHERE DOWNLOAD_DATE=V_CURRDATE );

    COMMIT;


    --SL switch amort of accru prev
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ A.PREV_FACNO ,
            A.PREV_CIFNO ,
            A.DOWNLOAD_DATE ,
            A.PREV_DATASOURCE ,
            A.PREV_PRDCODE ,		--20180808 use prev prd code
            C.TRXCODE ,
            C.CCY ,
            'AMORT' ,
            'ACT' ,
            'N' ,
            CASE WHEN C.FLAG_REVERSE = 'Y' THEN -1 * C.AMOUNT ELSE C.AMOUNT END ,
            SYSTIMESTAMP ,
            'SL ACRU SW' ,
            A.PREV_ACCTNO ,
            A.PREV_MASTERID ,
            C.FLAG_CF ,
            A.PREV_BRCODE ,
            A.PREV_PRDTYPE , --20180808 use prev prd type
            'ACCRU_SL' ,
            C.CF_ID
    FROM    IFRS_ACCT_SWITCH A
    JOIN IFRS_ACCT_SL_ACCRU_PREV C
      ON C.MASTERID = A.PREV_MASTERID
      AND C.STATUS = TO_CHAR (V_CURRDATE, 'YYYYMMDD')
      AND C.DOWNLOAD_DATE <= V_CURRDATE
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.PREV_SL_ECF = 'Y';

    COMMIT;

    -- REV = defa0 rev of unamort by currdate
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ FACNO ,
            CIFNO ,
            DOWNLOAD_DATE ,
            DATASOURCE ,
            PRDCODE ,
            TRXCODE ,
            CCY ,
            'DEFA0' ,
            'ACT' ,
            'Y' ,
            1 * ( CASE WHEN FLAG_REVERSE = 'Y' THEN -1 * AMOUNT ELSE AMOUNT END ) ,
            SYSTIMESTAMP ,
            'SL REV 1' ,
            ACCTNO ,
            MASTERID ,
            FLAG_CF ,
            BRCODE ,
            PRDTYPE ,
            'ITRCG_SL' ,
            CF_ID
    FROM    IFRS_ACCT_SL_COST_FEE_PREV
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND STATUS = 'REV'
	AND CREATEDBY = 'SL_SWITCH';

    COMMIT;


    -- REV2 = rev defa0 of unamort by prevdate
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ FACNO ,
            CIFNO ,
            V_CURRDATE ,
            DATASOURCE ,
            PRDCODE ,
            TRXCODE ,
            CCY ,
            'DEFA0' ,
            'ACT' ,
            'Y' ,
            1 * ( CASE WHEN FLAG_REVERSE = 'Y' THEN -1 * AMOUNT ELSE AMOUNT END ) ,
            SYSTIMESTAMP ,
            'SL REV 2' ,
            ACCTNO ,
            MASTERID ,
            FLAG_CF ,
            BRCODE ,
            PRDTYPE ,
            'ITRCG_SL' ,
            CF_ID
    FROM    IFRS_ACCT_SL_COST_FEE_PREV
    WHERE   DOWNLOAD_DATE = V_PREVDATE
    AND STATUS = 'REV2'
	AND CREATEDBY = 'SL_SWITCH';

    COMMIT;


   -- DEFA0 for new acct of SL switch
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ FACNO ,
            CIFNO ,
            V_CURRDATE ,
            DATASOURCE ,
            PRDCODE ,
            TRXCODE ,
            CCY ,
            'DEFA0' ,
            'ACT' ,
            'N' ,
            1 * ( CASE WHEN FLAG_REVERSE = 'Y' THEN -1 * AMOUNT ELSE AMOUNT END ) ,
            SYSTIMESTAMP ,
            'SL_SWITCH' ,
            ACCTNO ,
            MASTERID ,
            FLAG_CF ,
            BRCODE ,
            PRDTYPE ,
            'ITRCG_SL' ,
            CF_ID
    FROM    IFRS_ACCT_SL_COST_FEE_PREV
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND STATUS = 'ACT'
    AND SEQ = '0';

    COMMIT;


    ----JOURNAL SL SWITCH NO ECF
    IF V_SL_METHOD = 'NO_ECF'
	THEN
		INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
		( FACNO ,
		  CIFNO ,
		  DOWNLOAD_DATE ,
		  DATASOURCE ,
		  PRDCODE ,
		  TRXCODE ,
		  CCY ,
		  JOURNALCODE ,
		  STATUS ,
		  REVERSE ,
		  N_AMOUNT ,
		  CREATEDDATE ,
		  SOURCEPROCESS ,
		  ACCTNO ,
		  MASTERID ,
		  FLAG_CF ,
		  BRANCH ,
		  PRDTYPE ,
		  JOURNALCODE2 ,
		  CF_ID
		)
		SELECT  /*+ PARALLEL(12) */ FACNO ,
				CIFNO ,
				V_CURRDATE ,
				A.DATA_SOURCE ,
				PRD_CODE ,
				TRX_CODE ,
				CCY ,
				'DEFA0' ,
				'ACT' ,
				'Y' ,
				UNAMORT_VALUE ,
				SYSTIMESTAMP ,
				'SL_SWITCH' ,
				A.MASTERID ,
				A.MASTERID ,
				FLAG_CF ,
				BRCODE ,
				B.PRODUCT_TYPE ,
				'ITRCG_SL' ,
				ID_SL
		FROM    IFRS_ACF_SL_MSTR A
		JOIN	IFRS_MASTER_ACCOUNT B
		ON		A.MASTERID = B.MASTERID
		AND		A.EFFDATE = B.DOWNLOAD_DATE
		WHERE   EFFDATE = V_PREVDATE
		AND IFRS_STATUS = 'ACT'
		AND A.MASTERID IN (SELECT DISTINCT MASTERID FROM IFRS_ACF_SL_MSTR WHERE EFFDATE = V_CURRDATE AND IFRS_STATUS = 'SWC');

		COMMIT;


		INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
		( FACNO ,
		  CIFNO ,
		  DOWNLOAD_DATE ,
		  DATASOURCE ,
		  PRDCODE ,
		  TRXCODE ,
		  CCY ,
		  JOURNALCODE ,
		  STATUS ,
		  REVERSE ,
		  N_AMOUNT ,
		  CREATEDDATE ,
		  SOURCEPROCESS ,
		  ACCTNO ,
		  MASTERID ,
		  FLAG_CF ,
		  BRANCH ,
		  PRDTYPE ,
		  JOURNALCODE2 ,
		  CF_ID
		)
		SELECT  /*+ PARALLEL(12) */ FACNO ,
				CIFNO ,
				V_CURRDATE ,
				A.DATA_SOURCE ,
				PRD_CODE ,
				TRX_CODE ,
				CCY ,
				'DEFA0' ,
				'ACT' ,
				'N' ,
				UNAMORT_VALUE ,
				SYSTIMESTAMP ,
				'SL_SWITCH' ,
				A.MASTERID ,
				A.MASTERID ,
				FLAG_CF ,
				B.BRANCH_CODE ,
				B.PRODUCT_TYPE ,
				'ITRCG_SL' ,
				ID_SL
		FROM    IFRS_ACF_SL_MSTR A
		JOIN	IFRS_MASTER_ACCOUNT B
		ON		A.MASTERID = B.MASTERID
		WHERE   EFFDATE = V_PREVDATE
		AND B.DOWNLOAD_DATE = V_CURRDATE
		AND IFRS_STATUS = 'ACT'
		AND A.MASTERID IN ( SELECT DISTINCT MASTERID FROM IFRS_ACF_SL_MSTR WHERE EFFDATE = V_CURRDATE AND IFRS_STATUS = 'SWC');

		COMMIT;
    END IF;
    -- 20160407 SL stop reverse
    -- before sl acf run
    -- reverse unamortized and amort accru if exist
    -- unamortized may be used by other process

    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
    ( FACNO ,
      CIFNO ,
      DOWNLOAD_DATE ,
      DATASOURCE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      N_AMOUNT ,
      CREATEDDATE ,
      SOURCEPROCESS ,
      ACCTNO ,
      MASTERID ,
      FLAG_CF ,
      BRANCH ,
      PRDTYPE ,
      JOURNALCODE2 ,
      CF_ID
    )
    SELECT  /*+ PARALLEL(12) */ A.FACNO ,
            A.CIFNO ,
            V_CURRDATE AS DOWNLOAD_DATE ,
            A.DATASOURCE ,
            A.PRDCODE ,
            A.TRXCODE ,
            A.CCY ,
            'DEFA0' ,
            'ACT' ,
            'Y' ,
            CASE WHEN FLAG_REVERSE = 'Y' THEN -1 * AMOUNT ELSE AMOUNT END ,
            SYSTIMESTAMP ,
            'SL STOP REV 1' ,
            A.ACCTNO ,
            A.MASTERID ,
            A.FLAG_CF ,
            A.BRCODE ,
            A.PRDTYPE ,
            'ITRCG_SL',
            A.CF_ID
    FROM    IFRS_ACCT_SL_COST_FEE_PREV A -- 20130722 add join cond to pick latest cf prev
    JOIN VW_LAST_SL_CF_PREV_YEST C
      ON C.MASTERID = A.MASTERID
      AND C.DOWNLOAD_DATE = A.DOWNLOAD_DATE
      AND NVL(C.SEQ,'') = NVL(A.SEQ,'')
    JOIN IFRS_ACCT_SL_STOP_REV B
      ON B.DOWNLOAD_DATE = V_CURRDATE
      AND B.MASTERID = A.MASTERID
    WHERE   A.DOWNLOAD_DATE = V_PREVDATE
    AND A.STATUS = 'ACT';

    COMMIT;

    -- 20160407 amort yesterday accru
    -- block accru prev generation on SL_ECF

    IF V_PARAM_DISABLE_ACCRU_PREV = 0
    THEN

        INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
        ( FACNO ,
          CIFNO ,
          DOWNLOAD_DATE ,
          DATASOURCE ,
          PRDCODE ,
          TRXCODE ,
          CCY ,
          JOURNALCODE ,
          STATUS ,
          REVERSE ,
          N_AMOUNT ,
          CREATEDDATE ,
          SOURCEPROCESS ,
          ACCTNO ,
          MASTERID ,
          FLAG_CF ,
          BRANCH ,
          PRDTYPE ,
          JOURNALCODE2 ,
          CF_ID
        )
        SELECT  /*+ PARALLEL(12) */ FACNO ,
                CIFNO ,
                V_CURRDATE ,
                DATASOURCE ,
                PRDCODE ,
                TRXCODE ,
                CCY ,
                'AMORT' ,
                STATUS ,
                'N' ,
                N_AMOUNT ,
                SYSTIMESTAMP ,
                'SL STOP REV 2' ,
                ACCTNO ,
                MASTERID ,
                FLAG_CF ,
                BRANCH ,
                PRDTYPE ,
                'ACCRU_SL' ,
                CF_ID
        FROM    IFRS_ACCT_JOURNAL_INTM
        WHERE   DOWNLOAD_DATE = V_PREVDATE
        AND STATUS = 'ACT'
        AND JOURNALCODE = 'ACCRU_SL'
        AND REVERSE = 'N'
        AND SUBSTR(SOURCEPROCESS, 1, 2) = 'SL'
        AND MASTERID IN (SELECT  MASTERID FROM    IFRS_ACCT_SL_STOP_REV WHERE   DOWNLOAD_DATE = V_CURRDATE );

        COMMIT;

    ELSE
        INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_INTM
        ( FACNO ,
          CIFNO ,
          DOWNLOAD_DATE ,
          DATASOURCE ,
          PRDCODE ,
          TRXCODE ,
          CCY ,
          JOURNALCODE ,
          STATUS ,
          REVERSE ,
          N_AMOUNT ,
          CREATEDDATE ,
          SOURCEPROCESS ,
          ACCTNO ,
          MASTERID ,
          FLAG_CF ,
          BRANCH ,
          PRDTYPE ,
          JOURNALCODE2 ,
          CF_ID
        )
        SELECT  /*+ PARALLEL(12) */ FACNO ,
                CIFNO ,
                V_CURRDATE ,
                DATASOURCE ,
                PRDCODE ,
                TRXCODE ,
                CCY ,
                'DEFA0' ,
                STATUS ,
                'Y' ,
                -1 * N_AMOUNT ,
                SYSTIMESTAMP ,
                'SL STOP REV 2' ,
                ACCTNO ,
                MASTERID ,
                FLAG_CF ,
                BRANCH ,
                PRDTYPE ,
                'ITRCG_SL' ,
                CF_ID
        FROM    IFRS_ACCT_JOURNAL_INTM
        WHERE   DOWNLOAD_DATE = V_PREVDATE
        AND STATUS = 'ACT'
        AND JOURNALCODE = 'ACCRU_SL'
        AND REVERSE = 'N'
        AND SUBSTR(SOURCEPROCESS, 1, 2) = 'SL'
        AND MASTERID IN (SELECT  MASTERID FROM IFRS_ACCT_SL_STOP_REV WHERE DOWNLOAD_DATE = V_CURRDATE );

        COMMIT;

    END IF;


    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_IFRS_ACCT_SL_JOURNAL_INTM' ,'');

    COMMIT;

END ;