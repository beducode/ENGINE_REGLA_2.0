CREATE OR REPLACE PROCEDURE SP_IFRS_COST_FEE_STATUS
AS
  V_CURRDATE DATE ;
  V_PREVDATE DATE;
  V_CX NUMBER(10);
  V_EXISTS INT;
  V_PARAM_MAT_LEVEL INT;

BEGIN
    V_PARAM_MAT_LEVEL := 0;

    SELECT  MAX(CURRDATE), MAX(PREVDATE)
    INTO V_CURRDATE, V_PREVDATE
    FROM    IFRS_PRC_DATE_AMORT ;

    SELECT COMMONUSAGE
    INTO V_PARAM_MAT_LEVEL
	 FROM TBLM_COMMONCODEHEADER
	 WHERE COMMONCODE = 'SCM001';


    INSERT INTO IFRS_AMORT_LOG( DOWNLOAD_DATE , DTM , OPS ,PROCNAME , REMARK)
    VALUES(V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_IFRS_COST_FEE_STATUS' ,'');

    --reset
    UPDATE  /*+ PARALLEL(12) */ IFRS_ACCT_COST_FEE
    SET     STATUS = 'FRZNF' ,
            METHOD = 'X'
    WHERE   DOWNLOAD_DATE = V_CURRDATE
            AND STATUS <> 'PARAM' ; -- tran param not match
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE , DTM , OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_COST_FEE_STATUS' , '1') ;

    -- update from curr date
    -- check currency and due date
    MERGE  INTO IFRS_ACCT_COST_FEE A
    USING IFRS_IMA_AMORT_CURR B
    ON (A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
        AND B.MASTERID = A.MASTERID
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.STATUS=CASE WHEN A.STATUS = 'PARAM'  THEN 'PARAM'
                      WHEN A.CCY != B.CURRENCY THEN 'FRZCCY'
                      WHEN ( B.LOAN_DUE_DATE <= A.DOWNLOAD_DATE OR B.ACCOUNT_STATUS IN ('W','C','E','CE','CT','CN')) THEN 'PNL'
                      ELSE 'ACT'
                      END ,
        A.POS_AMOUNT = CASE WHEN B.OUTSTANDING = 0  THEN 100
                            ELSE A.AMOUNT / B.OUTSTANDING
                            END ,
        A.DATASOURCE = B.DATA_SOURCE ,
        A.PRD_TYPE = B.PRODUCT_TYPE ,
        A.PRD_CODE = B.PRODUCT_CODE;
    COMMIT;


    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_COST_FEE_STATUS' ,'2') ;

    -- closed will go PNL
    UPDATE  /*+ PARALLEL(12) */ IFRS_ACCT_COST_FEE
    SET     STATUS = 'PNL' ,
            CREATEDBY = 'CLOSED'
    WHERE   DOWNLOAD_DATE = V_CURRDATE
            AND MASTERID IN ( SELECT    DISTINCT MASTERID
                              FROM IFRS_ACCT_CLOSED
                              WHERE DOWNLOAD_DATE = V_CURRDATE )
            AND STATUS <> 'PARAM';
    COMMIT;


    INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE , DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_COST_FEE_STATUS' ,'3') ;

    -----------------------------INI DI REMARK---------------------------------------------------
    --20171129 untuk fee turun duluan tapi paymschd, belom turun.
    /*
    SELECT DISTINCT MASTER_ACCOUNT_ID
    INTO #PAYM_COMBINE
    FROM IFRS_PAYM_SCHD_COMBINE
    WHERE DOWNLOAD_DATE = V_CURRDATE;

    UPDATE DBO.IFRS_ACCT_COST_FEE
    SET STATUS = 'NPRCD'
    WHERE DOWNLOAD_DATE = @v_currdate
    AND
    MASTERID NOT IN (SELECT MASTERID FROM DBO.IMA_AMORT_CURR)

    UPDATE DBO.IFRS_ACCT_COST_FEE
    SET STATUS = 'NPRCD'
    WHERE DOWNLOAD_DATE = @v_currdate
    AND
    MASTERID NOT IN (SELECT DISTINCT MASTER_ACCOUNT_ID FROM #paym_combine)

      INSERT  INTO IFRS_AMORT_LOG
              ( DOWNLOAD_DATE ,
                DTM ,
                OPS ,
                PROCNAME ,
                REMARK
              )
      VALUES  ( @v_currdate ,
                CURRENT_TIMESTAMP ,
                'DEBUG' ,
                'SP_IFRS_COST_FEE_STATUS' ,
                '4'
              ) ;
    */
    -- mark amort method based on product param
    MERGE  INTO IFRS_ACCT_COST_FEE A
    USING(SELECT X.* , V_CURRDATE CURRDATE
          FROM IFRS_PRODUCT_PARAM X
          )B
    ON (A.DATASOURCE = B.DATA_SOURCE
            AND A.PRD_TYPE = B.PRD_TYPE
            AND A.PRD_CODE = B.PRD_CODE
            AND (A.CCY = B.CCY  OR B.CCY = 'ALL')
            AND A.DOWNLOAD_DATE = V_CURRDATE
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.METHOD=B.AMORT_TYPE;
    COMMIT;

    -- UPDATE AMORT TYPE FROM IMA LIMIT
    MERGE  INTO IFRS_ACCT_COST_FEE A
    USING IFRS_IMA_LIMIT B
    ON (A.FACNO = B.NO_REK_LBU
        AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
        AND A.DOWNLOAD_DATE = V_CURRDATE
        AND A.DATASOURCE <> 'ACCEPTANCE'
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.METHOD=CASE WHEN B.REVOLVING_FLAG = 1 THEN 'SL' ELSE 'EIR' END;

    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_COST_FEE_STATUS' ,'5') ;

    -- eir with zero os will go PNL
    UPDATE  /*+ PARALLEL(12) */ IFRS_ACCT_COST_FEE
    SET     STATUS = 'PNL' ,
            CREATEDBY = 'EIR_ZERO_OS'
    WHERE   STATUS = 'ACT'
            AND METHOD = 'EIR'
            AND DOWNLOAD_DATE = V_CURRDATE
            AND MASTERID IN (SELECT  MASTERID FROM IFRS_IMA_AMORT_CURR WHERE   COALESCE(OUTSTANDING,0) <= 0 );
    COMMIT;


    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_COST_FEE_STATUS' ,'5') ;

    IF (V_PARAM_MAT_LEVEL = 0) THEN
	BEGIN
		-- ABS MATERIALITY FEE BY PRODUCT
	    MERGE INTO IFRS_ACCT_COST_FEE A
		USING
		( SELECT A.ID
		  FROM IFRS_ACCT_COST_FEE A
			LEFT JOIN
			(SELECT X.*,V_CURRDATE CURRDATE FROM IFRS_PRODUCT_PARAM X) B
			ON
				(A.DATASOURCE = B.DATA_SOURCE OR B.DATA_SOURCE = 'ALL')
				AND (A.PRD_TYPE = B.PRD_TYPE OR B.PRD_TYPE = 'ALL')
				AND (A.PRD_CODE = B.PRD_CODE OR B.PRD_CODE = 'ALL')
				AND (A.CCY = B.CCY OR B.CCY = 'ALL')
				AND A.DOWNLOAD_DATE = B.CURRDATE
			LEFT JOIN IFRS_IMA_AMORT_CURR  C ON A.MASTERID = C.MASTERID AND A.DOWNLOAD_DATE = C.DOWNLOAD_DATE
			WHERE   A.FLAG_CF = 'F'
					AND A.STATUS = 'ACT'
					AND B.FEE_MAT_TYPE IN ('','ABS')
					AND ABS(A.AMOUNT * NVL(C.EXCHANGE_RATE,1)) < B.FEE_MAT_AMT
		) B ON (A.ID = B.ID)
		WHEN MATCHED THEN
		UPDATE SET A.CREATEDBY = 'ABS_MAT_FEE'
					,STATUS = 'PNL';

		COMMIT;
	END;
	ELSE
	BEGIN
		-- ABS MATERIALITY FEE BY TRANSACTION
		MERGE INTO IFRS_ACCT_COST_FEE A
		USING
		( SELECT A.ID
		  FROM IFRS_ACCT_COST_FEE A
			LEFT JOIN
			(SELECT X.*,V_CURRDATE CURRDATE FROM IFRS_TRANSACTION_PARAM X) B
			ON
				(A.DATASOURCE = B.DATA_SOURCE OR B.DATA_SOURCE = 'ALL')
				AND (A.PRD_TYPE = B.PRD_TYPE OR B.PRD_TYPE = 'ALL')
				AND (A.PRD_CODE = B.PRD_CODE OR B.PRD_CODE = 'ALL')
				AND (A.TRX_CODE = B.TRX_CODE OR B.TRX_CODE = 'ALL')
				AND (A.CCY = B.CCY OR B.CCY = 'ALL')
				AND A.DOWNLOAD_DATE = B.CURRDATE
			LEFT JOIN IFRS_IMA_AMORT_CURR  C ON A.MASTERID = C.MASTERID AND A.DOWNLOAD_DATE = C.DOWNLOAD_DATE
			WHERE   A.FLAG_CF = 'F'
					AND A.STATUS = 'ACT'
					--AND B.FEE_MAT_TYPE IN ('','ABS')
					--AND ABS(A.AMOUNT * NVL(C.EXCHANGE_RATE,1)) < B.FEE_MAT_AMT
		) B ON (A.ID = B.ID)
		WHEN MATCHED THEN
		UPDATE SET A.CREATEDBY = 'ABS_MAT_FEE'
					,STATUS = 'PNL';

		COMMIT;
	END;
	END IF;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_COST_FEE_STATUS' ,'6') ;

    COMMIT;

    /*
    UPDATE  IFRS_ACCT_COST_FEE
    SET     status = 'PNL'
    WHERE   DOWNLOAD_DATE = @v_currdate
            AND createdby = 'ABS_MAT_FEE'

    */

    IF (V_PARAM_MAT_LEVEL = 0) THEN
	BEGIN
		-- ABS MATERIALITY COST BY PRODUCT
	    MERGE INTO IFRS_ACCT_COST_FEE A
		USING
		( SELECT A.ID
		  FROM IFRS_ACCT_COST_FEE A
			LEFT JOIN
			(SELECT X.*,V_CURRDATE CURRDATE FROM IFRS_PRODUCT_PARAM X) B
            ON
                (A.DATASOURCE = B.DATA_SOURCE OR B.DATA_SOURCE = 'ALL')
                AND (A.PRD_TYPE = B.PRD_TYPE OR B.PRD_TYPE = 'ALL')
                AND (A.PRD_CODE = B.PRD_CODE OR B.PRD_CODE = 'ALL')
                AND (A.CCY = B.CCY OR B.CCY = 'ALL')
                AND A.DOWNLOAD_DATE = B.CURRDATE
            LEFT JOIN IFRS_IMA_AMORT_CURR  C ON A.MASTERID = C.MASTERID AND A.DOWNLOAD_DATE = C.DOWNLOAD_DATE
            WHERE   A.FLAG_CF = 'C'
                    AND A.STATUS = 'ACT'
                    AND B.FEE_MAT_TYPE IN ('','ABS')
                    AND ABS(A.AMOUNT * NVL(C.EXCHANGE_RATE,1)) < B.COST_MAT_AMT
		) B ON (A.ID = B.ID)
		WHEN MATCHED THEN
		UPDATE SET A.CREATEDBY = 'ABS_MAT_COST'
					,STATUS = 'PNL';

		COMMIT;
	END;
	ELSE
	BEGIN
		-- ABS MATERIALITY COST BY TRANSACTION
		MERGE INTO IFRS_ACCT_COST_FEE A
		USING
		( SELECT A.ID
		  FROM IFRS_ACCT_COST_FEE A
			LEFT JOIN
			(SELECT X.*,V_CURRDATE CURRDATE FROM IFRS_TRANSACTION_PARAM X) B
            ON
                (A.DATASOURCE = B.DATA_SOURCE OR B.DATA_SOURCE = 'ALL')
                AND (A.PRD_TYPE = B.PRD_TYPE OR B.PRD_TYPE = 'ALL')
                AND (A.PRD_CODE = B.PRD_CODE OR B.PRD_CODE = 'ALL')
                AND (A.TRX_CODE = B.TRX_CODE OR B.TRX_CODE = 'ALL')
                AND (A.CCY = B.CCY OR B.CCY = 'ALL')
                AND A.DOWNLOAD_DATE = B.CURRDATE
            LEFT JOIN IFRS_IMA_AMORT_CURR  C ON A.MASTERID = C.MASTERID AND A.DOWNLOAD_DATE = C.DOWNLOAD_DATE
            WHERE   A.FLAG_CF = 'C'
                    AND A.STATUS = 'ACT'
                    --AND B.FEE_MAT_TYPE IN ('','ABS')
                   -- AND ABS(A.AMOUNT * NVL(C.EXCHANGE_RATE,1)) < B.COST_MAT_AMT
		) B ON (A.ID = B.ID)
		WHEN MATCHED THEN
		UPDATE SET A.CREATEDBY = 'ABS_MAT_COST'
					,STATUS = 'PNL';

		COMMIT;
	END;
	END IF;


    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_COST_FEE_STATUS' ,'7') ;
    /*
    UPDATE  IFRS_ACCT_COST_FEE
    SET     status = 'PNL'
    WHERE   effdate = @v_currdate
            AND createdby = 'ABS_MAT_COST'


    INSERT  INTO IFRS_AMORT_LOG
            ( DOWNLOAD_DATE ,
              DTM ,
              OPS ,
              PROCNAME ,
              REMARK
            )
    VALUES  ( @v_currdate ,
              CURRENT_TIMESTAMP ,
              'DEBUG' ,
              'SP_IFRS_COST_FEE_STATUS' ,
              '9'
            ) ;
    */

    -- percent of OS fee
    MERGE INTO (SELECT * FROM IFRS_ACCT_COST_FEE
                WHERE STATUS='ACT')A
    USING (SELECT X.* , V_CURRDATE CURRDATE
           FROM IFRS_PRODUCT_PARAM X
           ) B
    ON ( A.DATASOURCE = B.DATA_SOURCE
            AND A.PRD_TYPE = B.PRD_TYPE
            AND A.PRD_CODE = B.PRD_CODE
            AND (A.CCY = B.CCY OR B.CCY = 'ALL')
            AND A.DOWNLOAD_DATE = B.CURRDATE
            AND A.FLAG_CF = 'F'
            AND B.COST_MAT_TYPE = 'POS'
            AND ABS(A.POS_AMOUNT) < B.FEE_MAT_AMT
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.CREATEDBY = 'POS_MAT_FEE'
       ,A.STATUS = 'PNL';
    COMMIT;


    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_COST_FEE_STATUS' ,'8') ;

    /*
    UPDATE  IFRS_ACCT_COST_FEE
    SET     status = 'PNL'
    WHERE   effdate = @v_currdate
            AND createdby = 'POS_MAT_FEE'


    INSERT  INTO IFRS_AMORT_LOG
            ( DOWNLOAD_DATE ,
              DTM ,
              OPS ,
              PROCNAME ,
              REMARK
            )
    VALUES  ( @v_currdate ,
              CURRENT_TIMESTAMP ,
              'DEBUG' ,
              'SP_IFRS_COST_FEE_STATUS' ,
              '11'
            ) ;
    */

    -- percent of OS cost
    MERGE INTO (SELECT * FROM IFRS_ACCT_COST_FEE
                WHERE STATUS='ACT')A
    USING (SELECT X.* , V_CURRDATE CURRDATE
           FROM IFRS_PRODUCT_PARAM X
           ) B
    ON ( A.DATASOURCE = B.DATA_SOURCE
            AND A.PRD_TYPE = B.PRD_TYPE
            AND A.PRD_CODE = B.PRD_CODE
            AND (A.CCY = B.CCY OR B.CCY = 'ALL')
            AND A.DOWNLOAD_DATE = B.CURRDATE
            AND A.FLAG_CF = 'C'
            AND B.COST_MAT_TYPE = 'POS'
            AND ABS(A.POS_AMOUNT) < B.COST_MAT_AMT
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.CREATEDBY = 'POS_MAT_COST'
       ,A.STATUS = 'PNL';
    COMMIT;


    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_COST_FEE_STATUS' ,'9') ;
    /*
    UPDATE  IFRS_ACCT_COST_FEE
    SET     status = 'PNL'
    WHERE   effdate = @v_currdate
            AND createdby = 'POS_MAT_COST'


    INSERT  INTO IFRS_AMORT_LOG
            ( DOWNLOAD_DATE ,
              DTM ,
              OPS ,
              PROCNAME ,
              REMARK
            )
    VALUES  ( @v_currdate ,
              CURRENT_TIMESTAMP ,
              'DEBUG' ,
              'SP_IFRS_COST_FEE_STATUS' ,
              '13'
            ) ;
    */
    /*
    --20151012 icbc changes : update SL method from pma.revolving_flag=Y
    UPDATE  IFRS_ACCT_COST_FEE
    SET     method = 'SL'
    WHERE   DOWNLOAD_DATE = @v_currdate
            AND MASTERID IN ( SELECT    MASTERID
                              FROM      IMA_AMORT_CURR
                              WHERE     DOWNLOAD_DATE = @v_currdate
                                        AND revolving_flag = 'Y' )


    INSERT  INTO IFRS_AMORT_LOG
            ( DOWNLOAD_DATE ,
              DTM ,
              OPS ,
              PROCNAME ,
              REMARK
            )
    VALUES  ( @v_currdate ,
              CURRENT_TIMESTAMP ,
              'DEBUG' ,
              'SP_IFRS_COST_FEE_STATUS' ,
              '14'
            ) ;
    */

    /* remarks dulu 20160524
    --20151012 icbc changes : update status to PNL if ACT SL dont have psak_paym_sched
    UPDATE  IFRS_ACCT_COST_FEE
    SET     status = 'PNL'
    WHERE   effdate = @v_currdate
            AND status = 'ACT'
            AND method = 'SL'
            AND MASTERID IN (
            SELECT DISTINCT
                    a.MASTER_ACCOUNT_ID
            FROM    psak_master_account a
                    LEFT JOIN PSAK_PAYM_SCHD b ON b.ACC_MSTR_ID = a.MASTER_ACCOUNT_ID
            WHERE   a.DOWNLOAD_DATE = @v_currdate
                    AND a.revolving_flag = 'Y'
                    AND b.ACC_MSTR_ID IS NULL )
    remarks dulu 20160524 */


    /* remarks dulu 20160524
    --20151202 icbc changes : update status to PNL if SL Account Max Payment Date <= Currdate
    TRUNCATE TABLE TMP_T1

    INSERT  INTO TMP_T1
            ( MASTERID
            )
            SELECT  ACC_MSTR_ID
            FROM    PSAK_PAYM_SCHD
            GROUP BY ACC_MSTR_ID
            HAVING  MAX(PMTDATE) <= @v_currdate


    UPDATE  IFRS_ACCT_COST_FEE
    SET     STATUS = 'PNL'
    WHERE   EFFDATE = @v_currdate
            AND STATUS = 'ACT'
            AND METHOD = 'SL'
            AND MASTERID IN ( SELECT    MASTERID
                              FROM      TMP_T1 )

    remarks dulu 20160524 */
    /*
    ----CTBC_20180525: ACT but dont have payment setting will go to FRZPYM

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T1' ;
    COMMIT;

    INSERT  INTO TMP_T1 ( MASTERID)
    SELECT  MASTERID
    FROM    IFRS_MASTER_PAYMENT_SETTING
    WHERE DOWNLOAD_DATE = V_CURRDATE
    GROUP BY MASTERID;
    COMMIT;


    UPDATE  IFRS_ACCT_COST_FEE
    SET     STATUS = 'FRZPYM'
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND STATUS = 'ACT'
    AND METHOD = 'EIR'
    AND FLAG_AL = 'A'
    AND MASTERID NOT IN ( SELECT MASTERID FROM TMP_T1 );
    COMMIT;

    */

    ----CTBC_20180525: ACT with Method SL but Loan_start_date or Loan_due date is null will go to PNL
    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_COST_FEE
    SET STATUS = 'PNL'
			, CREATEDBY = 'SL_START_ENDDT_NULL'
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND STATUS = 'ACT'
    AND METHOD = 'SL'
    AND FLAG_AL = 'A'
    AND MASTERID IN ( SELECT MASTERID
                      FROM IFRS_IMA_AMORT_CURR
                      WHERE AMORT_TYPE = 'SL'
                      AND DOWNLOAD_DATE = V_CURRDATE
                      AND (LOAN_START_DATE IS NULL OR LOAN_DUE_DATE IS NULL OR (LOAN_START_DATE > LOAN_DUE_DATE)));
    COMMIT;


    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_T1';
    COMMIT;
    -- act but no method will go to FRZMTD
    UPDATE  /*+ PARALLEL(12) */ IFRS_ACCT_COST_FEE
    SET     STATUS = 'FRZMTD'
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND STATUS = 'ACT'
    AND METHOD NOT IN ( 'SL', 'EIR' );
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_COST_FEE_STATUS' ,'10') ;


    -- can not process ACT reverse fee/cost if no prev cost fee for that account
    UPDATE  /*+ PARALLEL(12) */ IFRS_ACCT_COST_FEE
    SET     STATUS = 'FRZREV'
    WHERE   ID IN ( SELECT  A.ID
                    FROM IFRS_ACCT_COST_FEE A
                    LEFT JOIN IFRS_ACCT_COST_FEE B
                      ON B.DOWNLOAD_DATE <= V_CURRDATE
                      AND B.FLAG_REVERSE = 'N'
                      AND B.AMOUNT = A.AMOUNT
                      AND A.MASTERID = B.MASTERID
                      -- 20160411 only get data before prorate
                      AND B.ID = B.CF_ID
                      -- 20160411 status filter not needed
                      -- and b.status='ACT'
            WHERE   A.DOWNLOAD_DATE = V_CURRDATE
                    AND A.FLAG_REVERSE = 'Y'
                    AND A.STATUS = 'ACT'
                    AND B.MASTERID IS NULL );
    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_COST_FEE_STATUS' ,'11') ;

    -- fill cf id
    UPDATE  /*+ PARALLEL(12) */ IFRS_ACCT_COST_FEE
    SET     CF_ID = ID
    WHERE   STATUS IN ( 'ACT', 'PNL' )
    -- 20160411 update currdate data
    AND DOWNLOAD_DATE = V_CURRDATE;
    COMMIT;

    --  DROP TABLE #PAYM_COMBINE;


    -- Daniel Siswanto 2018 for CTBC, update cf_id_rev
    -- start : pairing today new reversal, if not found then reject from prorate processing
    -- insert log debug
    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_COST_FEE_STATUS' ,'REVERSAL-PAIRING');


    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_REV_PAIR';
    COMMIT;

    INSERT /*+ PARALLEL(12) */ INTO TMP_REV_PAIR(
    ID,CF_ID,PAIR_ID
    )
    SELECT A.ID
          ,A.CF_ID
          ,MIN(B.ID) AS PAIR_ID
    FROM IFRS_ACCT_COST_FEE A
    LEFT JOIN IFRS_ACCT_COST_FEE B
      ON --b.ACCRU_DATE=@v_currdate and
      B.FLAG_REVERSE='N'
      AND B.CF_ID_REV IS NULL
      AND B.MASTERID=A.MASTERID
      AND B.AMOUNT=A.AMOUNT
      AND B.CCY=A.CCY
      AND B.FLAG_CF=A.FLAG_CF
      AND B.TRX_CODE=A.TRX_CODE
    WHERE A.DOWNLOAD_DATE=V_CURRDATE
    AND A.FLAG_REVERSE='Y'
    GROUP BY A.ID,A.CF_ID;
    COMMIT;

   --if more than one PAIR_ID on table #rev_pair then only allow one and reject the others
  /*  MERGE INTO TMP_REV_PAIR A
    USING (SELECT PAIR_ID,MIN(ID) AS ALLOWED_ID
                  FROM TMP_REV_PAIR
                  GROUP BY PAIR_ID
                  HAVING COUNT(PAIR_ID)>1
                  ) B
    ON  (A.PAIR_ID=B.PAIR_ID
         AND A.ID<>B.ALLOWED_ID
        )
    WHEN MATCHED THEN
    UPDATE
    SET A.PAIR_ID =NULL;*/


    UPDATE /*+ PARALLEL(12) */ TMP_REV_PAIR
    SET PAIR_ID=NULL
    WHERE PAIR_ID IN (SELECT PAIR_ID
                  FROM TMP_REV_PAIR
                  GROUP BY PAIR_ID
                  HAVING COUNT(PAIR_ID)>1  )
    AND ID not in (SELECT MIN(ID) AS ALLOWED_ID
                  FROM TMP_REV_PAIR
                  GROUP BY PAIR_ID
                  HAVING COUNT(PAIR_ID)>1  );
    COMMIT;


    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_REV_PAIR2';
    V_CX:=1;

    BEGIN
      SELECT 1 INTO V_EXISTS FROM TMP_REV_PAIR WHERE PAIR_ID IS NULL AND ROWNUM <=1;
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        V_EXISTS :=NULL;
    END;

    --loop 5x max
    WHILE ( V_CX<=5 AND V_EXISTS=1 )
    LOOP
      -- 2nd pass pairing
      DELETE /*+ PARALLEL(12) */ FROM TMP_REV_PAIR2;
      COMMIT;

      INSERT /*+ PARALLEL(12) */ INTO TMP_REV_PAIR2(
      ID2,CF_ID2,PAIR_ID2
      )
      SELECT A.ID,A.CF_ID,MIN(B.ID) AS PAIR_ID
      FROM IFRS_ACCT_COST_FEE A
      LEFT JOIN IFRS_ACCT_COST_FEE B
        ON B.FLAG_REVERSE='N'
        AND B.CF_ID_REV IS NULL
        AND B.MASTERID=A.MASTERID
        AND B.AMOUNT=A.AMOUNT
        AND B.CCY=A.CCY
        AND B.FLAG_CF=A.FLAG_CF
        AND B.TRX_CODE=A.TRX_CODE
        AND B.ID NOT IN (SELECT PAIR_ID FROM TMP_REV_PAIR WHERE PAIR_ID IS NOT NULL)
      WHERE A.DOWNLOAD_DATE=V_CURRDATE
      AND A.FLAG_REVERSE='Y'
      AND A.ID IN (SELECT ID FROM TMP_REV_PAIR WHERE PAIR_ID IS NULL)
      GROUP BY A.ID,A.CF_ID;
      COMMIT;


      --20180305 2nd pass : if more than one PAIR_ID on table #rev_pair then only allow one and reject the others
      MERGE INTO TMP_REV_PAIR2 X
      USING (SELECT A.ID2, B.ALLOWED_ID,B.PAIR_ID, A.CF_ID2
             FROM TMP_REV_PAIR2 A
             JOIN (SELECT PAIR_ID2 AS PAIR_ID ,MIN(ID2) AS ALLOWED_ID
                    FROM TMP_REV_PAIR2
                    GROUP BY PAIR_ID2
                    HAVING COUNT(PAIR_ID2)>1
                    ) B
            ON A.PAIR_ID2=B.PAIR_ID
            AND A.ID2<>B.ALLOWED_ID
         )Z
      ON (X.ID2=Z.ID2 AND X.CF_ID2=Z.CF_ID2
      )
      WHEN MATCHED THEN
      UPDATE
      SET X.PAIR_ID2=NULL;


      COMMIT;

      --20180305 2nd pass update back to tmp_rev_pair
      MERGE INTO TMP_REV_PAIR A
      USING TMP_REV_PAIR2 B
      ON (B.ID2=A.ID
          AND B.PAIR_ID2 IS NOT NULL
         )
      WHEN MATCHED THEN
      UPDATE
      SET A.PAIR_ID=B.PAIR_ID2;
      COMMIT;

      --inc cx
      V_CX:=V_CX+1;
    END LOOP;

    -- if pair_id is null then pair not found then reject : mark on COST_FEE table as FRZ and delete from FAC_CF
    MERGE INTO IFRS_ACCT_COST_FEE A
    USING TMP_REV_PAIR B
    ON (B.CF_ID=A.CF_ID
        AND A.DOWNLOAD_DATE=V_CURRDATE
        AND B.PAIR_ID IS NULL
       )
    WHEN MATCHED THEN UPDATE
    SET A.STATUS='FRZREVPRO';
    COMMIT;


    DELETE /*+ PARALLEL(12) */ FROM TMP_REV_PAIR WHERE PAIR_ID IS NULL;
    COMMIT;

    --update cf_id_rev of pair_id
    MERGE INTO  IFRS_ACCT_COST_FEE A
    USING TMP_REV_PAIR B
    ON (B.CF_ID=A.CF_ID
        AND A.DOWNLOAD_DATE=V_CURRDATE
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.CF_ID_REV =B.PAIR_ID;
    COMMIT;

    --end   : pairing today new reversal
    INSERT  INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_IFRS_COST_FEE_STATUS' ,'');
    COMMIT;
END;