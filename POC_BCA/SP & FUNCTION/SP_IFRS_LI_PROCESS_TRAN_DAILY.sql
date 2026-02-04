CREATE OR REPLACE PROCEDURE  SP_IFRS_LI_PROCESS_TRAN_DAILY
AS
  V_CURRDATE DATE;
  V_PREVDATE DATE;

BEGIN
    SELECT MAX(CURRDATE)
            , MAX(PREVDATE) INTO V_CURRDATE, V_PREVDATE
    FROM IFRS_LI_PRC_DATE_AMORT;

    INSERT INTO IFRS_LI_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'START','SP_IFRS_LI_PROCESS_TRAN_DAILY','');

    COMMIT;

    --DELETE FIRST
    DELETE FROM IFRS_LI_ACCT_COST_FEE
    WHERE DOWNLOAD_DATE >= V_CURRDATE;

    COMMIT;

    -- FEE
    INSERT INTO IFRS_LI_ACCT_COST_FEE (
     DOWNLOAD_DATE
     ,MASTERID
     ,BRCODE
     ,CIFNO
     ,FACNO
     ,ACCTNO
     ,DATASOURCE
     ,PRD_TYPE
     ,PRD_CODE
     ,TRX_CODE
     ,CCY
     ,FLAG_CF
     ,FLAG_REVERSE
     ,METHOD
     ,STATUS
     ,SRCPROCESS
     ,AMOUNT
     ,INITIAL_AMOUNT -- TAMBAH INFORMASI INITIAL AMOUNT  UNTUK LIAB 20180824
     ,TAX_AMOUNT
     ,ISTAX_INCLUDE
     ,CREATEDDATE
     ,CREATEDBY
     ,TRX_REFF_NUMBER
     ,SOURCE_TABLE
     ,TRX_LEVEL
     )
    SELECT A.DOWNLOAD_DATE EFFDATE
     ,A.MASTERID MASTERID
     ,A.BRANCH_CODE BRCODE
     ,NULL CIFNO
     ,A.FACILITY_NUMBER FACNO
     ,A.ACCOUNT_NUMBER ACCTNO
     ,A.DATA_SOURCE DATASOURCE
     ,A.PRD_TYPE
     ,A.PRD_CODE
     ,A.TRX_CODE
     ,A.CCY CCY
     ,SUBSTR(COALESCE(B.IFRS_TXN_CLASS, 'F'), 1, 1) FLAG_CF
     ,SUBSTR(COALESCE(A.DEBET_CREDIT_FLAG, 'X'), 1, 1) FLAG_REVERSE
     ,'X' METHOD
     ,'ACT' STATUS
     ,'TRAN_DAILY' SRCPROCESS
     ,A.ORG_CCY_AMT +
		CASE WHEN A.ISTAX_INCLUDE  = 0
	    THEN A.ORG_CCY_AMT *  (A.TAX_PERCENTAGE / 100)
	    ELSE 0
	    END
	  , CASE WHEN A.ISTAX_INCLUDE  = 0       -- TAMBAH INFORMASI INITIAL AMOUNT  UNTUK LIAB 20180824
  THEN A.ORG_CCY_AMT
  ELSE A.ORG_CCY_AMT * (100 - A.TAX_PERCENTAGE)/100
    END    -- INITIAL AMOUNT
  ,A.ORG_CCY_AMT *  (A.TAX_PERCENTAGE / 100)  -- TAX AMOUNT
	  ,ISTAX_INCLUDE
     ,SYSDATE CREATEDDATE
     ,'SP_IFRS_TRAN_DAILY' CREATEDBY
     ,TRX_REFERENCE_NUMBER
     ,SOURCE_TABLE
     ,TRX_LEVEL
    FROM IFRS_LI_TRANSACTION_DAILY A
    JOIN (SELECT DISTINCT  DATA_SOURCE
                          ,PRD_TYPE
                          ,PRD_CODE
                          ,TRX_CODE
                          ,CCY
                          ,IFRS_TXN_CLASS
           FROM IFRS_LI_TRANSACTION_PARAM
           WHERE IFRS_TXN_CLASS IN ('FEE' ,'COST')
            AND AMORTIZATION_FLAG = 'Y'
        ) B ON (B.DATA_SOURCE = A.DATA_SOURCE OR NVL(B.DATA_SOURCE, 'ALL') = 'ALL')
     AND (B.PRD_TYPE = A.PRD_TYPE OR NVL(B.PRD_TYPE, 'ALL') = 'ALL')
     AND (B.PRD_CODE = A.PRD_CODE OR NVL(B.PRD_CODE, 'ALL') = 'ALL')
     AND B.TRX_CODE = A.TRX_CODE
     AND (B.CCY = A.CCY OR B.CCY = 'ALL')
    WHERE A.DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    --REVERSAL

	INSERT INTO IFRS_LI_ACCT_COST_FEE (
	  DOWNLOAD_DATE
	  ,MASTERID
	  ,BRCODE
	  ,CIFNO
	  ,FACNO
	  ,ACCTNO
	  ,DATASOURCE
	  ,PRD_TYPE
	  ,PRD_CODE
	  ,TRX_CODE
	  ,CCY
	  ,FLAG_CF
	  ,FLAG_REVERSE
	  ,METHOD
	  ,STATUS
	  ,SRCPROCESS
	  ,AMOUNT
	  ,INITIAL_AMOUNT -- TAMBAH INFORMASI INITIAL AMOUNT  UNTUK LIAB 20180824
	  ,TAX_AMOUNT
	  ,ISTAX_INCLUDE
	  ,CREATEDDATE
	  ,CREATEDBY
	  ,TRX_REFF_NUMBER
	  ,SOURCE_TABLE
	  ,TRX_LEVEL
	  )
	 SELECT
	   V_CURRDATE
	  ,A.MASTERID
	  ,A.BRCODE
	  ,A.CIFNO
	  ,A.FACNO
	  ,A.ACCTNO
	  ,A.DATASOURCE
	  ,A.PRD_TYPE
	  ,A.PRD_CODE
	  ,A.TRX_CODE
	  ,A.CCY
	  ,A.FLAG_CF
	  ,SUBSTR(COALESCE(B.DEBIT_CREDIT_FLAG, 'X'), 1, 1)
	  ,A.METHOD
	  ,A.STATUS
	  ,A.SRCPROCESS
	  ,A.AMOUNT
	  ,A.INITIAL_AMOUNT
	  ,A.TAX_AMOUNT
	  ,A.ISTAX_INCLUDE
	  ,A.CREATEDDATE
	  ,A.CREATEDBY
	  ,A.TRX_REFF_NUMBER
	  ,A.SOURCE_TABLE
	  ,A.TRX_LEVEL
	 FROM IFRS_LI_ACCT_COST_FEE A
	 INNER JOIN
	 TBLU_TRANS_LIAB B
	 ON
	 A.TRX_REFF_NUMBER = B.TRX_REF_NUMBER
	 AND
	 B.DOWNLOAD_DATE = V_CURRDATE
	 WHERE
	 A.STATUS = 'ACT';
	--REVERSAL

	COMMIT;

    --AND A.ACCOUNT_NUMBER <> A.FACILITY_NUMBER
    /*20171129 INSERT FROM COST FEE UNPROCESSED FROM PREVDATE*/
    INSERT INTO IFRS_LI_ACCT_COST_FEE (
     DOWNLOAD_DATE
     ,MASTERID
     ,BRCODE
     ,CIFNO
     ,FACNO
     ,ACCTNO
     ,DATASOURCE
     ,PRD_TYPE
     ,PRD_CODE
     ,TRX_CODE
     ,CCY
     ,FLAG_CF
     ,FLAG_REVERSE
     ,METHOD
     ,STATUS
     ,SRCPROCESS
     ,AMOUNT
     ,INITIAL_AMOUNT -- TAMBAH INFORMASI INITIAL AMOUNT  UNTUK LIAB 20180824
     ,TAX_AMOUNT
     ,ISTAX_INCLUDE
     ,CREATEDDATE
     ,CREATEDBY
     ,TRX_REFF_NUMBER
     ,SOURCE_TABLE
     ,TRX_LEVEL
     )
    SELECT V_CURRDATE
     ,MASTERID
     ,BRCODE
     ,CIFNO
     ,FACNO
     ,ACCTNO
     ,DATASOURCE
     ,PRD_TYPE
     ,PRD_CODE
     ,TRX_CODE
     ,CCY
     ,FLAG_CF
     ,CASE WHEN FLAG_AL = 'A' THEN --ASSETS
                                  CASE WHEN FLAG_CF = 'F' THEN CASE WHEN FLAG_REVERSE = 'N' THEN 'C' ELSE 'D' END
                                       ELSE CASE WHEN FLAG_REVERSE = 'N' THEN 'D' ELSE 'C' END
                                  END
      ELSE --LIAB
          CASE  WHEN FLAG_CF = 'F' THEN CASE WHEN FLAG_REVERSE = 'N' THEN 'C' ELSE 'Y' END
                ELSE CASE WHEN FLAG_REVERSE = 'N' THEN 'D' ELSE 'C' END
          END
      END AS FLAG_REVERSE
     ,METHOD
     ,'ACT'
     ,SRCPROCESS
     ,AMOUNT
     ,INITIAL_AMOUNT 	-- TAMBAH INFORMASI INITIAL AMOUNT  UNTUK LIAB 20180824
     ,TAX_AMOUNT
     ,ISTAX_INCLUDE
     ,CREATEDDATE
     ,CREATEDBY
     ,TRX_REFF_NUMBER
     ,SOURCE_TABLE
     ,TRX_LEVEL
    FROM IFRS_LI_ACCT_COST_FEE
    WHERE DOWNLOAD_DATE = V_PREVDATE
     AND STATUS = 'NPRCD';

    COMMIT;

    /*
  -- COST DIJADIKAN SATU SCRIPT
             INSERT  INTO IFRS_LI_ACCT_COST_FEE
                        ( DOWNLOAD_DATE ,
                          MASTERID ,
                          BRCODE ,
                          CIFNO ,
                          FACNO ,
                          ACCTNO ,
                          DATASOURCE ,
                          PRD_TYPE ,
                          PRD_CODE ,
                          TRX_CODE ,
                          CCY ,
                          FLAG_CF ,                             FLAG_REVERSE ,
                          METHOD ,
                          STATUS ,
                          SRCPROCESS ,
                          AMOUNT ,
                          CREATEDDATE ,
                          CREATEDBY



                        )
                        SELECT  A.DOWNLOAD_DATE EFFDATE ,
                                  A.MASTERID MASTERID ,
                                  A.BRANCH_CODE BRCODE ,
                                  NULL CIFNO ,
                                  A.FACILITY_NUMBER FACNO ,
                                  A.ACCOUNT_NUMBER ACCTNO ,
                                  A.DATA_SOURCE DATASOURCE ,
                                  A.PRD_TYPE ,
                                  A.PRD_CODE ,
                                  A.TRX_CODE ,
                                  A.CCY CCY ,
                                  'C' FLAG_CF ,
                                  SUBSTRING(COALESCE(A.DEBET_CREDIT_FLAG, 'X'), 1, 1) FLAG_REVERSE ,
                                  'X' METHOD ,
                                  'PARAM' STATUS ,
                                  'TRAN_DAILY' SRCPROCESS ,
                                  A.ORG_CCY_AMT AS AMOUNT ,
                                  CURRENT_TIMESTAMP CREATEDDATE ,
                                  'SP_PSAK_TRAN_DAILY' CREATEDBY
                        FROM     TRANSACTION_DAILY A
                                  JOIN ( SELECT DISTINCT
                                                        DATA_SOURCE ,
                                                        PRD_TYPE ,
                                                        PRD_CODE ,
                                                        TRX_CODE ,
                                                        CCY
                                            FROM      IFRS_LI_MASTER_TRANSACTION_PARAM
                                            WHERE     IFRS_TXN_CLASS = 'COST'
                                                        AND AMORTIZATION_FLAG = 'Y'
                                         ) B ON B.DATA_SOURCE = A.DATA_SOURCE
                                                  AND B.PRD_TYPE = A.PRD_TYPE
                                                  AND B.PRD_CODE = A.PRD_CODE
                                                  AND B.TRX_CODE = A.TRX_CODE
                                                  AND B.CCY = A.CCY
                        WHERE    A.DOWNLOAD_DATE = @V_CURRDATE

  */
    INSERT INTO IFRS_LI_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LI_PROCESS_TRAN_DAILY','INSERTED');

    COMMIT;

    -- UPDATE INFO FROM IMA_CURR
    /* FD 30042018: UPDATE SET DATA SOURCE DISINI JUGA, WHERE NYA HANYA BY MASTERID SAJA */
    MERGE INTO IFRS_LI_ACCT_COST_FEE A
    USING IFRS_LI_IMA_AMORT_CURR B
    ON (B.MASTERID = A.MASTERID
       --AND B.DATA_SOURCE = IFRS_LI_ACCT_COST_FEE.DATASOURCE
       AND A.DOWNLOAD_DATE = V_CURRDATE
       )
    WHEN MATCHED THEN
    UPDATE
    SET CIFNO = B.CUSTOMER_NUMBER
     ,PRD_CODE = B.PRODUCT_CODE
     ,PRD_TYPE = B.PRODUCT_TYPE
     ,DATASOURCE = B.DATA_SOURCE
     ,BRCODE = B.BRANCH_CODE
     ,FACNO = B.FACILITY_NUMBER  ;

    INSERT INTO IFRS_LI_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LI_PROCESS_TRAN_DAILY','UPD FROM IMA');

    COMMIT;

    /*
  -- UPDATE STATUS FROM TRAN PARAM

             UPDATE  IFRS_LI_ACCT_COST_FEE
             SET      FLAG_CF = SUBSTRING(COALESCE(B.IFRS_TXN_CLASS, 'F'), 1, 1) ,
                        STATUS = 'ACT'
             FROM     IFRS_LI_MASTER_TRANSACTION_PARAM B
             WHERE    B.DATA_SOURCE = IFRS_LI_ACCT_COST_FEE.DATASOURCE
                        AND B.PRD_TYPE = IFRS_LI_ACCT_COST_FEE.PRD_TYPE
                        AND B.PRD_CODE = IFRS_LI_ACCT_COST_FEE.PRD_CODE
                        AND B.TRX_CODE = IFRS_LI_ACCT_COST_FEE.TRX_CODE
                        AND B.CCY = IFRS_LI_ACCT_COST_FEE.CCY
                        AND IFRS_LI_ACCT_COST_FEE.DOWNLOAD_DATE = @V_CURRDATE
                        AND IFRS_LI_ACCT_COST_FEE.SRCPROCESS = 'TRAN_DAILY'
                        AND B.AMORTIZATION_FLAG = 'Y'
  */
    INSERT INTO IFRS_LI_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LI_PROCESS_TRAN_DAILY','UPD FROM TRAN PARAM');

    COMMIT;

    -- UPDATE FLAG_AL
    MERGE INTO IFRS_LI_ACCT_COST_FEE A
    USING IFRS_LI_PRODUCT_PARAM B
    ON ((B.DATA_SOURCE = A.DATASOURCE  OR NVL(B.DATA_SOURCE, 'ALL') = 'ALL')
         AND (B.PRD_TYPE = A.PRD_TYPE OR NVL(B.PRD_TYPE, 'ALL') = 'ALL')
         AND (B.PRD_CODE = A.PRD_CODE OR NVL(B.PRD_CODE, 'ALL') = 'ALL')
         AND (B.CCY = A.CCY OR NVL(B.CCY, 'ALL') = 'ALL')
         AND A.SRCPROCESS = 'TRAN_DAILY'
         AND A.DOWNLOAD_DATE = V_CURRDATE
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.FLAG_AL = COALESCE(B.FLAG_AL, 'L');

    INSERT INTO IFRS_LI_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LI_PROCESS_TRAN_DAILY','UPD FROM PROD PARAM');

    COMMIT;

    --UPDATE AMOUNT AND REV FLAG
    UPDATE IFRS_LI_ACCT_COST_FEE
    SET AMOUNT = CASE
   WHEN FLAG_AL = 'A'
    THEN CASE
      WHEN FLAG_CF = 'F'
       THEN - 1 * AMOUNT
      ELSE AMOUNT
      END
   ELSE CASE
     WHEN FLAG_CF = 'C'
      THEN - 1 * AMOUNT
     ELSE AMOUNT
     END
   END
  ,INITIAL_AMOUNT = CASE
   WHEN FLAG_AL = 'A'
    THEN CASE
      WHEN FLAG_CF = 'F'
       THEN - 1 * INITIAL_AMOUNT
      ELSE INITIAL_AMOUNT
      END
   ELSE CASE
     WHEN FLAG_CF = 'C'
      THEN - 1 * INITIAL_AMOUNT
     ELSE INITIAL_AMOUNT
     END
   END
  ,TAX_AMOUNT = CASE
   WHEN FLAG_AL = 'A'
    THEN CASE
      WHEN FLAG_CF = 'F'
       THEN - 1 * TAX_AMOUNT
      ELSE TAX_AMOUNT
      END
   ELSE CASE
     WHEN FLAG_CF = 'C'
      THEN - 1 * TAX_AMOUNT
     ELSE TAX_AMOUNT
     END
   END
     ,FLAG_REVERSE = CASE WHEN FLAG_AL = 'A' THEN --ASSETS
                                                  CASE WHEN FLAG_CF = 'F' THEN CASE WHEN FLAG_REVERSE = 'C' THEN 'N' ELSE 'Y' END
                                                       ELSE CASE WHEN FLAG_REVERSE = 'D' THEN 'N' ELSE 'Y' END
                                                  END
                     ELSE --LIAB
                         CASE WHEN FLAG_CF = 'F' THEN CASE WHEN FLAG_REVERSE = 'C' THEN 'N' ELSE 'Y' END
                              ELSE CASE WHEN FLAG_REVERSE = 'D' THEN 'N' ELSE 'Y' END
                         END
                     END
    WHERE DOWNLOAD_DATE = V_CURRDATE
     AND STATUS = 'ACT'
     AND SRCPROCESS = 'TRAN_DAILY';

    COMMIT;

    INSERT INTO IFRS_LI_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'DEBUG','SP_IFRS_LI_PROCESS_TRAN_DAILY','UPD AMT REV');

    INSERT INTO IFRS_LI_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
    VALUES(V_CURRDATE,SYSTIMESTAMP,'END','SP_IFRS_LI_PROCESS_TRAN_DAILY','');

    COMMIT;
END  ;