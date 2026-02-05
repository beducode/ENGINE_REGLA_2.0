CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_JOURNAL_DATA
AS
    V_CURRDATE DATE ;
    V_PREVDATE DATE ;
    V_PREVMONTH DATE;

BEGIN
	/******************************************************************************
    01. DECLARE VARIABLE
    *******************************************************************************/
    SELECT  MAX(CURRDATE), MAX(PREVDATE)
    INTO V_CURRDATE, V_PREVDATE
    FROM IFRS_PRC_DATE_AMORT;

    V_PREVMONTH := LAST_DAY(ADD_MONTHS(V_CURRDATE,-1))  ;

    INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  (V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_IFRS_ACCT_JOURNAL_DATA' ,'');

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_IFRS_ACCT_JOURNAL_INTM';

    INSERT /*+ PARALLEL(12) */ INTO GTMP_IFRS_ACCT_JOURNAL_INTM
    (
        ID,
        DOWNLOAD_DATE,
        MASTERID,
        FACNO,
        CIFNO,
        ACCTNO,
        DATASOURCE,
        PRDTYPE,
        PRDCODE,
        TRXCODE,
        CCY,
        JOURNALCODE,
        JOURNALCODE2,
        STATUS,
        REVERSE,
        FLAG_CF,
        N_AMOUNT,
        N_AMOUNT_IDR,
        SOURCEPROCESS,
        CREATEDDATE,
        CREATEDBY,
        BRANCH,
        IS_PNL,
        CF_ID,
        FLAG_AL,
        METHOD
    )
    SELECT /*+ PARALLEL(12) */
        ID,
        DOWNLOAD_DATE,
        MASTERID,
        FACNO,
        CIFNO,
        ACCTNO,
        DATASOURCE,
        PRDTYPE,
        PRDCODE,
        TRXCODE,
        CCY,
        JOURNALCODE,
        JOURNALCODE2,
        STATUS,
        REVERSE,
        FLAG_CF,
        N_AMOUNT,
        N_AMOUNT_IDR,
        SOURCEPROCESS,
        CREATEDDATE,
        CREATEDBY,
        BRANCH,
        IS_PNL,
        CF_ID,
        FLAG_AL,
        METHOD
	FROM IFRS_ACCT_JOURNAL_INTM
	WHERE DOWNLOAD_DATE = V_CURRDATE;
	COMMIT;

	/******************************************************************************
    02. SET METHOD EIR
    *******************************************************************************/

    UPDATE  /*+ PARALLEL(12) */ GTMP_IFRS_ACCT_JOURNAL_INTM
    SET     METHOD = 'EIR'
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND SUBSTR(SOURCEPROCESS, 1, 3) = 'EIR';

    COMMIT;

	/******************************************************************************
    03.journal intm flag_al fill from IMA_AMORT_CURR
    *******************************************************************************/
    MERGE INTO GTMP_IFRS_ACCT_JOURNAL_INTM A
    USING IFRS_IMA_AMORT_CURR B
    ON (B.MASTERID = A.MASTERID
        AND A.DOWNLOAD_DATE = V_CURRDATE
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.FLAG_AL = B.IAS_CLASS;

    COMMIT;

	--POPULATE EXCHANGE RATE PINDAH KE INITIAL UPDATE
	--EXEC SP_IFRS_POPULATE_EXCHANGE_RATE
	--UPDATE  A
	--SET     N_AMOUNT_IDR = A.N_AMOUNT * COALESCE(B.RATE_AMOUNT, 1)
	--FROM    DBO.GTMP_IFRS_ACCT_JOURNAL_INTM A
	--        LEFT JOIN IFRS_MASTER_EXCHANGE_RATE B ON A.CCY = B.CURRENCY  AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
	--WHERE   A.DOWNLOAD_DATE = @V_CURRDATE

	/******************************************************************************
    04. UPDATE AMOUNT IDR
    *******************************************************************************/
    MERGE INTO GTMP_IFRS_ACCT_JOURNAL_INTM A
    USING
    (SELECT A2.ID, N_AMOUNT * NVL(B2.RATE_AMOUNT,1) N_AMOUNT_IDR
     FROM GTMP_IFRS_ACCT_JOURNAL_INTM A2
     LEFT JOIN IFRS_MASTER_EXCHANGE_RATE  B2
     ON A2.DOWNLOAD_DATE=B2.DOWNLOAD_DATE
        AND A2.DOWNLOAD_DATE = V_CURRDATE
        AND A2.CCY=B2.CURRENCY
    ) B ON (A.ID = B.ID)
    WHEN MATCHED THEN
    UPDATE
    SET A.N_AMOUNT_IDR = B.N_AMOUNT_IDR;

    COMMIT;

	MERGE INTO IFRS_ACCT_JOURNAL_INTM A
	USING GTMP_IFRS_ACCT_JOURNAL_INTM B
	ON (A.ID = B.ID)
	WHEN MATCHED THEN
	UPDATE SET
		A.METHOD = B.METHOD,
		A.FLAG_AL = B.FLAG_AL,
		A.N_AMOUNT_IDR = B.N_AMOUNT_IDR;
	COMMIT;

	/******************************************************************************
    05. DELETE
    *******************************************************************************/
    DELETE  /*+ PARALLEL(12) */ FROM IFRS_ACCT_JOURNAL_DATA
    WHERE   DOWNLOAD_DATE >= V_CURRDATE;

    COMMIT;

	INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  (V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_IFRS_ACCT_JOURNAL_DATA' ,'CLEAN UP' );

    COMMIT;

	/******************************************************************************
    06. insert itrcg data_source ccy jenis_pinjaman combination
    *******************************************************************************/
    INSERT  /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_DATA
    ( DOWNLOAD_DATE ,
      MASTERID ,
      FACNO ,
      CIFNO ,
      ACCTNO ,
      DATASOURCE ,
      PRDTYPE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      FLAG_CF ,
      DRCR ,
      GLNO ,
      N_AMOUNT ,
      N_AMOUNT_IDR ,
      SOURCEPROCESS ,
      INTMID ,
      CREATEDDATE ,
      CREATEDBY ,
      BRANCH ,
      JOURNALCODE2 ,
      JOURNAL_DESC ,
      NOREF ,
      VALCTR_CODE ,
      GL_INTERNAL_CODE,
      METHOD,
--    RESERVED_VARCHAR_1,
--	  RESERVED_VARCHAR_2,
      GL_COSTCENTER
    )
    SELECT  /*+ PARALLEL(12) */ A.DOWNLOAD_DATE ,
            A.MASTERID ,
            A.FACNO ,
            A.CIFNO ,
            A.ACCTNO ,
            A.DATASOURCE ,
            A.PRDTYPE ,
            A.PRDCODE ,
            A.TRXCODE ,
            A.CCY ,
            A.JOURNALCODE ,
            A.STATUS ,
            A.REVERSE ,
            A.FLAG_CF ,
            CASE WHEN ( A.REVERSE = 'N'AND COALESCE(A.FLAG_AL, 'A') = 'A')
                      OR ( A.REVERSE = 'Y' AND COALESCE(A.FLAG_AL, 'A') <> 'A')
                      THEN CASE WHEN A.N_AMOUNT >= 0 AND A.FLAG_CF = 'F' AND A.JOURNALCODE IN ( 'ACCRU','AMORT' ) THEN B.DRCR
                                WHEN A.N_AMOUNT <= 0 AND A.FLAG_CF = 'C' AND A.JOURNALCODE IN ( 'ACCRU','AMORT' ) THEN B.DRCR
                                WHEN A.N_AMOUNT <= 0 AND A.FLAG_CF = 'F' AND A.JOURNALCODE IN ( 'DEFA0' ) THEN B.DRCR
                                WHEN A.N_AMOUNT >= 0 AND A.FLAG_CF = 'C' AND A.JOURNALCODE IN ( 'DEFA0' ) THEN B.DRCR
                                ELSE CASE WHEN B.DRCR = 'D' THEN 'C' ELSE 'D' END
                            END
                 ELSE CASE WHEN A.N_AMOUNT <= 0 AND A.FLAG_CF = 'F'  AND A.JOURNALCODE IN ( 'ACCRU','AMORT' )THEN B.DRCR
                           WHEN A.N_AMOUNT >= 0 AND A.FLAG_CF = 'C'  AND A.JOURNALCODE IN ( 'ACCRU','AMORT' )THEN B.DRCR
                           WHEN A.N_AMOUNT >= 0 AND A.FLAG_CF = 'F'  AND A.JOURNALCODE IN ( 'DEFA0' ) THEN B.DRCR
                           WHEN A.N_AMOUNT <= 0 AND A.FLAG_CF = 'C'  AND A.JOURNALCODE IN ( 'DEFA0' ) THEN B.DRCR
                           ELSE CASE WHEN B.DRCR = 'D' THEN 'C' ELSE 'D' END
                      END
            END AS DRCR ,
            B.GLNO ,
            ABS(A.N_AMOUNT) ,
            ABS(A.N_AMOUNT_IDR) ,
            A.SOURCEPROCESS ,
            A.ID ,
            SYSTIMESTAMP ,
            'SP_JOURNAL_DATA2' ,
            A.BRANCH ,
            A.JOURNALCODE2 ,
            B.JOURNAL_DESC ,
            B.JOURNALCODE ,
            B.COSTCENTER || '-' || COALESCE(IMC.APPLICATION_NO,IMP.APPLICATION_NO,'') ,
            B.GL_INTERNAL_CODE,
			METHOD,
--			COALESCE(IMC.RESERVED_VARCHAR_1, IMP.RESERVED_VARCHAR_1,''),
--          COALESCE(IMC.RESERVED_VARCHAR_2, IMP.RESERVED_VARCHAR_2,''),
			B.COSTCENTER
    FROM    GTMP_IFRS_ACCT_JOURNAL_INTM A
    LEFT JOIN IFRS_IMA_AMORT_CURR IMC ON A.MASTERID = IMC.MASTERID
    LEFT JOIN IFRS_IMA_AMORT_PREV IMP ON A.MASTERID = IMP.MASTERID
    JOIN IFRS_JOURNAL_PARAM B
      ON B.JOURNALCODE IN ('ITRCG','ITRCG1','ITRCG2')
      AND ( B.CCY = A.CCY OR B.CCY = 'ALL' )
      AND B.FLAG_CF = A.FLAG_CF
      AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME,IMP.GL_CONSTNAME,'')
      AND (A.TRXCODE=B.TRX_CODE  OR B.TRX_CODE = 'ALL')
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.JOURNALCODE = 'DEFA0'
    AND A.TRXCODE <> 'BENEFIT'
    AND A.METHOD = 'EIR'
    AND A.SOURCEPROCESS NOT IN ('EIR_REV_SWITCH','EIR_SWITCH');
    COMMIT;

	/******************************************************************************
    07. STAFF LOAN DEFA0
    *******************************************************************************/
    INSERT  /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_DATA
    ( DOWNLOAD_DATE ,
      MASTERID ,
      FACNO ,
      CIFNO ,
      ACCTNO ,
      DATASOURCE ,
      PRDTYPE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      FLAG_CF ,
      DRCR ,
      GLNO ,
      N_AMOUNT ,
      N_AMOUNT_IDR ,
      SOURCEPROCESS ,
      INTMID ,
      CREATEDDATE ,
      CREATEDBY ,
      BRANCH ,
      JOURNALCODE2 ,
      JOURNAL_DESC ,
      NOREF ,
      VALCTR_CODE ,
      GL_INTERNAL_CODE,
      METHOD,
--      RESERVED_VARCHAR_1,
--      RESERVED_VARCHAR_2
      GL_COSTCENTER
    )
    SELECT  /*+ PARALLEL(12) */ A.DOWNLOAD_DATE ,
            A.MASTERID ,
            A.FACNO ,
            A.CIFNO ,
            A.ACCTNO ,
            A.DATASOURCE ,
            A.PRDTYPE ,
            A.PRDCODE ,
            A.TRXCODE ,
            A.CCY ,
            A.JOURNALCODE ,
            A.STATUS ,
            A.REVERSE ,
            A.FLAG_CF ,
            CASE WHEN ( A.REVERSE = 'N'AND COALESCE(A.FLAG_AL, 'A') = 'A')
                      OR ( A.REVERSE = 'Y' AND COALESCE(A.FLAG_AL, 'A') <> 'A')
                      THEN CASE WHEN A.N_AMOUNT >= 0 AND A.FLAG_CF = 'F' AND A.JOURNALCODE IN ( 'ACCRU','AMORT' ) THEN B.DRCR
                                WHEN A.N_AMOUNT <= 0 AND A.FLAG_CF = 'C' AND A.JOURNALCODE IN ( 'ACCRU','AMORT' ) THEN B.DRCR
                                WHEN A.N_AMOUNT <= 0 AND A.FLAG_CF = 'F' AND A.JOURNALCODE IN ( 'DEFA0' ) THEN B.DRCR
                                WHEN A.N_AMOUNT >= 0 AND A.FLAG_CF = 'C' AND A.JOURNALCODE IN ( 'DEFA0' ) THEN B.DRCR
                                ELSE CASE WHEN B.DRCR = 'D' THEN 'C' ELSE 'D' END
                            END
                 ELSE CASE WHEN A.N_AMOUNT <= 0 AND A.FLAG_CF = 'F'  AND A.JOURNALCODE IN ( 'ACCRU','AMORT' )THEN B.DRCR
                           WHEN A.N_AMOUNT >= 0 AND A.FLAG_CF = 'C'  AND A.JOURNALCODE IN ( 'ACCRU','AMORT' )THEN B.DRCR
                           WHEN A.N_AMOUNT >= 0 AND A.FLAG_CF = 'F'  AND A.JOURNALCODE IN ( 'DEFA0' ) THEN B.DRCR
                           WHEN A.N_AMOUNT <= 0 AND A.FLAG_CF = 'C'  AND A.JOURNALCODE IN ( 'DEFA0' ) THEN B.DRCR
                           ELSE CASE WHEN B.DRCR = 'D' THEN 'C' ELSE 'D' END
                      END
            END AS DRCR ,
            B.GLNO ,
            ABS(A.N_AMOUNT) ,
            ABS(A.N_AMOUNT_IDR) ,
            A.SOURCEPROCESS ,
            A.ID ,
            SYSTIMESTAMP ,
            'SP_JOURNAL_DATA2' ,
            A.BRANCH ,
            A.JOURNALCODE2 ,
            B.JOURNAL_DESC ,
            B.JOURNALCODE ,
            B.COSTCENTER || '-' || COALESCE(IMC.APPLICATION_NO,IMP.APPLICATION_NO,'') ,
            B.GL_INTERNAL_CODE,
            METHOD,
--			COALESCE(IMC.RESERVED_VARCHAR_1, IMP.RESERVED_VARCHAR_1,''),
--          COALESCE(IMC.RESERVED_VARCHAR_2, IMP.RESERVED_VARCHAR_2,''),
            B.COSTCENTER
    FROM    GTMP_IFRS_ACCT_JOURNAL_INTM A
    LEFT JOIN IFRS_IMA_AMORT_CURR IMC ON A.MASTERID = IMC.MASTERID
    LEFT JOIN IFRS_IMA_AMORT_PREV IMP ON A.MASTERID = IMP.MASTERID
    JOIN IFRS_JOURNAL_PARAM B
      ON B.JOURNALCODE IN ('ITRCG','ITRCG1','ITRCG2','ITEMB')
      AND ( B.CCY = A.CCY OR B.CCY = 'ALL' )
      AND B.FLAG_CF = A.FLAG_CF
      AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME,IMP.GL_CONSTNAME,'')
      AND (A.TRXCODE =B.TRX_CODE OR B.TRX_CODE = 'ALL')
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.JOURNALCODE = 'DEFA0'
    AND A.TRXCODE = 'BENEFIT'
    AND A.METHOD = 'EIR'
    AND A.SOURCEPROCESS NOT IN ('EIR_REV_SWITCH','EIR_SWITCH');
    COMMIT;

	INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  (V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_JOURNAL_DATA' ,'ITRCG 2' );

    COMMIT;

	/******************************************************************************
    08. insert accru amort data source ccy PRDCODE combination
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_DATA
    ( DOWNLOAD_DATE ,
      MASTERID ,
      FACNO ,
      CIFNO ,
      ACCTNO ,
      DATASOURCE ,
      PRDTYPE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      FLAG_CF ,
      DRCR ,
      GLNO ,
      N_AMOUNT ,
      N_AMOUNT_IDR ,
      SOURCEPROCESS ,
      INTMID ,
      CREATEDDATE ,
      CREATEDBY ,
      BRANCH ,
      JOURNALCODE2 ,
      JOURNAL_DESC ,
      NOREF ,
      VALCTR_CODE ,
      GL_INTERNAL_CODE,
      METHOD,
--	  RESERVED_VARCHAR_1,
--	  RESERVED_VARCHAR_2,
	  GL_COSTCENTER
    )
    SELECT  /*+ PARALLEL(12) */ A.DOWNLOAD_DATE ,
            A.MASTERID ,
            A.FACNO ,
            A.CIFNO ,
            A.ACCTNO ,
            A.DATASOURCE ,
            A.PRDTYPE ,
            A.PRDCODE ,
            A.TRXCODE ,
            A.CCY ,
            A.JOURNALCODE ,
            A.STATUS ,
            A.REVERSE ,
            A.FLAG_CF ,
            CASE WHEN ( A.REVERSE = 'N'AND COALESCE(A.FLAG_AL, 'A') = 'A')
                      OR
                      ( A.REVERSE = 'Y' AND COALESCE(A.FLAG_AL, 'A') <> 'A')
                                       THEN CASE WHEN A.N_AMOUNT >= 0 AND A.FLAG_CF = 'F' AND A.JOURNALCODE IN ( 'ACCRU','AMORT' ) THEN B.DRCR
                               WHEN A.N_AMOUNT <= 0 AND A.FLAG_CF = 'C' AND A.JOURNALCODE IN ( 'ACCRU','AMORT' ) THEN B.DRCR
                               WHEN A.N_AMOUNT <= 0 AND A.FLAG_CF = 'F' AND A.JOURNALCODE IN ( 'DEFA0' )THEN B.DRCR
                               WHEN A.N_AMOUNT >= 0 AND A.FLAG_CF = 'C' AND A.JOURNALCODE IN ( 'DEFA0' ) THEN B.DRCR
                               ELSE CASE WHEN B.DRCR = 'D' THEN 'C' ELSE 'D' END
                           END
                 ELSE CASE WHEN A.N_AMOUNT <= 0 AND A.FLAG_CF = 'F' AND A.JOURNALCODE IN ( 'ACCRU', 'AMORT' ) THEN B.DRCR
                           WHEN A.N_AMOUNT >= 0 AND A.FLAG_CF = 'C' AND A.JOURNALCODE IN ( 'ACCRU', 'AMORT' ) THEN B.DRCR
                           WHEN A.N_AMOUNT >= 0 AND A.FLAG_CF = 'F' AND A.JOURNALCODE IN ( 'DEFA0' )THEN B.DRCR
                           WHEN A.N_AMOUNT <= 0 AND A.FLAG_CF = 'C' AND A.JOURNALCODE IN ( 'DEFA0' ) THEN B.DRCR
                           ELSE CASE WHEN B.DRCR = 'D' THEN 'C' ELSE 'D' END
                      END
            END AS DRCR ,
            B.GLNO ,
            ABS(A.N_AMOUNT) ,
            ABS(A.N_AMOUNT_IDR) ,
            A.SOURCEPROCESS ,
            A.ID ,
            SYSTIMESTAMP ,
            'SP_ACCT_JOURNAL_DATA2' ,
            A.BRANCH ,
            A.JOURNALCODE2 ,
            B.JOURNAL_DESC ,
            B.JOURNALCODE ,
            B.COSTCENTER || '-' || COALESCE(IMC.APPLICATION_NO,IMP.APPLICATION_NO,''),
            B.GL_INTERNAL_CODE,
            METHOD,
--			COALESCE(IMC.RESERVED_VARCHAR_1, IMP.RESERVED_VARCHAR_1,''),
--            COALESCE(IMC.RESERVED_VARCHAR_2, IMP.RESERVED_VARCHAR_2,''),
			B.COSTCENTER
    FROM    GTMP_IFRS_ACCT_JOURNAL_INTM A
    LEFT JOIN IFRS_IMA_AMORT_CURR IMC ON A.MASTERID = IMC.MASTERID
    LEFT JOIN IFRS_IMA_AMORT_PREV IMP ON A.MASTERID = IMP.MASTERID
    JOIN IFRS_JOURNAL_PARAM B ON B.JOURNALCODE IN ('ACCRU', 'EMPBE', 'EMACR')
    AND ( B.CCY = A.CCY OR B.CCY = 'ALL' )
    AND B.FLAG_CF = A.FLAG_CF
    AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME,IMP.GL_CONSTNAME,'')
    AND (A.TRXCODE=B.TRX_CODE  OR B.TRX_CODE = 'ALL')
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.JOURNALCODE IN ( 'ACCRU', 'AMORT')
    AND A.TRXCODE <> 'BENEFIT'
    AND A.METHOD = 'EIR';

    COMMIT;

	/******************************************************************************
    09. staff loan accru
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_DATA
    ( DOWNLOAD_DATE ,
      MASTERID ,
      FACNO ,
      CIFNO ,
      ACCTNO ,
      DATASOURCE ,
      PRDTYPE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      FLAG_CF ,
      DRCR ,
      GLNO ,
      N_AMOUNT ,
      N_AMOUNT_IDR ,
      SOURCEPROCESS ,
      INTMID ,
      CREATEDDATE ,
      CREATEDBY ,
      BRANCH ,
      JOURNALCODE2 ,
      JOURNAL_DESC ,
      NOREF ,
      VALCTR_CODE ,
      GL_INTERNAL_CODE ,
      METHOD ,
--	  RESERVED_VARCHAR_1 ,
--    RESERVED_VARCHAR_2 ,
      GL_COSTCENTER
    )
    SELECT  /*+ PARALLEL(12) */ A.DOWNLOAD_DATE ,
            A.MASTERID ,
            A.FACNO ,
            A.CIFNO ,
            A.ACCTNO ,
            A.DATASOURCE ,
            A.PRDTYPE ,
            A.PRDCODE ,
            A.TRXCODE ,
            A.CCY ,
            A.JOURNALCODE ,
            A.STATUS ,
            A.REVERSE ,
            A.FLAG_CF ,
            CASE WHEN ( A.REVERSE = 'N'AND COALESCE(A.FLAG_AL, 'A') = 'A')
                      OR
                      ( A.REVERSE = 'Y' AND COALESCE(A.FLAG_AL, 'A') <> 'A')
                                       THEN CASE WHEN A.N_AMOUNT >= 0 AND A.FLAG_CF = 'F' AND A.JOURNALCODE IN ( 'ACCRU','AMORT' ) THEN B.DRCR
                               WHEN A.N_AMOUNT <= 0 AND A.FLAG_CF = 'C' AND A.JOURNALCODE IN ( 'ACCRU','AMORT' ) THEN B.DRCR
                               WHEN A.N_AMOUNT <= 0 AND A.FLAG_CF = 'F' AND A.JOURNALCODE IN ( 'DEFA0' )THEN B.DRCR
                               WHEN A.N_AMOUNT >= 0 AND A.FLAG_CF = 'C' AND A.JOURNALCODE IN ( 'DEFA0' ) THEN B.DRCR
                               ELSE CASE WHEN B.DRCR = 'D' THEN 'C' ELSE 'D' END
                           END
                 ELSE CASE WHEN A.N_AMOUNT <= 0 AND A.FLAG_CF = 'F' AND A.JOURNALCODE IN ( 'ACCRU', 'AMORT' ) THEN B.DRCR
                           WHEN A.N_AMOUNT >= 0 AND A.FLAG_CF = 'C' AND A.JOURNALCODE IN ( 'ACCRU', 'AMORT' ) THEN B.DRCR
                           WHEN A.N_AMOUNT >= 0 AND A.FLAG_CF = 'F' AND A.JOURNALCODE IN ( 'DEFA0' )THEN B.DRCR
                           WHEN A.N_AMOUNT <= 0 AND A.FLAG_CF = 'C' AND A.JOURNALCODE IN ( 'DEFA0' ) THEN B.DRCR
                           ELSE CASE WHEN B.DRCR = 'D' THEN 'C' ELSE 'D' END
                      END
            END AS DRCR ,
            B.GLNO ,
            ABS(A.N_AMOUNT) ,
            ABS(A.N_AMOUNT_IDR) ,
            A.SOURCEPROCESS ,
            A.ID ,
            SYSTIMESTAMP ,
            'SP_ACCT_JOURNAL_DATA2' ,
            A.BRANCH ,
            --SUBSTRING (A.BRANCH, LEN (A.BRANCH) - 2, 3) AS BRANCH_CODE,
            B.JOURNALCODE ,
            B.JOURNAL_DESC ,
            B.JOURNALCODE ,
            B.COSTCENTER || '-' || COALESCE(IMC.APPLICATION_NO,IMP.APPLICATION_NO,''),
            B.GL_INTERNAL_CODE,
            METHOD,
--            COALESCE(IMC.RESERVED_VARCHAR_1, IMP.RESERVED_VARCHAR_1,''),
--            COALESCE(IMC.RESERVED_VARCHAR_2, IMP.RESERVED_VARCHAR_2,''),
		    B.COSTCENTER
    FROM    GTMP_IFRS_ACCT_JOURNAL_INTM A
    LEFT JOIN IFRS_IMA_AMORT_CURR IMC ON A.MASTERID = IMC.MASTERID
    LEFT JOIN IFRS_IMA_AMORT_PREV IMP ON A.MASTERID = IMP.MASTERID
    JOIN IFRS_JOURNAL_PARAM B
      ON B.JOURNALCODE IN ('ACCRU', 'EMPBE', 'EMACR','EBCTE')
      AND ( B.CCY = A.CCY OR B.CCY = 'ALL' )
      AND B.FLAG_CF = A.FLAG_CF
      AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME,IMP.GL_CONSTNAME,'')
      AND (A.TRXCODE=B.TRX_CODE  OR B.TRX_CODE = 'ALL')
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.JOURNALCODE IN ( 'ACCRU', 'AMORT' )
    AND A.TRXCODE = 'BENEFIT'
    AND A.METHOD = 'EIR';

    COMMIT;

    INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE , DTM , OPS , PROCNAME , REMARK )
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP , 'DEBUG' , 'SP_IFRS_ACCT_JOURNAL_DATA' ,'amort 2');

    COMMIT;

	/******************************************************************************
    10. ACCRU NOCF
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_DATA
    ( DOWNLOAD_DATE ,
      MASTERID ,
      FACNO ,
      CIFNO ,
      ACCTNO ,
      DATASOURCE ,
            PRDTYPE ,
      PRDCODE ,
      TRXCODE ,
      CCY ,
      JOURNALCODE ,
      STATUS ,
      REVERSE ,
      FLAG_CF ,
      DRCR ,
      GLNO ,
      N_AMOUNT ,
      N_AMOUNT_IDR ,
      SOURCEPROCESS ,
      INTMID ,
      CREATEDDATE ,
      CREATEDBY ,
      BRANCH ,
      JOURNALCODE2 ,
      JOURNAL_DESC ,
      NOREF ,
      VALCTR_CODE ,
      GL_INTERNAL_CODE,
      METHOD ,
--	  RESERVED_VARCHAR_1 ,
--    RESERVED_VARCHAR_2 ,
	  GL_COSTCENTER
    )
    SELECT /*+ PARALLEL(12) */ A.DOWNLOAD_DATE,
           A.MASTERID,
           A.FACNO,
           A.CIFNO,
           A.ACCTNO,
           A.DATASOURCE,
           A.PRDTYPE,
           A.PRDCODE,
           A.TRXCODE,
           A.CCY,
           A.JOURNALCODE,
           A.STATUS,
           A.REVERSE,
           A.FLAG_CF,
           CASE WHEN A.REVERSE = 'N' AND N_AMOUNT > 0 THEN B.DRCR
                WHEN A.REVERSE = 'Y' AND N_AMOUNT <= 0 THEN B.DRCR
                ELSE CASE WHEN B.DRCR = 'C' THEN 'D' ELSE 'C' END
           END AS DRCR,
           B.GLNO,
           ABS(A.N_AMOUNT) N_AMOUNT,
           ABS(A.N_AMOUNT_IDR) N_AMOUNT_IDR,
           A.SOURCEPROCESS,
           A.ID,
           SYSTIMESTAMP AS CREATEDDATE,
           'SP_ACCT_JOURNAL_DATA2' CREATEDBY,
           A.BRANCH,
           A.JOURNALCODE2,
           B.JOURNAL_DESC,
           B.JOURNALCODE AS NOREF,
           B.COSTCENTER || '-' || COALESCE (IMC.APPLICATION_NO, IMP.APPLICATION_NO, ''),
           B.GL_INTERNAL_CODE,
           A.METHOD,
--		   COALESCE(IMC.RESERVED_VARCHAR_1, IMP.RESERVED_VARCHAR_1,''),
--         COALESCE(IMC.RESERVED_VARCHAR_2, IMP.RESERVED_VARCHAR_2,''),
		   B.COSTCENTER
    FROM GTMP_IFRS_ACCT_JOURNAL_INTM A
    LEFT JOIN IFRS_IMA_AMORT_CURR IMC ON A.MASTERID = IMC.MASTERID
    LEFT JOIN IFRS_IMA_AMORT_PREV IMP ON A.MASTERID = IMP.MASTERID
    JOIN IFRS_JOURNAL_PARAM B
      ON B.JOURNALCODE = 'ACRU4'
      AND ( B.CCY = A.CCY OR B.CCY = 'ALL' )
      AND B.FLAG_CF = A.FLAG_CF
      AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME, IMP.GL_CONSTNAME,'')
    WHERE A.DOWNLOAD_DATE = V_CURRDATE
    AND A.JOURNALCODE IN ('ACRU4', 'AMRT4')
    AND A.TRXCODE <> 'BENEFIT'
    AND A.METHOD = 'EIR';

    COMMIT;

	/******************************************************************************
    11. JOURNAL SWITCH ACCOUNT
    *******************************************************************************/
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_DATA (
      DOWNLOAD_DATE
      ,MASTERID
      ,FACNO
      ,CIFNO
      ,ACCTNO
      ,DATASOURCE
      ,PRDTYPE
      ,PRDCODE
      ,TRXCODE
      ,CCY
      ,JOURNALCODE
      ,STATUS
      ,REVERSE
      ,FLAG_CF
      ,DRCR
      ,GLNO
      ,N_AMOUNT
      ,N_AMOUNT_IDR
      ,SOURCEPROCESS
      ,INTMID
      ,CREATEDDATE
      ,CREATEDBY
      ,BRANCH
      ,JOURNALCODE2
      ,JOURNAL_DESC
      ,NOREF
      ,VALCTR_CODE
      ,GL_INTERNAL_CODE
      ,METHOD
--      ,RESERVED_VARCHAR_1
--      ,RESERVED_VARCHAR_2
      ,GL_COSTCENTER
      )
     SELECT /*+ PARALLEL(12) */ A.DOWNLOAD_DATE
      ,A.MASTERID
      ,A.FACNO
      ,A.CIFNO
      ,A.ACCTNO
      ,A.DATASOURCE
      ,A.PRDTYPE
      ,A.PRDCODE
      ,A.TRXCODE
      ,A.CCY
      ,A.JOURNALCODE
      ,A.STATUS
      ,A.REVERSE
      ,A.FLAG_CF
      ,B.DRCR
      ,B.GLNO
      ,ABS(A.N_AMOUNT)
      ,ABS(A.N_AMOUNT_IDR)
      ,A.SOURCEPROCESS
      ,A.ID
      ,CURRENT_TIMESTAMP
      ,'SP_JOURNAL_DATA2'
      ,A.BRANCH
      ,B.JOURNALCODE
      ,B.JOURNAL_DESC
      ,B.JOURNALCODE
      ,B.COSTCENTER + '-' + COALESCE(IMC.APPLICATION_NO, IMP.APPLICATION_NO, '')
      ,B.GL_INTERNAL_CODE
      ,METHOD
--      ,COALESCE(IMC.RESERVED_VARCHAR_1, IMP.RESERVED_VARCHAR_1,'')
--      ,COALESCE(IMC.RESERVED_VARCHAR_2, IMP.RESERVED_VARCHAR_2,'')
      ,B.COSTCENTER
     FROM GTMP_IFRS_ACCT_JOURNAL_INTM A
     LEFT JOIN IFRS_IMA_AMORT_CURR IMC ON A.MASTERID = IMC.MASTERID
     LEFT JOIN IFRS_IMA_AMORT_PREV IMP ON A.MASTERID = IMP.MASTERID
     JOIN IFRS_JOURNAL_PARAM B ON B.JOURNALCODE IN ('RCLV')
      AND (B.CCY = A.CCY OR B.CCY = 'ALL')
      AND B.FLAG_CF = A.FLAG_CF
      AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME, IMP.GL_CONSTNAME, '')
      AND (
       A.TRXCODE = B.TRX_CODE
       OR B.TRX_CODE = 'ALL'
       )
      WHERE A.DOWNLOAD_DATE = V_CURRDATE
      AND A.JOURNALCODE = 'DEFA0'
      AND A.METHOD = 'EIR'
      AND A.SOURCEPROCESS = 'EIR_REV_SWITCH';

    COMMIT;

	/******************************************************************************
    12. RLCS NEW BRANCH
    *******************************************************************************/
    -- INSERT ITRCG DATA_SOURCE CCY JENIS_PINJAMAN COMBINATION
    INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_DATA (
      DOWNLOAD_DATE
      ,MASTERID
      ,FACNO
      ,CIFNO
      ,ACCTNO
      ,DATASOURCE
      ,PRDTYPE
      ,PRDCODE
      ,TRXCODE
      ,CCY
      ,JOURNALCODE
      ,STATUS
      ,REVERSE
      ,FLAG_CF
      ,DRCR
      ,GLNO
      ,N_AMOUNT
      ,N_AMOUNT_IDR
      ,SOURCEPROCESS
      ,INTMID
      ,CREATEDDATE
      ,CREATEDBY
      ,BRANCH
      ,JOURNALCODE2
      ,JOURNAL_DESC
      ,NOREF
      ,VALCTR_CODE
      ,GL_INTERNAL_CODE
      ,METHOD
--      ,RESERVED_VARCHAR_1
--      ,RESERVED_VARCHAR_2
      ,GL_COSTCENTER
      )
     SELECT /*+ PARALLEL(12) */ A.DOWNLOAD_DATE
      ,A.MASTERID
      ,A.FACNO
      ,A.CIFNO
      ,A.ACCTNO
      ,A.DATASOURCE
      ,A.PRDTYPE
      ,A.PRDCODE
      ,A.TRXCODE
      ,A.CCY
      ,A.JOURNALCODE
      ,A.STATUS
      ,A.REVERSE
      ,A.FLAG_CF
      ,B.DRCR
      ,B.GLNO
      ,ABS(A.N_AMOUNT)
      ,ABS(A.N_AMOUNT_IDR)
      ,A.SOURCEPROCESS
      ,A.ID
      ,CURRENT_TIMESTAMP
      ,'SP_JOURNAL_DATA2'
      ,A.BRANCH
      ,B.JOURNALCODE
      ,B.JOURNAL_DESC
      ,B.JOURNALCODE
      ,B.COSTCENTER + '-' + COALESCE(IMC.APPLICATION_NO, IMP.APPLICATION_NO, '')
      ,B.GL_INTERNAL_CODE
      ,METHOD
--      ,COALESCE(IMC.RESERVED_VARCHAR_1, IMP.RESERVED_VARCHAR_1,'')
--      ,COALESCE(IMC.RESERVED_VARCHAR_2, IMP.RESERVED_VARCHAR_2,'')
      ,B.COSTCENTER
     FROM GTMP_IFRS_ACCT_JOURNAL_INTM A
     LEFT JOIN IFRS_IMA_AMORT_CURR IMC ON A.MASTERID = IMC.MASTERID
     LEFT JOIN IFRS_IMA_AMORT_PREV IMP ON A.MASTERID = IMP.MASTERID
     JOIN IFRS_JOURNAL_PARAM B ON B.JOURNALCODE IN ('RCLS')
      AND (B.CCY = A.CCY OR B.CCY = 'ALL')
      AND B.FLAG_CF = A.FLAG_CF
      AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME, IMP.GL_CONSTNAME, '')
      AND (
       A.TRXCODE = B.TRX_CODE
       OR B.TRX_CODE = 'ALL'
       )
     WHERE A.DOWNLOAD_DATE = V_CURRDATE
      AND A.JOURNALCODE = 'DEFA0'
      AND A.METHOD = 'EIR'
      AND A.SOURCEPROCESS = 'EIR_SWITCH';

    COMMIT;

	INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE , DTM , OPS ,PROCNAME ,REMARK )
    VALUES  ( V_CURRDATE , SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_ACCT_JOURNAL_DATA' ,'journal SL');

    COMMIT;

	/******************************************************************************
    13. call journal SL GENERATED
    *******************************************************************************/
    SP_IFRS_EXEC_AND_LOG_PROCESS('SP_IFRS_ACCT_JRNL_DATA_SL','AMT','Y');

	INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK )
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP , 'DEBUG' ,'SP_IFRS_ACCT_JOURNAL_DATA' , 'journal SL done');

    COMMIT;

	/******************************************************************************
    14. UPDATE JOURNAL DATA
    *******************************************************************************/
    /*Pindahan dari atas 20160510*/
    UPDATE /*+ PARALLEL(12) */ IFRS_ACCT_JOURNAL_DATA
    SET     NOREF = CASE WHEN NOREF IN ('ITRCG','ITRCG_SL','ITRCG_NE') THEN '1'
                         WHEN NOREF IN ('ITRCG1','ITRCG_SL1') THEN '2'
                         WHEN NOREF IN ('ITRCG2','ITRCG_SL2') THEN '3'
                         WHEN NOREF IN ('EMPBE','EMPBE_SL') THEN '4'
                         WHEN NOREF IN ('EMACR','EMACR_SL') THEN '5'
                         WHEN NOREF = 'RLS' THEN '6'
                         ELSE '9'
                    END
                    + CASE WHEN REVERSE = 'Y' THEN '1' ELSE '2' END
                    + CASE WHEN DRCR = 'D' THEN '1'  ELSE '2' END
    WHERE   DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

	INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE ,DTM , OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE , SYSTIMESTAMP , 'DEBUG' ,'SP_IFRS_ACCT_JOURNAL_DATA' ,'fill noref done');

    COMMIT;

	/******************************************************************************
    15. UPDATE FAIR VALUE
    *******************************************************************************/
    /* FD: UPDATE MASTER ACCOUNT FAIR VALUE AMOUNT*/
    UPDATE /*+ PARALLEL(12) */ IFRS_MASTER_ACCOUNT
    SET     FAIR_VALUE_AMOUNT = COALESCE(OUTSTANDING, 0) + COALESCE(OUTSTANDING_IDC,0) + + COALESCE(UNAMORT_FEE_AMT,0) + COALESCE(UNAMORT_COST_AMT, 0)  + COALESCE(UNAMORT_BENEFIT, 0)
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND DATA_SOURCE = 'ILS';

    COMMIT;


    /******************************************************************************
    16. FACILITY JURNAL
    *******************************************************************************/

	SP_IFRS_EXEC_AND_LOG_PROCESS('SP_IFRS_ACCT_JRNL_DATA_FAC','AMT','Y');

    INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_IFRS_ACCT_JOURNAL_DATA' ,'');

    COMMIT;
END;