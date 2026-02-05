CREATE OR REPLACE PROCEDURE SP_IFRS_LI_ACCT_JOURNAL_DATA
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
    FROM IFRS_LI_PRC_DATE_AMORT    ;
    V_PREVMONTH := LAST_DAY(ADD_MONTHS(V_CURRDATE,-1))  ;

    INSERT  INTO IFRS_LI_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  (V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_IFRS_LI_ACCT_JOURNAL_DATA' ,'');

    COMMIT;

    /******************************************************************************
    02. SET METHOD EIR
    *******************************************************************************/

    UPDATE  IFRS_LI_ACCT_JOURNAL_INTM
    SET     METHOD = 'EIR'
    WHERE   DOWNLOAD_DATE = V_CURRDATE
    AND SUBSTR(SOURCEPROCESS, 1, 3) = 'EIR';
    COMMIT;

    /******************************************************************************
    03.journal intm flag_al fill from IMA_AMORT_CURR
    *******************************************************************************/
    MERGE INTO IFRS_LI_ACCT_JOURNAL_INTM A
    USING IFRS_LI_IMA_AMORT_CURR B
    ON (B.MASTERID = A.MASTERID
        AND A.DOWNLOAD_DATE = V_CURRDATE
       )
    WHEN MATCHED THEN
    UPDATE
    SET A.FLAG_AL = B.IAS_CLASS;

    COMMIT;


    --Populate exchange rate pindah ke initial update
    --EXEC SP_IFRS_POPULATE_EXCHANGE_RATE

    --UPDATE  a
    --SET     n_amount_idr = a.N_AMOUNT * coalesce(b.RATE_AMOUNT, 1)
    --FROM    dbo.IFRS_LI_ACCT_JOURNAL_INTM a
    --        LEFT JOIN IFRS_MASTER_EXCHANGE_RATE b ON a.CCY = b.CURRENCY  and a.DOWNLOAD_DATE = b.DOWNLOAD_DATE
    --WHERE   a.DOWNLOAD_DATE = @v_currdate

    /******************************************************************************
    04. UPDATE AMOUNT IDR
    *******************************************************************************/
    MERGE INTO IFRS_LI_ACCT_JOURNAL_INTM A
    USING
    (SELECT A2.ID, N_AMOUNT * NVL(B2.RATE_AMOUNT,1) N_AMOUNT_IDR
     FROM IFRS_ACCT_JOURNAL_INTM A2
     LEFT JOIN IFRS_MASTER_EXCHANGE_RATE B2
     ON A2.DOWNLOAD_DATE=B2.DOWNLOAD_DATE
        AND A2.DOWNLOAD_DATE = V_CURRDATE
        AND A2.CCY=B2.CURRENCY
    ) B ON (A.ID = B.ID)
    WHEN MATCHED THEN
    UPDATE
    SET A.N_AMOUNT_IDR = B.N_AMOUNT_IDR;
    COMMIT;

    /******************************************************************************
    05. DELETE
    *******************************************************************************/
    DELETE  FROM IFRS_LI_ACCT_JOURNAL_DATA
    WHERE   DOWNLOAD_DATE >= V_CURRDATE;

    COMMIT;



    INSERT  INTO IFRS_LI_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  (V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_IFRS_LI_ACCT_JOURNAL_DATA' ,'clean up' );

    COMMIT;

    /******************************************************************************
    06. insert itrcg data_source ccy jenis_pinjaman combination
    *******************************************************************************/
    INSERT  INTO IFRS_LI_ACCT_JOURNAL_DATA
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
    SELECT  A.DOWNLOAD_DATE ,
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
    FROM    IFRS_LI_ACCT_JOURNAL_INTM A
    LEFT JOIN IFRS_LI_IMA_AMORT_CURR IMC ON A.MASTERID = IMC.MASTERID
    LEFT JOIN IFRS_LI_IMA_AMORT_PREV IMP ON A.MASTERID = IMP.MASTERID
    JOIN IFRS_LI_JOURNAL_PARAM B
      ON B.JOURNALCODE IN ('ITRCG', 'ITRCG1','ITRCG2')
      AND A.JOURNALCODE2 = B.JOURNALCODE
      AND ( B.CCY = A.CCY OR B.CCY = 'ALL' )
      AND B.FLAG_CF = A.FLAG_CF
      AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME,IMP.GL_CONSTNAME,'')
      AND (A.TRXCODE=B.TRX_CODE  OR B.TRX_CODE = 'ALL'  )
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.JOURNALCODE = 'DEFA0'
    AND A.TRXCODE <> 'BENEFIT'
    AND A.METHOD = 'EIR'
    AND A.SOURCEPROCESS NOT IN ('EIR_REV_SWITCH','EIR_SWITCH');
    COMMIT;


    /******************************************************************************
    07. staff loan defa0
    *******************************************************************************/
    INSERT  INTO IFRS_LI_ACCT_JOURNAL_DATA
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
    SELECT  A.DOWNLOAD_DATE ,
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
            CASE WHEN ( A.REVERSE = 'N' AND COALESCE(A.FLAG_AL, 'A') = 'A')
                      OR ( A.REVERSE = 'Y' AND COALESCE(A.FLAG_AL, 'A') <> 'A')
                   THEN CASE WHEN A.N_AMOUNT >= 0 AND A.FLAG_CF = 'F' AND A.JOURNALCODE IN ( 'ACCRU','AMORT' ) THEN B.DRCR
                             WHEN A.N_AMOUNT <= 0 AND A.FLAG_CF = 'C' AND A.JOURNALCODE IN ( 'ACCRU', 'AMORT' )THEN B.DRCR
                                WHEN A.N_AMOUNT <= 0 AND A.FLAG_CF = 'F' AND A.JOURNALCODE IN ( 'DEFA0' ) THEN B.DRCR
                                WHEN A.N_AMOUNT >= 0 AND A.FLAG_CF = 'C' AND A.JOURNALCODE IN ( 'DEFA0' ) THEN B.DRCR
                             ELSE CASE WHEN B.DRCR = 'D' THEN 'C' ELSE 'D'  END
                        END
                 ELSE CASE WHEN A.N_AMOUNT <= 0 AND A.FLAG_CF = 'F' AND A.JOURNALCODE IN ( 'ACCRU','AMORT' ) THEN B.DRCR
                           WHEN A.N_AMOUNT >= 0 AND A.FLAG_CF = 'C' AND A.JOURNALCODE IN ( 'ACCRU','AMORT' ) THEN B.DRCR
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
            B.JOURNALCODE ,
            B.JOURNAL_DESC ,
            B.JOURNALCODE ,
            B.COSTCENTER || '-' || COALESCE(IMC.APPLICATION_NO,IMP.APPLICATION_NO,'') ,
            B.GL_INTERNAL_CODE,
            METHOD,
--			COALESCE(IMC.RESERVED_VARCHAR_1, IMP.RESERVED_VARCHAR_1,''),
--          COALESCE(IMC.RESERVED_VARCHAR_2, IMP.RESERVED_VARCHAR_2,''),
            B.COSTCENTER
    FROM    IFRS_LI_ACCT_JOURNAL_INTM A
    LEFT JOIN IFRS_LI_IMA_AMORT_CURR IMC ON A.MASTERID = IMC.MASTERID
    LEFT JOIN IFRS_LI_IMA_AMORT_PREV IMP ON A.MASTERID = IMP.MASTERID
    JOIN IFRS_LI_JOURNAL_PARAM B
      ON B.JOURNALCODE IN ('ITRCG', 'ITRCG1','ITRCG2', 'ITEMB')
      AND ( B.CCY = A.CCY OR B.CCY = 'ALL' )
      AND COALESCE(B.FLAG_CF,'-') NOT IN ('F', 'C' )
      AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME,IMP.GL_CONSTNAME,'')
      AND (A.TRXCODE=B.TRX_CODE  OR B.TRX_CODE = 'ALL'  )
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.JOURNALCODE = 'DEFA0'
    AND A.TRXCODE = 'BENEFIT'
    AND A.METHOD = 'EIR'
    AND A.SOURCEPROCESS NOT IN ('EIR_REV_SWITCH','EIR_SWITCH');

    COMMIT;

    INSERT  INTO IFRS_LI_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  (V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_LI_ACCT_JOURNAL_DATA' ,'ITRCG2' );

    COMMIT;

    /******************************************************************************
    08. insert accru amort data source ccy PRDCODE combination
    *******************************************************************************/
    INSERT  INTO IFRS_LI_ACCT_JOURNAL_DATA
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
    SELECT  A.DOWNLOAD_DATE ,
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
            B.COSTCENTER || '-' || COALESCE(IMC.APPLICATION_NO,IMP.APPLICATION_NO,'') ,
            B.GL_INTERNAL_CODE,
            METHOD,
--			COALESCE(IMC.RESERVED_VARCHAR_1, IMP.RESERVED_VARCHAR_1,''),
--            COALESCE(IMC.RESERVED_VARCHAR_2, IMP.RESERVED_VARCHAR_2,''),
			B.COSTCENTER
    FROM    IFRS_LI_ACCT_JOURNAL_INTM A
    LEFT JOIN IFRS_LI_IMA_AMORT_CURR IMC ON A.MASTERID = IMC.MASTERID
    LEFT JOIN IFRS_LI_IMA_AMORT_PREV IMP ON A.MASTERID = IMP.MASTERID
    JOIN IFRS_LI_JOURNAL_PARAM B ON B.JOURNALCODE IN ('ACCRU', 'EMPBE', 'EMACR')
    AND ( B.CCY = A.CCY OR B.CCY = 'ALL' )
    AND B.FLAG_CF = A.FLAG_CF
    AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME,IMP.GL_CONSTNAME,'')
    AND (A.TRXCODE=B.TRX_CODE  OR B.TRX_CODE = 'ALL'  )
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.JOURNALCODE IN ( 'ACCRU', 'AMORT')
    AND A.TRXCODE <> 'BENEFIT'
    AND A.METHOD = 'EIR';


    COMMIT;


    ---EARLY TERMINATE IFRS9
    INSERT INTO IFRS_LI_ACCT_JOURNAL_DATA
    (
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
    SELECT A.DOWNLOAD_DATE
      ,A.MASTERID
      ,A.FACNO
      ,A.CIFNO
      ,A.ACCTNO
      ,A.DATASOURCE
      ,A.PRDTYPE
      ,A.PRDCODE
      ,A.TRXCODE
      ,A.CCY
      ,B.JOURNALCODE
      ,A.STATUS
      ,A.REVERSE
      ,A.FLAG_CF
      ,B.DRCR
      ,B.GLNO
      ,ABS(COST_FEE_PREV.AMOUNT)
      ,ABS(COST_FEE_PREV.AMOUNT) * IMC.EXCHANGE_RATE
      ,A.SOURCEPROCESS
      ,A.ID
      ,CURRENT_TIMESTAMP
      ,'SP_ACCT_JOURNAL_DATA2'
      ,A.BRANCH
      ,A.JOURNALCODE2
      ,B.JOURNAL_DESC
      ,B.JOURNALCODE
      ,B.COSTCENTER + '-' + COALESCE(IMC.APPLICATION_NO, IMP.APPLICATION_NO, '')
      ,B.GL_INTERNAL_CODE
      ,A.METHOD
--      ,COALESCE(IMC.RESERVED_VARCHAR_1, IMP.RESERVED_VARCHAR_1, '')
--      ,COALESCE(IMC.RESERVED_VARCHAR_2, IMP.RESERVED_VARCHAR_2, '')
      ,B.COSTCENTER
     FROM IFRS_LI_ACCT_EIR_COST_FEE_PREV COST_FEE_PREV
     INNER JOIN VW_LI_LAST_EIR_CF_PREV C ON COST_FEE_PREV.MASTERID = C.MASTERID AND COST_FEE_PREV.SEQ = C.SEQ AND COST_FEE_PREV.DOWNLOAD_DATE = C.DOWNLOAD_DATE
     LEFT JOIN IFRS_LI_ACCT_JOURNAL_INTM A ON A.MASTERID = COST_FEE_PREV.MASTERID AND A.DOWNLOAD_DATE = V_CURRDATE AND A.TRXCODE <> 'BENEFIT' AND A.METHOD = 'EIR' AND REVERSE = 'N'
     LEFT JOIN IFRS_LI_IMA_AMORT_CURR IMC ON A.MASTERID = IMC.MASTERID
     LEFT JOIN IFRS_LI_IMA_AMORT_PREV IMP ON A.MASTERID = IMP.MASTERID
     JOIN IFRS_LI_JOURNAL_PARAM B ON B.JOURNALCODE IN (
       'OTHER'
       )
      AND (
       B.CCY = A.CCY
       OR B.CCY = 'ALL'
       )
      AND B.FLAG_CF = A.FLAG_CF
      AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME, IMP.GL_CONSTNAME, '')
      AND (
       A.TRXCODE = B.TRX_CODE
       OR B.TRX_CODE = 'ALL'
       )
    WHERE COST_FEE_PREV.DOWNLOAD_DATE = V_PREVDATE
    AND COST_FEE_PREV.MASTERID IN
     (
       SELECT A.MASTERID FROM IFRS_LI_ACCT_COST_FEE A
       JOIN IFRS_LI_STG_TRANSACTION_DAILY B
       ON
       A.TRX_REFF_NUMBER = B.TRANSACTION_REFERENCE_NUMBER
       AND
       B.DOWNLOAD_DATE = V_CURRDATE
       AND
       B.TERMINATE_FLAG = 'Y'
       WHERE
       A.STATUS = 'ACT'
     );
    COMMIT;
    ---EARLY TERMINATE IFRS9

    /******************************************************************************
    09. staff loan accru
    *******************************************************************************/
    INSERT  INTO IFRS_LI_ACCT_JOURNAL_DATA
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
    SELECT  A.DOWNLOAD_DATE ,
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
            B.COSTCENTER || '-' || COALESCE(IMC.APPLICATION_NO,IMP.APPLICATION_NO,'') ,
            B.GL_INTERNAL_CODE,
            METHOD,
--            COALESCE(IMC.RESERVED_VARCHAR_1, IMP.RESERVED_VARCHAR_1,''),
--            COALESCE(IMC.RESERVED_VARCHAR_2, IMP.RESERVED_VARCHAR_2,''),
		    B.COSTCENTER
    FROM    IFRS_LI_ACCT_JOURNAL_INTM A
    LEFT JOIN IFRS_LI_IMA_AMORT_CURR IMC ON A.MASTERID = IMC.MASTERID
    LEFT JOIN IFRS_LI_IMA_AMORT_PREV IMP ON A.MASTERID = IMP.MASTERID
    JOIN IFRS_LI_JOURNAL_PARAM B
      ON B.JOURNALCODE IN ('ACCRU', 'EMPBE', 'EMACR','EBCTE')
      AND ( B.CCY = A.CCY OR B.CCY = 'ALL' )
      AND COALESCE(B.FLAG_CF,'-') NOT IN ('F', 'C' )
      AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME,IMP.GL_CONSTNAME,'')
      AND (A.TRXCODE=B.TRX_CODE  OR B.TRX_CODE = 'ALL'  )
    WHERE   A.DOWNLOAD_DATE = V_CURRDATE
    AND A.JOURNALCODE IN ( 'ACCRU', 'AMORT' )
    AND A.TRXCODE = 'BENEFIT'
    AND A.METHOD = 'EIR';


    COMMIT;



    INSERT  INTO IFRS_LI_AMORT_LOG ( DOWNLOAD_DATE , DTM , OPS , PROCNAME , REMARK )
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP , 'DEBUG' , 'SP_IFRS_LI_ACCT_JOURNAL_DATA' ,'amort 2');


    COMMIT;

    /******************************************************************************
    10. IF TODAY IS END OF MONTH
    *******************************************************************************/
    IF V_CURRDATE = LAST_DAY(V_CURRDATE)
    THEN


        --INSERT MARK TO MARKET
        EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_IFRS_LI_EIR_ADJUSTMENT';

        INSERT INTO TMP_IFRS_LI_EIR_ADJUSTMENT(
        DOWNLOAD_DATE,
        MASTERID,
        ACCOUNT_NUMBER,
        IFRS9_CLASS,
        LOAN_START_DATE,
        LOAN_DUE_DATE,
        OUTSTANDING,
        INTEREST_RATE,
        EIR,
        FAIR_VALUE_AMT ,
        MARKET_RATE,
        TOTAL_PV_CF,
        TOT_ADJUST,
        JOURNALCODE
        )
        SELECT  DOWNLOAD_DATE,
                MASTERID,
                ACCOUNT_NUMBER,
                IFRS9_CLASS,
                LOAN_START_DATE,
                LOAN_DUE_DATE,
                OUTSTANDING,
                INTEREST_RATE,
                EIR,
                FAIR_VALUE_AMT ,
                MARKET_RATE,
                TOTAL_PV_CF,
                TOT_ADJUST,
                CASE WHEN A.TOT_ADJUST >= 0    AND A.IFRS9_CLASS = 'FVTPL' THEN 'FVTPLG'
                     WHEN A.TOT_ADJUST < 0    AND A.IFRS9_CLASS = 'FVTPL' THEN 'FVTPLL'
                     WHEN A.TOT_ADJUST >= 0    AND A.IFRS9_CLASS = 'FVOCI' THEN 'FVOCIG'
                     WHEN A.TOT_ADJUST < 0    AND A.IFRS9_CLASS = 'FVOCI' THEN 'FVOCIL' END JOURNALCODE
        FROM IFRS_LI_EIR_ADJUSTMENT A;

        COMMIT;

        INSERT  INTO IFRS_LI_ACCT_JOURNAL_DATA
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
--	      RESERVED_VARCHAR_1,
--        RESERVED_VARCHAR_2,
          GL_COSTCENTER
      )
      SELECT A.DOWNLOAD_DATE ,
             A.MASTERID ,
             IMC.FACILITY_NUMBER ,
             IMC.CUSTOMER_NUMBER ,
             A.ACCOUNT_NUMBER ,
             IMC.DATA_SOURCE ,
             IMC.PRODUCT_TYPE ,
             IMC.PRODUCT_CODE,
             B.TRX_CODE ,
             IMC.CURRENCY ,
             B.JOURNALCODE ,
             'ACT' STATUS ,
             'N' REVERSE ,
             B.FLAG_CF ,
             B.DRCR,
             B.GLNO ,
             ABS(A.TOT_ADJUST) ,
             ABS(A.TOT_ADJUST * COALESCE(IMC.EXCHANGE_RATE,1)) ,
             'Mark To Market' AS SOURCEPROCESS ,
             NULL  ,
             SYSTIMESTAMP ,
             'SP_JOURNAL_DATA2' ,
             IMC.BRANCH_CODE ,
             NULL JOURNALCODE2 ,
             B.JOURNAL_DESC ,
             B.JOURNALCODE ,
             B.COSTCENTER || '-' || COALESCE(IMC.APPLICATION_NO,'') ,
             B.GL_INTERNAL_CODE,
             NULL METHOD,
--	         COALESCE(IMC.RESERVED_VARCHAR_1, IMP.RESERVED_VARCHAR_1, ''),
--             COALESCE(IMC.RESERVED_VARCHAR_2, IMP.RESERVED_VARCHAR_2, ''),
             B.COSTCENTER
      FROM TMP_IFRS_LI_EIR_ADJUSTMENT A
      LEFT JOIN IFRS_LI_IMA_AMORT_CURR IMC ON A.MASTERID = IMC.MASTERID
      LEFT JOIN IFRS_LI_IMA_AMORT_PREV IMP ON A.MASTERID = IMP.MASTERID
      JOIN IFRS_LI_JOURNAL_PARAM B ON B.JOURNALCODE = A.JOURNALCODE AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME,'')
      WHERE A.DOWNLOAD_DATE = V_CURRDATE;

      COMMIT;

      --REVERSE MARK TO MARKET
      INSERT  INTO IFRS_LI_ACCT_JOURNAL_DATA
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
	  RESERVED_VARCHAR_1,
          RESERVED_VARCHAR_2,
          RESERVED_VARCHAR_3,
          GL_COSTCENTER
      )
      SELECT V_CURRDATE DOWNLOAD_DATE ,
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
             'Y' REVERSE ,
             FLAG_CF ,
             CASE WHEN DRCR = 'D' THEN 'C' ELSE 'D' END DRCR ,
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
	     RESERVED_VARCHAR_1,
             RESERVED_VARCHAR_2,
             RESERVED_VARCHAR_3,
             GL_COSTCENTER
      FROM  IFRS_LI_ACCT_JOURNAL_DATA
      WHERE DOWNLOAD_DATE = V_PREVMONTH
      AND JOURNALCODE IN ('FVTPLG','FVTPLL', 'FVOCIG','FVOCIL')
      AND REVERSE = 'N';


    COMMIT;


    END IF;

    /******************************************************************************
    11. ACCRU NOCF
    *******************************************************************************/
    INSERT  INTO IFRS_LI_ACCT_JOURNAL_DATA
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
--      RESERVED_VARCHAR_1 ,
--      RESERVED_VARCHAR_2 ,
      GL_COSTCENTER
    )
    SELECT A.DOWNLOAD_DATE,
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
    FROM IFRS_LI_ACCT_JOURNAL_INTM A
    LEFT JOIN IFRS_LI_IMA_AMORT_CURR IMC ON A.MASTERID = IMC.MASTERID
    LEFT JOIN IFRS_LI_IMA_AMORT_PREV IMP ON A.MASTERID = IMP.MASTERID
    JOIN IFRS_LI_JOURNAL_PARAM B
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
    12. JOURNAL SWITCH ACCOUNT
    *******************************************************************************/
    --SP_IFRS_EXEC_AND_LOG_PROCESS('SP_IFRS_LI_SW_JRNL_DATA_ITRCG');


    COMMIT;

    --RLCV OLD BRANCH
    -- INSERT ITRCG DATA_SOURCE CCY JENIS_PINJAMAN COMBINATION
     INSERT INTO IFRS_LI_ACCT_JOURNAL_DATA (
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
    --  ,RESERVED_VARCHAR_1
    --  ,RESERVED_VARCHAR_2
      ,GL_COSTCENTER
      )
     SELECT A.DOWNLOAD_DATE
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
    --  ,COALESCE(IMC.RESERVED_VARCHAR_1, IMP.RESERVED_VARCHAR_1, '')
    --  ,COALESCE(IMC.RESERVED_VARCHAR_2, IMP.RESERVED_VARCHAR_2, '')
      ,B.COSTCENTER
     FROM IFRS_LI_ACCT_JOURNAL_INTM A
     LEFT JOIN IFRS_LI_IMA_AMORT_CURR IMC ON A.MASTERID = IMC.MASTERID
     LEFT JOIN IFRS_LI_IMA_AMORT_PREV IMP ON A.MASTERID = IMP.MASTERID
     JOIN IFRS_LI_JOURNAL_PARAM B ON B.JOURNALCODE IN (
       'RCLV'
       )
      --AND A.JOURNALCODE2 = B.JOURNALCODE --IFRS9 FUNDING
      AND (
       B.CCY = A.CCY
       OR B.CCY = 'ALL'
       )
      AND B.FLAG_CF = A.FLAG_CF
      AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME, IMP.GL_CONSTNAME, '')
      AND (
       A.TRXCODE = B.TRX_CODE
       OR B.TRX_CODE = 'ALL'
       )
     WHERE A.DOWNLOAD_DATE = V_CURRDATE
      AND A.JOURNALCODE = 'DEFA0'
      AND A.TRXCODE <> 'BENEFIT'
      AND A.METHOD = 'EIR'
      AND A.SOURCEPROCESS = 'EIR_REV_SWITCH';

      COMMIT;


    --RLCS NEW BRANCH
     -- INSERT ITRCG DATA_SOURCE CCY JENIS_PINJAMAN COMBINATION
     INSERT INTO IFRS_LI_ACCT_JOURNAL_DATA (
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
    --  ,RESERVED_VARCHAR_1
    --  ,RESERVED_VARCHAR_2
      ,GL_COSTCENTER
      )
     SELECT A.DOWNLOAD_DATE
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
    --  ,COALESCE(IMC.RESERVED_VARCHAR_1, IMP.RESERVED_VARCHAR_1, '')
    --  ,COALESCE(IMC.RESERVED_VARCHAR_2, IMP.RESERVED_VARCHAR_2, '')
      ,B.COSTCENTER
     FROM IFRS_LI_ACCT_JOURNAL_INTM A
     LEFT JOIN IFRS_LI_IMA_AMORT_CURR IMC ON A.MASTERID = IMC.MASTERID
     LEFT JOIN IFRS_LI_IMA_AMORT_PREV IMP ON A.MASTERID = IMP.MASTERID
     JOIN IFRS_LI_JOURNAL_PARAM B ON B.JOURNALCODE IN (
       'RCLS'
       )
      AND (
       B.CCY = A.CCY
       OR B.CCY = 'ALL'
       )
      AND B.FLAG_CF = A.FLAG_CF
      AND B.GL_CONSTNAME = COALESCE(IMC.GL_CONSTNAME, IMP.GL_CONSTNAME, '')
      AND (
       A.TRXCODE = B.TRX_CODE
       OR B.TRX_CODE = 'ALL'
       )
     WHERE A.DOWNLOAD_DATE = V_CURRDATE
      AND A.JOURNALCODE = 'DEFA0'
      AND A.TRXCODE <> 'BENEFIT'
      AND A.METHOD = 'EIR'
      AND A.SOURCEPROCESS = 'EIR_SWITCH';

      COMMIT;

    /******************************************************************************
    13. call journal SL GENERATED
    *******************************************************************************/
    INSERT  INTO IFRS_LI_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK )
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP , 'DEBUG' ,'SP_IFRS_LI_ACCT_JOURNAL_DATA' , 'journal SL');

    SP_IFRS_EXEC_AND_LOG_PROCESS('SP_IFRS_LI_ACCT_JRNL_DATA_SL');

    INSERT  INTO IFRS_LI_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK )
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP , 'DEBUG' ,'SP_IFRS_LI_ACCT_JOURNAL_DATA' , 'journal SL done');
    COMMIT;

    /* JOURNAL FACILITY LEVEL FOR PNL EXPIRED 20180501*/
     ---CORPORATE
     INSERT INTO IFRS_LI_ACCT_JOURNAL_DATA (
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
      ,RESERVED_VARCHAR_1
      ,RESERVED_VARCHAR_2
      ,RESERVED_VARCHAR_3
      )
     SELECT B.DOWNLOAD_DATE
      ,A.TRX_FACILITY_NO
      ,A.TRX_FACILITY_NO
      ,NULL
      ,A.TRX_FACILITY_NO
      ,NULL
      ,NULL
      ,NULL
      ,A.TRX_CODE
      ,A.TRX_CCY
      ,'PNL'
      ,'ACT' STATUS
      ,'N' REVERSE
      ,D.VALUE1 FLAG_CF
      ,SUBSTR(D.VALUE2, 1, 1)
      ,D.VALUE3 GLNO
      ,A.REMAINING
      ,A.REMAINING * RATE.RATE_AMOUNT
      ,'CORP FACILITY EXP' AS SOURCEPROCESS
      ,NULL
      ,CURRENT_TIMESTAMP
      ,'SP_ACCT_JOURNAL_DATA'
      ,'' BRANCH_CODE--,E.BRANCH_CODE
      ,'PNL' JOURNALCODE2
      ,D.DESCRIPTION
      ,NULL
      ,NULL
      ,NULL GL_INTERNAL_CODE
      ,NULL METHOD
      ,NULL AS RESERVED_VARCHAR_1
      ,NULL AS RESERVED_VARCHAR_2
      ,NULL AS RESERVED_VARCHAR_3
     FROM IFRS_TRX_FACILITY A
     --LEFT JOIN IFRS_LI_MASTER_ACCOUNT IMA ON A.MASTERID = IMA.MASTERID AND A.DOWNLOAD_DATE = IMA.DOWNLOAD_DATE
     LEFT JOIN IFRS_MASTER_PARENT_LIMIT B ON A.TRX_FACILITY_NO = B.LIMIT_PARENT_NO
      AND B.DOWNLOAD_DATE = V_CURRDATE
     LEFT JOIN IFRS_LI_TRANSACTION_PARAM C ON A.TRX_CODE = C.TRX_CODE
      AND (
       A.TRX_CCY = C.CCY
       OR C.CCY = 'ALL'
       )
     LEFT JOIN TBLM_COMMONCODEDETAIL D ON SUBSTR(C.IFRS_TXN_CLASS, 1, 1) = D.VALUE1
      AND D.COMMONCODE = 'B103'
     LEFT JOIN IFRS_MASTER_EXCHANGE_RATE RATE ON A.TRX_CCY = RATE.CURRENCY
      AND RATE.DOWNLOAD_DATE = V_CURRDATE
     LEFT JOIN IFRS_IMA_LIMIT E ON A.TRX_FACILITY_NO = E.MASTERID
      AND E.DOWNLOAD_DATE = V_CURRDATE
     WHERE A.REMAINING > 0
      AND A.FACILITY_EXPIRED_DATE + 1 = V_CURRDATE
      AND A.STATUS = 'P'
      AND A.REVID IS NULL
      AND A.PKID NOT IN (
       SELECT DISTINCT REVID
       FROM IFRS_TRX_FACILITY
       WHERE REVID IS NOT NULL
       )
      AND B.SME_FLAG = 0;

      COMMIT;

     ---SME
     INSERT INTO IFRS_LI_ACCT_JOURNAL_DATA (
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
      ,RESERVED_VARCHAR_1
      ,RESERVED_VARCHAR_2
      ,RESERVED_VARCHAR_3
      )
     SELECT B.DOWNLOAD_DATE
      ,A.TRX_FACILITY_NO
      ,A.TRX_FACILITY_NO
      ,NULL
      ,A.TRX_FACILITY_NO
      ,NULL
      ,NULL
      ,NULL
      ,A.TRX_CODE
      ,A.TRX_CCY
      ,'PNL'
      ,'ACT' STATUS
      ,'N' REVERSE
      ,D.VALUE1 FLAG_CF
      ,SUBSTR(D.VALUE2, 1, 1)
      ,D.VALUE3 GLNO
      ,A.REMAINING
      ,A.REMAINING * RATE.RATE_AMOUNT
      ,'SME FACILITY EXP' AS SOURCEPROCESS
      ,NULL
      ,CURRENT_TIMESTAMP
      ,'SP_ACCT_JOURNAL_DATA'
      ,'' BRANCH_CODE--,E.BRANCH_CODE
      ,'PNL' JOURNALCODE2
      ,D.DESCRIPTION
      ,NULL
      ,NULL
      ,NULL GL_INTERNAL_CODE
      ,NULL METHOD
      ,NULL AS RESERVED_VARCHAR_1
      ,NULL AS RESERVED_VARCHAR_2
      ,NULL AS RESERVED_VARCHAR_3
     FROM IFRS_TRX_FACILITY A
     LEFT JOIN IFRS_MASTER_PARENT_LIMIT B ON A.TRX_FACILITY_NO = B.LIMIT_PARENT_NO
      AND B.DOWNLOAD_DATE = V_CURRDATE
     LEFT JOIN IFRS_LI_TRANSACTION_PARAM C ON A.TRX_CODE = C.TRX_CODE
      AND (
       A.TRX_CCY = C.CCY
       OR C.CCY = 'ALL'
       )
     LEFT JOIN TBLM_COMMONCODEDETAIL D ON SUBSTR(C.IFRS_TXN_CLASS, 1, 1) = D.VALUE1
      AND D.COMMONCODE = 'B104'
     LEFT JOIN IFRS_MASTER_EXCHANGE_RATE RATE ON A.TRX_CCY = RATE.CURRENCY
      AND RATE.DOWNLOAD_DATE = V_CURRDATE
     LEFT JOIN IFRS_IMA_LIMIT E ON A.TRX_FACILITY_NO = E.MASTERID
      AND E.DOWNLOAD_DATE = V_CURRDATE
     WHERE A.REMAINING > 0
      AND A.FACILITY_EXPIRED_DATE + 1 = V_CURRDATE
      AND A.STATUS = 'P'
      AND A.REVID IS NULL
      AND A.PKID NOT IN (
       SELECT DISTINCT REVID
       FROM IFRS_TRX_FACILITY
       WHERE REVID IS NOT NULL
       )
      AND B.SME_FLAG = 1;

      COMMIT;

  INSERT  INTO IFRS_LI_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK )
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP , 'DEBUG' ,'SP_IFRS_LI_ACCT_JOURNAL_DATA' , 'journal facility done');
    COMMIT;

    /******************************************************************************
    16. UPDATE JOURNAL DATA
    *******************************************************************************/
    /*Pindahan dari atas 20160510*/
    UPDATE  IFRS_LI_ACCT_JOURNAL_DATA
    SET     NOREF = CASE WHEN NOREF IN ('ITRCG','ITRCG_SL','ITRCG_NE') THEN '1'
                         WHEN NOREF IN ('ITRCG1','ITRCG_SL1') THEN '2'
                         WHEN NOREF IN ('ITRCG2','ITRCG_SL2') THEN '3'
                         WHEN NOREF IN ('EMPBE','EMPBE_SL') THEN '4'
                         WHEN NOREF IN ('EMACR','EMACR_SL') THEN '5'
                         WHEN NOREF = 'RLS' THEN '6'
                         ELSE '9'
                    END
                    + CASE WHEN REVERSE = 'Y' THEN '1'   ELSE '2'  END
                    + CASE WHEN DRCR = 'D' THEN '1'  ELSE '2'  END
    WHERE   DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;

    INSERT  INTO IFRS_LI_AMORT_LOG ( DOWNLOAD_DATE ,DTM , OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE , SYSTIMESTAMP , 'DEBUG' ,'SP_IFRS_LI_ACCT_JOURNAL_DATA' ,'fill noref done');

    COMMIT;
    /******************************************************************************
    15. UPDATE FAIR VALUE
    *******************************************************************************/
    /* FD: UPDATE MASTER ACCOUNT FAIR VALUE AMOUNT*/
    UPDATE  IFRS_LI_MASTER_ACCOUNT
    SET     FAIR_VALUE_AMOUNT = COALESCE(OUTSTANDING, 0) + COALESCE(OUTSTANDING_IDC,0) + + COALESCE(UNAMORT_FEE_AMT,0) + COALESCE(UNAMORT_COST_AMT, 0)
    WHERE   DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;


    INSERT  INTO IFRS_LI_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_IFRS_LI_ACCT_JOURNAL_DATA' ,'');

    COMMIT;


END;