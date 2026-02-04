CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_JOURNAL_DATA_RUN
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

    COMMIT;******************************************************************************
    07. STAFF LOAN DEFA0
    *******************************************************************************/
    INSERT  INTO IFRS_ACCT_JOURNAL_DATA
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
            B.COSTCENTER,
            B.GL_INTERNAL_CODE,
            METHOD,
--			COALESCE(IMC.RESERVED_VARCHAR_1, IMP.RESERVED_VARCHAR_1,''),
--          COALESCE(IMC.RESERVED_VARCHAR_2, IMP.RESERVED_VARCHAR_2,''),
            B.COSTCENTER
    FROM    IFRS_ACCT_JOURNAL_INTM A
    JOIN (SELECT MASTERID, GL_CONSTNAME FROM IFRS_MASTER_ACCOUNT WHERE DOWNLOAD_DATE = V_CURRDATE AND STAFF_LOAN_FLAG = 1) C
            ON A.MASTERID = C.MASTERID
    JOIN IFRS_JOURNAL_PARAM B
      ON B.JOURNALCODE IN ('ITRCG','ITRCG1','ITRCG2','ITEMB')
      AND ( B.CCY = A.CCY OR B.CCY = 'ALL' )
      AND B.FLAG_CF = A.FLAG_CF
      AND B.GL_CONSTNAME = C.GL_CONSTNAME
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
    09. staff loan accru
    *******************************************************************************/
    INSERT  INTO IFRS_ACCT_JOURNAL_DATA
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
            B.COSTCENTER ,
            B.GL_INTERNAL_CODE,
            METHOD,
--            COALESCE(IMC.RESERVED_VARCHAR_1, IMP.RESERVED_VARCHAR_1,''),
--            COALESCE(IMC.RESERVED_VARCHAR_2, IMP.RESERVED_VARCHAR_2,''),
		    B.COSTCENTER
    FROM    IFRS_ACCT_JOURNAL_INTM A
    JOIN (SELECT MASTERID, GL_CONSTNAME FROM IFRS_MASTER_ACCOUNT WHERE DOWNLOAD_DATE = V_CURRDATE AND STAFF_LOAN_FLAG = 1) C
            ON A.MASTERID = C.MASTERID
    JOIN IFRS_JOURNAL_PARAM B
      ON B.JOURNALCODE IN ('ACCRU', 'EMPBE', 'EMACR','EBCTE')
      AND ( B.CCY = A.CCY OR B.CCY = 'ALL' )
      AND B.FLAG_CF = A.FLAG_CF
      AND B.GL_CONSTNAME = C.GL_CONSTNAME
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
    14. UPDATE JOURNAL DATA
    *******************************************************************************/
    /*Pindahan dari atas 20160510*/
    UPDATE  IFRS_ACCT_JOURNAL_DATA
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
    WHERE   DOWNLOAD_DATE = V_CURRDATE
            AND TRXCODE = 'BENEFIT';

    COMMIT;

	INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE ,DTM , OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE , SYSTIMESTAMP , 'DEBUG' ,'SP_IFRS_ACCT_JOURNAL_DATA' ,'fill noref done');

    MERGE INTO IFRS_ACCT_JOURNAL_DATA A
    USING (SELECT MASTERID,BRANCH_CODE FROM IFRS_MASTER_aCCOUNT WHERE DOWNLOAD_DATE = V_CURRDATE AND DATA_SOURCE = 'ILS')B
    ON (A.MASTERID = B.MASTERID AND A.DOWNLOAD_DATE = V_CURRDATE AND A.DATASOURCE = 'ILS')
    WHEN MATCHED THEN UPDATE SET A.BRANCH = B.BRANCH_CODE;
    COMMIT;

    UPDATE IFRS_ACCT_JOURNAL_DATA
    SET BRANCH = CASE WHEN LENGTH(BRANCH) > 4 THEN SUBSTR(BRANCH,4,4) ELSE BRANCH END
                    WHERE DOWNLOAD_DATE = V_CURRDATE;COMMIT;

------------------------------------------------------------------------------------------------
--GL_OUTBOUND
------------------------------------------------------------------------------------------------
   /* DELETE IFRS_GL_OUTBOUND_IMP WHERE DOWNLOAD_DATE = V_CURRDATE;COMMIT;
    DELETE IFRS_GL_OUTBOUND_AMT WHERE DOWNLOAD_DATE = V_CURRDATE;COMMIT;

    INSERT INTO IFRS_GL_OUTBOUND_IMP
        SELECT
        V_CURRDATE,
        'VLJ'                                                       AAK_DBID,
        'BCA'                                                       AAK_CORP,
        A.BRANCH_CODE||'FRS99'                                      AAK_JRNLID,
        A.DOWNLOAD_DATE-TO_DATE('1900-01-01','YYYY-MM-DD')+1        AAK_EFFDT,
        A.BRANCH_CODE||REPLACE(A.GL_ACCOUNT,'.','')                 AAK_VLMKEY,
        CASE
            WHEN REMARKS = 'BKPI'
            THEN CASE
                    WHEN DATA_SOURCE = 'ILS'
                    THEN CASE
                            WHEN REVERSAL_FLAG = 'Y'
                            THEN '001'
                            ELSE '009'
                         END
                    WHEN DATA_SOURCE = 'CRD'
                    THEN CASE
                            WHEN REVERSAL_FLAG = 'Y'
                            THEN '002'
                            ELSE '009'
                         END
                    WHEN DATA_SOURCE = 'KTP'
                    THEN CASE
                            WHEN REVERSAL_FLAG = 'Y'
                            THEN '003'
                            ELSE '009'
                         END
                    WHEN DATA_SOURCE = 'BTRD'
                    THEN CASE
                            WHEN REVERSAL_FLAG = 'Y'
                            THEN '004'
                            ELSE '009'
                         END
                    WHEN DATA_SOURCE = 'RKN'
                    THEN CASE
                            WHEN REVERSAL_FLAG = 'Y'
                            THEN '005'
                            ELSE '009'
                         END
                 END
            WHEN REMARKS = 'BKPI2'
            THEN CASE
                    WHEN DATA_SOURCE = 'ILS'
                    THEN '006'
                    WHEN DATA_SOURCE = 'CRD'
                    THEN '007'
                    WHEN DATA_SOURCE = 'BTRD'
                    THEN '008'
                 END
            WHEN REMARKS = 'BKIUW'
            THEN CASE
                    WHEN DATA_SOURCE = 'ILS'
                    THEN CASE
                            WHEN REVERSAL_FLAG = 'N'
                            THEN '101'
                            ELSE '102'
                         END
                    WHEN DATA_SOURCE = 'KTP'
                    THEN CASE
                            WHEN REVERSAL_FLAG = 'N'
                            THEN '105'
                            ELSE '106'
                         END
                    WHEN DATA_SOURCE = 'BTRD'
                    THEN CASE
                            WHEN REVERSAL_FLAG = 'N'
                            THEN '107'
                            ELSE '108'
                         END
                 END
            WHEN REMARKS = 'IRBS' AND DATA_SOURCE = 'ILS'
            THEN CASE
                    WHEN REVERSAL_FLAG = 'N'
                    THEN '103'
                    ELSE '104'
                 END
        END                                                         AAK_VLMKEY_SEQ,
        '                         '                                 AAK_VLMKEY_FILLER,
        A.CURRENCY                                                  AAK_CURRCD,
        ' '                                                         AAK_SLID,
        ' '                                                         AAK_SLAC,
        ' '                                                         AAK_SOURCE,
        B.JOURNAL_DESC                                              AAK_DESC,
        'CY'                                                        AAK_JA,
        'CP'                                                        AAK_JT,
        CASE WHEN TXN_TYPE = 'DB' THEN 'D' ELSE 'C' END             AAK_DCCD,
        CASE WHEN TXN_TYPE = 'CR' THEN '-' ELSE NULL END            AAK_RP_SIGN,
        AMOUNT_IDR                                                  AAK_AMT_RP,
        CASE WHEN TXN_TYPE = 'CR' THEN '-' ELSE NULL END            AAK_VA_SIGN,
        AMOUNT                                                      AAK_AMT_VA
        FROM IFRS_IMP_JOURNAL_DATA A
        JOIN IFRS_JOURNAL_PARAM B
        ON A.GL_ACCOUNT = B.GLNO
        WHERE DOWNLOAD_DATE = V_CURRDATE;
        COMMIT;
*/
        ------------------

    DELETE IFRS_GL_OUTBOUND_AMT WHERE DOWNLOAD_DATE = V_CURRDATE;COMMIT;

    INSERT INTO IFRS_GL_OUTBOUND_AMT
        SELECT
        V_CURRDATE,
        'VLJ'                                                       AAK_DBID,
        'BCA'                                                       AAK_CORP,
        A.BRANCH||'FRS99'                                           AAK_JRNLID,
        A.DOWNLOAD_DATE-TO_DATE('1900-01-01','YYYY-MM-DD')+1        AAK_EFFDT,
        A.BRANCH||REPLACE(A.GLNO,'.','')                            AAK_VLMKEY,
        CASE WHEN DATASOURCE = 'ILS'
             THEN CASE
                    WHEN JOURNALCODE2 IN ('ITEMB','EMPBE','EBCTE')
                    THEN CASE
                            WHEN REVERSE = 'N'
                            THEN '201'
                            ELSE '202'
                         END
                    WHEN JOURNALCODE2 = 'RECORE'
                    THEN CASE
                            WHEN REVERSE = 'N'
                            THEN '301'
                            ELSE '302'
                        END
                    WHEN JOURNALCODE2 = 'ACCRU'
                    THEN CASE
                            WHEN REVERSE = 'N'
                            THEN '303'
                            ELSE '304'
                         END
                    WHEN JOURNALCODE2 = 'ITRCG'
                    THEN CASE
                            WHEN REVERSE = 'N'
                            THEN '305'
                            ELSE '306'
                         END
                 END
        END                                                         AAK_VLMKEY_SEQ,
        '                         '                                 AAK_VLMKEY_FILLER,
        A.CCY                                                       AAK_CURRCD,
        ' '                                                         AAK_SLID,
        ' '                                                         AAK_SLAC,
        ' '                                                         AAK_SOURCE,
        B.JOURNAL_DESC                                              AAK_DESC,
        'CY'                                                        AAK_JA,
        'CP'                                                        AAK_JT,
        A.DRCR                                                      AAK_DCCD,
        CASE WHEN A.DRCR = 'C' THEN '-' ELSE NULL END               AAK_RP_SIGN,
        N_AMOUNT_IDR                                                AAK_AMT_RP,
        CASE WHEN A.DRCR = 'C' THEN '-' ELSE NULL END               AAK_VA_SIGN,
        N_AMOUNT                                                    AAK_AMT_VA
        FROM IFRS_ACCT_JOURNAL_DATA A
        JOIN IFRS_JOURNAL_PARAM B
        ON A.GLNO = B.GLNO
        WHERE DOWNLOAD_DATE = V_CURRDATE;

    COMMIT;
    END;