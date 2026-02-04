CREATE OR REPLACE PROCEDURE SP_IFRS_JOURNAL_DKP (v_DOWNLOADDATE DATE DEFAULT('1-JAN-1900'),v_DOWNLOADDATEPREV DATE DEFAULT('1-JAN-1900'))
AS
V_CURRDATE DATE;
V_PREVDATE DATE;
BEGIN

EXECUTE IMMEDIATE 'alter session enable parallel dml';

IF V_DOWNLOADDATE = '1-JAN-1900'
  THEN
    SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;
  ELSE
    V_CURRDATE := v_DOWNLOADDATE;
  END IF;

  IF v_DOWNLOADDATEPREV = '1-JAN-1900'
  THEN
    SELECT CURRDATE INTO V_PREVDATE FROM IFRS_PRC_DATE;
  ELSE
    V_PREVDATE := v_DOWNLOADDATEPREV;
  END IF;


EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_ACCT_AMORT_RPT_REKON_DKP';
EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_IMP_JOURNAL_DATA_DKP';

INSERT /*+ PARALLEL(8) */ INTO IFRS_ACCT_AMORT_RPT_REKON_DKP
SELECT /*+ PARALLEL(8) */ DATA_SOURCE,
       JOURNAL_ID,
       SEQ,
       BRANCH_CODE,
       JOURNAL_CODE,
       CURRENCY,
       DB,
       CR,
       AMOUNT,
       AMOUNT_IDR
  FROM (select *
          from (SELECT DATA_SOURCE,
                       JOURNAL_ID,
                       SEQ,
                       BRANCH_CODE,
                       JOURNAL_CODE,
                       DRCR,
                       iaarr.CURRENCY CURRENCY,
                       GLNO,
                       PRODUCT_TYPE,
                       round(AMOUNT,2) AMOUNT ,
                       round(amount * ime1.rate_amount,2) amount_idr
                  FROM (select DOWNLOAD_DATE,
                               DATA_SOURCE,
                               JOURNAL_ID,
                               SEQ,
                               BRANCH_CODE,
                               JOURNAL_CODE,
                               DRCR,
                               CURRENCY,
                               GLNO,
                               PRODUCT_TYPE,
                               SUM(AMOUNT) AMOUNT
                          from ifrs_acct_amort_rpt_rekon A
                         WHERE last_day(DOWNLOAD_DATE) = V_CURRDATE
                               AND JOURNAL_CODE <> 'MATURE'
                         GROUP BY DATA_SOURCE,
                                  DOWNLOAD_DATE,
                                  JOURNAL_ID,
                                  SEQ,
                                  BRANCH_CODE,
                                  JOURNAL_CODE,
                                  GLNO,
                                  DRCR,
                                  CURRENCY,
                                  PRODUCT_TYPE
                        HAVING SUM(AMOUNT) <> 0) iaarr,
                       ifrs_master_exchange_rate ime1
                 where ime1.download_date =
                       (select max(ime2.download_date)
                          from ifrs_master_exchange_rate ime2
                         where ime2.download_date <= V_CURRDATE)
                   and iaarr.currency = ime1.currency
                   and last_day(iaarr.download_date) = V_CURRDATE))
PIVOT(MAX(GLNO)
   FOR DRCR IN('D' DB,
               'C' CR)) JOUR
UNION ALL
SELECT /*+ PARALLEL(8) */ iaarr.DATA_SOURCE,
       iaarr.JOURNAL_ID,
       iaarr.SEQ,
       iaarr.BRANCH_CODE,
       iaarr.JOURNAL_CODE,
       iaarr.CURRENCY,
       iaarr.DB,
       iaarr.CR,
       ROUND (iaarr.AMOUNT, 2)                    AMOUNT,
       ROUND (iaarr.amount * ime1.rate_amount, 2) amount_idr
  FROM (  SELECT DOWNLOAD_DATE,
                 DATA_SOURCE,
                 JOURNAL_ID,
                 SEQ,
                 BRANCH_CODE,
                 JOURNAL_CODE,
                 CURRENCY,
                 MIN (CASE RN WHEN 1 THEN GLNO ELSE GLNO END)   DB,
                 MAX (CASE RN WHEN 2 THEN GLNO ELSE GLNO END)   CR,
                 MIN (CASE RN WHEN 1 THEN AMOUNT ELSE AMOUNT END) AMOUNT
            FROM (  SELECT A.DOWNLOAD_DATE,
                           A.DATA_SOURCE,
                           A.JOURNAL_ID,
                           A.SEQ,
                           A.BRANCH_CODE,
                           A.JOURNAL_CODE,
                           A.CURRENCY,
                           A.PRODUCT_TYPE,
                           A.GLNO,
                           B.GL_CONSTNAME,
                           A.DRCR,
                           SUM (A.AMOUNT) AMOUNT,
                           ROW_NUMBER ()
                              OVER (PARTITION BY B.GL_CONSTNAME ORDER BY A.DRCR)
                              AS rn
                      FROM ifrs_acct_amort_rpt_rekon A, IFRS_MASTER_JOURNAL_PARAM B
                     WHERE     last_day(A.DOWNLOAD_DATE) = V_CURRDATE
                           AND A.journal_code = 'MATURE'
                           AND A.GLNO = B.GL_NO
                           AND A.journal_code = B.journalcode
                  GROUP BY A.DOWNLOAD_DATE,
                           A.DATA_SOURCE,
                           A.JOURNAL_ID,
                           A.SEQ,
                           A.BRANCH_CODE,
                           A.JOURNAL_CODE,
                           A.CURRENCY,
                           A.PRODUCT_TYPE,
                           A.GLNO,
                           B.GL_CONSTNAME,
                           A.DRCR
                    HAVING SUM (A.AMOUNT) <> 0
                  ORDER BY RN)
        GROUP BY DOWNLOAD_DATE,
                 DATA_SOURCE,
                 JOURNAL_ID,
                 SEQ,
                 BRANCH_CODE,
                 JOURNAL_CODE,
                 CURRENCY,
                 GL_CONSTNAME) iaarr,
       ifrs_master_exchange_rate ime1
 WHERE     ime1.download_date = (SELECT MAX (ime2.download_date)
                                   FROM ifrs_master_exchange_rate ime2
                                  WHERE ime2.download_date <= V_CURRDATE)
       AND iaarr.currency = ime1.currency
       AND last_day(iaarr.download_date) = V_CURRDATE;
COMMIT;



/*
INSERT INTO IFRS_IMP_JOURNAL_DATA_DKP
SELECT distinct DATA_SOURCE,
       JOURNAL_ID,
       SEQ,
       BRANCH_CODE,
       REMARKS,
       DB,
       CR,
       CURRENCY,
       AMOUNT,
       AMOUNT_IDR
FROM (
SELECT iaarr.DATA_SOURCE,
       iaarr.JOURNAL_ID,
       iaarr.SEQ,
       iaarr.BRANCH_CODE,
       iaarr.REMARKS,
       iaarr.CURRENCY,
       iaarr.DB,
       iaarr.CR,
       ROUND (iaarr.AMOUNT, 2)                    AMOUNT,
       ROUND (iaarr.amount * ime1.rate_amount, 2) amount_idr
  FROM ( SELECT DOWNLOAD_DATE,
                 DATA_SOURCE,
                 JOURNAL_ID,
                 SEQ,
                 BRANCH_CODE,
                 REMARKS,
                 CURRENCY,
                 MIN (CASE RN WHEN 1 THEN GL_ACCOUNT ELSE GL_ACCOUNT END)   DB,
                 MAX (CASE RN WHEN 2 THEN GL_ACCOUNT ELSE GL_ACCOUNT END)   CR,
                 MIN (CASE RN WHEN 1 THEN AMOUNT ELSE AMOUNT END) AMOUNT
                 FROM (
select a.download_date,
       a.data_source,
       A.JOURNAL_ID,
                           A.SEQ,
                           A.BRANCH_CODE,
                           A.REMARKS,
                           A.CURRENCY,
                           A.PRD_TYPE,
                           A.GL_ACCOUNT,
                           B.GL_CONSTNAME,
                           A.TXN_TYPE,
                           SUM(A.AMOUNT) AMOUNT,
                           ROW_NUMBER ()
                              OVER (PARTITION BY B.GL_CONSTNAME ORDER BY A.TXN_TYPE)
                              AS rn
                             from (
						SELECT download_date,
                               DATA_SOURCE,
                               (BRANCH_CODE || 'FRS99') JOURNAL_ID,
                               CASE
                                 WHEN REMARKS = 'BKPI' THEN
                                  CASE
                                    WHEN DATA_SOURCE = 'ILS' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'Y' THEN
                                        '009'
                                       ELSE
                                        '001'
                                     END
                                    WHEN DATA_SOURCE = 'CRD' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'Y' THEN
                                        '009'
                                       ELSE
                                        '002'
                                     END
                                    WHEN DATA_SOURCE = 'KTP' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'Y' THEN
                                        '009'
                                       ELSE
                                        '003'
                                     END
                                    WHEN DATA_SOURCE = 'BTRD' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'Y' THEN
                                        '009'
                                       ELSE
                                        '004'
                                     END
                                    WHEN DATA_SOURCE = 'RKN' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'Y' THEN
                                        '009'
                                       ELSE
                                        '005'
                                     END
                                  END
                                 WHEN REMARKS = 'BKPI2' THEN
                                  CASE
                                    WHEN DATA_SOURCE = 'ILS' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'Y' THEN
                                        '009'
                                       ELSE
                                        '006'
                                     END
                                    WHEN DATA_SOURCE = 'CRD' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'Y' THEN
                                        '009'
                                       ELSE
                                        '007'
                                     END
                                    WHEN DATA_SOURCE = 'BTRD' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'Y' THEN
                                        '009'
                                       ELSE
                                        '008'
                                     END
                                    WHEN DATA_SOURCE = 'LIMIT' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'Y' THEN
                                        '009'
                                       ELSE
                                        '006'
                                     END
                                  END
                                 WHEN REMARKS = 'BKIUW' THEN
                                  CASE
                                    WHEN DATA_SOURCE = 'ILS' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'N' THEN
                                        '101'
                                       ELSE
                                        '102'
                                     END
                                    WHEN DATA_SOURCE = 'KTP' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'N' THEN
                                        '105'
                                       ELSE
                                        '106'
                                     END
                                    WHEN DATA_SOURCE = 'BTRD' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'N' THEN
                                        '107'
                                       ELSE
                                        '108'
                                     END
                                  END
                                 WHEN REMARKS = 'IRBS' AND DATA_SOURCE = 'ILS' THEN
                                  CASE
                                    WHEN REVERSAL_FLAG = 'N' THEN
                                     '103'
                                    ELSE
                                     '104'
                                  END
                               END SEQ,
                               BRANCH_CODE,
                               REMARKS,
                               CURRENCY,
                               AMOUNT,
                               GL_ACCOUNT,
                               TXN_TYPE,
                               PRD_TYPE
                          FROM IFRS_IMP_JOURNAL_DATA A
                         WHERE last_day(DOWNLOAD_DATE) = v_DOWNLOADDATE
                         )A , IFRS_MASTER_JOURNAL_PARAM B
                         WHERE A.REMARKS =  B.journalcode
                         AND A.GL_ACCOUNT = B.GL_NO
                         GROUP BY a.download_date,
					       a.data_source,
					       A.JOURNAL_ID,
                           A.SEQ,
                           A.BRANCH_CODE,
                           A.REMARKS,
                           A.CURRENCY,
                           A.PRD_TYPE,
                           A.GL_ACCOUNT,
                           B.GL_CONSTNAME,
                           A.TXN_TYPE
                           HAVING SUM (A.AMOUNT) <> 0
                           ORDER BY RN)
                            GROUP BY DOWNLOAD_DATE,
                 DATA_SOURCE,
                 JOURNAL_ID,
                 SEQ,
                 BRANCH_CODE,
                 REMARKS,
                 CURRENCY,
                 GL_CONSTNAME) iaarr,
       ifrs_master_exchange_rate ime1
 WHERE     ime1.download_date = (SELECT MAX (ime2.download_date)
                                   FROM ifrs_master_exchange_rate ime2
                                  WHERE ime2.download_date <= v_DOWNLOADDATE)
       AND iaarr.currency = ime1.currency
       AND last_day(iaarr.download_date) = v_DOWNLOADDATE) JOUR;
COMMIT;
*/

INSERT /*+ PARALLEL(8) */ INTO IFRS_IMP_JOURNAL_DATA_DKP
SELECT /*+ PARALLEL(8) */ DATA_SOURCE,
       JOURNAL_ID,
       SEQ,
       BRANCH_CODE,
       REMARKS,
       DB,
       CR,
       CURRENCY,
       AMOUNT,
       AMOUNT_IDR
  FROM (SELECT *
          FROM (SELECT DATA_SOURCE,
                       JOURNAL_ID,
                       SEQ,
                       BRANCH_CODE,
                       REMARKS,
                       iaarr.CURRENCY,
                       AMOUNT,
                       AMOUNT * ime1.rate_amount AMOUNT_IDR,
                       GL_ACCOUNT,
                       TXN_TYPE,
                       PRD_TYPE
                  FROM (SELECT download_date,
                               DATA_SOURCE,
                               (BRANCH_CODE || 'FRS99') JOURNAL_ID,
                               CASE
                                 WHEN REMARKS = 'BKPI' THEN
                                  CASE
                                    WHEN DATA_SOURCE = 'ILS' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'Y' THEN
                                        '009'
                                       ELSE
                                        '001'
                                     END
                                    WHEN DATA_SOURCE = 'CRD' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'Y' THEN
                                        '009'
                                       ELSE
                                        '002'
                                     END
                                    WHEN DATA_SOURCE = 'KTP' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'Y' THEN
                                        '009'
                                       ELSE
                                        '003'
                                     END
                                    WHEN DATA_SOURCE = 'BTRD' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'Y' THEN
                                        '009'
                                       ELSE
                                        '004'
                                     END
                                    WHEN DATA_SOURCE = 'RKN' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'Y' THEN
                                        '009'
                                       ELSE
                                        '005'
                                     END
                                  END
                                 WHEN REMARKS = 'BKPI2' THEN
                                  CASE
                                    WHEN DATA_SOURCE = 'ILS' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'Y' THEN
                                        '009'
                                       ELSE
                                        '006'
                                     END
                                    WHEN DATA_SOURCE = 'CRD' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'Y' THEN
                                        '009'
                                       ELSE
                                        '007'
                                     END
                                    WHEN DATA_SOURCE = 'BTRD' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'Y' THEN
                                        '009'
                                       ELSE
                                        '008'
                                     END
                                    WHEN DATA_SOURCE = 'LIMIT' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'Y' THEN
                                        '009'
                                       ELSE
                                        '006'
                                     END
                                  END
                                 WHEN REMARKS = 'BKIUW' THEN
                                  CASE
                                    WHEN DATA_SOURCE = 'ILS' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'N' THEN
                                        '101'
                                       ELSE
                                        '102'
                                     END
                                    WHEN DATA_SOURCE = 'KTP' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'N' THEN
                                        '105'
                                       ELSE
                                        '106'
                                     END
                                    WHEN DATA_SOURCE = 'BTRD' THEN
                                     CASE
                                       WHEN REVERSAL_FLAG = 'N' THEN
                                        '107'
                                       ELSE
                                        '108'
                                     END
                                  END
                                 WHEN REMARKS = 'IRBS' AND DATA_SOURCE = 'ILS' THEN
                                  CASE
                                    WHEN REVERSAL_FLAG = 'N' THEN
                                     '103'
                                    ELSE
                                     '104'
                                  END
                               END SEQ,
                               BRANCH_CODE,
                               REMARKS,
                               CURRENCY,
                               SUM(AMOUNT) AMOUNT,
                               GL_ACCOUNT,
                               TXN_TYPE,
                               PRD_TYPE
                          FROM IFRS_IMP_JOURNAL_DATA A
                         WHERE last_day(DOWNLOAD_DATE) = V_CURRDATE
                         GROUP BY DATA_SOURCE,
                                  download_date,
                                  BRANCH_CODE,
                                  REMARKS,
                                  REVERSAL_FLAG,
                                  GL_ACCOUNT,
                                  TXN_TYPE,
                                  CURRENCY,
                                  PRD_TYPE
                        HAVING SUM(AMOUNT) <> 0) iaarr,
                       ifrs_master_exchange_rate ime1
                 where ime1.download_date =
                       (select max(ime2.download_date)
                          from ifrs_master_exchange_rate ime2
                         where last_day(ime2.download_date) <= V_CURRDATE)
                   and iaarr.currency = ime1.currency
                   and last_day(iaarr.download_date) = V_CURRDATE)
PIVOT(MAX(GL_ACCOUNT)
   FOR TXN_TYPE IN('DB' DB,
                   'CR' CR))) JOUR;
COMMIT;



END;