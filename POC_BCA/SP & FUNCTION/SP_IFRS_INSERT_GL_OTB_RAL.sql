CREATE OR REPLACE PROCEDURE SP_IFRS_INSERT_GL_OTB_RAL(v_DOWNLOADDATECUR  DATE DEFAULT ('1-JAN-1900'),
                                                       v_DOWNLOADDATEPREV DATE DEFAULT ('1-JAN-1900')) AS
  V_CURRDATE DATE;
  V_PREVDATE DATE;
BEGIN
  /******************************************************************************
  01. DECLARE VARIABLE
  *******************************************************************************/

/*
SELECT MAX(CURRDATE),
       MAX(PREVDATE)
  INTO V_CURRDATE,
       V_PREVDATE
  FROM IFRS_PRC_DATE_AMORT;
*/

SELECT MAX (CURRDATE),
       LAST_DAY(ADD_MONTHS(MAX(PREVDATE), -1))
      --MAX (PREVDATE)
 INTO V_CURRDATE, V_PREVDATE
 FROM IFRS_PRC_DATE;


IF NVL(v_DOWNLOADDATECUR,
       '1-JAN-1900') <> '1-JAN-1900'
THEN
  V_CURRDATE := v_DOWNLOADDATECUR;
END IF;

IF NVL(v_DOWNLOADDATEPREV,
       '1-JAN-1900') <> '1-JAN-1900'
THEN
  V_PREVDATE := v_DOWNLOADDATEPREV;
END IF;

EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_IFRS_IMP_JOURNAL_DATA';

INSERT INTO TMP_IFRS_IMP_JOURNAL_DATA
SELECT * FROM IFRS_IMP_JOURNAL_DATA_RAL WHERE last_day(DOWNLOAD_DATE) = V_CURRDATE;

COMMIT;

EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_ACCT_AMORT_RPT_REKON';

INSERT INTO TMP_ACCT_AMORT_RPT_REKON
select * from ifrs_acct_amort_rpt_ral where last_day(download_date) = V_CURRDATE;

COMMIT;

EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_FS_ACCT_AMORT_RPT_REKON';

INSERT INTO TMP_FS_ACCT_AMORT_RPT_REKON
select * from IFRS_FS_ACCT_AMORT_RPT_RAL where last_day(download_date) = V_CURRDATE;

COMMIT;

DELETE IFRS_GL_OUTBOUND_IMP_RAL WHERE last_day(download_date) = V_CURRDATE;

COMMIT;


insert into IFRS_GL_OUTBOUND_IMP_RAL
  (download_date,
   aak_dbid,
   aak_corp,
   aak_jrnlid,
   aak_effdt,
   aak_vlmkey,
   aak_vlmkey_seq,
   aak_vlmkey_filler,
   aak_currcd,
   aak_slid,
   aak_slac,
   aak_source,
   aak_desc,
   aak_ja,
   aak_jt,
   aak_dccd,
   aak_rp_sign,
   aak_amt_rp,
   aak_va_sign,
   aak_amt_va)
SELECT iaarr.DOWNLOAD_DATE,
       AAK_DBID,
       AAK_CORP,
       AAK_JRNLID,
       AAK_EFFDT,
       AAK_VLMKEY,
       AAK_VLMKEY_SEQ,
       AAK_VLMKEY_FILLER,
       AAK_CURRCD,
       AAK_SLID,
       AAK_SLAC,
       AAK_SOURCE,
       AAK_DESC,
       AAK_JA,
       AAK_JT,
       AAK_DCCD,
       AAK_VA_SIGN AAK_RP_SIGN,
       round(AAK_AMT_VA * RATE_AMOUNT,2) AAK_AMT_RP,
       AAK_VA_SIGN,
       round(AAK_AMT_VA,2) AAK_AMT_VA
  FROM (select DOWNLOAD_DATE,
               AAK_DBID,
               AAK_CORP,
               AAK_JRNLID,
               AAK_EFFDT,
               AAK_VLMKEY,
               AAK_VLMKEY_SEQ,
               '                         ' AAK_VLMKEY_FILLER,
               AAK_CURRCD,
               ' ' AAK_SLID,
               ' ' AAK_SLAC,
               ' ' AAK_SOURCE,
               AAK_DESC,
               'CY' AAK_JA,
               'CP' AAK_JT,
               AAK_DCCD,
               AAK_VA_SIGN,
               sum(AAK_AMT_VA) AAK_AMT_VA
          from (SELECT iijd.download_date,
                       'VLJ' AAK_DBID,
                       'BCA' AAK_CORP,
                       (iijd.branch_code || 'FRS99') aak_jrnlid,
                       iijd.DOWNLOAD_DATE -
                       TO_DATE('1900-01-01',
                               'YYYY-MM-DD') + 1 AAK_EFFDT,
                       iijd.branch_code || replace(iijd.gl_account,
                                                   '.',
                                                   '') AAK_VLMKEY,
                       CASE
                         WHEN REMARKS = 'BKPI' THEN
                          CASE
                            WHEN DATA_SOURCE = 'ILS' OR DATA_SOURCE = 'PBMM' THEN
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
                            WHEN DATA_SOURCE = 'ILS' OR DATA_SOURCE = 'PBMM' THEN
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
                            WHEN DATA_SOURCE = 'ILS' OR DATA_SOURCE = 'PBMM' THEN
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
                         WHEN REMARKS = 'IRBS' AND (DATA_SOURCE = 'ILS' OR DATA_SOURCE = 'PBMM') THEN
                          CASE
                            WHEN REVERSAL_FLAG = 'N' THEN
                             '103'
                            ELSE
                             '104'
                          END
                       END AAK_VLMKEY_SEQ,
                       iijd.currency AAK_CURRCD,
                       para.journal_desc AAK_DESC,
                       case
                         when iijd.txn_type = 'CR' THEN
                          'C'
                         ELSE
                          'D'
                       END AAK_DCCD,
                       --CASE WHEN iijd.txn_type = 'CR' THEN '-' ELSE NULL END       AAK_RP_SIGN,
                       --iijd.amount_idr                                             AAK_AMT_RP,
                       CASE
                         WHEN iijd.txn_type = 'CR' THEN
                          '-'
                         ELSE
                          NULL
                       END AAK_VA_SIGN,
                       iijd.amount AAK_AMT_VA
                  FROM TMP_IFRS_IMP_JOURNAL_DATA iijd,
                       IFRS_JOURNAL_PARAM    para
                 WHERE 1 = 1
--                   AND last_day(DOWNLOAD_DATE) = V_CURRDATE
                   AND iijd.gl_account = para.glno
                   AND iijd.journal_desc = para.gl_constname
                   and iijd.REMARKS = para.journalcode
                   and iijd.reversal_flag = 'N')
         group by DOWNLOAD_DATE,
                  AAK_DBID,
                  AAK_CORP,
                  AAK_JRNLID,
                  AAK_EFFDT,
                  AAK_VLMKEY,
                  AAK_VLMKEY_SEQ,
                  AAK_CURRCD,
                  AAK_DESC,
                  AAK_DCCD,
                  AAK_VA_SIGN
        HAVING SUM(AAK_AMT_VA) <> 0
        ) iaarr,
       ifrs_master_exchange_rate ime1
 where ime1.download_date =
       (select max(ime2.download_date)
          from ifrs_master_exchange_rate ime2
         where last_day(ime2.download_date) <= V_CURRDATE)
   and iaarr.AAK_CURRCD = ime1.currency
   and last_day(iaarr.download_date) = V_CURRDATE;

COMMIT;






DELETE IFRS_GL_OUTBOUND_IMP_RAL_R WHERE last_day(download_date) = V_CURRDATE;
COMMIT;


insert into IFRS_GL_OUTBOUND_IMP_RAL_R
  (download_date,
   aak_dbid,
   aak_corp,
   aak_jrnlid,
   aak_effdt,
   aak_vlmkey,
   aak_vlmkey_seq,
   aak_vlmkey_filler,
   aak_currcd,
   aak_slid,
   aak_slac,
   aak_source,
   aak_desc,
   aak_ja,
   aak_jt,
   aak_dccd,
   aak_rp_sign,
   aak_amt_rp,
   aak_va_sign,
   aak_amt_va)
SELECT iaarr.DOWNLOAD_DATE,
       AAK_DBID,
       AAK_CORP,
       AAK_JRNLID,
       AAK_EFFDT,
       AAK_VLMKEY,
       AAK_VLMKEY_SEQ,
       AAK_VLMKEY_FILLER,
       AAK_CURRCD,
       AAK_SLID,
       AAK_SLAC,
       AAK_SOURCE,
       AAK_DESC,
       AAK_JA,
       AAK_JT,
       AAK_DCCD,
       AAK_VA_SIGN AAK_RP_SIGN,
       round(AAK_AMT_VA * RATE_AMOUNT,2) AAK_AMT_RP,
       AAK_VA_SIGN,
       round(AAK_AMT_VA,2) AAK_AMT_VA
  FROM (select DOWNLOAD_DATE,
               AAK_DBID,
               AAK_CORP,
               AAK_JRNLID,
               AAK_EFFDT,
               AAK_VLMKEY,
               AAK_VLMKEY_SEQ,
               '                         ' AAK_VLMKEY_FILLER,
               AAK_CURRCD,
               ' ' AAK_SLID,
               ' ' AAK_SLAC,
               ' ' AAK_SOURCE,
               AAK_DESC,
               'CY' AAK_JA,
               'CP' AAK_JT,
               AAK_DCCD,
               AAK_VA_SIGN,
               sum(AAK_AMT_VA) AAK_AMT_VA
          from (SELECT iijd.download_date,
                       'VLJ' AAK_DBID,
                       'BCA' AAK_CORP,
                       (iijd.branch_code || 'FRS99') aak_jrnlid,
                       iijd.DOWNLOAD_DATE -
                       TO_DATE('1900-01-01',
                               'YYYY-MM-DD') + 1 AAK_EFFDT,
                       iijd.branch_code || replace(iijd.gl_account,
                                                   '.',
                                                   '') AAK_VLMKEY,
                       CASE
                         WHEN REMARKS = 'BKPI' THEN
                          CASE
                            WHEN DATA_SOURCE = 'ILS' OR DATA_SOURCE = 'PBMM' THEN
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
                            WHEN DATA_SOURCE = 'ILS' OR DATA_SOURCE = 'PBMM' THEN
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
                            WHEN DATA_SOURCE = 'ILS' OR DATA_SOURCE = 'PBMM' THEN
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
                         WHEN REMARKS = 'IRBS' AND (DATA_SOURCE = 'ILS' OR DATA_SOURCE = 'PBMM') THEN
                          CASE
                            WHEN REVERSAL_FLAG = 'N' THEN
                             '103'
                            ELSE
                             '104'
                          END
                       END AAK_VLMKEY_SEQ,
                       iijd.currency AAK_CURRCD,
                       para.journal_desc AAK_DESC,
                       case
                         when iijd.txn_type = 'CR' THEN
                          'C'
                         ELSE
                          'D'
                       END AAK_DCCD,
                       --CASE WHEN iijd.txn_type = 'CR' THEN '-' ELSE NULL END       AAK_RP_SIGN,
                       --iijd.amount_idr                                             AAK_AMT_RP,
                       CASE
                         WHEN iijd.txn_type = 'CR' THEN
                          '-'
                         ELSE
                          NULL
                       END AAK_VA_SIGN,
                       iijd.amount AAK_AMT_VA
                  FROM TMP_IFRS_IMP_JOURNAL_DATA iijd,
                       IFRS_JOURNAL_PARAM    para
                 WHERE 1 = 1
--                   AND last_day(DOWNLOAD_DATE) = V_CURRDATE
                   AND iijd.gl_account = para.glno
                   AND iijd.journal_desc = para.gl_constname
                   and iijd.REMARKS = para.journalcode
                   and iijd.reversal_flag = 'Y')
         group by DOWNLOAD_DATE,
                  AAK_DBID,
                  AAK_CORP,
                  AAK_JRNLID,
                  AAK_EFFDT,
                  AAK_VLMKEY,
                  AAK_VLMKEY_SEQ,
                  AAK_CURRCD,
                  AAK_DESC,
                  AAK_DCCD,
                  AAK_VA_SIGN
        HAVING SUM(AAK_AMT_VA) <> 0
        ) iaarr,
       ifrs_master_exchange_rate ime1
 where ime1.download_date =
       (select max(ime2.download_date)
          from ifrs_master_exchange_rate ime2
         where last_day(ime2.download_date) <= V_CURRDATE)
   and iaarr.AAK_CURRCD = ime1.currency
   and last_day(iaarr.download_date) = V_CURRDATE;

COMMIT;










DELETE IFRS_GL_OUTBOUND_AMT_RAL WHERE last_day(download_date) = V_CURRDATE;
COMMIT;


insert into IFRS_GL_OUTBOUND_AMT_RAL
(download_date,
aak_dbid,
aak_corp,
aak_jrnlid,
aak_effdt,
aak_vlmkey,
aak_vlmkey_seq,
aak_vlmkey_filler,
aak_currcd,
aak_slid,
aak_slac,
aak_source,
aak_desc,
aak_ja,
aak_jt,
aak_dccd,
aak_rp_sign,
aak_amt_rp,
aak_va_sign,
aak_amt_va
)
select iaarr.DOWNLOAD_DATE,
       AAK_DBID,
       AAK_CORP,
       AAK_JRNLID,
       AAK_EFFDT,
       AAK_VLMKEY,
       AAK_VLMKEY_SEQ,
       AAK_VLMKEY_FILLER,
       AAK_CURRCD,
       AAK_SLID,
       AAK_SLAC,
       AAK_SOURCE,
       AAK_DESC,
       AAK_JA,
       AAK_JT,
       AAK_DCCD,
       AAK_VA_SIGN AAK_RP_SIGN,
       round(AAK_AMT_VA * ime1.rate_amount, 2) AAK_AMT_RP,
       AAK_VA_SIGN,
       round(AAK_AMT_VA, 2) AAK_AMT_VA
  from (select DOWNLOAD_DATE,
               AAK_DBID,
               AAK_CORP,
               AAK_JRNLID,
               AAK_EFFDT,
               AAK_VLMKEY,
               AAK_VLMKEY_SEQ,
               '                         ' AAK_VLMKEY_FILLER,
               AAK_CURRCD,
               ' ' AAK_SLID,
               ' ' AAK_SLAC,
               ' ' AAK_SOURCE,
               AAK_DESC,
               'CY' AAK_JA,
               'CP' AAK_JT,
               AAK_DCCD,
               AAK_VA_SIGN,
               sum(AAK_AMT_VA) AAK_AMT_VA
          from (select iaarr.download_date download_date,
                       'VLJ' AAK_DBID,
                       'BCA' AAK_CORP,
                       iaarr.journal_id AAK_JRNLID,
                       iaarr.DOWNLOAD_DATE -
                       TO_DATE('1900-01-01',
                               'YYYY-MM-DD') + 1 AAK_EFFDT,
                       iaarr.branch_code || replace(iaarr.glno,
                                                    '.',
                                                    '') AAK_VLMKEY,
                       iaarr.seq AAK_VLMKEY_SEQ,
                       iaarr.currency AAK_CURRCD,
                       para.journal_desc AAK_DESC,
                       iaarr.drcr AAK_DCCD,
                       CASE
                         WHEN iaarr.DRCR = 'C' THEN
                          '-'
                         ELSE
                          NULL
                       END AAK_VA_SIGN,
                       iaarr.amount AAK_AMT_VA
                  from TMP_ACCT_AMORT_RPT_REKON iaarr,
                       IFRS_JOURNAL_PARAM        para
                 where 1 = 1
--                   and last_day(iaarr.download_date) = V_CURRDATE
                   and para.glno = iaarr.glno
                   and iaarr.amount_idr <> 0
                   and iaarr.reverse = 'N'
                   and iaarr.journal_code = para.journalcode)
         group by DOWNLOAD_DATE,
                  AAK_DBID,
                  AAK_CORP,
                  AAK_JRNLID,
                  AAK_EFFDT,
                  AAK_VLMKEY,
                  AAK_VLMKEY_SEQ,
                  AAK_CURRCD,
                  AAK_DESC,
                  AAK_DCCD,
                  AAK_VA_SIGN) iaarr,
       ifrs_master_exchange_rate ime1
 where ime1.download_date =
       (select max(ime2.download_date)
          from ifrs_master_exchange_rate ime2
         where last_day(ime2.download_date) <= V_CURRDATE)
   and iaarr.AAK_CURRCD = ime1.currency
   and last_day(iaarr.download_date) = V_CURRDATE;


 commit;






DELETE IFRS_GL_OUTBOUND_AMT_R_RAL WHERE last_day(download_date) = V_CURRDATE;
COMMIT;


insert into IFRS_GL_OUTBOUND_AMT_R_RAL
(download_date,
aak_dbid,
aak_corp,
aak_jrnlid,
aak_effdt,
aak_vlmkey,
aak_vlmkey_seq,
aak_vlmkey_filler,
aak_currcd,
aak_slid,
aak_slac,
aak_source,
aak_desc,
aak_ja,
aak_jt,
aak_dccd,
aak_rp_sign,
aak_amt_rp,
aak_va_sign,
aak_amt_va
)
select iaarr.DOWNLOAD_DATE,
       AAK_DBID,
       AAK_CORP,
       AAK_JRNLID,
       AAK_EFFDT,
       AAK_VLMKEY,
       AAK_VLMKEY_SEQ,
       AAK_VLMKEY_FILLER,
       AAK_CURRCD,
       AAK_SLID,
       AAK_SLAC,
       AAK_SOURCE,
       AAK_DESC,
       AAK_JA,
       AAK_JT,
       AAK_DCCD,
       AAK_VA_SIGN AAK_RP_SIGN,
       round(AAK_AMT_VA * ime1.rate_amount, 2) AAK_AMT_RP,
       AAK_VA_SIGN,
       round(AAK_AMT_VA, 2) AAK_AMT_VA
  from (select DOWNLOAD_DATE,
               AAK_DBID,
               AAK_CORP,
               AAK_JRNLID,
               AAK_EFFDT,
               AAK_VLMKEY,
               AAK_VLMKEY_SEQ,
               '                         ' AAK_VLMKEY_FILLER,
               AAK_CURRCD,
               ' ' AAK_SLID,
               ' ' AAK_SLAC,
               ' ' AAK_SOURCE,
               AAK_DESC,
               'CY' AAK_JA,
               'CP' AAK_JT,
               AAK_DCCD,
               AAK_VA_SIGN,
               sum(AAK_AMT_VA) AAK_AMT_VA
          from (select iaarr.download_date download_date,
                       'VLJ' AAK_DBID,
                       'BCA' AAK_CORP,
                       iaarr.journal_id AAK_JRNLID,
                       iaarr.DOWNLOAD_DATE -
                       TO_DATE('1900-01-01',
                               'YYYY-MM-DD') + 1 AAK_EFFDT,
                       iaarr.branch_code || replace(iaarr.glno,
                                                    '.',
                                                    '') AAK_VLMKEY,
                       iaarr.seq AAK_VLMKEY_SEQ,
                       iaarr.currency AAK_CURRCD,
                       para.journal_desc AAK_DESC,
                       iaarr.drcr AAK_DCCD,
                       CASE
                         WHEN iaarr.DRCR = 'C' THEN
                          '-'
                         ELSE
                          NULL
                       END AAK_VA_SIGN,
                       iaarr.amount AAK_AMT_VA
                  from TMP_ACCT_AMORT_RPT_REKON iaarr,
                       IFRS_JOURNAL_PARAM        para
                 where 1 = 1
--                   and last_day(iaarr.download_date) = V_CURRDATE
                   and para.glno = iaarr.glno
                   and iaarr.amount_idr <> 0
                   and iaarr.reverse = 'Y'
                   and iaarr.journal_code = para.journalcode)
         group by DOWNLOAD_DATE,
                  AAK_DBID,
                  AAK_CORP,
                  AAK_JRNLID,
                  AAK_EFFDT,
                  AAK_VLMKEY,
                  AAK_VLMKEY_SEQ,
                  AAK_CURRCD,
                  AAK_DESC,
                  AAK_DCCD,
                  AAK_VA_SIGN) iaarr,
       ifrs_master_exchange_rate ime1
 where ime1.download_date =
       (select max(ime2.download_date)
          from ifrs_master_exchange_rate ime2
         where last_day(ime2.download_date) <= V_CURRDATE)
   and iaarr.AAK_CURRCD = ime1.currency
   and last_day(iaarr.download_date) = V_CURRDATE;

 commit;

--RAL : delete current bentuk FS Amort if any (BCA 20210730)
DELETE IFRS_GL_OUTBOUND_FS_AMT_RAL WHERE last_day(download_date) = V_CURRDATE;
COMMIT;

--RAL : bentuk FS Amort (BCA 20210730)
INSERT INTO IFRS_GL_OUTBOUND_FS_AMT_RAL (download_date,
                                     aak_dbid,
                                     aak_corp,
                                     aak_jrnlid,
                                     aak_effdt,
                                     aak_vlmkey,
                                     aak_vlmkey_seq,
                                     aak_vlmkey_filler,
                                     aak_currcd,
                                     aak_slid,
                                     aak_slac,
                                     aak_source,
                                     aak_desc,
                                     aak_ja,
                                     aak_jt,
                                     aak_dccd,
                                     aak_rp_sign,
                                     aak_amt_rp,
                                     aak_va_sign,
                                     aak_amt_va)
   SELECT iaarr.DOWNLOAD_DATE,
          AAK_DBID,
          AAK_CORP,
          AAK_JRNLID,
          AAK_EFFDT,
          AAK_VLMKEY,
          AAK_VLMKEY_SEQ,
          AAK_VLMKEY_FILLER,
          AAK_CURRCD,
          AAK_SLID,
          AAK_SLAC,
          AAK_SOURCE,
          AAK_DESC,
          AAK_JA,
          AAK_JT,
          AAK_DCCD,
          AAK_VA_SIGN                              AAK_RP_SIGN,
          ROUND (ROUND (AAK_AMT_VA, 2)  * ime1.rate_amount, 2) AAK_AMT_RP,
          AAK_VA_SIGN,
          ROUND (AAK_AMT_VA, 2)                    AAK_AMT_VA
     FROM (  SELECT DOWNLOAD_DATE,
                    AAK_DBID,
                    AAK_CORP,
                    AAK_JRNLID,
                    AAK_EFFDT,
                    AAK_VLMKEY,
                    AAK_VLMKEY_SEQ,
                    '                         ' AAK_VLMKEY_FILLER,
                    AAK_CURRCD,
                    ' '                       AAK_SLID,
                    ' '                       AAK_SLAC,
                    ' '                       AAK_SOURCE,
                    AAK_DESC,
                    'CY'                      AAK_JA,
                    'CP'                      AAK_JT,
                    AAK_DCCD,
                    AAK_VA_SIGN,
                    SUM (AAK_AMT_VA)          AAK_AMT_VA
               FROM (SELECT iaarr.download_date download_date,
                            'VLJ'             AAK_DBID,
                            'BCA'             AAK_CORP,
                            iaarr.journal_id  AAK_JRNLID,
                              iaarr.DOWNLOAD_DATE
                            - TO_DATE ('1900-01-01', 'YYYY-MM-DD')
                            + 1
                               AAK_EFFDT,
                            iaarr.branch_code || REPLACE (iaarr.glno, '.', '')
                               AAK_VLMKEY,
                            iaarr.seq         AAK_VLMKEY_SEQ,
                            iaarr.currency    AAK_CURRCD,
                            para.journal_desc AAK_DESC,
                            iaarr.drcr        AAK_DCCD,
                            CASE WHEN iaarr.DRCR = 'C' THEN '-' ELSE NULL END
                               AAK_VA_SIGN,
                            iaarr.amount_ccy  AAK_AMT_VA
                       FROM TMP_FS_ACCT_AMORT_RPT_REKON iaarr,
                            IFRS_JOURNAL_PARAM         para
                      WHERE     1 = 1
--                            AND LAST_DAY (iaarr.download_date) = V_CURRDATE
                            AND para.glno = iaarr.glno
                            AND iaarr.amount_ccy <> 0
                            AND iaarr.reverse = 'N'
                            AND iaarr.journal_code = para.journalcode)
           GROUP BY DOWNLOAD_DATE,
                    AAK_DBID,
                    AAK_CORP,
                    AAK_JRNLID,
                    AAK_EFFDT,
                    AAK_VLMKEY,
                    AAK_VLMKEY_SEQ,
                    AAK_CURRCD,
                    AAK_DESC,
                    AAK_DCCD,
                    AAK_VA_SIGN) iaarr,
          ifrs_master_exchange_rate ime1
    WHERE     ime1.download_date =
                 (SELECT MAX (ime2.download_date)
                    FROM ifrs_master_exchange_rate ime2
                   WHERE LAST_DAY (ime2.download_date) <= V_CURRDATE)
          AND iaarr.AAK_CURRCD = ime1.currency
          AND LAST_DAY (iaarr.download_date) = V_CURRDATE;

COMMIT;

--RAL : delete current reverse FS Amort if any (BCA 20210730)
DELETE IFRS_GL_OUTBOUND_FS_AMTR_RAL WHERE last_day(download_date) = V_CURRDATE;
COMMIT;

--RAL : reverse FS Amort (BCA 20210730)
INSERT INTO IFRS_GL_OUTBOUND_FS_AMTR_RAL (download_date,
                                       aak_dbid,
                                       aak_corp,
                                       aak_jrnlid,
                                       aak_effdt,
                                       aak_vlmkey,
                                       aak_vlmkey_seq,
                                       aak_vlmkey_filler,
                                       aak_currcd,
                                       aak_slid,
                                       aak_slac,
                                       aak_source,
                                       aak_desc,
                                       aak_ja,
                                       aak_jt,
                                       aak_dccd,
                                       aak_rp_sign,
                                       aak_amt_rp,
                                       aak_va_sign,
                                       aak_amt_va)
   SELECT iaarr.DOWNLOAD_DATE,
          AAK_DBID,
          AAK_CORP,
          AAK_JRNLID,
          AAK_EFFDT,
          AAK_VLMKEY,
          AAK_VLMKEY_SEQ,
          AAK_VLMKEY_FILLER,
          AAK_CURRCD,
          AAK_SLID,
          AAK_SLAC,
          AAK_SOURCE,
          AAK_DESC,
          AAK_JA,
          AAK_JT,
          AAK_DCCD,
          AAK_VA_SIGN                              AAK_RP_SIGN,
          ROUND (ROUND (AAK_AMT_VA, 2) * ime1.rate_amount, 2) AAK_AMT_RP,
          AAK_VA_SIGN,
          ROUND (AAK_AMT_VA, 2)                    AAK_AMT_VA
     FROM (  SELECT DOWNLOAD_DATE,
                    AAK_DBID,
                    AAK_CORP,
                    AAK_JRNLID,
                    AAK_EFFDT,
                    AAK_VLMKEY,
                    AAK_VLMKEY_SEQ,
                    '                         ' AAK_VLMKEY_FILLER,
                    AAK_CURRCD,
                    ' '                       AAK_SLID,
                    ' '                       AAK_SLAC,
                    ' '                       AAK_SOURCE,
                    AAK_DESC,
                    'CY'                      AAK_JA,
                    'CP'                      AAK_JT,
                    AAK_DCCD,
                    AAK_VA_SIGN,
                    SUM (AAK_AMT_VA)          AAK_AMT_VA
               FROM (SELECT iaarr.download_date download_date,
                            'VLJ'             AAK_DBID,
                            'BCA'             AAK_CORP,
                            iaarr.journal_id  AAK_JRNLID,
                              iaarr.DOWNLOAD_DATE
                            - TO_DATE ('1900-01-01', 'YYYY-MM-DD')
                            + 1
                               AAK_EFFDT,
                            iaarr.branch_code || REPLACE (iaarr.glno, '.', '')
                               AAK_VLMKEY,
                            iaarr.seq         AAK_VLMKEY_SEQ,
                            iaarr.currency    AAK_CURRCD,
                            para.journal_desc AAK_DESC,
                            iaarr.drcr        AAK_DCCD,
                            CASE WHEN iaarr.DRCR = 'C' THEN '-' ELSE NULL END
                               AAK_VA_SIGN,
                            iaarr.amount_ccy  AAK_AMT_VA
                       FROM TMP_FS_ACCT_AMORT_RPT_REKON iaarr,
                            IFRS_JOURNAL_PARAM         para
                      WHERE     1 = 1
--                            AND LAST_DAY (iaarr.download_date) = V_CURRDATE
                            AND para.glno = iaarr.glno
                            AND iaarr.amount_ccy <> 0
                            AND iaarr.reverse = 'Y'
                            AND iaarr.journal_code = para.journalcode)
           GROUP BY DOWNLOAD_DATE,
                    AAK_DBID,
                    AAK_CORP,
                    AAK_JRNLID,
                    AAK_EFFDT,
                    AAK_VLMKEY,
                    AAK_VLMKEY_SEQ,
                    AAK_CURRCD,
                    AAK_DESC,
                    AAK_DCCD,
                    AAK_VA_SIGN) iaarr,
          ifrs_master_exchange_rate ime1
    WHERE     ime1.download_date =
                 (SELECT MAX (ime2.download_date)
                    FROM ifrs_master_exchange_rate ime2
                   WHERE LAST_DAY (ime2.download_date) <= V_CURRDATE)
          AND iaarr.AAK_CURRCD = ime1.currency
          AND LAST_DAY (iaarr.download_date) = V_CURRDATE;

COMMIT;

END;