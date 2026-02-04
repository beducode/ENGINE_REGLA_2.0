CREATE OR REPLACE PROCEDURE SP_IFRS_ACCT_RECORE_JRNL_DATA(v_DOWNLOADDATECUR  DATE DEFAULT ('1-JAN-1900'),
                                                          v_DOWNLOADDATEPREV DATE DEFAULT ('1-JAN-1900')) AS
  V_CURRDATE DATE;
  V_PREVDATE DATE;
BEGIN
  /******************************************************************************
  01. DECLARE VARIABLE
  *******************************************************************************/
  SELECT MAX(CURRDATE),
         MAX(PREVDATE)
    INTO V_CURRDATE,
         V_PREVDATE
    FROM IFRS_PRC_DATE_AMORT;

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

  DELETE /*+ PARALLEL(12) */ FROM IFRS_ACCT_JOURNAL_DATA
   WHERE DOWNLOAD_DATE = V_CURRDATE
     AND UPPER(JOURNALCODE) IN ('RECORE');COMMIT;

  INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_DATA
    (DOWNLOAD_DATE,
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
     DRCR,
     GLNO,
     N_AMOUNT,
     N_AMOUNT_IDR,
     SOURCEPROCESS,
     INTMID,
     CREATEDDATE,
     CREATEDBY,
     BRANCH,
     JOURNAL_DESC,
     NOREF,
     VALCTR_CODE,
     GL_INTERNAL_CODE,
     METHOD,
     GL_COSTCENTER)
    SELECT /*+ PARALLEL(12) */ V_CURRDATE,
           IMA.MASTERID,
           IMA.FACILITY_NUMBER,
           IMA.CUSTOMER_NUMBER,
           IMA.ACCOUNT_NUMBER,
           IMA.DATA_SOURCE,
           IMA.PRODUCT_TYPE,
           IMA.PRODUCT_CODE,
           C.TRX_CODE,
           IMA.CURRENCY,
           'RECORE',
           'RECORE',
           'ACT',
           'N',
           C.FLAG_CF,
           CASE
             WHEN C.DRCR = 'DB' THEN
              'D'
             WHEN C.DRCR = 'CR' THEN
              'C'
           END,
           C.GL_NO,
           round(NVL(IMA.RESERVED_AMOUNT_5, 0),2),
           round(NVL(IMA.RESERVED_AMOUNT_5, 0) * NVL(IMA.EXCHANGE_RATE, 1),2),
           'RECORE',
           IMA.PKID,
           SYSTIMESTAMP,
           'SP_IFRS_ACCT_RECORE_JRNL_DATA',
           IMA.BRANCH_CODE,
           C.JOURNAL_DESC,
           'RE_' || IMA.MASTERID ||
           TO_CHAR(V_CURRDATE,
                   'YYYYMMDD'),
           '',
           '',
           'RECORE',
           '000'
      FROM IFRS_MASTER_ACCOUNT       IMA,
           IFRS_MASTER_JOURNAL_PARAM C
     WHERE C.GL_CONSTNAME = IMA.GL_CONSTNAME
       AND UPPER(C.JOURNALCODE) IN ('RECORE')
       AND IMA.DOWNLOAD_DATE = V_CURRDATE
       AND IMA.DATA_SOURCE = 'ILS'
       AND IMA.ACCOUNT_STATUS = 'A'
       AND NVL(IMA.RESERVED_AMOUNT_5, 0) <> 0;
  COMMIT;

  INSERT /*+ PARALLEL(12) */ INTO IFRS_ACCT_JOURNAL_DATA
    (DOWNLOAD_DATE,
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
     DRCR,
     GLNO,
     N_AMOUNT,
     N_AMOUNT_IDR,
     SOURCEPROCESS,
     INTMID,
     CREATEDDATE,
     CREATEDBY,
     BRANCH,
     JOURNAL_DESC,
     NOREF,
     VALCTR_CODE,
     GL_INTERNAL_CODE,
     METHOD,
     GL_COSTCENTER)
    SELECT /*+ PARALLEL(12) */ V_CURRDATE,
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
           'Y',
           FLAG_CF,
           CASE
             WHEN DRCR = 'D' THEN
              'C'
             ELSE
              'D'
           END,
           GLNO,
           N_AMOUNT,
           N_AMOUNT_IDR,
           SOURCEPROCESS,
           INTMID,
           CREATEDDATE,
           CREATEDBY,
           BRANCH,
           JOURNAL_DESC,
           NOREF,
           VALCTR_CODE,
           GL_INTERNAL_CODE,
           METHOD,
           GL_COSTCENTER
      FROM IFRS_ACCT_JOURNAL_DATA B
     WHERE B.DOWNLOAD_DATE = V_PREVDATE
       AND REVERSE = 'N'
       AND UPPER(JOURNALCODE) IN ('RECORE');

  COMMIT;
END;