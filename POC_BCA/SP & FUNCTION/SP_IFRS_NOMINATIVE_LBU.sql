CREATE OR REPLACE PROCEDURE SP_IFRS_NOMINATIVE_LBU (v_DOWNLOADDATECUR  DATE DEFAULT ('1-JAN-1900'),v_DOWNLOADDATEPREV  DATE DEFAULT ('1-JAN-1900'))
AS

           V_CURRDATE DATE;

BEGIN

EXECUTE IMMEDIATE 'alter session enable parallel dml';


    IF v_DOWNLOADDATECUR = '1-JAN-1900'
    THEN
        SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;
    ELSE
        V_CURRDATE := v_DOWNLOADDATECUR;
    END IF;

DELETE /*+ PARALLEL(8) */ IFRS_NOMINATIVE_LBU WHERE REPORT_DATE = V_CURRDATE;COMMIT;

EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_IFRS_NOMINATIVE';

INSERT /*+ PARALLEL(8) */ INTO GTMP_IFRS_NOMINATIVE
SELECT /*+ PARALLEL(8) */ * FROM IFRS_NOMINATIVE WHERE REPORT_DATE = V_CURRDATE;COMMIT;

EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_IMA_LBU';

INSERT /*+ PARALLEL(8) */ INTO TMP_IMA_LBU
SELECT /*+ PARALLEL(8) */ MASTERID, IS_IMPAIRED,SPPI_RESULT,ACCOUNT_STATUS, ECL_AMOUNT FROM  IFRS_MASTER_ACCOUNT WHERE DOWNLOAD_DATE = V_CURRDATE;COMMIT;

INSERT /*+ PARALLEL(8) */ INTO IFRS_NOMINATIVE_LBU
(
PKID                        ,
REPORT_DATE                 ,
APLIKASI                    ,
NOREK_LBU                   ,
BRANCH                      ,
LBU_FORM                    ,
MU                          ,
STAGE                       ,
BS_FLAG                     ,
PORTFOLIO                   ,
INSTRUMENT                  ,
UNUSED_CCY                  ,
UNUSED_LCL                  ,
EAD_OFF_BS_CCF_CCY          ,
EAD_OFF_BS_CCF_LCL          ,
OUTSTANDING_ON_BS_CCY       ,
OUTSTANDING_ON_BS_LCL       ,
OUTSTANDING_OFF_BS_CCY      ,
OUTSTANDING_OFF_BS_LCL      ,
UNAMORT_FEE_CCY             ,
UNAMORT_FEE_LCL             ,
INTEREST_ACCRUED_CCY        ,
INTEREST_ACCRUED_LCL        ,
UNWINDING_INTEREST_CCY      ,
UNWINDING_INTEREST_LCL      ,
AMORTISASI_FEE_CCY          ,
AMORTISASI_FEE_LCL          ,
CADANGAN_KOLEKTIF_CCY       ,
CADANGAN_KOLEKTIF_LCL       ,
CADANGAN_INDIVIDUAL_CCY     ,
CADANGAN_INDIVIDUAL_LCL     ,
CADANGAN_KOLEKTIF_COM_CCY   ,
CADANGAN_KOLEKTIF_COM_LCL   ,
CADANGAN_INDIV_COM_CCY ,
CADANGAN_INDIV_COM_LCL ,
AMORT_FEE_AMT_ILS_CCY       ,
AMORT_FEE_AMT_ILS_LCL
)
SELECT /*+ PARALLEL(8) */
0                                                                   AS PKID,
REPORT_DATE                                                         AS REPORT_DATE,
CASE
    WHEN DATA_SOURCE = 'KTP'
    THEN 'OPI'
    WHEN LENGTH(NOREK_LBU) = 12 AND DATA_SOURCE = 'RKN'
    THEN 'GLM'
    WHEN LENGTH(NOREK_LBU) <> 12 AND DATA_SOURCE = 'RKN'
    THEN 'NOS'
    WHEN DATA_SOURCE = 'PBMM'
    THEN 'ILS'
    WHEN DATA_SOURCE = 'BTRD'
    THEN 'BTR'
    ELSE DATA_SOURCE
END                                                                 AS APLIKASI,
NOREK_LBU                                                           AS NOREK_LBU,
BRANCH_CODE                                                         AS BRANCH,
CASE WHEN LBU_foRm IS NOT NULL THEN 'LB'||LPAD('000',2-LENGTH(LBU_foRm))||LBU_foRm
ELSE NULL END                                                       AS LBU_FORM,
CURRENCY                                                            AS MU,
STAGE                                                               AS STAGE,
BS_FLAG                                                             AS BS_FLAG,
PORTFOLIO                                                           AS PORTFOLIO,
INSTRUMENT                                                          AS INSTRUMENT,
CASE
    WHEN DATA_SOURCE = 'ILS'
    THEN NVL(AVAILABLE_BI_AMT_CCY,0)
    WHEN DATA_SOURCE = 'CRD'
    THEN NVL(UNUSED_AMT_CCY,0)
    ELSE 0
END                                                                 AS UNUSED_CCY,
CASE
    WHEN DATA_SOURCE = 'ILS'
    THEN NVL(AVAILABLE_BI_AMT_LCL,0)
    WHEN DATA_SOURCE = 'CRD'
    THEN NVL(UNUSED_AMT_LCL,0)
    ELSE 0
END                                                                 AS UNUSED_LCL,
SUM(NVL(CCF_AMOUNT_CCY,0))                                          AS EAD_OFF_BS_CCF_CCY,
SUM(NVL(CCF_AMOUNT_LCL,0))                                          AS EAD_OFF_BS_CCF_LCL,
SUM(NVL(OUTSTANDING_ON_BS_CCY,0))                                   AS OUTSTANDING_ON_BS_CCY,
SUM(NVL(OUTSTANDING_ON_BS_LCL,0))                                   AS OUTSTANDING_ON_BS_LCL,
SUM(NVL(OUTSTANDING_OFF_BS_CCY,0))                                  AS OUTSTANDING_OFF_BS,
SUM(NVL(OUTSTANDING_OFF_BS_LCL,0))                                  AS OUTSTANDING_OFF_BS_LCL,
SUM(NVL(UNAMORT_FEE_AMT_CCY,0))                                     AS UNAMORT_FEE_CCY,
SUM(NVL(UNAMORT_FEE_AMT_LCL,0))                                     AS UNAMORT_FEE_LCL,
CASE WHEN DATA_SOURCE = 'PBMM'
     THEN SUM(NVL(INTEREST_RECEIVABLE_CCY,0)) ELSE  SUM(NVL(SALDO_YADIT_CCY,0))
END                                                                 AS INTEREST_ACCRUED_CCY,
CASE WHEN DATA_SOURCE = 'PBMM'
     THEN SUM(NVL(INTEREST_RECEIVABLE_LCL,0)) ELSE  SUM(NVL(SALDO_YADIT_LCL,0))
END                                                                  AS INTEREST_ACCRUED_LCL,
SUM(NVL(IA_UNWINDING_INTEREST_CCY,0))                               AS UNWINDING_INTEREST_CCY,
SUM(NVL(IA_UNWINDING_INTEREST_LCL,0))                               AS UNWINDING_INTEREST_LCL,
SUM(NVL(AMORT_FEE_CCY,0))                                           AS AMORTISASI_FEE_CCY,
SUM(NVL(AMORT_FEE_LCL,0))                                           AS AMORTISASI_FEE_LCL,
CASE
    WHEN STAGE = 1
    THEN SUM(NVL(RESERVED_AMOUNT_2,0))
    ELSE 0
END                                                                 AS CADANGAN_KOLEKTIF_CCY,
CASE
    WHEN STAGE = 1
    THEN SUM(NVL(RESERVED_AMOUNT_3,0))
    ELSE 0
END                                                                 AS CADANGAN_KOLEKTIF_LCL,
CASE
    WHEN STAGE <> 1
    THEN SUM(NVL(RESERVED_AMOUNT_2,0))
    ELSE 0
END                                                                 AS CADANGAN_INDIVIDUAL_CCY,
CASE
    WHEN STAGE <> 1
    THEN SUM(NVL(RESERVED_AMOUNT_3,0))
    ELSE 0
END                                                                 AS CADANGAN_INDIVIDUAL_LCL,
CASE
    WHEN STAGE = 1
    THEN SUM(NVL(ECL_OFF_BS_CCY,0))
    ELSE 0
END                                                                 AS CADANGAN_KOLEKTIF_COM_CCY,
CASE
    WHEN STAGE = 1
    THEN SUM(NVL(ECL_OFF_BS_LCL,0))
    ELSE 0
END                                                                 AS CADANGAN_KOLEKTIF_COM_LCL,
CASE
    WHEN STAGE <> 1
    THEN SUM(NVL(ECL_OFF_BS_CCY,0))
    ELSE 0
END                                                                 AS CADANGAN_INDIV_COM_CCY,
CASE
    WHEN STAGE <> 1
    THEN SUM(NVL(ECL_OFF_BS_LCL,0))
    ELSE 0
END                                                                 AS CADANGAN_INDIV_COM_LCL,
SUM(NVL(AMORT_FEE_AMT_ILS_CCY,0))                                   AS AMORT_FEE_AMT_ILS_CCY,
SUM(NVL(AMORT_FEE_AMT_ILS_LCL,0))                                   AS AMORT_FEE_AMT_ILS_LCL
FROM GTMP_IFRS_NOMINATIVE A
JOIN TMP_IMA_LBU B
ON (A.MASTERID = B.MASTERID)
WHERE 1 = 1
--AND REPORT_DATE = V_CURRDATE
AND ((IFRS9_CLASS IN ('AMORT','FVTOCI') AND B.SPPI_RESULT IS NOT NULL AND DATA_SOURCE = 'ILS') OR DATA_SOURCE IN ('PBMM','RKN','BTRD','KTP') AND IS_IMPAIRED = 1)
AND A.ACCOUNT_STATUS = 'A'
GROUP BY
REPORT_DATE,
DATA_SOURCE,
NOREK_LBU,
BRANCH_CODE,
LBU_foRm,
CURRENCY,
STAGE,
BS_FLAG,
PORTFOLIO,
INSTRUMENT,
AVAILABLE_BI_AMT_CCY,
UNUSED_AMT_CCY,
AVAILABLE_BI_AMT_LCL,
UNUSED_AMT_LCL;COMMIT;

INSERT /*+ PARALLEL(8) */ INTO IFRS_NOMINATIVE_LBU
(
PKID                        ,
REPORT_DATE                 ,
APLIKASI                    ,
NOREK_LBU                   ,
BRANCH                      ,
LBU_FORM                    ,
MU                          ,
STAGE                       ,
BS_FLAG                     ,
PORTFOLIO                   ,
INSTRUMENT                  ,
UNUSED_CCY                  ,
UNUSED_LCL                  ,
EAD_OFF_BS_CCF_CCY          ,
EAD_OFF_BS_CCF_LCL          ,
OUTSTANDING_ON_BS_CCY       ,
OUTSTANDING_ON_BS_LCL       ,
OUTSTANDING_OFF_BS_CCY      ,
OUTSTANDING_OFF_BS_LCL      ,
UNAMORT_FEE_CCY             ,
UNAMORT_FEE_LCL             ,
INTEREST_ACCRUED_CCY        ,
INTEREST_ACCRUED_LCL        ,
UNWINDING_INTEREST_CCY      ,
UNWINDING_INTEREST_LCL      ,
AMORTISASI_FEE_CCY          ,
AMORTISASI_FEE_LCL          ,
CADANGAN_KOLEKTIF_CCY       ,
CADANGAN_KOLEKTIF_LCL       ,
CADANGAN_INDIVIDUAL_CCY     ,
CADANGAN_INDIVIDUAL_LCL     ,
CADANGAN_KOLEKTIF_COM_CCY   ,
CADANGAN_KOLEKTIF_COM_LCL   ,
CADANGAN_INDIV_COM_CCY ,
CADANGAN_INDIV_COM_LCL ,
AMORT_FEE_AMT_ILS_CCY       ,
AMORT_FEE_AMT_ILS_LCL
)
SELECT /*+ PARALLEL(8) */
0                                                                   AS PKID,
REPORT_DATE                                                         AS REPORT_DATE,
CASE
    WHEN DATA_SOURCE = 'KTP'
    THEN 'OPI'
    WHEN LENGTH(NOREK_LBU) = 12 AND DATA_SOURCE = 'RKN'
    THEN 'GLM'
    WHEN LENGTH(NOREK_LBU) <> 12 AND DATA_SOURCE = 'RKN'
    THEN 'NOS'
    WHEN DATA_SOURCE = 'BTRD'
    THEN 'BTR'
    ELSE DATA_SOURCE
END                                                                 AS APLIKASI,
NOREK_LBU                                                           AS NOREK_LBU,
BRANCH_CODE                                                         AS BRANCH,
CASE WHEN LBU_foRm IS NOT NULL THEN 'LB'||LPAD('000',2-LENGTH(LBU_foRm))||LBU_foRm
ELSE NULL END                                                       AS LBU_FORM,
CURRENCY                                                            AS MU,
STAGE                                                               AS STAGE,
BS_FLAG                                                             AS BS_FLAG,
PORTFOLIO                                                           AS PORTFOLIO,
INSTRUMENT                                                          AS INSTRUMENT,
CASE
    WHEN DATA_SOURCE = 'ILS'
    THEN NVL(AVAILABLE_BI_AMT_CCY,0)
    WHEN DATA_SOURCE = 'CRD'
    THEN NVL(UNUSED_AMT_CCY,0)
    ELSE 0
END                                                                 AS UNUSED_CCY,
CASE
    WHEN DATA_SOURCE = 'ILS'
    THEN NVL(AVAILABLE_BI_AMT_LCL,0)
    WHEN DATA_SOURCE = 'CRD'
    THEN NVL(UNUSED_AMT_LCL,0)
    ELSE 0
END                                                                 AS UNUSED_LCL,
SUM(NVL(CCF_AMOUNT_CCY,0))                                          AS EAD_OFF_BS_CCF_CCY,
SUM(NVL(CCF_AMOUNT_LCL,0))                                          AS EAD_OFF_BS_CCF_LCL,
SUM(NVL(OUTSTANDING_ON_BS_CCY,0))                                   AS OUTSTANDING_ON_BS_CCY,
SUM(NVL(OUTSTANDING_ON_BS_LCL,0))                                   AS OUTSTANDING_ON_BS_LCL,
SUM(NVL(OUTSTANDING_OFF_BS_CCY,0))                                  AS OUTSTANDING_OFF_BS,
SUM(NVL(OUTSTANDING_OFF_BS_LCL,0))                                  AS OUTSTANDING_OFF_BS_LCL,
SUM(NVL(UNAMORT_FEE_AMT_CCY,0))                                     AS UNAMORT_FEE_CCY,
SUM(NVL(UNAMORT_FEE_AMT_LCL,0))                                     AS UNAMORT_FEE_LCL,
SUM(NVL(SALDO_YADIT_CCY,0))                                         AS INTEREST_ACCRUED_CCY,
SUM(NVL(SALDO_YADIT_LCL,0))                                         AS INTEREST_ACCRUED_LCL,
SUM(NVL(IA_UNWINDING_INTEREST_CCY,0))                               AS UNWINDING_INTEREST_CCY,
SUM(NVL(IA_UNWINDING_INTEREST_LCL,0))                               AS UNWINDING_INTEREST_LCL,
SUM(NVL(AMORT_FEE_CCY,0))                                           AS AMORTISASI_FEE_CCY,
SUM(NVL(AMORT_FEE_LCL,0))                                           AS AMORTISASI_FEE_LCL,
CASE
    WHEN STAGE = 1
    THEN SUM(NVL(RESERVED_AMOUNT_2,0))
    ELSE 0
END                                                                 AS CADANGAN_KOLEKTIF_CCY,
CASE
    WHEN STAGE = 1
    THEN SUM(NVL(RESERVED_AMOUNT_3,0))
    ELSE 0
END                                                                 AS CADANGAN_KOLEKTIF_LCL,
CASE
    WHEN STAGE <> 1
    THEN SUM(NVL(RESERVED_AMOUNT_2,0))
    ELSE 0
END                                                                 AS CADANGAN_INDIVIDUAL_CCY,
CASE
    WHEN STAGE <> 1
    THEN SUM(NVL(RESERVED_AMOUNT_3,0))
    ELSE 0
END                                                                 AS CADANGAN_INDIVIDUAL_LCL,
CASE
    WHEN STAGE = 1
    THEN SUM(NVL(ECL_OFF_BS_CCY,0))
    ELSE 0
END                                                                 AS CADANGAN_KOLEKTIF_COM_CCY,
CASE
    WHEN STAGE = 1
    THEN SUM(NVL(ECL_OFF_BS_LCL,0))
    ELSE 0
END                                                                 AS CADANGAN_KOLEKTIF_COM_LCL,
CASE
    WHEN STAGE <> 1
    THEN SUM(NVL(ECL_OFF_BS_CCY,0))
    ELSE 0
END                                                                 AS CADANGAN_INDIV_COM_CCY,
CASE
    WHEN STAGE <> 1
    THEN SUM(NVL(ECL_OFF_BS_LCL,0))
    ELSE 0
END                                                                 AS CADANGAN_INDIV_COM_LCL,
SUM(NVL(AMORT_FEE_AMT_ILS_CCY,0))                                   AS AMORT_FEE_AMT_ILS_CCY,
SUM(NVL(AMORT_FEE_AMT_ILS_LCL,0))                                   AS AMORT_FEE_AMT_ILS_LCL
FROM GTMP_IFRS_NOMINATIVE A
JOIN TMP_IMA_LBU B
ON (A.MASTERID = B.MASTERID)
WHERE 1 = 1
--AND REPORT_DATE = V_CURRDATE
AND DATA_SOURCE = 'LIMIT' AND IS_IMPAIRED = 1 AND ECL_AMOUNT > 0
AND A.ACCOUNT_STATUS = 'A'
GROUP BY
REPORT_DATE,
DATA_SOURCE,
NOREK_LBU,
BRANCH_CODE,
LBU_foRm,
CURRENCY,
STAGE,
BS_FLAG,
PORTFOLIO,
INSTRUMENT,
AVAILABLE_BI_AMT_CCY,
UNUSED_AMT_CCY,
AVAILABLE_BI_AMT_LCL,
UNUSED_AMT_LCL;COMMIT;


INSERT /*+ PARALLEL(8) */ INTO IFRS_NOMINATIVE_LBU
(
PKID                        ,
REPORT_DATE                 ,
APLIKASI                    ,
NOREK_LBU                   ,
BRANCH                      ,
LBU_FORM                    ,
MU                          ,
STAGE                       ,
BS_FLAG                     ,
PORTFOLIO                   ,
INSTRUMENT                  ,
UNUSED_CCY                  ,
UNUSED_LCL                  ,
EAD_OFF_BS_CCF_CCY          ,
EAD_OFF_BS_CCF_LCL          ,
OUTSTANDING_ON_BS_CCY       ,
OUTSTANDING_ON_BS_LCL       ,
OUTSTANDING_OFF_BS_CCY      ,
OUTSTANDING_OFF_BS_LCL      ,
UNAMORT_FEE_CCY             ,
UNAMORT_FEE_LCL             ,
INTEREST_ACCRUED_CCY        ,
INTEREST_ACCRUED_LCL        ,
UNWINDING_INTEREST_CCY      ,
UNWINDING_INTEREST_LCL      ,
AMORTISASI_FEE_CCY          ,
AMORTISASI_FEE_LCL          ,
CADANGAN_KOLEKTIF_CCY       ,
CADANGAN_KOLEKTIF_LCL       ,
CADANGAN_INDIVIDUAL_CCY     ,
CADANGAN_INDIVIDUAL_LCL     ,
CADANGAN_KOLEKTIF_COM_CCY   ,
CADANGAN_KOLEKTIF_COM_LCL   ,
CADANGAN_INDIV_COM_CCY ,
CADANGAN_INDIV_COM_LCL ,
AMORT_FEE_AMT_ILS_CCY       ,
AMORT_FEE_AMT_ILS_LCL
)
SELECT /*+ PARALLEL(8) */
0                                                                   AS PKID,
REPORT_DATE                                                         AS REPORT_DATE,
CASE
    WHEN DATA_SOURCE = 'KTP'
    THEN 'OPI'
    WHEN LENGTH(NOREK_LBU) = 12 AND DATA_SOURCE = 'RKN'
    THEN 'GLM'
    WHEN LENGTH(NOREK_LBU) <> 12 AND DATA_SOURCE = 'RKN'
    THEN 'NOS'
    WHEN DATA_SOURCE = 'BTRD'
    THEN 'BTR'
    ELSE DATA_SOURCE
END                                                                 AS APLIKASI,
NOREK_LBU                                                           AS NOREK_LBU,
BRANCH_CODE                                                         AS BRANCH,
CASE WHEN LBU_foRm IS NOT NULL THEN 'LB'||LPAD('000',2-LENGTH(LBU_foRm))||LBU_foRm
ELSE NULL END                                                       AS LBU_FORM,
CURRENCY                                                            AS MU,
STAGE                                                               AS STAGE,
BS_FLAG                                                             AS BS_FLAG,
PORTFOLIO                                                           AS PORTFOLIO,
INSTRUMENT                                                          AS INSTRUMENT,
CASE
    WHEN DATA_SOURCE = 'ILS'
    THEN SUM(NVL(AVAILABLE_BI_AMT_CCY,0))
    WHEN DATA_SOURCE = 'CRD'
    THEN SUM(NVL(UNUSED_AMT_CCY,0))
    ELSE 0
END                                                                 AS UNUSED_CCY,
CASE
    WHEN DATA_SOURCE = 'ILS'
    THEN SUM(NVL(AVAILABLE_BI_AMT_LCL,0))
    WHEN DATA_SOURCE = 'CRD'
    THEN SUM(NVL(UNUSED_AMT_LCL,0))
    ELSE 0
END                                                                 AS UNUSED_LCL,
SUM(NVL(CCF_AMOUNT_CCY,0))                                          AS EAD_OFF_BS_CCF_CCY,
SUM(NVL(CCF_AMOUNT_LCL,0))                                          AS EAD_OFF_BS_CCF_LCL,
SUM(NVL(OUTSTANDING_ON_BS_CCY,0))                                   AS OUTSTANDING_ON_BS_CCY,
SUM(NVL(OUTSTANDING_ON_BS_LCL,0))                                   AS OUTSTANDING_ON_BS_LCL,
SUM(NVL(OUTSTANDING_OFF_BS_CCY,0))                                  AS OUTSTANDING_OFF_BS,
SUM(NVL(OUTSTANDING_OFF_BS_LCL,0))                                  AS OUTSTANDING_OFF_BS_LCL,
SUM(NVL(UNAMORT_FEE_AMT_CCY,0))                                     AS UNAMORT_FEE_CCY,
SUM(NVL(UNAMORT_FEE_AMT_LCL,0))                                     AS UNAMORT_FEE_LCL,
SUM(NVL(SALDO_YADIT_CCY,0))                                         AS INTEREST_ACCRUED_CCY,
SUM(NVL(SALDO_YADIT_LCL,0))                                         AS INTEREST_ACCRUED_LCL,
SUM(NVL(IA_UNWINDING_INTEREST_CCY,0))                               AS UNWINDING_INTEREST_CCY,
SUM(NVL(IA_UNWINDING_INTEREST_LCL,0))                               AS UNWINDING_INTEREST_LCL,
SUM(NVL(AMORT_FEE_CCY,0))                                           AS AMORTISASI_FEE_CCY,
SUM(NVL(AMORT_FEE_LCL,0))                                           AS AMORTISASI_FEE_LCL,
CASE
    WHEN STAGE = 1
    THEN SUM(NVL(RESERVED_AMOUNT_2,0))
    ELSE 0
END                                                                 AS CADANGAN_KOLEKTIF_CCY,
CASE
    WHEN STAGE = 1
    THEN SUM(NVL(RESERVED_AMOUNT_3,0))
    ELSE 0
END                                                                 AS CADANGAN_KOLEKTIF_LCL,
CASE
    WHEN STAGE <> 1
    THEN SUM(NVL(RESERVED_AMOUNT_2,0))
    ELSE 0
END                                                                 AS CADANGAN_INDIVIDUAL_CCY,
CASE
    WHEN STAGE <> 1
    THEN SUM(NVL(RESERVED_AMOUNT_3,0))
    ELSE 0
END                                                                 AS CADANGAN_INDIVIDUAL_LCL,
CASE
    WHEN STAGE = 1
    THEN SUM(NVL(ECL_OFF_BS_CCY,0))
    ELSE 0
END                                                                 AS CADANGAN_KOLEKTIF_COM_CCY,
CASE
    WHEN STAGE = 1
    THEN SUM(NVL(ECL_OFF_BS_LCL,0))
    ELSE 0
END                                                                 AS CADANGAN_KOLEKTIF_COM_LCL,
CASE
    WHEN STAGE <> 1
    THEN SUM(NVL(ECL_OFF_BS_CCY,0))
    ELSE 0
END                                                                 AS CADANGAN_INDIV_COM_CCY,
CASE
    WHEN STAGE <> 1
    THEN SUM(NVL(ECL_OFF_BS_LCL,0))
    ELSE 0
END                                                                 AS CADANGAN_INDIV_COM_LCL,
SUM(NVL(AMORT_FEE_AMT_ILS_CCY,0))                                   AS AMORT_FEE_AMT_ILS_CCY,
SUM(NVL(AMORT_FEE_AMT_ILS_LCL,0))                                   AS AMORT_FEE_AMT_ILS_LCL
FROM GTMP_IFRS_NOMINATIVE A
JOIN TMP_IMA_LBU B
ON (A.MASTERID = B.MASTERID)
WHERE 1 = 1
--AND REPORT_DATE = V_CURRDATE
AND DATA_SOURCE = 'CRD' AND IS_IMPAIRED = 1 AND STAGE IS NOT NULL
GROUP BY
REPORT_DATE,
DATA_SOURCE,
NOREK_LBU,
BRANCH_CODE,
LBU_foRm,
CURRENCY,
STAGE,
BS_FLAG,
PORTFOLIO,
INSTRUMENT;
COMMIT;

UPDATE /*+ PARALLEL(8) */ IFRS_NOMINATIVE_LBU SET
UNUSED_CCY_SIGN                 = CASE WHEN UNUSED_CCY                  < 0 THEN '-' ELSE NULL END,
UNUSED_LCL_SIGN                 = CASE WHEN UNUSED_LCL                  < 0 THEN '-' ELSE NULL END,
EAD_OFF_BS_CCF_CCY_SIGN         = CASE WHEN EAD_OFF_BS_CCF_CCY          < 0 THEN '-' ELSE NULL END,
EAD_OFF_BS_CCF_LCL_SIGN         = CASE WHEN EAD_OFF_BS_CCF_LCL          < 0 THEN '-' ELSE NULL END,
OUTSTANDING_ON_BS_CCY_SIGN      = CASE WHEN OUTSTANDING_ON_BS_CCY       < 0 THEN '-' ELSE NULL END,
OUTSTANDING_ON_BS_LCL_SIGN      = CASE WHEN OUTSTANDING_ON_BS_LCL       < 0 THEN '-' ELSE NULL END,
OUTSTANDING_OFF_BS_CCY_SIGN     = CASE WHEN OUTSTANDING_OFF_BS_CCY      < 0 THEN '-' ELSE NULL END,
OUTSTANDING_OFF_BS_LCL_SIGN     = CASE WHEN OUTSTANDING_OFF_BS_LCL      < 0 THEN '-' ELSE NULL END,
UNAMORT_FEE_CCY_SIGN            = CASE WHEN UNAMORT_FEE_CCY             < 0 THEN '-' ELSE NULL END,
UNAMORT_FEE_LCL_SIGN            = CASE WHEN UNAMORT_FEE_LCL             < 0 THEN '-' ELSE NULL END,
INTEREST_ACCRUED_CCY_SIGN       = CASE WHEN INTEREST_ACCRUED_CCY        < 0 THEN '-' ELSE NULL END,
INTEREST_ACCRUED_LCL_SIGN       = CASE WHEN INTEREST_ACCRUED_LCL        < 0 THEN '-' ELSE NULL END,
UNWINDING_INTEREST_CCY_SIGN     = CASE WHEN UNWINDING_INTEREST_CCY      < 0 THEN '-' ELSE NULL END,
UNWINDING_INTEREST_LCL_SIGN     = CASE WHEN UNWINDING_INTEREST_LCL      < 0 THEN '-' ELSE NULL END,
AMORTISASI_FEE_CCY_SIGN         = CASE WHEN AMORTISASI_FEE_CCY          < 0 THEN '-' ELSE NULL END,
AMORTISASI_FEE_LCL_SIGN         = CASE WHEN AMORTISASI_FEE_LCL          < 0 THEN '-' ELSE NULL END,
CADANGAN_KOLEKTIF_CCY_SIGN      = CASE WHEN CADANGAN_KOLEKTIF_CCY       < 0 THEN '-' ELSE NULL END,
CADANGAN_KOLEKTIF_LCL_SIGN      = CASE WHEN CADANGAN_KOLEKTIF_LCL       < 0 THEN '-' ELSE NULL END,
CADANGAN_INDIVIDUAL_CCY_SIGN    = CASE WHEN CADANGAN_INDIVIDUAL_CCY     < 0 THEN '-' ELSE NULL END,
CADANGAN_INDIVIDUAL_LCL_SIGN    = CASE WHEN CADANGAN_INDIVIDUAL_LCL     < 0 THEN '-' ELSE NULL END,
CADANGAN_KOLEKTIF_COM_CCY_SIGN  = CASE WHEN CADANGAN_KOLEKTIF_COM_CCY   < 0 THEN '-' ELSE NULL END,
CADANGAN_KOLEKTIF_COM_LCL_SIGN  = CASE WHEN CADANGAN_KOLEKTIF_COM_LCL   < 0 THEN '-' ELSE NULL END,
CADANGAN_INDIV_COM_CCY_SIGN     = CASE WHEN CADANGAN_INDIV_COM_CCY      < 0 THEN '-' ELSE NULL END,
CADANGAN_INDIV_COM_LCL_SIGN     = CASE WHEN CADANGAN_INDIV_COM_LCL      < 0 THEN '-' ELSE NULL END,
AMORT_FEE_AMT_ILS_CCY_SIGN      = CASE WHEN AMORT_FEE_AMT_ILS_CCY       < 0 THEN '-' ELSE NULL END,
AMORT_FEE_AMT_ILS_LCL_SIGN      = CASE WHEN AMORT_FEE_AMT_ILS_LCL       < 0 THEN '-' ELSE NULL END,
UNUSED_CCY                      = ABS(UNUSED_CCY)                ,
UNUSED_LCL                      = ABS(UNUSED_LCL)                ,
EAD_OFF_BS_CCF_CCY              = ABS(EAD_OFF_BS_CCF_CCY)        ,
EAD_OFF_BS_CCF_LCL              = ABS(EAD_OFF_BS_CCF_LCL)        ,
OUTSTANDING_ON_BS_CCY           = ABS(OUTSTANDING_ON_BS_CCY)     ,
OUTSTANDING_ON_BS_LCL           = ABS(OUTSTANDING_ON_BS_LCL)     ,
OUTSTANDING_OFF_BS_CCY          = ABS(OUTSTANDING_OFF_BS_CCY)    ,
OUTSTANDING_OFF_BS_LCL          = ABS(OUTSTANDING_OFF_BS_LCL)    ,
UNAMORT_FEE_CCY                 = ABS(UNAMORT_FEE_CCY)           ,
UNAMORT_FEE_LCL                 = ABS(UNAMORT_FEE_LCL)           ,
INTEREST_ACCRUED_CCY            = ABS(INTEREST_ACCRUED_CCY)      ,
INTEREST_ACCRUED_LCL            = ABS(INTEREST_ACCRUED_LCL)      ,
UNWINDING_INTEREST_CCY          = ABS(UNWINDING_INTEREST_CCY)    ,
UNWINDING_INTEREST_LCL          = ABS(UNWINDING_INTEREST_LCL)    ,
AMORTISASI_FEE_CCY              = ABS(AMORTISASI_FEE_CCY)        ,
AMORTISASI_FEE_LCL              = ABS(AMORTISASI_FEE_LCL)        ,
CADANGAN_KOLEKTIF_CCY           = ABS(CADANGAN_KOLEKTIF_CCY)     ,
CADANGAN_KOLEKTIF_LCL           = ABS(CADANGAN_KOLEKTIF_LCL)     ,
CADANGAN_INDIVIDUAL_CCY         = ABS(CADANGAN_INDIVIDUAL_CCY)   ,
CADANGAN_INDIVIDUAL_LCL         = ABS(CADANGAN_INDIVIDUAL_LCL)   ,
CADANGAN_KOLEKTIF_COM_CCY       = ABS(CADANGAN_KOLEKTIF_COM_CCY) ,
CADANGAN_KOLEKTIF_COM_LCL       = ABS(CADANGAN_KOLEKTIF_COM_LCL) ,
CADANGAN_INDIV_COM_CCY          = ABS(CADANGAN_INDIV_COM_CCY)    ,
CADANGAN_INDIV_COM_LCL          = ABS(CADANGAN_INDIV_COM_LCL)    ,
AMORT_FEE_AMT_ILS_CCY           = ABS(AMORT_FEE_AMT_ILS_CCY)     ,
AMORT_FEE_AMT_ILS_LCL           = ABS(AMORT_FEE_AMT_ILS_LCL)
WHERE REPORT_DATE = V_CURRDATE;
COMMIT;

UPDATE /*+ PARALLEL(8) */ IFRS_NOMINATIVE_LBU
SET APLIKASI = 'ILS'
WHERE APLIKASI = 'LIMIT'
AND REPORT_DATE = V_CURRDATE;

COMMIT;
END;