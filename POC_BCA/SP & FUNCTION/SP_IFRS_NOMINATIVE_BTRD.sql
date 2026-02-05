CREATE OR REPLACE PROCEDURE SP_IFRS_NOMINATIVE_BTRD (v_DOWNLOADDATECUR DATE DEFAULT ('1-JAN-1900'),v_DOWNLOADDATEPREV DATE DEFAULT ('1-JAN-1900'),v_RUNNING_MODE NUMBER DEFAULT 0)
AS
       V_CURRDATE DATE;
BEGIN
    V_CURRDATE := v_DOWNLOADDATECUR;

    IF v_RUNNING_MODE = 0 THEN
        DELETE /*+ PARALLEL(8) */ IFRS.IFRS_NOMINATIVE WHERE REPORT_DATE = V_CURRDATE AND DATA_SOURCE = 'BTRD';
    ELSE
        DELETE /*+ PARALLEL(8) */ IFRS.IFRS_NOMINATIVE WHERE REPORT_DATE = V_CURRDATE AND DATA_SOURCE = 'BTRD'
        AND MASTERID IN (SELECT MASTERID FROM IFRS.TMP_IMA_PARTIAL);
    END IF;
    COMMIT;

    INSERT /*+ PARALLEL(8) */ INTO IFRS.IFRS_NOMINATIVE
            (
            MASTERID                  ,
            COMMITMENT_FLAG           ,
            ACCOUNT_STATUS            ,
            SPECIAL_REASON            ,
            REVOLVING_FLAG            ,
            IMPAIRED_FLAG             ,
            REPORT_DATE               ,
            SOURCE_TYPE               ,
            DATA_SOURCE               ,
            BRANCH_CODE               ,
            LBU_FORM                  ,
            NOREK_LBU                 ,
            FACILITY_NUMBER           ,
            ACCOUNT_NUMBER            ,
            CUSTOMER_NUMBER           ,
            CUSTOMER_NAME             ,
            GOL_DEB                   ,
            PRODUCT_GROUP             ,
            PRODUCT_TYPE              ,
            PRODUCT_CODE              ,
            PRODUCT_DESC              ,
            PRODUCT_CODE_GL           ,
            START_DATE                ,
            MATURITY_DATE             ,
            RATING_CODE               ,
            EXTERNAL_RATING           ,
            IMP_RATING                ,
            SEGMENTATION              ,
            BUCKET_NAME               ,
            BI_COLLECTABILITY         ,
            WRITEOFF_FLAG             ,
            STAGE                     ,
            ASSESSMENT_IMP            ,
            IMP_CHANGE_REASON         ,
            IMP_100PERCENT_REASON     ,
            CONTRACTUAL_INTEREST_RATE ,
            IFRS9_CLASS               ,
            CURRENCY                  ,
            EXCHANGE_RATE             ,
            OUTSTANDING_PRINCIPAL_CCY ,
            OUTSTANDING_PRINCIPAL_LCL ,
            OUTSTANDING_PRINCIPAL_DIFF,
            CARRYING_AMOUNT_CCY       ,
            CARRYING_AMOUNT_LCL       ,
            EAD_AMOUNT_CCY            ,
            EAD_AMOUNT_LCL            ,
            SALDO_YADIT_CCY           ,
            SALDO_YADIT_LCL           ,
            STAGE_1_CCY               ,
            STAGE_1_LCL               ,
            STAGE_2_CCY               ,
            STAGE_2_LCL               ,
            STAGE_3_CCY               ,
            STAGE_3_LCL               ,
            ECL_INDIVIDUAL_CCY        ,
            ECL_INDIVIDUAL_LCL        ,
            ECL_WORSTCASE_CCY         ,
            ECL_WORTCASE_LCL          ,
            ECL_COLLECTIVE_CCY        ,
            ECL_COLLECTIVE_LCL        ,
            ECL_TOTAL_CCY             ,
            ECL_TOTAL_LCL             ,
            ECL_TOTAL_DIFF            ,
            PV_EXPECTED_CF_IA_CCY     ,
            PV_EXPECTED_CF_IA_LCL     ,
            IA_UNWINDING_INTEREST_CCY ,
            IA_UNWINDING_INTEREST_LCL  ,
            BS_FLAG                   ,
            SWIFT_CODE                ,
            BI_CODE                   ,
            GROUP_SEGMENT             ,
            SEGMENT                   ,
            SUB_SEGMENT               ,
            ECL_ON_BS_CCY             ,
            ECL_ON_BS_LCL             ,
            ECL_OFF_BS_CCY            ,
            ECL_OFF_BS_LCL            ,
            CCF_AMOUNT_CCY            ,
            CCF_AMOUNT_LCL            ,
            CCF_RATE                  ,
            PREPAYMENT_AMOUNT_CCY     ,
            PREPAYMENT_AMOUNT_LCL     ,
            PREPAYMENT_RATE           ,
            OUTSTANDING_ON_BS_CCY     ,
            OUTSTANDING_OFF_BS_CCY    ,
            OUTSTANDING_ON_BS_LCL     ,
            OUTSTANDING_OFF_BS_LCL    ,
            CREATEDBY                 ,
            CREATEDDATE               ,
            CREATEDHOST               ,
            RESERVED_AMOUNT_2         ,
            RESERVED_AMOUNT_3         ,
            RESERVED_AMOUNT_4         ,
            RESERVED_AMOUNT_5         ,
            RESERVED_VARCHAR_3		  ,

			RESERVED_RATE_1			  ,
			RESERVED_VARCHAR_4		  ,
			RESERVED_FLAG_1			  ,
			RESERVED_FLAG_2,
            RESERVED_FLAG_6,
            RESERVED_VARCHAR_9,
            RESERVED_VARCHAR_10,
            RESERVED_VARCHAR_11,
            RESERVED_VARCHAR_12
            )
    SELECT /*+ PARALLEL(8) */ A.MASTERID                                                                     AS MASTERID,
           CASE
             WHEN NVL(A.COMMITTED_FLAG, 1) = 1 THEN
              'Y'
             ELSE
              'N'
           END                                                                            AS COMMITMENT_FLAG,
           A.ACCOUNT_STATUS                                                               AS ACCOUNT_STATUS,
           A.RESERVED_VARCHAR_25                                                          AS SPECIAL_REASON,
           CASE
             WHEN A.REVOLVING_FLAG = 1 THEN
              'Y'
             WHEN A.REVOLVING_FLAG = 0 THEN
              'N'
           END                                                                            AS REVOLVING_FLAG,
           A.IMPAIRED_FLAG                                                                AS IMPAIRED_FLAG,
           A.DOWNLOAD_DATE                                                                AS REPORT_DATE,
           'TRADE' AS SOURCE_TYPE,
           A.DATA_SOURCE                                                                  AS DATA_SOURCE,
           A.BRANCH_CODE                                                                  AS BRANCH_CODE,
           B.RESERVED_VARCHAR_1                                                           AS LBU_FORM,
           CASE -- WHEN A.CREATEDBY  = 'DKP' AND A.PRODUCT_CODE IN ('078','079') THEN A.ACCOUNT_NUMBER
             WHEN B.RESERVED_VARCHAR_1 IN ('7', '10', '22') THEN
              SUBSTR(A.ACCOUNT_NUMBER, 1, 1) || '-' ||
              SUBSTR(A.ACCOUNT_NUMBER, 2, 6) || '-' ||
              SUBSTR(A.ACCOUNT_NUMBER, 8, 3)
             WHEN B.RESERVED_VARCHAR_1 = '43' THEN
              SUBSTR(A.ACCOUNT_NUMBER, 1, 1) || '-' ||
              SUBSTR(A.ACCOUNT_NUMBER, 2, 6) || '-'
             ELSE
              NULL
           END                                                                            AS NOREK_LBU,
           A.FACILITY_NUMBER                                                              AS FACILITY_NUMBER,
           A.ACCOUNT_NUMBER                                                               AS ACCOUNT_NUMBER,
           A.CUSTOMER_NUMBER                                                              AS CUSTOMER_NUMBER,
           A.CUSTOMER_NAME                                                                AS CUSTOMER_NAME,
           A.RESERVED_VARCHAR_2                                                           AS GOL_DEB,
           A.PRODUCT_GROUP                                                                AS PRODUCT_GROUP,
           A.PRODUCT_TYPE                                                                 AS PRODUCT_TYPE,
           A.PRODUCT_CODE                                                                 AS PRODUCT_CODE,
           B.PRD_DESC                                                                     AS PRODUCT_DESC,
           B.PRD_TYPE                                                                     AS PRODUCT_CODE_GL,
           A.LOAN_START_DATE                                                              AS START_DATE,
           A.LOAN_DUE_DATE                                                                AS MATURITY_DATE,
           A.RESERVED_VARCHAR_22                                                          AS RATING_CODE,
           CASE
           --WHEN A.SUB_SEGMENT IN ('BANK REV - EXTERNAL VALAS','BANK REV - EXTERNAL IDR')
             WHEN A.SUB_SEGMENT IN
                  ('BANK BTRD - EXTERNAL VALAS',
                   'BANK BTRD - EXTERNAL IDR') THEN
              A.RATING_CODE
             ELSE
              NULL
           END                                                                            AS EXTERNAL_RATING,
           A.RATING_CODE                                                                  AS IMP_RATING,
           A.SEGMENT                                                                      AS SEGMENTATION,
           A.BUCKET_ID                                                                    AS BUCKET_NAME,
           A.BI_COLLECTABILITY                                                            AS BI_COLLECTABILITY,
           CASE WHEN NVL(A.WRITEOFF_FLAG, '0') = '0' THEN
              'N'
             ELSE
              'Y'
           END                                                                            AS WRITEOFF_FLAG,

		   CASE WHEN NVL(A.RESERVED_FLAG_4, 0) = 0 THEN
		      A.CR_STAGE
			ELSE
			  '1'
		   END 																		      AS STAGE,

           A.IMPAIRED_FLAG                                                                AS ASSESSMENT_IMP,
           ''                                                                             AS IMP_CHANGE_REASON,
           ''                                                                             AS IMP_100PERCENT_REASON,
           A.INTEREST_RATE                                                                AS CONTRACTUAL_INTEREST_RATE,
           A.IFRS9_CLASS                                                                  AS IFRS9_CLASS,
           A.CURRENCY                                                                     AS CURRENCY,
           NVL(A.EXCHANGE_RATE, 0)                                                        AS EXCHANGE_RATE,
           NVL(A.OUTSTANDING, 0)                                                          AS OUTSTANDING_PRINCIPAL_CCY,
           NVL(A.OUTSTANDING, 0) * NVL(A.EXCHANGE_RATE, 0)                                AS OUTSTANDING_PRINCIPAL_LCL,
           (NVL(A.OUTSTANDING, 0) - NVL(C.OUTSTANDING, 1)) * NVL(A.EXCHANGE_RATE, 1)      AS OUTSTANDING_PRINCIPAL_DIFF,
           CASE WHEN NVL(A.FAIR_VALUE_AMOUNT, 0) <= 0 THEN
              0
             ELSE
              NVL(A.FAIR_VALUE_AMOUNT, 0)
           END                                                                            AS CARRYING_AMOUNT_CCY,
           CASE WHEN NVL(A.FAIR_VALUE_AMOUNT, 0) <= 0 THEN
              0
             ELSE
              NVL(A.FAIR_VALUE_AMOUNT, 0)
           END * NVL(A.EXCHANGE_RATE, 1)                                                  AS CARRYING_AMOUNT_LCL,
           NVL(A.EAD_AMOUNT, 0)                                                           AS EAD_AMOUNT_CCY,
           NVL(A.EAD_AMOUNT, 0) * NVL(A.EXCHANGE_RATE, 1)                                 AS EAD_AMOUNT_LCL,
           NVL(A.INTEREST_ACCRUED, 0)                                                     AS SALDO_YADIT_CCY,
           NVL(A.INTEREST_ACCRUED, 0) * NVL(A.EXCHANGE_RATE, 1)                           AS SALDO_YADIT_LCL,
           NVL(A.ECL_12_AMOUNT, 0)                                                        AS STAGE_1_CCY,
           NVL(A.ECL_12_AMOUNT, 0) * NVL(A.EXCHANGE_RATE, 1)                              AS STAGE_1_LCL,
           NVL(A.ECL_LIFETIME_AMOUNT, 0)                                                  AS STAGE_2_CCY,
           NVL(A.ECL_LIFETIME_AMOUNT, 0) * NVL(A.EXCHANGE_RATE, 1)                        AS STAGE_2_LCL,
           NVL(A.ECL_AMOUNT, 0)                                                           AS STAGE_3_CCY,
           NVL(A.ECL_AMOUNT, 0) * NVL(A.EXCHANGE_RATE, 1)                                 AS STAGE_3_LCL,
           CASE WHEN A.IMPAIRED_FLAG = 'I' THEN
              NVL(A.ECL_AMOUNT, 0)
             ELSE
              0
           END                                                                            AS ECL_INDIVIDUAL_CCY,
           CASE WHEN A.IMPAIRED_FLAG = 'I' THEN
              NVL(A.ECL_AMOUNT, 0) * NVL(A.EXCHANGE_RATE, 1)
             ELSE
              0
           END                                                                            AS ECL_INDIVIDUAL_LCL,
           CASE WHEN A.IMPAIRED_FLAG = 'W' THEN
              NVL(A.ECL_AMOUNT, 0)
             ELSE
              0
           END                                                                            AS ECL_WORSTCASE_CCY,
           CASE WHEN A.IMPAIRED_FLAG = 'W' THEN
              NVL(A.ECL_AMOUNT, 0) * NVL(A.EXCHANGE_RATE, 1)
             ELSE
              0
           END                                                                            AS ECL_WORTCASE_LCL,
           CASE WHEN A.IMPAIRED_FLAG = 'C' THEN
              NVL(A.ECL_AMOUNT, 0)
             ELSE
              0
           END                                                                            AS ECL_COLLECTIVE_CCY,
           CASE WHEN A.IMPAIRED_FLAG = 'C' THEN
              NVL(A.ECL_AMOUNT, 0) * NVL(A.EXCHANGE_RATE, 1)
             ELSE
              0
           END                                                                            AS ECL_COLLECTIVE_LCL,
           NVL(A.ECL_AMOUNT, 0)                                                           AS ECL_TOTAL_CCY,
           NVL(A.ECL_AMOUNT, 0) * NVL(A.EXCHANGE_RATE, 1)                                 AS ECL_TOTAL_LCL,
           (NVL(A.ECL_AMOUNT, 0) - NVL(C.ECL_AMOUNT, 0)) * NVL(A.EXCHANGE_RATE, 1)        AS ECL_TOTAL_DIFF,
           PAYM.SUM_PV_AMOUNT                                                             AS PV_EXPECTED_CF_IA_CCY,
           PAYM.SUM_PV_AMOUNT * NVL(A.EXCHANGE_RATE, 1)                                   AS PV_EXPECTED_CF_IA_LCL,
           NVL(A.IA_UNWINDING_AMOUNT, 0)                                                  AS IA_UNWINDING_INTEREST_CCY,
           NVL(A.IA_UNWINDING_AMOUNT, 0) * NVL(A.EXCHANGE_RATE, 1)                        AS IA_UNWINDING_INTEREST_LCL,
           CASE WHEN A.RESERVED_FLAG_1 = 1 THEN
                  'Y'
                WHEN A.RESERVED_FLAG_1 = 0 THEN
                  'N'
           END                                                                            AS BS_FLAG,
           A.RESERVED_VARCHAR_15                                                          AS SWIFT_CODE,
           A.RESERVED_VARCHAR_23                                                          AS BI_CODE,
           A.GROUP_SEGMENT                                                                AS GROUP_SEGMENT,
           A.SEGMENT                                                                      AS SEGMENT,
           A.SUB_SEGMENT                                                                  AS SUB_SEGMENT,
           NVL(A.RESERVED_AMOUNT_18, 0)                                                   AS ECL_ON_BS_CCY,
           NVL(A.RESERVED_AMOUNT_18, 0) * NVL(A.EXCHANGE_RATE, 1)                         AS ECL_ON_BS_LCL,
           NVL(A.RESERVED_AMOUNT_19, 0)                                                   AS ECL_OFF_BS_CCY,
           NVL(A.RESERVED_AMOUNT_19, 0) * NVL(A.EXCHANGE_RATE, 1)                         AS ECL_OFF_BS_LCL,
           NVL(A.RESERVED_RATE_3, 0)                                                      AS CCF_AMOUNT_CCY,
           NVL(A.RESERVED_RATE_3, 0) * NVL(A.EXCHANGE_RATE, 1)                            AS CCF_AMOUNT_LCL,
           A.RESERVED_RATE_1                                                              AS CCF_RATE,
           NVL(A.RESERVED_RATE_4, 0)                                                      AS PREPAYMENT_AMOUNT_CCY,
           NVL(A.RESERVED_RATE_4, 0) * NVL(A.EXCHANGE_RATE, 1)                            AS PREPAYMENT_AMOUNT_LCL,
           A.RESERVED_RATE_2                                                              AS PREPAYMENT_RATE,
           (CASE WHEN (A.DATA_SOURCE = 'ILS' AND A.PRODUCT_CODE LIKE 'B%') OR
                      (A.DATA_SOURCE = 'KTP' AND NVL(A.RESERVED_FLAG_1, 0) = 0) OR
                      --(A.DATA_SOURCE = 'LIMIT' AND NVL (A.RESERVED_FLAG_1, 1) <> 0) OR
                      (A.DATA_SOURCE = 'BTRD' AND NVL(A.RESERVED_FLAG_1, 0) = 0) THEN
                      0
                    ELSE
                      A.OUTSTANDING
           END)                                                                           AS OUTSTANDING_ON_BS_CCY,
           (CASE WHEN (A.DATA_SOURCE = 'ILS' AND A.PRODUCT_CODE LIKE 'B%') OR
                      (A.DATA_SOURCE = 'KTP' AND NVL(A.RESERVED_FLAG_1, 0) = 0) OR
                      --(A.DATA_SOURCE = 'LIMIT' AND NVL (A.RESERVED_FLAG_1, 1) <> 0) OR
                      (A.DATA_SOURCE = 'BTRD' AND NVL(A.RESERVED_FLAG_1, 0) = 0) THEN
                   NVL(A.OUTSTANDING, 0)
                 ELSE
                   NVL(A.RESERVED_AMOUNT_14, 0)
           END)                                                                           AS OUTSTANDING_OFF_BS_CCY,
           (CASE WHEN (A.DATA_SOURCE = 'ILS' AND A.PRODUCT_CODE LIKE 'B%') OR
                      (A.DATA_SOURCE = 'KTP' AND NVL(A.RESERVED_FLAG_1, 0) = 0) OR
                      --(A.DATA_SOURCE = 'LIMIT' AND NVL (A.RESERVED_FLAG_1, 1) <> 0) OR
                      (A.DATA_SOURCE = 'BTRD' AND NVL(A.RESERVED_FLAG_1, 0) = 0) THEN
                   0
                 ELSE
                   A.OUTSTANDING
           END) * NVL(A.EXCHANGE_RATE, 1)                                                 AS OUTSTANDING_ON_BS_LCL,
           (CASE WHEN (A.DATA_SOURCE = 'ILS' AND A.PRODUCT_CODE LIKE 'B%') OR
                      (A.DATA_SOURCE = 'KTP' AND NVL(A.RESERVED_FLAG_1, 0) = 0) OR
                      --(A.DATA_SOURCE = 'LIMIT' AND NVL (A.RESERVED_FLAG_1, 1) <> 0) OR
                      (A.DATA_SOURCE = 'BTRD' AND NVL(A.RESERVED_FLAG_1, 0) = 0) THEN
                   NVL(A.OUTSTANDING, 0)
                 ELSE
                   NVL(A.RESERVED_AMOUNT_14, 0)
           END * NVL(A.EXCHANGE_RATE, 1))                                                 AS OUTSTANDING_OFF_BS_LCL,
           'ADMIN'                                                                        AS CREATEDBY,
           SYSDATE                                                                        AS CREATEDDATE,
           'LOCALHOST'                                                                    AS CREATEDHOST,
           NVL(A.RESERVED_RATE_5,0)                                                       AS ECL_AMOUNT_ALL_FINAL_CCY,
           NVL(A.RESERVED_RATE_5,0) * NVL(A.EXCHANGE_RATE, 1)                             AS ECL_AMOUNT_ALL_FINAL_LCL,
           NVL(A.RESERVED_RATE_6,0)                                                       AS ECL_AMOUNT_FINAL_TOTAL_CCY,
           NVL(A.RESERVED_RATE_6,0) * NVL(A.EXCHANGE_RATE, 1)                             AS ECL_AMOUNT_FINAL_TOTAL_LCL,
           A.BUCKET_GROUP                                                                 AS BUCKET_GROUP,

		   A.TENOR																		  AS TENOR,
		   A.CR_STAGE                                                                     AS STAGE_ORIGINAL,
		   CASE WHEN NVL(A.RESERVED_FLAG_4, 0) = 0 THEN
		      0  -- 'N'
			ELSE
			  1  -- 'Y'
		   END 																	          AS FLAG_COVID,
		   CASE WHEN NVL(A.RESTRUCTURE_FLAG, 0) = 0 THEN
		      0  -- 'N'
		     ELSE
		      1  -- 'Y'
		   END 																			  AS FLAG_RESTRUCTURE,
          A.RESERVED_FLAG_10 LPEI_FLAG,
          BRANCH.REGION_CODE,
          BRANCH.BRANCH_NAME,
          BRANCH.MAIN_BRANCH_CD,
          BRANCH.MAIN_BRANCH_DESC

      FROM (SELECT *
              FROM IFRS.GTMP_IFRS_MASTER_ACCOUNT
             WHERE DOWNLOAD_DATE = v_DOWNLOADDATECUR
               AND DATA_SOURCE = 'BTRD') A
      LEFT JOIN (SELECT DISTINCT PRD_TYPE,
                                 PRD_CODE,
                                 PRD_GROUP,
                                 PRD_DESC,
                                 RESERVED_VARCHAR_1
                   FROM IFRS.IFRS_MASTER_PRODUCT_PARAM
                  WHERE DATA_SOURCE = 'BTRD') B
        ON TRIM(A.PRODUCT_CODE) = TRIM(B.PRD_CODE)
      LEFT JOIN (SELECT *
                   FROM IFRS.GTMP_IFRS_MASTER_ACCOUNT_PREV
                  WHERE DOWNLOAD_dATE = v_DOWNLOADDATEPREV
                    AND DATA_SOURCE = 'BTRD') C
        ON A.MASTERID = C.MASTERID
      LEFT JOIN (SELECT SUM(NVL(A.PV_AMOUNT, 0)) AS SUM_PV_AMOUNT,
                        A.MASTERID
                   FROM IFRS.TBLT_PAYMENTEXPECTED A
                  INNER JOIN (SELECT MAX(EFFECTIVE_DATE) AS MAX_EFF_DATE,
                                    MASTERID
                               FROM IFRS.TBLT_PAYMENTEXPECTED
                              WHERE EFFECTIVE_DATE <= v_DOWNLOADDATECUR
                              GROUP BY MASTERID) B
                     ON A.MASTERID = B.MASTERID
                    AND A.EFFECTIVE_DATE = B.MAX_EFF_DATE
                  GROUP BY A.MASTERID) PAYM
        ON A.MASTERID = PAYM.MASTERID
    --WHERE A.ACCOUNT_STATUS = 'A'
            LEFT JOIN
            (
                select BRANCH_NUM,
                       REGION_CODE,
                       BRANCH_NAME,
                       MAIN_BRANCH_CD,
                       MAIN_BRANCH_DESC
                from IFRS.IFRS_MASTER_BRANCH where DOWNLOAD_DATE=v_DOWNLOADDATECUR  ) BRANCH
                ON A.BRANCH_CODE=BRANCH.BRANCH_NUM
                                             ;

    COMMIT;

    END;