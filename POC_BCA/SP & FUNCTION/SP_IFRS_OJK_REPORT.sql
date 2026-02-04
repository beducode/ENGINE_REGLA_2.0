CREATE OR REPLACE PROCEDURE SP_IFRS_OJK_REPORT(v_DOWNLOADDATECUR  DATE DEFAULT ('1-JAN-1900'),v_DOWNLOADDATEPREV  DATE DEFAULT ('1-JAN-1900'))
                                               AS
  V_CURRDATE DATE;
  V_PREVDATE DATE;
  --V_SPNAME   VARCHAR2(150);
BEGIN

    IF v_DOWNLOADDATECUR = '1-JAN-1900'
    THEN
        SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;
    ELSE
        V_CURRDATE := v_DOWNLOADDATECUR;
    END IF;

    /* Formatted on 8/28/2019 6:00:54 PM (QP5 v5.294) */
    DELETE IFRS_OJK_REPORT WHERE DOWNLOAD_DATE = V_CURRDATE;

    /* Formatted on 8/28/2019 5:57:44 PM (QP5 v5.294) */
    /* Formatted on 8/28/2019 6:00:54 PM (QP5 v5.294) */
    INSERT INTO IFRS_OJK_REPORT (DOWNLOAD_DATE,
                             DATA_SOURCE,
                             IAS_CLASS,
                             IFRS9_CLASS,
                             CR_STAGE,
                             BI_COLLECTABILITY,
                             SEGMENT,
                             RESERVED_VARCHAR_4,
                             PRODUCT_GROUP,
                             PRODUCT_TYPE,
                             PRODUCT_CODE,
                             UNUSED_AMOUNT,
                             NILAI_TERCATAT_ON,
                             NILAI_TERCATAT_OFF,
                             TOTAL_ECL_ON_BS,
                             TOTAL_ECL_OFF_BS,
                             ASET_KEUANGAN,
                             TRA,
                             CREATEDDATE,
                             CREATEDBY,
                             CREATEDHOST,
                             CURRENCY,
                             INTEREST_ACCRUED)
    SELECT
    X.DOWNLOAD_DATE, X.DATA_SOURCE, X.IAS_CLASS, X.IFRS9_CLASS, X.CR_STAGE, X.BI_COLLECTABILITY, X.SEGMENT, X.RESERVED_VARCHAR_4, X.PRODUCT_GROUP,
    X.PRODUCT_TYPE, X.PRODUCT_CODE, SUM(UNUSED_AMOUNT), SUM(NILAI_TERCATAT_ON), SUM(NILAI_TERCATAT_OFF), SUM(TOTAL_ECL_ON_BS), SUM(TOTAL_ECL_OFF_BS),
    '' AS ASET_KEUANGAN, '' AS TRA, X.CREATEDDATE, X.CREATEDBY, X.CREATEDHOST,X.CURRENCY,SUM(X.INTEREST_ACCRUED)INTEREST_ACCRUED
    FROM (
     SELECT A.DOWNLOAD_DATE,
            A.DATA_SOURCE,
            A.IAS_CLASS,
            A.IFRS9_CLASS,
            A.CR_STAGE,
            A.BI_COLLECTABILITY,
            A.SEGMENT,
            CASE WHEN A.RESERVED_VARCHAR_5 IN ('T/DVALAS', 'T/DIDR') THEN RESERVED_VARCHAR_5 ELSE A.RESERVED_VARCHAR_4 END AS RESERVED_VARCHAR_4,
            A.PRODUCT_GROUP,
            A.PRODUCT_TYPE,
            A.PRODUCT_CODE,
            A.CURRENCY,
            NVL (C.UNUSED_AMOUNT, 0) * NVL(A.EXCHANGE_RATE, 1) UNUSED_AMOUNT,
            (
            CASE
                WHEN  ( A.DATA_SOURCE = 'ILS' AND A.PRODUCT_CODE LIKE 'B%')
                    OR ( A.DATA_SOURCE = 'KTP' AND NVL (A.RESERVED_FLAG_1, 0) = 0 )
                    OR ( A.DATA_SOURCE = 'BTRD' AND NVL (A.RESERVED_FLAG_1, 0) = 0 )
                THEN 0
                ELSE
                    CASE
                        WHEN A.IFRS9_CLASS = 'AMORT'
                        THEN
							CASE WHEN A.DATA_SOURCE = 'KTP' THEN
								CASE
									WHEN NVL(A.RESERVED_AMOUNT_8, 0) = 0
									THEN A.OUTSTANDING
									ELSE A.RESERVED_AMOUNT_8
								END
							ELSE
								CASE
									WHEN NVL(A.FAIR_VALUE_AMOUNT, 0) = 0
									THEN A.OUTSTANDING
									ELSE A.FAIR_VALUE_AMOUNT
								END
							END
                    ELSE NVL(A.MARKET_RATE, 0)
                END * NVL (A.EXCHANGE_RATE, 1)
            END
            )  AS NILAI_TERCATAT_ON, -----------------------TERCATAT ON
            (
                CASE
			WHEN  ( A.DATA_SOURCE = 'ILS' AND A.PRODUCT_CODE LIKE 'B%')
			   OR ( A.DATA_SOURCE = 'KTP' AND NVL (A.RESERVED_FLAG_1, 0) = 0 )
			   OR ( A.DATA_SOURCE = 'BTRD' AND NVL (A.RESERVED_FLAG_1, 0) = 0 )
			THEN NVL (A.OUTSTANDING, 0)
			ELSE NVL (C.UNUSED_AMOUNT, 0)
		END * NVL (A.EXCHANGE_RATE, 1)
            ) AS NILAI_TERCATAT_OFF,----------------------TERCATAT OFF
            NVL(A.INTEREST_ACCRUED,0) * NVL(A.EXCHANGE_RATE,0)INTEREST_ACCRUED,
            C.ECL_AMOUNT_OFF_BS * A.EXCHANGE_RATE TOTAL_ECL_OFF_BS,
            C.ECL_AMOUNT_ON_BS_FINAL * A.EXCHANGE_RATE TOTAL_ECL_ON_BS,
            C.ECL_AMOUNT_FINAL,
            C.CCF_AMOUNT,
            (C.FAIR_VALUE_AMOUNT + CASE WHEN C.INTEREST_ACCRUED < 0 THEN 0 ELSE C.INTEREST_ACCRUED END - C.PREPAYMENT_AMOUNT + C.CCF_AMOUNT) EAD_AMOUNT,
            'ADMIN' CREATEDBY,
            'LOCALHOST'  CREATEDHOST,
            SYSDATE CREATEDDATE
       FROM IFRS_MASTER_ACCOUNT A
            LEFT JOIN IFRS_ECL_RESULT_DETAIL C ON A.MASTERID = C.MASTERID AND A.DOWNLOAD_DATE = C.DOWNLOAD_DATE
      WHERE A.DOWNLOAD_DATE = V_CURRDATE  AND NVL(A.OUTSTANDING,0) >= 0 AND (A.DATA_SOURCE = 'CRD' OR
                                                                                A.DATA_SOURCE = 'ILS' AND ACCOUNT_STATUS = 'A' OR
                                                                                A.DATA_SOURCE = 'KTP' AND ACCOUNT_STATUS = 'A' OR
                                                                                A.DATA_SOURCE = 'RKN' AND ACCOUNT_STATUS = 'A' OR
                                                                                A.DATA_SOURCE = 'LIMIT' AND ACCOUNT_STATUS = 'A' OR
                                                                                A.DATA_SOURCE = 'BTRD' AND ACCOUNT_STATUS = 'A' AND RESERVED_VARCHAR_23 <> 0    )
         ) X
   GROUP BY X.DOWNLOAD_DATE,
            X.DATA_SOURCE,
            X.IAS_CLASS,
            X.IFRS9_CLASS,
            X.CR_STAGE,
            X.BI_COLLECTABILITY,
            X.SEGMENT,
            X.RESERVED_VARCHAR_4,
            X.PRODUCT_GROUP,
            X.PRODUCT_TYPE,
            X.PRODUCT_CODE,
            X.CREATEDDATE,
            X.CREATEDBY,
            X.CREATEDHOST,
            X.CURRENCY
   ORDER BY X.DOWNLOAD_DATE,
            X.DATA_SOURCE,
            X.IAS_CLASS,
            X.IFRS9_CLASS,
            X.CR_STAGE,
            X.BI_COLLECTABILITY,
            X.SEGMENT,
            X.RESERVED_VARCHAR_4,
            X.PRODUCT_GROUP,
            X.PRODUCT_TYPE,
            X.PRODUCT_CODE,
            X.CREATEDDATE,
            X.CREATEDBY,
            X.CREATEDHOST,
            X.CURRENCY;

            COMMIT;


    INSERT INTO IFRS_OJK_REPORT
    (
    DOWNLOAD_DATE,
    DATa_SOURCE,
    IAS_CLASS,
    IFRS9_CLASS,
    CR_STAGE,
    BI_COLLECTABILITY,
    SEGMENT,
    RESERVED_VARCHAR_4,
    PRODUCT_GROUP,
    NILAI_TERCATAT_ON,
    ASET_KEUANGAN,
    CREATEDBY,
    CREATEDDATE,
    CREATEDHOST
    )
    SELECT
      A.DOWNLOAD_DATE, A.DATA_SOURCE, A.IAS_CLASS, A.IFRS9_CLASS, A.CR_STAGE, A.BI_COLLECTABILITY, A.SEGMENT,
      'PENEMPATAN PADA BANK INDONESIA' AS RESERVED_VARCHAR_4, A.PRODUCT_GROUP,
      SUM(CASE WHEN A.FAIR_VALUE_AMOUNT = 0 THEN A.OUTSTANDING * A.EXCHANGE_RATE
          ELSE A.FAIR_VALUE_AMOUNT * A.EXCHANGE_RATE END) AS NILAI_TERCATAT_ON,
          'PENEMPATAN PADA BANK INDONESIA' AS ASET_KEUANGAN, 'ADMIN' AS CREATEDBY, SYSDATE AS CREATEDDATE, 'LOCALHOST' AS CREATEDHOST
      FROM IFRS_MASTER_ACCOUNT A
      WHERE DOWNLOAD_DATE = V_CURRDATE
      AND CUSTOMER_NUMBER IN ('00020409707', '00019597820')
      AND DATA_SOURCE = 'KTP' AND PRODUCT_GROUP = 'MM'
      GROUP BY A.DOWNLOAD_DATE, A.DATA_SOURCE, A.IAS_CLASS, A.IFRS9_CLASS, A.CR_STAGE, A.BI_COLLECTABILITY, A.SEGMENT,
                   A.RESERVED_VARCHAR_4, A.PRODUCT_GROUP;
    COMMIT;

    DELETE IFRS_OJK_REPORT WHERE RESERVED_VARCHAR_4 = 'T/DVALAS' AND DOWNLOAD_DATE = V_CURRDATE;COMMIT;
    DELETE IFRS_OJK_REPORT WHERE PRODUCT_CODE = 'DEPO-BI' AND DOWNLOAD_DATE = V_CURRDATE;COMMIT;


    COMMIT;

    /*QUERY INI HARUS DI RUNNING 2X*/
    UPDATE IFRS_OJK_REPORT
    SET ASET_KEUANGAN =
        CASE
            WHEN RESERVED_VARCHAR_4 IN ('PENEMPATAN PADA BANK INDONESIA', 'T/DVALAS', 'T/DIDR') THEN 'PENEMPATAN PADA BANK INDONESIA'
            WHEN  (DATA_SOURCE = 'ILS' AND NVL(SEGMENT, ' ') NOT LIKE '%BG%') --BANK GUARANTEE
                    OR (DATA_SOURCE = 'ILS' AND PRODUCT_CODE IN ('221', '222',	'223',	'224')) --TAGIHAN AKSEPTASI
                    OR (DATA_SOURCE = 'CRD')
                    OR (DATA_SOURCE = 'KTP' AND SEGMENT LIKE '%PBMM%')
                    THEN 'CREDIT YANG DIBERIKAN'
             WHEN PRODUCT_GROUP LIKE '%REPO%' THEN 'TAGIHAN REVERSE REPO'
             WHEN (DATA_SOURCE = 'KTP' AND SEGMENT LIKE '%BOND%')  --LIHAT INVESTMENT TYPE
                  OR (DATA_SOURCE = 'BTRD' AND PRODUCT_CODE IN ('SFF','CAC',	'CAD',	'CAL',		'ENG',	'ENH',	'ENL',	'ENN',	'ENO',	'ENW',	'ENX',	'ENZ',	'LAC',	'LAD',	'LAG',	'LAL',				'OCD',	'OCN',	'SAC',	'SAD',	'SAL',		'SFO',	'211',	'212',	'213',	'214',	'EUN')) --FORFAITING
                  OR (DATA_SOURCE = 'BTRD' AND PRODUCT_CODE IN ('SDF'))
                  OR (DATA_SOURCE = 'BTRD' AND PRODUCT_CODE = '078') --BANKER'S ACCEPTANCE
                  OR (DATA_SOURCE = 'BTRD' AND PRODUCT_CODE = '079') --FUNDED RISK PARTICIPATION
                  OR (DATA_SOURCE = 'BTRD' AND PRODUCT_CODE IN ('LAC-E',	'LAC-L',	'SAC-E',	'SAC-L')) --FORFAITING
                  OR (DATA_SOURCE = 'KTP' AND PRODUCT_GROUP = 'MUTFUND')
                  OR (DATA_SOURCE = 'KTP' AND RESERVED_VARCHAR_4 = 'TRD')
                  THEN 'SURAT BERHARGA YANG DIMILIKI'
             WHEN (DATA_SOURCE = 'KTP' AND NVL(SEGMENT, ' ') NOT LIKE '%BOND%' AND NVL(PRODUCT_GROUP, ' ') NOT LIKE '%REPO%')
                   OR (DATA_SOURCE = 'RKN') THEN 'PENEMPATAN PADA BANK LAIN'
             WHEN (DATA_SOURCE = 'BTRD' AND PRODUCT_CODE IN ('ABV',	'AC1',	'AC2',	'AC3',	'AC4',	'AC5',	'AC6',	'AC7',	'AC8',	'ACA',	'ACC',	'ACI',	'ACJ',	'ACK',	'ACM',	'ACN ',	'ACO',	'ACP',	'ACQ',	'ACR',	'ACS',	'ACT',	'ACU',	'ACW',	'ACX',	'ADC',	'AFI',	'AFV',	'AL1',	'AL2',	'APF',	'ASB',	'ASC',	'ATL',	'ATS',	'AUL',	'AUP',	'AUV',	'AUW',	'DTL',	'DTS',	'IL2',	'IL3',	'IL4',	'IL5',	'IL6',	'IL7',	'IL8',	'ILA',	'ILB',	'ILC',	'ILD',	'ILE',	'ILJ',	'ILK',	'ILM',	'ILN',	'ILO',	'ILP',	'ILR',	'ILS',	'ILT',	'ILU',	'ILW',	'ILX',	'SL1',	'SL2',	'UAL',	'UAP',	'UAV',	'UAW',	'UBV',	'UFV',	'UPF',	'USB',	'USC',	'SAB',	'CAB',	'LAB'))
                  AND (DATA_SOURCE = 'BTRD' AND NOT (PRODUCT_TYPE IN ('ILS', 'ILR', 'ILC', 'ILU', 'ILK') AND PRODUCT_CODE IN ('ILS', 'ILR', 'ILC', 'ILU', 'ILK')))
              THEN 'AKSEPTASI'
             --REVERSE REPO --AMORTISED COST
             WHEN (DATA_SOURCE = 'BTRD' AND PRODUCT_CODE IN ('LDP', 'LDG', 'LDL', 'SDL', 'CDL')) THEN 'ASET KEUANGAN LAINNYA'
             WHEN (ASET_KEUANGAN IS NULL AND NVL(DATA_SOURCE, ' ') <> 'LIMIT' AND NVL(SEGMENT,' ') NOT LIKE '%BG%') THEN  'ASET KEUANGAN LAINNYA'
         END
        WHERE DOWNLOAD_DATE = V_CURRDATE;
    COMMIT;

    UPDATE IFRS_OJK_REPORT
    SET TRA =
            CASE
                WHEN (DATA_SOURCE = 'ILS' AND NVL(SEGMENT,' ') NOT LIKE '%BG%') --BANK GUARANTEE
                                        OR (DATA_SOURCE = 'ILS' AND PRODUCT_CODE IN ('221', '222',    '223',    '224')) --TAGIHAN AKSEPTASI
                                        OR (DATA_SOURCE = 'CRD')
                                        OR (DATA_SOURCE = 'KTP' AND SEGMENT LIKE '%PBMM%')
                        --WE OFF BALANCE SHEET
                                    THEN 'KELONGGARAN TARIK'
                WHEN (DATA_SOURCE = 'BTRD')  THEN 'IRREVOCABLE LC'
                WHEN DATA_SOURCE = 'ILS' AND SEGMENT LIKE '%BG%' THEN 'GARANSI YANG DIBERIKAN'
                WHEN DATA_SOURCE = 'LIMIT' AND PRODUCT_CODE = 'KLG' THEN 'GARANSI YANG DIBERIKAN'
                WHEN DATA_SOURCE = 'LIMIT' AND PRODUCT_CODE <> 'KLG' THEN 'KELONGGARAN TARIK'
            END
        WHERE DOWNLOAD_DATE = V_CURRDATE;
     COMMIT;
END;