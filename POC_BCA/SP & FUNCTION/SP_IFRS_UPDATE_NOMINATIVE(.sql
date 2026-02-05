CREATE OR REPLACE PROCEDURE SP_IFRS_UPDATE_NOMINATIVE(
    v_DOWNLOADDATECUR DATE DEFAULT ('1-JAN-1900'),
    v_DOWNLOADDATEPREV DATE DEFAULT ('1-JAN-1900'))
AS
    V_CURRDATE DATE;
    V_PREVDATE DATE;
    V_COUNT    NUMBER;
BEGIN

    /*NO NEED TO GATHER STATS - RAL*/
--    DBMS_STATS.UNLOCK_TABLE_STATS ( OWNNAME=>'IFRS', TABNAME=>'IFRS_NOMINATIVE');
--    DBMS_STATS.GATHER_TABLE_STATS ( OWNNAME=>'IFRS', TABNAME=>'IFRS_NOMINATIVE',DEGREE=>2);


    V_CURRDATE := v_DOWNLOADDATECUR;

    UPDATE /*+ PARALLEL(8) */ IFRS.IFRS_NOMINATIVE A
    SET PV_EXPECTED_CF_IA_CCY = 0,
        PV_EXPECTED_CF_IA_LCL = 0
    WHERE REPORT_DATE = V_CURRDATE
      AND IMPAIRED_FLAG = 'I'
      AND NVL(OUTSTANDING_OFF_BS_CCY, 0)
              + NVL(OUTSTANDING_ON_BS_CCY, 0) = 0
      AND PV_EXPECTED_CF_IA_CCY <> 0;

    COMMIT;

    UPDATE /*+ PARALLEL(8) */ IFRS.IFRS_NOMINATIVE A
    SET PV_EXPECTED_CF_IA_CCY = 0,
        PV_EXPECTED_CF_IA_LCL = 0
    WHERE REPORT_DATE = V_CURRDATE
      AND NVL(OUTSTANDING_OFF_BS_CCY, 0)
              + NVL(OUTSTANDING_ON_BS_CCY, 0) = 0
      AND PV_EXPECTED_CF_IA_CCY <> 0
      AND EXISTS
        (SELECT 1
         FROM IFRS.TBLU_DCF_BULK B
         WHERE B.EFFECTIVE_DATE = V_CURRDATE
           AND B.CUSTOMER_NUMBER = A.CUSTOMER_NUMBER);

    COMMIT;


    UPDATE /*+ PARALLEL(8) */ IFRS.IFRS_nominative
    SET RESERVED_AMOUNT_1 =
                    NVL(OUTSTANDING_ON_BS_CCY, 0)
                    - NVL(UNAMORT_FEE_AMT_CCY, 0)
                + NVL(IA_UNWINDING_INTEREST_CCY, 0) /*NILAI NJUM*/
    WHERE REPORT_DATE = V_CURRDATE;

    COMMIT;


/*PROSES STAGE TERBURUK PERCUSTOMER UNTUK BTRD*/
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS.IFRS_NOMINATIVE_CUST_STAGE_MAX';
    INSERT /*+ PARALLEL(8) */ INTO IFRS.IFRS_NOMINATIVE_CUST_STAGE_MAX (REPORT_DATE,
                                                                   customer_number,
                                                                   CUST_STAGE_MAX)
    SELECT /*+ PARALLEL(8) */ REPORT_DATE,
                              customer_number,
                              MAX(NVL(RESERVED_VARCHAR_4, '1')) CUST_STAGE_MAX
    FROM IFRS.IFRS_nominative a
    WHERE a.report_date = V_CURRDATE
      and RESERVED_VARCHAR_4 > 1
    GROUP BY customer_number, REPORT_DATE;

    COMMIT;

    -----    TUNING RAL LEO -- 31 MARCH 2022

    SELECT COUNT(1)
    into V_COUNT
    FROM (select REPORT_DATE, CUSTOMER_NUMBER, GROUP_SEGMENT, STAGE
          from IFRS.IFRS_NOMINATIVE
          where REPORT_DATE = V_CURRDATE
            AND GROUP_SEGMENT = 'BANK_BTRD') NOMI,
         IFRS.IFRS_NOMINATIVE_CUST_STAGE_MAX CUST
    WHERE NOMI.CUSTOMER_NUMBER = CUST.CUSTOMER_NUMBER
      AND NOMI.STAGE <> CUST.CUST_STAGE_MAX;

--     SELECT COUNT(1)
--     into V_COUNT
--     FROM IFRS.IFRS_nominative NOMI,
--          IFRS.IFRS_NOMINATIVE_CUST_STAGE_MAX CUST
--     WHERE NOMI.CUSTOMER_NUMBER = CUST.CUSTOMER_NUMBER
--       AND NOMI.REPORT_DATE = CUST.REPORT_DATE
--       AND CUST.CUST_STAGE_MAX > 1
--       AND NOMI.STAGE <> CUST.CUST_STAGE_MAX
--       AND NOMI.REPORT_DATE = V_CURRDATE
--       AND NOMI.GROUP_SEGMENT = 'BANK_BTRD';

    IF V_COUNT != 0 THEN

        EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS.TMP_NOMI_CUST_MAX';

        INSERT /*+ PARALLEL(8) */ INTO IFRS.TMP_NOMI_CUST_MAX
        SELECT /*+ PARALLEL(8) */ NOMI.REPORT_DATE,
                                  NOMI.MASTERID,
                                  NOMI.STAGE,
                                  CUST.CUST_STAGE_MAX
        FROM IFRS.IFRS_nominative NOMI,
             IFRS.IFRS_NOMINATIVE_CUST_STAGE_MAX CUST
        WHERE NOMI.CUSTOMER_NUMBER = CUST.CUSTOMER_NUMBER
          AND NOMI.REPORT_DATE = CUST.REPORT_DATE
          AND CUST.CUST_STAGE_MAX > 1
          AND NOMI.STAGE <> CUST.CUST_STAGE_MAX
          AND NOMI.REPORT_DATE = V_CURRDATE
          AND NOMI.GROUP_SEGMENT = 'BANK_BTRD';

        COMMIT;

        ----- END TUNING RAL LEO -- 31 MARCH 2022

        MERGE INTO IFRS.IFRS_NOMINATIVE IFRS
        USING IFRS.TMP_NOMI_CUST_MAX HSL
        ON (IFRS.MASTERID = HSL.MASTERID
            AND IFRS.REPORT_DATE = HSL.REPORT_DATE)
        WHEN MATCHED
            THEN
            UPDATE
            SET STAGE = CUST_STAGE_MAX
            WHERE REPORT_DATE = V_CURRDATE;

        COMMIT;

    END IF;
    --LEO

    /*PROSES END STAGE TERBURUK PERCUSTOMER UNTUK BTRD*/

    -----    OVERRIDE STAGE WC TO STAGE 2 -- 31 MARCH 2022

    DBMS_STATS.UNLOCK_TABLE_STATS(OWNNAME=>'IFRS', TABNAME=>'TBLU_WORSTCASE_LIST');
    DBMS_STATS.GATHER_TABLE_STATS(OWNNAME=>'IFRS', TABNAME=>'TBLU_WORSTCASE_LIST', DEGREE=>2);

    UPDATE /*+ PARALLEL(8) */ IFRS.IFRS_NOMINATIVE
    SET STAGE = 2
    WHERE 1 = 1
      AND REPORT_DATE = v_DOWNLOADDATECUR
      AND CUSTOMER_NUMBER IN (SELECT CUSTOMER_NUMBER FROM IFRS.TBLU_WORSTCASE_LIST WHERE DOWNLOAD_DATE = V_DOWNLOADDATECUR)
      AND STAGE = 1
      AND NOT (nvl(ASSESSMENT_IMP, 0) = 'C' and BI_COLLECTABILITY = '1');

    COMMIT;

    ----- END OF OVERRIDE STAGE WC TO STAGE 2 -- 31 MARCH 2022

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS.GTMP_IFRS_NOMINATIVE_UPDATE';

    INSERT /*+ PARALLEL(8) */ INTO IFRS.GTMP_IFRS_NOMINATIVE_UPDATE (REPORT_DATE,
                                                                MASTERID,
                                                                DATA_SOURCE,
                                                                LBU_FORM)
    SELECT /*+ PARALLEL(8) */ REPORT_DATE,
                              MASTERID,
                              DATA_SOURCE,
                              LBU_FORM
    FROM IFRS.IFRS_NOMINATIVE
    WHERE REPORT_DATE = V_CURRDATE;
    COMMIT;

    /*PROSES IMA TEMP to replace IMA in queries below - RAL Added*/
    SP_IFRS_INSERT_GTMP_UPD_IMA(V_CURRDATE);

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS.IFRS_IFRS_NOMINATIVE_UPD_TMP';
    INSERT /*+ PARALLEL(8) */ INTO IFRS.IFRS_IFRS_NOMINATIVE_UPD_TMP (MASTERID,
                                                                 DOWNLOAD_DATE,
                                                                 DATA_SOURCE,
                                                                 PRODUCT_CODE,
                                                                 PRODUCT_TYPE,
                                                                 CUSTOMER_NUMBER,
                                                                 NILAI_TERCATAT_ON,
                                                                 NILAI_TERCATAT_OFF,
                                                                 ASET_KEUANGAN,
                                                                 TRA,
                                                                 ASET_KEUANGAN_NEW)
    SELECT /*+ PARALLEL(8) */ DISTINCT OJK.MASTERID,
                                       OJK.DOWNLOAD_DATE,
                                       OJK.DATA_SOURCE,
                                       OJK.PRODUCT_CODE,
                                       OJK.PRODUCT_TYPE,
                                       OJK.CUSTOMER_NUMBER,
                                       NVL(OJK.NILAI_TERCATAT_ON, 0)  NILAI_TERCATAT_ON,
                                       NVL(OJK.NILAI_TERCATAT_OFF, 0) NILAI_TERCATAT_OFF,
                                       CASE
                                           WHEN RESERVED_VARCHAR_4 IN
                                                ('PENEMPATAN PADA BANK INDONESIA',
                                                 'T/DVALAS',
                                                 'T/DIDR')
                                               THEN
                                               'PENEMPATAN PADA BANK INDONESIA'
                                           WHEN (DATA_SOURCE = 'ILS'
                                               AND NVL(SEGMENT, ' ') NOT LIKE '%BG%')
                                               OR (DATA_SOURCE = 'ILS'
                                                   AND PRODUCT_CODE IN ('221',
                                                                        '222',
                                                                        '223',
                                                                        '224'))
                                               OR (DATA_SOURCE = 'CRD')
                                               OR (DATA_SOURCE IN ('PBMM', 'KTP') AND SEGMENT LIKE '%PBMM%')
                                               THEN
                                               'CREDIT YANG DIBERIKAN'
                                           WHEN PRODUCT_GROUP LIKE '%REPO%'
                                               THEN
                                               'TAGIHAN REVERSE REPO'
                                           WHEN (DATA_SOURCE = 'KTP' AND SEGMENT LIKE '%BOND%')
                                               OR (DATA_SOURCE = 'BTRD'
                                                   AND PRODUCT_CODE IN ('SFF',
                                                                        'CAC',
                                                                        'CAD',
                                                                        'CAL',
                                                                        'ENG',
                                                                        'ENH',
                                                                        'ENL',
                                                                        'ENN',
                                                                        'ENO',
                                                                        'ENW',
                                                                        'ENX',
                                                                        'ENZ',
                                                                        'LAC',
                                                                        'LAD',
                                                                        'LAG',
                                                                        'LAL',
                                                                        'OCD',
                                                                        'OCN',
                                                                        'SAC',
                                                                        'SAD',
                                                                        'SAL',
                                                                        'SFO',
                                                                        '211',
                                                                        '212',
                                                                        '213',
                                                                        '214',
                                                                        'EUN'))
                                               OR (DATA_SOURCE = 'BTRD' AND PRODUCT_CODE IN ('SDF'))
                                               OR (DATA_SOURCE = 'BTRD' AND PRODUCT_CODE = '078')
                                               OR (DATA_SOURCE = 'BTRD' AND PRODUCT_CODE = '079')
                                               OR (DATA_SOURCE = 'BTRD'
                                                   AND PRODUCT_CODE IN ('LAC-E',
                                                                        'LAC-L',
                                                                        'SAC-E',
                                                                        'SAC-L'))
                                               OR (DATA_SOURCE = 'KTP' AND PRODUCT_GROUP = 'MUTFUND')
                                               OR (DATA_SOURCE = 'KTP' AND RESERVED_VARCHAR_4 = 'TRD')
                                               THEN
                                               'SURAT BERHARGA YANG DIMILIKI'
                                           WHEN (DATA_SOURCE = 'KTP'
                                               AND NVL(SEGMENT, ' ') NOT LIKE '%BOND%'
                                               AND NVL(PRODUCT_GROUP, ' ') NOT LIKE '%REPO%')
                                               OR (DATA_SOURCE = 'RKN')
                                               THEN
                                               'PENEMPATAN PADA BANK LAIN'
                                           WHEN (DATA_SOURCE = 'BTRD'
                                               AND PRODUCT_CODE IN ('ABV',
                                                                    'AC1',
                                                                    'AC2',
                                                                    'AC3',
                                                                    'AC4',
                                                                    'AC5',
                                                                    'AC6',
                                                                    'AC7',
                                                                    'AC8',
                                                                    'ACA',
                                                                    'ACC',
                                                                    'ACI',
                                                                    'ACJ',
                                                                    'ACK',
                                                                    'ACM',
                                                                    'ACO',
                                                                    'ACP',
                                                                    'ACQ',
                                                                    'ACR',
                                                                    'ACS',
                                                                    'ACT',
                                                                    'ACU',
                                                                    'ACW',
                                                                    'ACX',
                                                                    'ADC',
                                                                    'AFI',
                                                                    'AFV',
                                                                    'AL1',
                                                                    'AL2',
                                                                    'APF',
                                                                    'ASB',
                                                                    'ASC',
                                                                    'ATL',
                                                                    'ATS',
                                                                    'AUL',
                                                                    'AUP',
                                                                    'AUV',
                                                                    'AUW',
                                                                    'DTL',
                                                                    'DTS',
                                                                    'IL2',
                                                                    'IL3',
                                                                    'IL4',
                                                                    'IL5',
                                                                    'IL6',
                                                                    'IL7',
                                                                    'IL8',
                                                                    'ILA',
                                                                    'ILB',
                                                                    'ILC',
                                                                    'ILD',
                                                                    'ILE',
                                                                    'ILJ',
                                                                    'ILK',
                                                                    'ILM',
                                                                    'ILN',
                                                                    'ILO',
                                                                    'ILP',
                                                                    'ILR',
                                                                    'ILS',
                                                                    'ILT',
                                                                    'ILU',
                                                                    'ILW',
                                                                    'ILX',
                                                                    'SL1',
                                                                    'SL2',
                                                                    'UAL',
                                                                    'UAP',
                                                                    'UAV',
                                                                    'UAW',
                                                                    'UBV',
                                                                    'UFV',
                                                                    'UPF',
                                                                    'USB',
                                                                    'USC',
                                                                    'SAB',
                                                                    'CAB',
                                                                    'LAB',
                                                                    'ACN '))
                                               AND (DATA_SOURCE = 'BTRD'
                                                   AND NOT (PRODUCT_TYPE IN ('ILS',
                                                                             'ILR',
                                                                             'ILC',
                                                                             'ILU',
                                                                             'ILK')
                                                       AND PRODUCT_CODE IN ('ILS',
                                                                            'ILR',
                                                                            'ILC',
                                                                            'ILU',
                                                                            'ILK')))
                                               THEN
                                               'AKSEPTASI'
                                           WHEN (DATA_SOURCE = 'BTRD'
                                               AND PRODUCT_CODE IN ('LDP',
                                                                    'LDG',
                                                                    'LDL',
                                                                    'SDL',
                                                                    'CDL'))
                                               THEN
                                               'ASET KEUANGAN LAINNYA'
                                           WHEN (NVL(DATA_SOURCE, ' ') <> 'LIMIT'
                                               AND NVL(SEGMENT, ' ') NOT LIKE '%BG%')
                                               THEN
                                               'ASET KEUANGAN LAINNYA'
                                           END
                                                                      ASET_KEUANGAN,
                                       CASE
                                           WHEN (DATA_SOURCE = 'ILS'
                                               AND NVL(SEGMENT, ' ') NOT LIKE '%BG%')
                                               OR (DATA_SOURCE = 'ILS'
                                                   AND PRODUCT_CODE IN ('221',
                                                                        '222',
                                                                        '223',
                                                                        '224'))
                                               OR (DATA_SOURCE = 'CRD')
                                               OR (DATA_SOURCE IN ('PBMM', 'KTP') AND SEGMENT LIKE '%PBMM%')
                                               THEN
                                               'KELONGGARAN TARIK'
                                           WHEN (DATA_SOURCE = 'BTRD')
                                               THEN
                                               'IRREVOCABLE LC'
                                           WHEN DATA_SOURCE = 'ILS' AND SEGMENT LIKE '%BG%'
                                               THEN
                                               'GARANSI YANG DIBERIKAN'
                                           WHEN DATA_SOURCE = 'LIMIT' AND PRODUCT_CODE = 'KLG'
                                               THEN
                                               'GARANSI YANG DIBERIKAN'
                                           WHEN DATA_SOURCE = 'LIMIT' AND PRODUCT_CODE <> 'KLG'
                                               THEN
                                               'KELONGGARAN TARIK'
                                           END
                                                                      TRA,
                                       -- 600084535 - Perubahan agar CIS 104014030000 di hardcode menjadi Aset Keuangan Lainnya
                                       CASE
                                           WHEN CUSTOMER_NUMBER in ( '0230012','104014030000')
                                               THEN
                                               'ASET KEUANGAN LAINNYA'
                                           WHEN RESERVED_VARCHAR_4 IN
                                                ('PENEMPATAN PADA BANK INDONESIA',
                                                 'T/DVALAS',
                                                 'T/DIDR')
                                               THEN
                                               'PENEMPATAN PADA BANK INDONESIA'
                                           WHEN (DATA_SOURCE = 'ILS'
                                               AND NVL(SEGMENT, ' ') NOT LIKE '%BG%')
                                               OR (DATA_SOURCE = 'ILS'
                                                   AND PRODUCT_CODE IN ('221',
                                                                        '222',
                                                                        '223',
                                                                        '224'))
                                               OR (DATA_SOURCE = 'CRD')
                                               OR (DATA_SOURCE IN ('PBMM', 'KTP') AND SEGMENT LIKE '%PBMM%')
                                               THEN
                                               'CREDIT YANG DIBERIKAN'
                                           WHEN PRODUCT_GROUP LIKE '%REPO%'
                                               THEN
                                               'TAGIHAN REVERSE REPO'
                                           WHEN (DATA_SOURCE = 'KTP' AND SEGMENT LIKE '%BOND%' AND
                                                 PRODUCT_CODE NOT IN ('NCD-IDR'))
                                               --OR (DATA_SOURCE = 'KTP' AND PRODUCT_GROUP = 'MUTFUND' AND PRODUCT_CODE NOT IN ('NCD-IDR'))
                                               OR (DATA_SOURCE = 'KTP' AND SEGMENT = 'MUTUAL FUND' AND
                                                   PRODUCT_CODE NOT IN ('NCD-IDR'))
                                               OR (DATA_SOURCE = 'KTP' AND RESERVED_VARCHAR_4 = 'TRD' AND
                                                   PRODUCT_CODE NOT IN ('NCD-IDR'))
                                               THEN
                                               'SURAT BERHARGA YANG DIMILIKI'
                                           WHEN (DATA_SOURCE = 'KTP'
                                               AND NVL(SEGMENT, ' ') NOT LIKE '%BOND%'
                                               AND NVL(SEGMENT, ' ') <> 'MUTUAL FUND'
                                               AND NVL(PRODUCT_GROUP, ' ') NOT LIKE '%REPO%')
                                               OR (DATA_SOURCE = 'RKN' AND CUSTOMER_NUMBER NOT IN ( '0230012','104014030000') AND
                                                   NVL(SEGMENT, ' ') NOT LIKE '%NOSTRO%')
                                               OR (DATA_SOURCE = 'KTP' AND PRODUCT_CODE IN ('NCD-IDR'))
                                               THEN
                                               'PENEMPATAN PADA BANK LAIN'
                                           WHEN (DATA_SOURCE = 'RKN' AND NVL(SEGMENT, ' ') LIKE '%NOSTRO%' AND
                                                 CUSTOMER_NUMBER NOT IN ( '0230012','104014030000'))
                                               THEN
                                               'GIRO PADA BANK LAIN'
                                           WHEN (DATA_SOURCE = 'BTRD' AND LBU_FORM = 7)
                                               THEN
                                               'WESEL TAGIH'
                                           WHEN (DATA_SOURCE = 'BTRD' AND LBU_FORM = 22)
                                               THEN
                                               'ASET KEUANGAN LAINNYA'
                                           WHEN (DATA_SOURCE = 'BTRD' AND LBU_FORM = 10)
                                               OR (DATA_SOURCE = 'BTRD'
                                                   AND PRODUCT_CODE IN ('ILS',
                                                                        'ILR',
                                                                        'ILC',
                                                                        'ILU',
                                                                        'ILK'))
                                               THEN
                                               'AKSEPTASI'
                                           WHEN (NVL(DATA_SOURCE, ' ') <> 'LIMIT'
                                               AND NVL(SEGMENT, ' ') NOT LIKE '%BG%'
                                               AND NVL(SEGMENT, ' ') NOT LIKE '%NOSTRO%')
                                               THEN
                                               'ASET KEUANGAN LAINNYA'
                                           END
                                                                      ASET_KEUANGAN_NEW
    FROM (SELECT IMA.MASTERID,
                 IMA.DOWNLOAD_DATE,
                 IMA.DATA_SOURCE,
                 IMA.PRODUCT_CODE,
                 IMA.PRODUCT_TYPE,
                 IMA.PRODUCT_GROUP,
                 IMA.SEGMENT,
                 IMA.CUSTOMER_NUMBER,
                 IMA.RESERVED_VARCHAR_4,
                 IMA.NILAI_TERCATAT_ON,
                 IMA.NILAI_TERCATAT_OFF,
                 NOM.LBU_FORM
          FROM (SELECT A.MASTERID,
                       A.DOWNLOAD_DATE,
                       A.DATA_SOURCE,
                       A.PRODUCT_CODE,
                       A.PRODUCT_TYPE,
                       A.PRODUCT_GROUP,
                       A.SEGMENT,
                       A.CUSTOMER_NUMBER,
                       'PENEMPATAN PADA BANK INDONESIA'
                           AS RESERVED_VARCHAR_4,
                       CASE
                           WHEN NVL(A.FAIR_VALUE_AMOUNT, 0) = 0
                               THEN
                                   NVL(A.OUTSTANDING, 0)
                                   * NVL(A.EXCHANGE_RATE, 1)
                           ELSE
                                   NVL(A.FAIR_VALUE_AMOUNT, 0)
                                   * NVL(A.EXCHANGE_RATE, 1)
                           END
                           AS NILAI_TERCATAT_ON, --- 6
                       (CASE
                            WHEN (A.DATA_SOURCE = 'ILS'
                                AND A.PRODUCT_CODE LIKE 'B%')
                                OR (A.DATA_SOURCE = 'KTP'
                                    AND NVL(A.RESERVED_FLAG_1, 0) =
                                        0)
                                OR (A.DATA_SOURCE = 'BTRD'
                                    AND NVL(A.RESERVED_FLAG_1, 0) =
                                        0)
                                THEN
                                NVL(A.OUTSTANDING, 0)
                            ELSE
                                NVL(A.RESERVED_AMOUNT_14, 0)
                            END
                           * NVL(A.EXCHANGE_RATE, 1))
                              NILAI_TERCATAT_OFF ---7
                FROM IFRS.GTMP_IFRS_MASTER_ACCOUNT_UPD A
                WHERE DOWNLOAD_DATE = V_CURRDATE
                  AND CUSTOMER_NUMBER IN
                      ('00020409707', '00019597820')
                  AND DATA_SOURCE = 'KTP'
                  AND PRODUCT_GROUP = 'MM'
                  AND PRODUCT_CODE <> 'DEPO-BI'
                UNION
                SELECT A.MASTERID,
                       A.DOWNLOAD_DATE,
                       A.DATA_SOURCE,
                       A.PRODUCT_CODE,
                       A.PRODUCT_TYPE,
                       A.PRODUCT_GROUP,
                       A.SEGMENT,
                       A.CUSTOMER_NUMBER,
                       CASE
                           WHEN A.RESERVED_VARCHAR_5 IN
                                ('T/DVALAS', 'T/DIDR')
                               THEN
                               RESERVED_VARCHAR_5
                           ELSE
                               A.RESERVED_VARCHAR_4
                           END
                           AS RESERVED_VARCHAR_4,
                       (CASE
                            WHEN (A.DATA_SOURCE = 'ILS'
                                AND A.PRODUCT_CODE LIKE 'B%')
                                OR (A.DATA_SOURCE = 'KTP'
                                    AND NVL(A.RESERVED_FLAG_1, 0) = 0)
                                OR (A.DATA_SOURCE = 'BTRD'
                                    AND NVL(A.RESERVED_FLAG_1, 0) = 0)
                                THEN
                                0
                            ELSE
                                    CASE
                                        WHEN A.IFRS9_CLASS = 'AMORT'
                                            THEN
                                            CASE
                                                WHEN A.DATA_SOURCE = 'KTP'
                                                    THEN
                                                    CASE
                                                        WHEN NVL(
                                                                     A.RESERVED_AMOUNT_8,
                                                                     0) = 0
                                                            THEN
                                                            A.OUTSTANDING
                                                        ELSE
                                                            A.RESERVED_AMOUNT_8
                                                        END
                                                ELSE
                                                    CASE
                                                        WHEN NVL(
                                                                     A.FAIR_VALUE_AMOUNT,
                                                                     0) = 0
                                                            THEN
                                                            A.OUTSTANDING
                                                        ELSE
                                                            A.FAIR_VALUE_AMOUNT
                                                        END
                                                END
                                        ELSE
                                            NVL(A.MARKET_RATE, 0)
                                        END
                                    * NVL(A.EXCHANGE_RATE, 1)
                           END)
                           AS NILAI_TERCATAT_ON,
                       (CASE
                            WHEN (A.DATA_SOURCE = 'ILS'
                                AND A.PRODUCT_CODE LIKE 'B%')
                                OR (A.DATA_SOURCE = 'KTP'
                                    AND NVL(A.RESERVED_FLAG_1, 0) =
                                        0)
                                OR (A.DATA_SOURCE = 'BTRD'
                                    AND NVL(A.RESERVED_FLAG_1, 0) =
                                        0)
                                THEN
                                NVL(A.OUTSTANDING, 0)
                            ELSE
                                NVL(C.UNUSED_AMOUNT, 0)
                            END
                           * NVL(A.EXCHANGE_RATE, 1))
                           AS NILAI_TERCATAT_OFF
                FROM IFRS.GTMP_IFRS_MASTER_ACCOUNT_UPD A
                         LEFT JOIN (SELECT * FROM IFRS.IFRS_ECL_RESULT_DETAIL WHERE DOWNLOAD_DATE = V_CURRDATE) C
                                   ON C.MASTERID = A.MASTERID
                                       AND C.DOWNLOAD_DATE = A.DOWNLOAD_DATE
                                       AND C.DOWNLOAD_DATE = V_CURRDATE
                WHERE A.DOWNLOAD_DATE = V_CURRDATE
                  AND NVL(A.OUTSTANDING, 0) >= 0
                  AND (A.DATA_SOURCE <> 'KTP'
                    OR (A.DATA_SOURCE = 'KTP'
                        AND (A.PRODUCT_GROUP <> 'MM'
                            OR A.CUSTOMER_NUMBER NOT IN
                               ('00020409707',
                                '00019597820'))))
                  AND (A.DATA_SOURCE = 'CRD'
                    OR A.DATA_SOURCE = 'ILS'
                    OR A.DATA_SOURCE = 'KTP'
                    OR A.DATA_SOURCE = 'RKN'
                    OR A.DATA_SOURCE = 'LIMIT'
                    OR A.DATA_SOURCE = 'PBMM'
                    OR (A.DATA_SOURCE = 'BTRD'
                        AND RESERVED_VARCHAR_23 <> 0))) IMA
                   LEFT JOIN IFRS.GTMP_IFRS_NOMINATIVE_UPDATE NOM
                             ON IMA.DOWNLOAD_DATE = NOM.REPORT_DATE
                                 AND IMA.MASTERID = NOM.MASTERID
                                 AND IMA.DATA_SOURCE = NOM.DATA_SOURCE
                                 AND NOM.REPORT_DATE = V_CURRDATE) OJK;
    COMMIT;

    /*NO NEED TO GATHER STATS - RAL*/
--    DBMS_STATS.UNLOCK_TABLE_STATS ( OWNNAME=>'IFRS', TABNAME=>'IFRS_IFRS_NOMINATIVE_UPD_TMP');
--    DBMS_STATS.GATHER_TABLE_STATS ( OWNNAME=>'IFRS', TABNAME=>'IFRS_IFRS_NOMINATIVE_UPD_TMP',DEGREE=>2);


--   MERGE /*+ index(IFRS_NOMINATIVE IDX_IFRS_NOMINATIVE) */  INTO IFRS.IFRS_NOMINATIVE NOMI
--        USING (SELECT *
--                 FROM IFRS.IFRS_IFRS_NOMINATIVE_UPD_TMP
--                WHERE DOWNLOAD_DATE = V_CURRDATE) OJ
--           ON (    NOMI.REPORT_DATE = V_CURRDATE
--               AND NOMI.MASTERID = OJ.MASTERID
--               AND NOMI.REPORT_DATE = OJ.DOWNLOAD_DATE
--               AND NOMI.DATA_SOURCE = OJ.DATA_SOURCE)
--   WHEN MATCHED
--   THEN
--      UPDATE SET NOMI.RESERVED_AMOUNT_6 = OJ.NILAI_TERCATAT_ON,
--                 NOMI.RESERVED_AMOUNT_7 = OJ.NILAI_TERCATAT_OFF,
--                 NOMI.RESERVED_VARCHAR_1 = OJ.ASET_KEUANGAN,
--                 NOMI.RESERVED_VARCHAR_2 = OJ.TRA,
--                 NOMI.RESERVED_VARCHAR_5 = OJ.ASET_KEUANGAN_NEW
--    WHERE NOMI.REPORT_DATE = V_CURRDATE;

    /*Simplifying Query - RAL Added*/
    MERGE INTO IFRS.IFRS_NOMINATIVE NOMI
    USING IFRS.IFRS_IFRS_NOMINATIVE_UPD_TMP OJ
    ON (NOMI.MASTERID = OJ.MASTERID
        AND NOMI.REPORT_DATE = OJ.DOWNLOAD_DATE
        AND NOMI.DATA_SOURCE = OJ.DATA_SOURCE)
    WHEN MATCHED
        THEN
        UPDATE
        SET NOMI.RESERVED_AMOUNT_6  = OJ.NILAI_TERCATAT_ON,
            NOMI.RESERVED_AMOUNT_7  = OJ.NILAI_TERCATAT_OFF,
            NOMI.RESERVED_VARCHAR_1 = OJ.ASET_KEUANGAN,
            NOMI.RESERVED_VARCHAR_2 = OJ.TRA,
            NOMI.RESERVED_VARCHAR_5 = OJ.ASET_KEUANGAN_NEW;


    COMMIT;

    -- AMORT99_FS

    update IFRS.IFRS_NOMINATIVE
    set RESERVED_AMOUNT_8 = 0
    where REPORT_DATE=V_CURRDATE and DATA_SOURCE in ('ILS','LIMIT');

    commit;

    MERGE INTO IFRS.IFRS_NOMINATIVE NOMI
    USING IFRS.IFRS_AMORT99_FS A99
    ON (NOMI.MASTERID=A99.MASTER_ID)
    WHEN MATCHED
        THEN
        UPDATE SET NOMI.RESERVED_AMOUNT_8 = nvl(A99.AMORTISATION_99_FILTERED_CCY,0)
    WHERE NOMI.REPORT_DATE=V_CURRDATE;

    COMMIT;
    /*




    MERGE INTO IFRS.IFRS_NOMINATIVE NOMI
    USING (SELECT DISTINCT OJK.MASTERID,
                  OJK.DOWNLOAD_DATE,
                  OJK.DATA_SOURCE,
         OJK.PRODUCT_CODE,
         OJK.PRODUCT_TYPE,
         OJK.CUSTOMER_NUMBER,
                  NVL(OJK.NILAI_TERCATAT_ON,0) NILAI_TERCATAT_ON ,
                  NVL(OJK.NILAI_TERCATAT_OFF,0) NILAI_TERCATAT_OFF,
                  CASE
                    WHEN RESERVED_VARCHAR_4 IN ('PENEMPATAN PADA BANK INDONESIA',
                                                'T/DVALAS',
                                                'T/DIDR') THEN
                     'PENEMPATAN PADA BANK INDONESIA'
                    WHEN (DATA_SOURCE = 'ILS' AND NVL(SEGMENT, ' ') NOT LIKE '%BG%') OR
                         (DATA_SOURCE = 'ILS' AND PRODUCT_CODE IN ('221', '222', '223', '224')) OR (DATA_SOURCE = 'CRD') OR
                         (DATA_SOURCE = 'KTP' AND SEGMENT LIKE '%PBMM%') THEN
                     'CREDIT YANG DIBERIKAN'
                    WHEN PRODUCT_GROUP LIKE '%REPO%' THEN
                     'TAGIHAN REVERSE REPO'
                    WHEN (DATA_SOURCE = 'KTP' AND SEGMENT LIKE '%BOND%') OR
                         (DATA_SOURCE = 'BTRD' AND PRODUCT_CODE IN ('SFF', 'CAC', 'CAD', 'CAL', 'ENG', 'ENH', 'ENL',
                                                                    'ENN', 'ENO', 'ENW', 'ENX', 'ENZ', 'LAC', 'LAD',
                                                                    'LAG', 'LAL', 'OCD', 'OCN', 'SAC', 'SAD', 'SAL',
                                                                    'SFO', '211', '212', '213', '214', 'EUN')) OR
                         (DATA_SOURCE = 'BTRD' AND PRODUCT_CODE IN ('SDF')) OR
                         (DATA_SOURCE = 'BTRD' AND PRODUCT_CODE = '078') OR
                         (DATA_SOURCE = 'BTRD' AND PRODUCT_CODE = '079') OR
                         (DATA_SOURCE = 'BTRD' AND PRODUCT_CODE IN ('LAC-E', 'LAC-L', 'SAC-E', 'SAC-L')) OR
                         (DATA_SOURCE = 'KTP' AND PRODUCT_GROUP = 'MUTFUND') OR
                         (DATA_SOURCE = 'KTP' AND RESERVED_VARCHAR_4 = 'TRD') THEN
                     'SURAT BERHARGA YANG DIMILIKI'
                    WHEN (DATA_SOURCE = 'KTP' AND NVL(SEGMENT, ' ') NOT LIKE '%BOND%' AND
                         NVL(PRODUCT_GROUP, ' ') NOT LIKE '%REPO%') OR (DATA_SOURCE = 'RKN') THEN
                     'PENEMPATAN PADA BANK LAIN'
                    WHEN (DATA_SOURCE = 'BTRD' AND PRODUCT_CODE IN ('ABV', 'AC1', 'AC2', 'AC3', 'AC4', 'AC5', 'AC6',
                                                                    'AC7', 'AC8', 'ACA', 'ACC', 'ACI', 'ACJ', 'ACK',
                                                                    'ACM', 'ACO', 'ACP', 'ACQ', 'ACR', 'ACS', 'ACT',
                                                                    'ACU', 'ACW', 'ACX', 'ADC', 'AFI', 'AFV', 'AL1',
                                                                    'AL2', 'APF', 'ASB', 'ASC', 'ATL', 'ATS', 'AUL',
                                                                    'AUP', 'AUV', 'AUW', 'DTL', 'DTS', 'IL2', 'IL3',
                                                                    'IL4', 'IL5', 'IL6', 'IL7', 'IL8', 'ILA', 'ILB',
                                                                    'ILC', 'ILD', 'ILE', 'ILJ', 'ILK', 'ILM', 'ILN',
                                                                    'ILO', 'ILP', 'ILR', 'ILS', 'ILT', 'ILU', 'ILW',
                                                                    'ILX', 'SL1', 'SL2', 'UAL', 'UAP', 'UAV', 'UAW',
                                                                    'UBV', 'UFV', 'UPF', 'USB', 'USC', 'SAB', 'CAB',
                                                                    'LAB', 'ACN ')) AND
                         (DATA_SOURCE = 'BTRD' AND NOT (PRODUCT_TYPE IN ('ILS', 'ILR', 'ILC', 'ILU', 'ILK') AND
                          PRODUCT_CODE IN ('ILS', 'ILR', 'ILC', 'ILU', 'ILK'))) THEN
                     'AKSEPTASI'
                    WHEN (DATA_SOURCE = 'BTRD' AND PRODUCT_CODE IN ('LDP', 'LDG', 'LDL', 'SDL', 'CDL')) THEN
                     'ASET KEUANGAN LAINNYA'
                    WHEN (NVL(DATA_SOURCE, ' ') <> 'LIMIT' AND NVL(SEGMENT,' ') NOT LIKE '%BG%') THEN
                     'ASET KEUANGAN LAINNYA'
                  END ASET_KEUANGAN,
                  CASE
                    WHEN (DATA_SOURCE = 'ILS' AND NVL(SEGMENT, ' ') NOT LIKE '%BG%') OR
                         (DATA_SOURCE = 'ILS' AND PRODUCT_CODE IN ('221', '222', '223', '224')) OR
                         (DATA_SOURCE = 'CRD') OR
                         (DATA_SOURCE = 'KTP' AND SEGMENT LIKE '%PBMM%') THEN
                     'KELONGGARAN TARIK'
                    WHEN (DATA_SOURCE = 'BTRD') THEN
                     'IRREVOCABLE LC'
                    WHEN DATA_SOURCE = 'ILS' AND SEGMENT LIKE '%BG%' THEN
                     'GARANSI YANG DIBERIKAN'
                    WHEN DATA_SOURCE = 'LIMIT' AND PRODUCT_CODE = 'KLG' THEN
                     'GARANSI YANG DIBERIKAN'
                    WHEN DATA_SOURCE = 'LIMIT' AND PRODUCT_CODE <> 'KLG' THEN
                     'KELONGGARAN TARIK'
                  END TRA,
                  CASE
                    WHEN CUSTOMER_NUMBER = '0230012' THEN 'ASET KEUANGAN LAINNYA'
                    WHEN RESERVED_VARCHAR_4 IN ('PENEMPATAN PADA BANK INDONESIA',
                                                'T/DVALAS',
                                                'T/DIDR') THEN
                     'PENEMPATAN PADA BANK INDONESIA'
                    WHEN (DATA_SOURCE = 'ILS' AND NVL(SEGMENT, ' ') NOT LIKE '%BG%') OR
                         (DATA_SOURCE = 'ILS' AND PRODUCT_CODE IN ('221', '222', '223', '224')) OR (DATA_SOURCE = 'CRD') OR
                         (DATA_SOURCE = 'KTP' AND SEGMENT LIKE '%PBMM%') THEN
                     'CREDIT YANG DIBERIKAN'
                    WHEN PRODUCT_GROUP LIKE '%REPO%' THEN
                     'TAGIHAN REVERSE REPO'
                    WHEN (DATA_SOURCE = 'KTP' AND SEGMENT LIKE '%BOND%') OR
                         (DATA_SOURCE = 'KTP' AND PRODUCT_GROUP = 'MUTFUND') OR
                         (DATA_SOURCE = 'KTP' AND RESERVED_VARCHAR_4 = 'TRD') THEN
                     'SURAT BERHARGA YANG DIMILIKI'
                    WHEN (DATA_SOURCE = 'KTP' AND NVL(SEGMENT, ' ') NOT LIKE '%BOND%' AND
                         NVL(PRODUCT_GROUP, ' ') NOT LIKE '%REPO%') OR
                         (DATA_SOURCE = 'RKN') OR
                         (DATA_SOURCE = 'KTP' AND PRODUCT_CODE IN ('NCD-IDR'))THEN
                     'PENEMPATAN PADA BANK LAIN'
                    WHEN (DATA_SOURCE = 'BTRD' AND 7 = (SELECT LBU_FORM FROM IFRS.IFRS_NOMINATIVE N WHERE data_source = 'BTRD' AND N.REPORT_DATE = V_CURRDATE AND N.MASTERID = OJK.MASTERID )) THEN
                     'WESEL TAGIH'
                    WHEN (DATA_SOURCE = 'BTRD' AND 22 = (SELECT LBU_FORM FROM IFRS.IFRS_NOMINATIVE N WHERE data_source = 'BTRD' AND N.REPORT_DATE = V_CURRDATE AND N.MASTERID = OJK.MASTERID )) THEN
                     'ASET KEUANGAN LAINNYA'
         WHEN (DATA_SOURCE = 'BTRD' AND 10 = (SELECT LBU_FORM FROM IFRS.IFRS_NOMINATIVE N WHERE data_source = 'BTRD' AND N.REPORT_DATE = V_CURRDATE AND N.MASTERID = OJK.MASTERID )) OR
                         (DATA_SOURCE = 'BTRD' AND PRODUCT_CODE IN ('ILS', 'ILR', 'ILC', 'ILU', 'ILK')) THEN
                     'AKSEPTASI'
         WHEN (NVL(DATA_SOURCE, ' ') <> 'LIMIT' AND NVL(SEGMENT,' ') NOT LIKE '%BG%') THEN
                     'ASET KEUANGAN LAINNYA'
         END ASET_KEUANGAN_NEW
             FROM (SELECT A.MASTERID,
                          A.DOWNLOAD_DATE,
                          A.DATA_SOURCE,
                          A.PRODUCT_CODE,
                          A.PRODUCT_TYPE,
                          A.PRODUCT_GROUP,
                          A.SEGMENT,
                          A.CUSTOMER_NUMBER,
                          'PENEMPATAN PADA BANK INDONESIA' AS RESERVED_VARCHAR_4,
                          CASE WHEN NVL(A.FAIR_VALUE_AMOUNT, 0) = 0 THEN
                             NVL(A.OUTSTANDING, 0) * NVL(A.EXCHANGE_RATE, 1)
                            ELSE
                             NVL(A.FAIR_VALUE_AMOUNT, 0) * NVL(A.EXCHANGE_RATE, 1)
                          END AS NILAI_TERCATAT_ON,  --- 6
                          (CASE WHEN (A.DATA_SOURCE = 'ILS' AND A.PRODUCT_CODE LIKE 'B%') OR
                                 (A.DATA_SOURCE = 'KTP' AND NVL(A.RESERVED_FLAG_1, 0) = 0) OR
                                 (A.DATA_SOURCE = 'BTRD' AND NVL(A.RESERVED_FLAG_1, 0) = 0) THEN
                             NVL(A.OUTSTANDING, 0)
                            ELSE
                             NVL(A.RESERVED_AMOUNT_14, 0)
                          END * NVL(A.EXCHANGE_RATE, 1)) NILAI_TERCATAT_OFF  ---7
                     FROM IFRS.IFRS_MASTER_ACCOUNT A
                    WHERE DOWNLOAD_DATE = V_CURRDATE
                      AND CUSTOMER_NUMBER IN ('00020409707',
                                              '00019597820')
                      AND DATA_SOURCE = 'KTP'
                      AND PRODUCT_GROUP = 'MM'
                      AND PRODUCT_CODE <> 'DEPO-BI'
                      UNION
    SELECT A.MASTERID,
           A.DOWNLOAD_DATE,
           A.DATA_SOURCE,
           A.PRODUCT_CODE,
           A.PRODUCT_TYPE,
           A.PRODUCT_GROUP,
           A.SEGMENT,
           A.CUSTOMER_NUMBER,
           CASE WHEN A.RESERVED_VARCHAR_5 IN ('T/DVALAS',
                                              'T/DIDR') THEN
              RESERVED_VARCHAR_5
             ELSE
              A.RESERVED_VARCHAR_4
           END AS RESERVED_VARCHAR_4,
           (CASE WHEN (A.DATA_SOURCE = 'ILS' AND A.PRODUCT_CODE LIKE 'B%') OR
                  (A.DATA_SOURCE = 'KTP' AND NVL(A.RESERVED_FLAG_1, 0) = 0) OR
                  (A.DATA_SOURCE = 'BTRD' AND NVL(A.RESERVED_FLAG_1, 0) = 0) THEN
              0
             ELSE
              CASE WHEN A.IFRS9_CLASS = 'AMORT' THEN
                 CASE WHEN A.DATA_SOURCE = 'KTP' THEN
                    CASE WHEN NVL(A.RESERVED_AMOUNT_8, 0) = 0 THEN
                       A.OUTSTANDING
                      ELSE
                       A.RESERVED_AMOUNT_8
                    END
                 ELSE
                    CASE WHEN NVL(A.FAIR_VALUE_AMOUNT, 0) = 0 THEN
                       A.OUTSTANDING
                      ELSE
                       A.FAIR_VALUE_AMOUNT
                    END
                 END
              ELSE
                 NVL(A.MARKET_RATE, 0)
              END * NVL(A.EXCHANGE_RATE, 1)
           END) AS NILAI_TERCATAT_ON,
           (CASE WHEN (A.DATA_SOURCE = 'ILS' AND A.PRODUCT_CODE LIKE 'B%') OR
                      (A.DATA_SOURCE = 'KTP' AND NVL(A.RESERVED_FLAG_1, 0) = 0) OR
                      (A.DATA_SOURCE = 'BTRD' AND NVL(A.RESERVED_FLAG_1, 0) = 0) THEN
              NVL(A.OUTSTANDING, 0)
             ELSE
              NVL(C.UNUSED_AMOUNT, 0)
           END * NVL(A.EXCHANGE_RATE, 1)) AS NILAI_TERCATAT_OFF
      FROM IFRS.IFRS_MASTER_ACCOUNT A
      LEFT JOIN IFRS.IFRS_ECL_RESULT_DETAIL C
        ON A.MASTERID = C.MASTERID
       AND A.DOWNLOAD_DATE = C.DOWNLOAD_DATE
     WHERE A.DOWNLOAD_DATE = V_CURRDATE
       AND NVL(A.OUTSTANDING, 0) >= 0
       AND (A.DATA_SOURCE <> 'KTP' OR
           (A.DATA_SOURCE = 'KTP' AND (A.PRODUCT_GROUP <> 'MM' OR
            A.CUSTOMER_NUMBER NOT IN ('00020409707', '00019597820'))))
       AND (A.DATA_SOURCE = 'CRD' OR
           A.DATA_SOURCE = 'ILS' OR
           A.DATA_SOURCE = 'KTP' OR
           A.DATA_SOURCE = 'RKN' OR
           A.DATA_SOURCE = 'LIMIT' OR
           A.DATA_SOURCE = 'BTRD' AND
           RESERVED_VARCHAR_23 <> 0)
                      ) OJK) OJ
    ON (NOMI.REPORT_DATE = V_CURRDATE AND NOMI.MASTERID = OJ.MASTERID AND NOMI.REPORT_DATE = OJ.DOWNLOAD_DATE AND NOMI.DATA_SOURCE = OJ.DATA_SOURCE)
    WHEN MATCHED THEN
      UPDATE SET NOMI.RESERVED_AMOUNT_6 = OJ.NILAI_TERCATAT_ON,
                 NOMI.RESERVED_AMOUNT_7 = OJ.NILAI_TERCATAT_OFF,
                 NOMI.RESERVED_VARCHAR_1 = OJ.ASET_KEUANGAN,
                 NOMI.RESERVED_VARCHAR_2 = OJ.TRA,
                 NOMI.RESERVED_VARCHAR_5 = OJ.ASET_KEUANGAN_NEW;

    COMMIT;
    */

/*
  UPDATE IFRS.IFRS_nominative
     SET ECL_ON_BS_CCY = NVL(RESERVED_AMOUNT_1, 0),
         ECL_ON_BS_LCL = NVL(RESERVED_AMOUNT_1, 0) * NVL(EXCHANGE_RATE, 1)
   where REPORT_DATE = V_CURRDATE
     AND ECL_ON_BS_CCY > RESERVED_AMOUNT_1;

  COMMIT;
*/
END;