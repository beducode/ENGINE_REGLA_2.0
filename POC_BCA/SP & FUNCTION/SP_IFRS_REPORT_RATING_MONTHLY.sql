CREATE OR REPLACE PROCEDURE SP_IFRS_REPORT_RATING_MONTHLY AS
    v_curr_date        DATE;
    v_count            NUMBER;

BEGIN

    SELECT CURRDATE
    INTO v_curr_date
    FROM IFRS.IFRS_PRC_DATE;

    SELECT COUNT(1)
    INTO v_count
    FROM IFRS.IFRS_REPORT_RATING_MONTHLY
    WHERE DOWNLOAD_DATE = v_curr_date;

    IF v_count > 0 THEN
        DELETE FROM IFRS.IFRS_REPORT_RATING_MONTHLY WHERE DOWNLOAD_DATE = v_curr_date;
        DBMS_OUTPUT.PUT_LINE('Deleted ' || SQL%ROWCOUNT || ' existing records from IFRS_REPORT_RATING_MONTHLY');
    END IF;

    DBMS_OUTPUT.PUT_LINE('Starting IFRS Report Rating Summary for date: ' || TO_CHAR(v_curr_date, 'DD-MON-YYYY'));

    COMMIT;

    INSERT /*+ PARALLEL(8) */ INTO IFRS.IFRS_REPORT_RATING_MONTHLY (DOWNLOAD_DATE,
                                                            RATING,
                                                            LANCAR,
                                                            DPK_LESS_EQUAL_30,
                                                            DPK_30_MORE,
                                                            CKPN_LGD,
                                                            CKPN_365,
                                                            CKPN_100,
                                                            TOTAL)
    SELECT /*+ PARALLEL(8) */ v_curr_date,
                              RATING_CODE,
                              NVL("01. Lancar", 0),
                              NVL("02. DPK <=30", 0),
                              NVL("03. DPK 30+", 0),
                              NVL("04. CKPN LGD", 0),
                              NVL("05. CKPN 365", 0),
                              NVL("06. CKPN 100", 0),
                              NVL("01. Lancar", 0) + NVL("02. DPK <=30", 0) + NVL("03. DPK 30+", 0) +
                              NVL("04. CKPN LGD", 0) + NVL("05. CKPN 365", 0) +
                              NVL("06. CKPN 100", 0)
    FROM (SELECT /*+ PARALLEL(8) */ RATING_CODE,
                 CASE
                     -- 01. Lancar
                     WHEN BI_COLLECTABILITY = '1' THEN '01. Lancar'
                     -- 02. DPK <=30
                     WHEN BI_COLLECTABILITY = '2' AND DAY_PAST_DUE <= 30 THEN '02. DPK <=30'
                     -- 03. DPK 30+
                     WHEN BI_COLLECTABILITY = '2' AND DAY_PAST_DUE BETWEEN 31 AND 90
                         THEN '03. DPK 30+'
                     -- 05. CKPN 365
                     WHEN SPECIAL_REASON = 'CKPN 365'
                         THEN '05. CKPN 365'
                     -- 06. CKPN 100
                     WHEN SPECIAL_REASON = 'CKPN 100%'
                         THEN '06. CKPN 100'
                     -- 04. CKPN LGD
                     WHEN BI_COLLECTABILITY IN ('3', '4', '5')
                         THEN '04. CKPN LGD'
                     END AS DPD_GROUP,
                 OUTSTANDING_ON_BS_LCL
          FROM IFRS.IFRS_NOMINATIVE
          WHERE DATA_SOURCE IN ('ILS', 'LIMIT')
            and ACCOUNT_STATUS = 'A'
            and SEGMENT = 'SME'
            and REPORT_DATE = v_curr_date
            and not PRODUCT_CODE like '7%')
        PIVOT (
        SUM(OUTSTANDING_ON_BS_LCL) FOR DPD_GROUP IN (
            '01. Lancar' AS "01. Lancar",
            '02. DPK <=30' AS "02. DPK <=30",
            '03. DPK 30+' AS "03. DPK 30+",
            '04. CKPN LGD' AS "04. CKPN LGD",
            '05. CKPN 365' AS "05. CKPN 365",
            '06. CKPN 100' AS "06. CKPN 100"
            )
        )
    UNION ALL

    SELECT /*+ PARALLEL(8) */ v_curr_date,
                              'TOTAL',
                              SUM(NVL("01. Lancar", 0)),
                              SUM(NVL("02. DPK <=30", 0)),
                              SUM(NVL("03. DPK 30+", 0)),
                              SUM(NVL("04. CKPN LGD", 0)),
                              SUM(NVL("05. CKPN 365", 0)),
                              SUM(NVL("06. CKPN 100", 0)),
                              SUM(
                                      NVL("01. Lancar", 0) + NVL("02. DPK <=30", 0) + NVL("03. DPK 30+", 0) +
                                      NVL("04. CKPN LGD", 0) + NVL("05. CKPN 365", 0) +
                                      NVL("06. CKPN 100", 0)
                              )
    FROM (SELECT RATING_CODE,
                 CASE
                     -- 01. Lancar
                     WHEN BI_COLLECTABILITY = '1' THEN '01. Lancar'
                     -- 02. DPK <=30
                     WHEN BI_COLLECTABILITY = '2' AND DAY_PAST_DUE <= 30 THEN '02. DPK <=30'
                     -- 03. DPK 30+
                     WHEN BI_COLLECTABILITY = '2' AND DAY_PAST_DUE BETWEEN 31 AND 90
                         THEN '03. DPK 30+'
                     -- 05. CKPN 365
                     WHEN SPECIAL_REASON = 'CKPN 365'
                         THEN '05. CKPN 365'
                     -- 06. CKPN 100
                     WHEN SPECIAL_REASON = 'CKPN 100%'
                         THEN '06. CKPN 100'
                     -- 04. CKPN LGD
                     WHEN BI_COLLECTABILITY IN ('3', '4', '5')
                         THEN '04. CKPN LGD'
                     END AS DPD_GROUP,
                 OUTSTANDING_ON_BS_LCL
          FROM IFRS.IFRS_NOMINATIVE
          WHERE DATA_SOURCE IN ('ILS', 'LIMIT')
            and ACCOUNT_STATUS = 'A'
            and SEGMENT = 'SME'
            and REPORT_DATE = v_curr_date
            and not PRODUCT_CODE like '7%')
        PIVOT (
        SUM(OUTSTANDING_ON_BS_LCL)
        FOR DPD_GROUP IN (
            '01. Lancar' AS "01. Lancar", '02. DPK <=30' AS "02. DPK <=30", '03. DPK 30+' AS "03. DPK 30+", '04. CKPN LGD' AS "04. CKPN LGD", '05. CKPN 365' AS "05. CKPN 365", '06. CKPN 100' AS "06. CKPN 100"
            )
        );

    commit;
END;