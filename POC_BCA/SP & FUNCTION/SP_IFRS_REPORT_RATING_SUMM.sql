CREATE OR REPLACE PROCEDURE SP_IFRS_REPORT_RATING_SUMM AS
    v_curr_date DATE;
    v_count     NUMBER;
BEGIN

    SELECT CURRDATE
    INTO v_curr_date
    FROM IFRS.IFRS_PRC_DATE;

    SELECT COUNT(1)
    INTO v_count
    FROM IFRS.IFRS_REPORT_RATING_SUMM
    WHERE DOWNLOAD_DATE = v_curr_date;

    IF v_count > 0 THEN
        DELETE FROM IFRS.IFRS_REPORT_RATING_SUMM WHERE DOWNLOAD_DATE = v_curr_date;
        DBMS_OUTPUT.PUT_LINE('Deleted ' || SQL%ROWCOUNT || ' existing records from IFRS_REPORT_RATING_SUMM');
    END IF;

    DBMS_OUTPUT.PUT_LINE('Starting IFRS Report Rating Summary for date: ' || TO_CHAR(v_curr_date, 'DD-MON-YYYY'));

    INSERT /*+ PARALLEL(8) */ INTO IFRS.IFRS_REPORT_RATING_SUMM (DOWNLOAD_DATE,
                                         RATING,
                                         LANCAR,
                                         DPK_LESS_EQUAL_30,
                                         DPK_30_MORE,
                                         CKPN_LGD,
                                         CKPN_365,
                                         CKPN_100_LGD,
                                         CKPN_100_H,
                                         TOTAL)
    SELECT /*+ PARALLEL(8) */ v_curr_date,
           RATING,
           NVL("01. Lancar", 0),
           NVL("02. DPK <=30", 0),
           NVL("03. DPK 30+", 0),
           NVL("04. CKPN LGD", 0),
           NVL("05. CKPN 365", 0),
           NVL("06. CKPN 100 DPD", 0),
           NVL("07. CKPN 100 H", 0),
           NVL("01. Lancar", 0) + NVL("02. DPK <=30", 0) + NVL("03. DPK 30+", 0) +
           NVL("04. CKPN LGD", 0) + NVL("05. CKPN 365", 0) +
           NVL("06. CKPN 100 DPD", 0) + NVL("07. CKPN 100 H", 0)
    FROM (SELECT RATING, DPD_GROUP, OUTSTANDING
          FROM IFRS.IFRS_REPORT_RATING
          WHERE DOWNLOAD_DATE = (SELECT CURRDATE FROM IFRS_PRC_DATE))
        PIVOT (
        SUM(OUTSTANDING)
        FOR DPD_GROUP IN (
            '01. Lancar' AS "01. Lancar",
            '02. DPK <=30' AS "02. DPK <=30",
            '03. DPK 30+' AS "03. DPK 30+",
            '04. CKPN LGD' AS "04. CKPN LGD",
            '05. CKPN 365' AS "05. CKPN 365",
            '06. CKPN 100 DPD' AS "06. CKPN 100 DPD",
            '07. CKPN 100 H' AS "07. CKPN 100 H"
            )
        )

    UNION ALL

    SELECT /*+ PARALLEL(8) */  v_curr_date,
           'TOTAL',
           SUM(NVL("01. Lancar", 0)),
           SUM(NVL("02. DPK <=30", 0)),
           SUM(NVL("03. DPK 30+", 0)),
           SUM(NVL("04. CKPN LGD", 0)),
           SUM(NVL("05. CKPN 365", 0)),
           SUM(NVL("06. CKPN 100 DPD", 0)),
           SUM(NVL("07. CKPN 100 H", 0)),
           SUM(
                   NVL("01. Lancar", 0) + NVL("02. DPK <=30", 0) + NVL("03. DPK 30+", 0) +
                   NVL("04. CKPN LGD", 0) + NVL("05. CKPN 365", 0) +
                   NVL("06. CKPN 100 DPD", 0) + NVL("07. CKPN 100 H", 0)
           )
    FROM (SELECT RATING, DPD_GROUP, OUTSTANDING
          FROM IFRS.IFRS_REPORT_RATING
          WHERE DOWNLOAD_DATE = (SELECT CURRDATE FROM IFRS_PRC_DATE))
        PIVOT (
        SUM(OUTSTANDING)
        FOR DPD_GROUP IN (
            '01. Lancar' AS "01. Lancar",
            '02. DPK <=30' AS "02. DPK <=30",
            '03. DPK 30+' AS "03. DPK 30+",
            '04. CKPN LGD' AS "04. CKPN LGD",
            '05. CKPN 365' AS "05. CKPN 365",
            '06. CKPN 100 DPD' AS "06. CKPN 100 DPD",
            '07. CKPN 100 H' AS "07. CKPN 100 H"
            )
        );

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        RAISE;
END;