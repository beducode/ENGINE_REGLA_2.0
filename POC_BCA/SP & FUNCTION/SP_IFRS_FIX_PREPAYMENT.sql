CREATE OR REPLACE PROCEDURE SP_IFRS_FIX_PREPAYMENT (p_START_DATE DATE)
AS
    v_START_DATE DATE;
    v_END_DATE   DATE;
    v_LOOP_DATE  DATE;
BEGIN
    v_START_DATE := LAST_DAY(p_START_DATE);
    v_LOOP_DATE := LAST_DAY(p_START_DATE);

    SELECT LAST_DAY(ADD_MONTHS(CURRDATE,-1))
      INTO v_END_DATE
      FROM IFRS.IFRS_PRC_DATE;

    WHILE v_LOOP_DATE <= v_END_DATE LOOP

        ------------------------------------------------------------------
        -- A: seg lama 543/545 -> seg baru 253
        ------------------------------------------------------------------
        INSERT /*+ PARALLEL(IFRS_PREPAYMENT_DETAIL,4) */
        INTO IFRS.IFRS_PREPAYMENT_DETAIL (
            DOWNLOAD_DATE,
            REPORT_DATE,
            MASTERID,
            ACCOUNT_NUMBER,
            SEGMENTATION_ID,
            PREPAYMENT_SEGMENT,
            CURRENCY,
            OUTSTANDING,
            PREPAYMENT,
            SCHEDULE,
            ACTUAL,
            RATE_AMOUNT,
            SMM,
            INCREMENTS,
            DURATION,
            COMPONENT_TYPE,
            DATA_SOURCE,
            CREATEDBY,
            CREATEDDATE,
            CREATEDHOST,
            UPDATEDBY,
            UPDATEDDATE,
            UPDATEDHOST,
            CUSTOMER_NUMBER,
            CUSTOMER_NAME
        )
        SELECT
            a.DOWNLOAD_DATE,
            a.REPORT_DATE,
            a.MASTERID,
            a.ACCOUNT_NUMBER,
            '253' AS SEGMENTATION_ID,
            a.PREPAYMENT_SEGMENT,
            a.CURRENCY,
            a.OUTSTANDING,
            a.PREPAYMENT,
            a.SCHEDULE,
            a.ACTUAL,
            a.RATE_AMOUNT,
            a.SMM,
            a.INCREMENTS,
            a.DURATION,
            a.COMPONENT_TYPE,
            a.DATA_SOURCE,
            'SYSTEM' AS CREATEDBY,
            SYSDATE  AS CREATEDDATE,
            'SYSTEM' AS CREATEDHOST,
            NULL AS UPDATEDBY,
            NULL AS UPDATEDDATE,
            NULL AS UPDATEDHOST,
            a.CUSTOMER_NUMBER,
            a.CUSTOMER_NAME
        FROM IFRS.IFRS_PREPAYMENT_DETAIL a
        WHERE a.DOWNLOAD_DATE = v_LOOP_DATE
          AND a.SEGMENTATION_ID IN ('543','545')
          -- hanya account yg muncul SEKALI untuk seg lama pd tanggal ini
          AND a.ACCOUNT_NUMBER IN (
                SELECT ACCOUNT_NUMBER
                FROM IFRS.IFRS_PREPAYMENT_DETAIL b
                WHERE b.DOWNLOAD_DATE = v_LOOP_DATE
                  AND b.SEGMENTATION_ID IN ('543','545')
                GROUP BY ACCOUNT_NUMBER
                HAVING COUNT(*) = 1
          )
          -- hindari duplikasi seg baru pd tanggal ini
          AND NOT EXISTS (
                SELECT 1
                FROM IFRS.IFRS_PREPAYMENT_DETAIL b
                WHERE b.DOWNLOAD_DATE   = v_LOOP_DATE
                  AND b.ACCOUNT_NUMBER  = a.ACCOUNT_NUMBER
                  AND b.SEGMENTATION_ID = '253'
          );

        ------------------------------------------------------------------
        -- B: seg lama 547/549 -> seg baru 252
        ------------------------------------------------------------------
        INSERT /*+ PARALLEL(IFRS_PREPAYMENT_DETAIL,4) */
        INTO IFRS.IFRS_PREPAYMENT_DETAIL (
            DOWNLOAD_DATE,
            REPORT_DATE,
            MASTERID,
            ACCOUNT_NUMBER,
            SEGMENTATION_ID,
            PREPAYMENT_SEGMENT,
            CURRENCY,
            OUTSTANDING,
            PREPAYMENT,
            SCHEDULE,
            ACTUAL,
            RATE_AMOUNT,
            SMM,
            INCREMENTS,
            DURATION,
            COMPONENT_TYPE,
            DATA_SOURCE,
            CREATEDBY,
            CREATEDDATE,
            CREATEDHOST,
            UPDATEDBY,
            UPDATEDDATE,
            UPDATEDHOST,
            CUSTOMER_NUMBER,
            CUSTOMER_NAME
        )
        SELECT
            a.DOWNLOAD_DATE,
            a.REPORT_DATE,
            a.MASTERID,
            a.ACCOUNT_NUMBER,
            '252' AS SEGMENTATION_ID,
            a.PREPAYMENT_SEGMENT,
            a.CURRENCY,
            a.OUTSTANDING,
            a.PREPAYMENT,
            a.SCHEDULE,
            a.ACTUAL,
            a.RATE_AMOUNT,
            a.SMM,
            a.INCREMENTS,
            a.DURATION,
            a.COMPONENT_TYPE,
            a.DATA_SOURCE,
            'SYSTEM' AS CREATEDBY,
            SYSDATE  AS CREATEDDATE,
            'SYSTEM' AS CREATEDHOST,
            NULL AS UPDATEDBY,
            NULL AS UPDATEDDATE,
            NULL AS UPDATEDHOST,
            a.CUSTOMER_NUMBER,
            a.CUSTOMER_NAME
        FROM IFRS.IFRS_PREPAYMENT_DETAIL a
        WHERE a.DOWNLOAD_DATE = v_LOOP_DATE
          AND a.SEGMENTATION_ID IN ('547','549')
          AND a.ACCOUNT_NUMBER IN (
                SELECT ACCOUNT_NUMBER
                FROM IFRS.IFRS_PREPAYMENT_DETAIL b
                WHERE b.DOWNLOAD_DATE = v_LOOP_DATE
                  AND b.SEGMENTATION_ID IN ('547','549')
                GROUP BY ACCOUNT_NUMBER
                HAVING COUNT(*) = 1
          )
          AND NOT EXISTS (
                SELECT 1
                FROM IFRS.IFRS_PREPAYMENT_DETAIL b
                WHERE b.DOWNLOAD_DATE   = v_LOOP_DATE
                  AND b.ACCOUNT_NUMBER  = a.ACCOUNT_NUMBER
                  AND b.SEGMENTATION_ID = '252'
          );

        COMMIT;

        v_LOOP_DATE := ADD_MONTHS(v_LOOP_DATE, 1);
    END LOOP;

    v_LOOP_DATE := LAST_DAY(p_START_DATE);

    WHILE v_LOOP_DATE <= v_END_DATE LOOP

    DELETE IFRS.IFRS_PREPAYMENT_HEADER WHERE DOWNLOAD_DATE = v_LOOP_DATE;

    COMMIT;

    INSERT INTO IFRS_PREPAYMENT_HEADER (DOWNLOAD_DATE,
                                       SEGMENTATION_ID,
                                       SEGMENTATION_NAME,
                                       PREPAYMENT_RULE_ID,
                                       PREPAYMENT_RULE_NAME,
                                       AVERAGE_SMM,
                                       PREPAYMENT_RATE,
                                       DURATION)
            SELECT v_LOOP_DATE DOWNLOAD_DATE,
                   A.SEGMENTATION_ID,
                   NVL (C.SEGMENT, '-') SEGMENTATION_NAME,
                   NVL (D.PKID, 0) PREPAYMENT_RULE_ID,
                   NVL (D.PREPAYMENT_RULE_NAME, '-') PREPAYMENT_RULE_NAME,
                   AVG (A.SMM) AVERAGE_SMM,
                   ROUND (
                      1
                      - POWER (
                           (1 - AVG (A.SMM)),
                           CASE
                              WHEN A.INCREMENTS = 1 THEN 12
                              WHEN A.INCREMENTS = 3 THEN 4
                              WHEN A.INCREMENTS = 6 THEN 2
                              WHEN A.INCREMENTS = 12 THEN 1
                           END),
                      4)
                      PREPAYMENT_RATE,
                   CASE
                      WHEN A.INCREMENTS = 1 THEN 12
                      WHEN A.INCREMENTS = 3 THEN 4
                      WHEN A.INCREMENTS = 6 THEN 2
                      WHEN A.INCREMENTS = 12 THEN 1
                   END
                      DURATION
              FROM IFRS_PREPAYMENT_DETAIL A
                   LEFT JOIN IFRS_MSTR_SEGMENT_RULES_HEADER C
                      ON A.SEGMENTATION_ID = C.PKID
                   LEFT JOIN IFRS_PREPAYMENT_RULES_CONFIG D
                      ON C.PKID = D.SEGMENTATION_ID
             WHERE     A.DOWNLOAD_DATE <= v_LOOP_DATE
                   AND D.AVERAGE_METHOD = 'Simple'
                   AND A.SMM >= 0
                   AND A.DOWNLOAD_dATE <= v_LOOP_DATE
                   AND A.DURATION = 12
          GROUP BY A.SEGMENTATION_ID,
                   C.SEGMENT,
                   D.PKID,
                   D.PREPAYMENT_RULE_NAME,
                   A.INCREMENTS;
        COMMIT;

        v_LOOP_DATE := ADD_MONTHS(v_LOOP_DATE, 1);

        UPDATE IFRS.IFRS_DATE_DAY1
            SET CURRDATE = v_LOOP_DATE,
                PREVDATE = ADD_MONTHS(v_LOOP_DATE, -1);

        COMMIT;

    END LOOP;
END;