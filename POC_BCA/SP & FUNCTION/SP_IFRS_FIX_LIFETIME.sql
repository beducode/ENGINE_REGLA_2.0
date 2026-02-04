CREATE OR REPLACE PROCEDURE SP_IFRS_FIX_LIFETIME (p_START_DATE DATE)
AS
    v_START_DATE DATE;
    v_END_DATE   DATE;
    v_LOOP_DATE  DATE;
BEGIN
    -- 1) Bulatkan start ke akhir bulan
    v_START_DATE := LAST_DAY(p_START_DATE);
    v_LOOP_DATE := LAST_DAY(p_START_DATE);

    -- 2) Ambil end_date dari prc_date
    SELECT LAST_DAY(ADD_MONTHS(CURRDATE,-1))
      INTO v_END_DATE
      FROM IFRS.IFRS_PRC_DATE;

    ------------------------------------------------------------------
    -- : segment 535/537 - AUTO MOBIL
    ------------------------------------------------------------------
    INSERT /*+ PARALLEL(IFRS_LIFETIME_DETAIL,4) */
    INTO IFRS.IFRS_LIFETIME_DETAIL (
      DOWNLOAD_DATE, ACCOUNT_NUMBER, CUSTOMER_NAME,
      CUSTOMER_NUMBER, DATA_SOURCE, LOAN_DUE_DATE,
      LOAN_START_DATE, END_DATE, LIFETIME_PERIOD,
      GOL_DEB, SEGMENTATION_ID, REVOLVING_FLAG,
      DAY_PAST_DUE_DATE, NPL_DATE,
      CREATEDBY, CREATEDDATE, CREATEDHOST,
      UPDATEDBY, UPDATEDDATE, UPDATEDHOST
    )
    SELECT
      a.DOWNLOAD_DATE,
      a.ACCOUNT_NUMBER,
      a.CUSTOMER_NAME,
      a.CUSTOMER_NUMBER,
      a.DATA_SOURCE,
      a.LOAN_DUE_DATE,
      a.LOAN_START_DATE,
      a.END_DATE,
      a.LIFETIME_PERIOD,
      a.GOL_DEB,
      '179'                                                                                         AS SEGMENTATION_ID,
      a.REVOLVING_FLAG,
      a.DAY_PAST_DUE_DATE,
      a.NPL_DATE,
      'SYSTEM'                                                                                            AS CREATEDBY,
      SYSDATE                                                                                           AS CREATEDDATE,
      'SYSTEM'                                                                                          AS CREATEDHOST,
      NULL AS UPDATEDBY,
      NULL AS UPDATEDDATE,
      NULL AS UPDATEDHOST
    FROM IFRS.IFRS_LIFETIME_DETAIL a
    WHERE a.SEGMENTATION_ID IN ('535','537')
      AND a.DOWNLOAD_DATE BETWEEN v_START_DATE AND v_END_DATE
      AND a.ACCOUNT_NUMBER IN (
        SELECT ACCOUNT_NUMBER
        FROM IFRS.IFRS_LIFETIME_DETAIL b
        WHERE b.SEGMENTATION_ID IN ('535','537')
          AND b.DOWNLOAD_DATE BETWEEN v_START_DATE AND v_END_DATE
        GROUP BY ACCOUNT_NUMBER
        HAVING COUNT(*) = 1
      )
      AND NOT EXISTS (
        SELECT 1
        FROM IFRS.IFRS_LIFETIME_DETAIL b
        WHERE b.ACCOUNT_NUMBER  = a.ACCOUNT_NUMBER
          AND b.DOWNLOAD_DATE   = a.DOWNLOAD_DATE
          AND b.SEGMENTATION_ID = '179'
      );

    ------------------------------------------------------------------
    -- B: seg 539/541 - KKB RODA 2
    ------------------------------------------------------------------
    INSERT /*+ PARALLEL(IFRS_LIFETIME_DETAIL,4) */
    INTO IFRS.IFRS_LIFETIME_DETAIL (
      DOWNLOAD_DATE, ACCOUNT_NUMBER, CUSTOMER_NAME,
      CUSTOMER_NUMBER, DATA_SOURCE, LOAN_DUE_DATE,
      LOAN_START_DATE, END_DATE, LIFETIME_PERIOD,
      GOL_DEB, SEGMENTATION_ID, REVOLVING_FLAG,
      DAY_PAST_DUE_DATE, NPL_DATE,
      CREATEDBY, CREATEDDATE, CREATEDHOST,
      UPDATEDBY, UPDATEDDATE, UPDATEDHOST
    )
    SELECT
      a.DOWNLOAD_DATE,
      a.ACCOUNT_NUMBER,
      a.CUSTOMER_NAME,
      a.CUSTOMER_NUMBER,
      a.DATA_SOURCE,
      a.LOAN_DUE_DATE,
      a.LOAN_START_DATE,
      a.END_DATE,
      a.LIFETIME_PERIOD,
      a.GOL_DEB,
      '172'                                                                                         AS SEGMENTATION_ID,
      a.REVOLVING_FLAG,
      a.DAY_PAST_DUE_DATE,
      a.NPL_DATE,
      'SYSTEM'                                                                                            AS CREATEDBY,
      SYSDATE                                                                                           AS CREATEDDATE,
      'SYSTEM'                                                                                          AS CREATEDHOST,
      NULL AS UPDATEDBY,
      NULL AS UPDATEDDATE,
      NULL AS UPDATEDHOST
    FROM IFRS.IFRS_LIFETIME_DETAIL a
    WHERE a.SEGMENTATION_ID IN ('539','541')
      AND a.DOWNLOAD_DATE BETWEEN v_START_DATE AND v_END_DATE
      AND a.ACCOUNT_NUMBER IN (
        SELECT ACCOUNT_NUMBER
        FROM IFRS.IFRS_LIFETIME_DETAIL b
        WHERE b.SEGMENTATION_ID IN ('539','541')
          AND b.DOWNLOAD_DATE BETWEEN v_START_DATE AND v_END_DATE
        GROUP BY ACCOUNT_NUMBER
        HAVING COUNT(*) = 1
      )
      AND NOT EXISTS (
        SELECT 1
        FROM IFRS.IFRS_LIFETIME_DETAIL b
        WHERE b.ACCOUNT_NUMBER  = a.ACCOUNT_NUMBER
          AND b.DOWNLOAD_DATE   = a.DOWNLOAD_DATE
          AND b.SEGMENTATION_ID = '172'
      );

    COMMIT;

    WHILE v_LOOP_DATE <= v_END_DATE LOOP

    DELETE IFRS.IFRS_LIFETIME_HEADER WHERE DOWNLOAD_DATE = v_LOOP_DATE;

    COMMIT;

    INSERT /*+ PARALLEL(12) */ INTO IFRS.IFRS_LIFETIME_HEADER (PKID,
                                        DOWNLOAD_DATE,
                                        LIFETIME_SEGMENT_ID,
                                        LIFETIME_CONFIG_ID,
                                        LIFETIME_SEGMENT,
                                        LIFETIME_PERIOD)
             SELECT 0,
                    v_LOOP_DATE,
                    B.SEGMENTATION_ID,
                    C.PKID,
                    C.LIFETIME_RULE_NAME,
                    --CASE WHEN NVL(LIFETIME_OVERRIDE,0) <> 0 THEN LIFETIME_OVERRIDE ELSE
                    CASE
                       WHEN C.LIFETIME_METHOD = 1 THEN LM1
                       WHEN C.LIFETIME_METHOD = 2 THEN LM2
                       WHEN C.LIFETIME_METHOD = 3 THEN LM3
                    END --END
                       LIFETIME_CALCULATION
               FROM  (  SELECT SEGMENTATION_ID,
                              (CEIL (AVG (LIFETIME_PERIOD))) LM1,
                              (ROUND (
                                  PERCENTILE_CONT (0.9) WITHIN GROUP (ORDER BY LIFETIME_PERIOD)))
                                 LM2,
                               (CEIL (AVG (LIFETIME_PERIOD))) LM3
                         FROM IFRS.IFRS_LIFETIME_DETAIL
                     GROUP BY SEGMENTATION_ID)B
                          JOIN IFRS.IFRS_LIFETIME_RULES_CONFIG C
                          ON B.SEGMENTATION_ID = C.SEGMENTATION_ID
           ORDER BY B.SEGMENTATION_ID, C.PKID;

    COMMIT;

    INSERT /*+ PARALLEL(12) */ INTO IFRS.IFRS_LIFETIME_HEADER (PKID,
                                          DOWNLOAD_DATE,
                                          LIFETIME_SEGMENT_ID,
                                          LIFETIME_CONFIG_ID,
                                          LIFETIME_SEGMENT,
                                          LIFETIME_PERIOD)
             SELECT 0,
                    v_LOOP_DATE,
                    C.SEGMENTATION_ID,
                    C.PKID,
                    C.LIFETIME_RULE_NAME,
                    12
               FROM IFRS_LIFETIME_RULES_CONFIG C
               WHERE PKID NOT IN (SELECT LIFETIME_CONFIG_ID FROM IFRS.IFRS_LIFETIME_HEADER WHERE DOWNLOAD_DATE = v_LOOP_DATE);

    COMMIT;

    v_LOOP_DATE := ADD_MONTHS(v_LOOP_DATE, 1);

    UPDATE IFRS.IFRS_DATE_DAY1
        SET CURRDATE = v_LOOP_DATE,
            PREVDATE = ADD_MONTHS(v_LOOP_DATE, -1);

    COMMIT;

    END LOOP;
END;