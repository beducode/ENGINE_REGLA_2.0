CREATE OR REPLACE PROCEDURE SP_IFRS_REPORT_RATING
AS
    v_curr_date        DATE;
    v_count            NUMBER;
    v_max_created_date DATE;

BEGIN
    SELECT CURRDATE
    INTO v_curr_date
    FROM IFRS.IFRS_PRC_DATE;

    DBMS_OUTPUT.PUT_LINE('Starting IFRS Report Rating for date: ' || TO_CHAR(v_curr_date, 'DD-MON-YYYY'));

    SELECT MAX(CREATED_DATE)
    INTO v_max_created_date
    FROM IFRS.TBLU_REPLACEMENT_RATING;

    UPDATE /*+ PARALLEL(8) */ IFRS.IFRS_MASTER_ACCOUNT
    SET RATING_CODE = 'UNK'
    WHERE DOWNLOAD_DATE = v_curr_date
      AND DATA_SOURCE IN ('ILS', 'LIMIT');
    commit;
    MERGE INTO IFRS.IFRS_MASTER_ACCOUNT A
    USING (SELECT DOWNLOAD_DATE, RATING_CODE, CUSTOMER_NUMBER
           FROM IFRS.IFRS_ASMR_RATING) B
    ON (A.DOWNLOAD_DATE = B.DOWNLOAD_DATE AND A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER)
    WHEN MATCHED THEN
        UPDATE
        SET A.RATING_CODE = B.RATING_CODE
        WHERE A.DOWNLOAD_DATE = v_curr_date
          AND A.DATA_SOURCE IN ('ILS', 'LIMIT');
    commit;

    DBMS_OUTPUT.PUT_LINE('Updated ' || SQL%ROWCOUNT || ' records in IFRS_MASTER_ACCOUNT');

    IFRS.SP_IFRS_GENERATE_RULE_SEGMENT('PORTFOLIO_SEG', 'M');
    IFRS.SP_IFRS_UPDATE_SEGMENT;

    DBMS_OUTPUT.PUT_LINE('Successfully generated and updated segments');
    SELECT COUNT(1)
    INTO v_count
    FROM IFRS.IFRS_REPORT_RATING
    WHERE DOWNLOAD_DATE = v_curr_date;

    IF v_count > 0 THEN
        DELETE FROM IFRS.IFRS_REPORT_RATING WHERE DOWNLOAD_DATE = v_curr_date;
        DBMS_OUTPUT.PUT_LINE('Deleted ' || SQL%ROWCOUNT || ' existing records from IFRS_REPORT_RATING');
    END IF;


    INSERT /*+ PARALLEL(8) */ INTO IFRS.IFRS_REPORT_RATING (DOWNLOAD_DATE,
                                                       CUSTOMER_NUMBER,
                                                       ACCOUNT_NUMBER,
                                                       SEGMENT,
                                                       BI_COLLECTABILITY,
                                                       DAY_PAST_DUE,
                                                       OUTSTANDING,
                                                       FLAG_H,
                                                       DPD_EOM,
                                                       DPD_DES_24,
                                                       DPD_GROUP,
                                                       RATING)
    SELECT /*+ PARALLEL(8) */ A.DOWNLOAD_DATE,
                              A.CUSTOMER_NUMBER,
                              A.ACCOUNT_NUMBER,
                              A.SEGMENT,
                              A.BI_COLLECTABILITY,
                              A.DAY_PAST_DUE,
                              (A.OUTSTANDING * A.EXCHANGE_RATE),
                              A.RESERVED_VARCHAR_9 AS FLAG_H,
                              greatest(case
                                           when A.DAY_PAST_DUE > 0 then
                                               A.DAY_PAST_DUE +
                                               (TRUNC(LAST_DAY(A.DOWNLOAD_DATE)) - TRUNC(A.DOWNLOAD_DATE))
                                           else 0
                                           end, 0) AS DPD_EOM,
                              GREATEST(
                                      DAY_PAST_DUE - TRUNC(A.DOWNLOAD_DATE - TO_DATE('31-DEC-2024', 'DD-MON-YYYY'))
                                  ,
                                      0
                              )                    AS DPD_DES_24,

                              CASE
                                  -- 01. Lancar
                                  WHEN A.BI_COLLECTABILITY = '1' AND greatest(case
                                                                                  when A.DAY_PAST_DUE > 0 then
                                                                                      A.DAY_PAST_DUE +
                                                                                      (TRUNC(LAST_DAY(A.DOWNLOAD_DATE)) - TRUNC(A.DOWNLOAD_DATE))
                                                                                  else 0
                                                                                  end, 0) <= 90 THEN '01. Lancar'
                                  -- 02. DPK <30
                                  WHEN A.BI_COLLECTABILITY = '2' AND greatest(case
                                                                                  when A.DAY_PAST_DUE > 0 then
                                                                                      A.DAY_PAST_DUE +
                                                                                      (TRUNC(LAST_DAY(A.DOWNLOAD_DATE)) - TRUNC(A.DOWNLOAD_DATE))
                                                                                  else 0
                                                                                  end, 0) <= 30 THEN '02. DPK <=30'
                                  -- 03. DPK 30+
                                  WHEN A.BI_COLLECTABILITY = '2' AND greatest(case
                                                                                  when A.DAY_PAST_DUE > 0 then
                                                                                      A.DAY_PAST_DUE +
                                                                                      (TRUNC(LAST_DAY(A.DOWNLOAD_DATE)) - TRUNC(A.DOWNLOAD_DATE))
                                                                                  else 0
                                                                                  end, 0) BETWEEN 31 AND 90
                                      THEN '03. DPK 30+'
                                  -- 07. CKPN 100 H
                                  WHEN A.BI_COLLECTABILITY IN ('3', '4', '5') AND INSTR(RESERVED_VARCHAR_9, 'H') >= 1
                                      THEN '07. CKPN 100 H'
                                  -- 05. CKPN 365
                                  WHEN A.BI_COLLECTABILITY IN ('3', '4', '5') AND greatest(case
                                                                                               when A.DAY_PAST_DUE > 0
                                                                                                   then
                                                                                                   A.DAY_PAST_DUE +
                                                                                                   (TRUNC(LAST_DAY(A.DOWNLOAD_DATE)) - TRUNC(A.DOWNLOAD_DATE))
                                                                                               else 0
                                                                                               end, 0) >= 365 AND
                                       (GREATEST(
                                               DAY_PAST_DUE -
                                               TRUNC(A.DOWNLOAD_DATE - TO_DATE('31-DEC-2024', 'DD-MON-YYYY'))
                                           ,
                                               0
                                        )) < 365
                                      THEN '05. CKPN 365'
                                  -- 06. CKPN 100 DPD
                                  WHEN A.BI_COLLECTABILITY IN ('3', '4', '5') AND greatest(case
                                                                                               when A.DAY_PAST_DUE > 0
                                                                                                   then
                                                                                                   A.DAY_PAST_DUE +
                                                                                                   (TRUNC(LAST_DAY(A.DOWNLOAD_DATE)) - TRUNC(A.DOWNLOAD_DATE))
                                                                                               else 0
                                                                                               end, 0) >= 365 AND
                                       (GREATEST(
                                               DAY_PAST_DUE -
                                               TRUNC(A.DOWNLOAD_DATE - TO_DATE('31-DEC-2024', 'DD-MON-YYYY'))
                                           ,
                                               0
                                        )) >= 365
                                      THEN '06. CKPN 100 DPD'
                                  -- 04. CKPN LGD
                                  WHEN A.BI_COLLECTABILITY IN ('3', '4', '5') OR greatest(case
                                                                                              when A.DAY_PAST_DUE > 0
                                                                                                  then
                                                                                                  A.DAY_PAST_DUE +
                                                                                                  (TRUNC(LAST_DAY(A.DOWNLOAD_DATE)) - TRUNC(A.DOWNLOAD_DATE))
                                                                                              else 0
                                                                                              end, 0) > 90
                                      THEN '04. CKPN LGD'
                                  END              AS DPD_GROUP,
                              CASE
                                  WHEN A.BI_COLLECTABILITY = '1' AND R.RATING IN ('RR9', 'RR10') AND
                                       REPLACE_R.REPLACEMENT_RATING IS NOT NULL THEN REPLACE_R.REPLACEMENT_RATING
                                  ELSE COALESCE(R.RATING, 'UNK')
                                  END              AS RATING
    FROM IFRS.IFRS_MASTER_ACCOUNT A
             LEFT JOIN (SELECT DOWNLOAD_DATE,
                               CUSTOMER_NUMBER,
                               GOL_DEB,
                               RATING_CODE AS RATING
                        FROM IFRS.IFRS_ASMR_RATING
                        WHERE DOWNLOAD_DATE = v_curr_date) R
                       ON A.DOWNLOAD_DATE = R.DOWNLOAD_DATE
                           AND A.CUSTOMER_NUMBER = R.CUSTOMER_NUMBER
             LEFT JOIN (SELECT CUSTOMER_NUMBER, REPLACEMENT_RATING
                        FROM IFRS.TBLU_REPLACEMENT_RATING
                        WHERE CREATED_DATE = v_max_created_date) REPLACE_R
                       ON A.CUSTOMER_NUMBER = REPLACE_R.CUSTOMER_NUMBER
    WHERE A.DATA_SOURCE IN ('ILS', 'LIMIT')
      and A.ACCOUNT_STATUS = 'A'
      and A.SEGMENT = 'SME'
      and A.DOWNLOAD_DATE = v_curr_date
      and not PRODUCT_CODE like '7%';

    commit;

    IFRS.SP_IFRS_REPORT_RATING_SUMM();

    DBMS_OUTPUT.PUT_LINE('Successfully insert data IFRS_REPORT_RATING');

EXCEPTION
    WHEN NO_DATA_FOUND THEN
        DBMS_OUTPUT.PUT_LINE('Error: Current date not found in IFRS_PRC_DATE table');
        ROLLBACK;
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('Error occurred: ' || SQLERRM);
        ROLLBACK;
END;