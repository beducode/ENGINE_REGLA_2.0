CREATE OR REPLACE PROCEDURE SP_UPDATE_LAST_PAYMENT_DATE
AS
    V_CURRDATE DATE;
BEGIN

    SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;

    MERGE INTO GTMP_IFRS_MASTER_ACCOUNT A
         USING (SELECT  DISTINCT
                        DOWNLOAD_DATE,
                        MASTERID,
                        ACCOUNT_NUMBER,
                        COMPONENT_TYPE,
                        MAX (INCREMENTS) INCREMENTS,
                        PMT_DATE,
                        MAX (DATE_START) DATE_START,
                        MAX (DATE_END) DATE_END
                FROM IFRS_MASTER_PAYMENT_SETTING
                WHERE DOWNLOAD_DATE = V_CURRDATE
                     AND V_CURRDATE BETWEEN DATE_START AND DATE_END
                     AND COMPONENT_TYPE <> 1
                GROUP BY DOWNLOAD_DATE,
                         MASTERID,
                         ACCOUNT_NUMBER,
                         COMPONENT_TYPE,
                         PMT_DATE
                UNION ALL
                SELECT  DISTINCT
                        A.DOWNLOAD_DATE,
                        A.MASTERID,
                        A.ACCOUNT_NUMBER,
                        A.COMPONENT_TYPE,
                        A.INCREMENTS,
                        A.PMT_DATE,
                        A.DATE_START,
                        A.DATE_END
                FROM IFRS_MASTER_PAYMENT_SETTING A
                JOIN
                (SELECT MASTERID, MAX(DATE_END) MAX_DATE_END , MAX(INCREMENTS)INCREMENTS
                 FROM IFRS_MASTER_PAYMENT_SETTING
                 WHERE DOWNLOAD_DATE = V_CURRDATE
                     AND DATE_END > V_CURRDATE
                     AND COMPONENT_TYPE <> 1
                     AND MASTERID NOT IN
                     (
                         SELECT MASTERID
                         FROM IFRS_MASTER_PAYMENT_SETTING
                         WHERE DOWNLOAD_DATE = V_CURRDATE
                         AND V_CURRDATE BETWEEN DATE_START AND DATE_END
                         AND COMPONENT_TYPE <> 1
                     )
                 GROUP BY MASTERID
                ) B
                ON A.DOWNLOAD_DATE = V_CURRDATE
                     AND A.MASTERID = B.MASTERID
                     AND A.DATE_END = B.MAX_DATE_END
                     AND A.COMPONENT_TYPE <> 1
                     AND A.TIMES_ORG > 0
                     AND A.INCREMENTS = B.INCREMENTS
                ) B
            ON (    A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
                AND A.DOWNLOAD_DATE = V_CURRDATE
                AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE)
    WHEN MATCHED
    THEN
       UPDATE SET
          LAST_PAYMENT_DATE =
             CASE
                WHEN REVOLVING_FLAG = '1'
                THEN A.LOAN_START_DATE
                ELSE
                   CASE
                      WHEN B.INCREMENTS = 1
                      THEN
                         CASE
                            WHEN
                                PMT_DATE < EXTRACT (DAY FROM LAST_DAY(TO_DATE(
                                1 || '-' || EXTRACT (MONTH FROM ADD_MONTHS (DOWNLOAD_DATE,-INCREMENTS)) || '-' ||
                                EXTRACT (YEAR FROM ADD_MONTHS (DOWNLOAD_DATE, -INCREMENTS)),'DD-MM-YYYY')))
                            THEN
                                TO_DATE(
                                PMT_DATE || '-' || EXTRACT (MONTH FROM ADD_MONTHS (DOWNLOAD_DATE,-INCREMENTS)) || '-' ||
                                EXTRACT (YEAR FROM ADD_MONTHS (DOWNLOAD_DATE, -INCREMENTS)),'DD-MM-YYYY')
                            ELSE
                                LAST_DAY(TO_DATE(
                                1 || '-' || EXTRACT (MONTH FROM ADD_MONTHS (DOWNLOAD_DATE,-INCREMENTS)) || '-' ||
                                EXTRACT (YEAR FROM ADD_MONTHS (DOWNLOAD_DATE, -INCREMENTS)),'DD-MM-YYYY'))
                         END
                      WHEN B.INCREMENTS = 3
                      THEN
                         CASE
                            WHEN
                                EXTRACT (MONTH FROM DATE_START) <= 3
                            THEN
                                CASE
                                    WHEN
                                        PMT_DATE < EXTRACT (DAY FROM LAST_DAY(TO_DATE(
                                        1 || '-' || EXTRACT (MONTH FROM ADD_MONTHS (DATE_START,INCREMENTS * 3)) || '-' ||
                                        EXTRACT (YEAR FROM ADD_MONTHS (DOWNLOAD_DATE, -INCREMENTS)),'DD-MM-YYYY')))
                                    THEN
                                        TO_DATE(
                                        PMT_DATE || '-' || EXTRACT (MONTH FROM ADD_MONTHS (DATE_START,INCREMENTS * 3)) || '-' ||
                                        EXTRACT (YEAR FROM ADD_MONTHS (DOWNLOAD_DATE, -INCREMENTS)),'DD-MM-YYYY')
                                    ELSE
                                        LAST_DAY(TO_DATE(
                                        1 || '-' || EXTRACT (MONTH FROM ADD_MONTHS (DATE_START,INCREMENTS * 3)) || '-' ||
                                        EXTRACT (YEAR FROM ADD_MONTHS (DOWNLOAD_DATE, -INCREMENTS)),'DD-MM-YYYY'))
                                END
                            WHEN
                                EXTRACT (MONTH FROM DATE_START) <= 6
                            THEN
                                CASE
                                    WHEN
                                        PMT_DATE < EXTRACT (DAY FROM LAST_DAY(TO_DATE(
                                        1 || '-' || EXTRACT (MONTH FROM ADD_MONTHS (DATE_START,INCREMENTS * 2)) || '-' ||
                                        EXTRACT (YEAR FROM ADD_MONTHS (DOWNLOAD_DATE, -INCREMENTS)),'DD-MM-YYYY')))
                                    THEN
                                        TO_DATE(
                                        PMT_DATE || '-' || EXTRACT (MONTH FROM ADD_MONTHS (DATE_START,INCREMENTS * 2)) || '-' ||
                                        EXTRACT (YEAR FROM ADD_MONTHS (DOWNLOAD_DATE, -INCREMENTS)),'DD-MM-YYYY')
                                    ELSE
                                        LAST_DAY(TO_DATE(
                                        1 || '-' || EXTRACT (MONTH FROM ADD_MONTHS (DATE_START,INCREMENTS * 2)) || '-' ||
                                        EXTRACT (YEAR FROM ADD_MONTHS (DOWNLOAD_DATE, -INCREMENTS)),'DD-MM-YYYY'))
                                END
                            WHEN
                                EXTRACT (MONTH FROM DATE_START) <= 9
                            THEN
                               CASE
                                   WHEN
                                            PMT_DATE < EXTRACT (DAY FROM LAST_DAY(TO_DATE(
                                            1 || '-' || EXTRACT (MONTH FROM ADD_MONTHS (DATE_START,INCREMENTS * 1)) || '-' ||
                                            EXTRACT (YEAR FROM ADD_MONTHS (DOWNLOAD_DATE, -INCREMENTS)),'DD-MM-YYYY')))
                                   THEN
                                            TO_DATE(
                                            PMT_DATE || '-' || EXTRACT (MONTH FROM ADD_MONTHS (DATE_START,INCREMENTS * 1)) || '-' ||
                                            EXTRACT (YEAR FROM ADD_MONTHS (DOWNLOAD_DATE, -INCREMENTS)),'DD-MM-YYYY')
                                   ELSE
                                            LAST_DAY(TO_DATE(
                                            1 || '-' || EXTRACT (MONTH FROM ADD_MONTHS (DATE_START,INCREMENTS * 1)) || '-' ||
                                            EXTRACT (YEAR FROM ADD_MONTHS (DOWNLOAD_DATE, -INCREMENTS)),'DD-MM-YYYY'))
                               END
                         END
                      WHEN B.INCREMENTS = 6
                      THEN
                         CASE
                            WHEN EXTRACT (MONTH FROM DATE_START) <= 6
                            THEN
                               CASE
                                   WHEN
                                            PMT_DATE < EXTRACT (DAY FROM LAST_DAY(TO_DATE(
                                            1 || '-' || EXTRACT (MONTH FROM ADD_MONTHS (DATE_START,INCREMENTS * 1)) || '-' ||
                                            EXTRACT (YEAR FROM ADD_MONTHS (DOWNLOAD_DATE, -INCREMENTS)),'DD-MM-YYYY')))
                                   THEN
                                            TO_DATE(
                                            PMT_DATE || '-' || EXTRACT (MONTH FROM ADD_MONTHS (DATE_START,INCREMENTS * 1)) || '-' ||
                                            EXTRACT (YEAR FROM ADD_MONTHS (DOWNLOAD_DATE, -INCREMENTS)),'DD-MM-YYYY')
                                   ELSE
                                            LAST_DAY(TO_DATE(
                                            1 || '-' || EXTRACT (MONTH FROM ADD_MONTHS (DATE_START,INCREMENTS * 1)) || '-' ||
                                            EXTRACT (YEAR FROM ADD_MONTHS (DOWNLOAD_DATE, -INCREMENTS)),'DD-MM-YYYY'))
                               END
                            ELSE
                               CASE
                                   WHEN
                                            PMT_DATE < EXTRACT (DAY FROM LAST_DAY(TO_DATE(
                                            1 || '-' || EXTRACT (MONTH FROM DATE_START) || '-' ||
                                            EXTRACT (YEAR FROM ADD_MONTHS (DOWNLOAD_DATE, -INCREMENTS)),'DD-MM-YYYY')))
                                   THEN
                                            TO_DATE(
                                            PMT_DATE || '-' || EXTRACT (MONTH FROM DATE_START) || '-' ||
                                            EXTRACT (YEAR FROM ADD_MONTHS (DOWNLOAD_DATE, -INCREMENTS)),'DD-MM-YYYY')
                                   ELSE
                                            LAST_DAY(TO_DATE(
                                            1 || '-' || EXTRACT (MONTH FROM DATE_START) || '-' ||
                                            EXTRACT (YEAR FROM ADD_MONTHS (DOWNLOAD_DATE, -INCREMENTS)),'DD-MM-YYYY'))
                               END
                         END
                      WHEN B.INCREMENTS = 12
                      THEN
                         CASE
                            WHEN
                                PMT_DATE < EXTRACT (DAY FROM LAST_DAY(TO_DATE(
                                1 || '-' || EXTRACT (MONTH FROM ADD_MONTHS (DATE_START,-INCREMENTS)) || '-' ||
                                EXTRACT (YEAR FROM ADD_MONTHS (DOWNLOAD_DATE, -INCREMENTS)),'DD-MM-YYYY')))
                            THEN
                                TO_DATE(
                                PMT_DATE || '-' || EXTRACT (MONTH FROM ADD_MONTHS (DATE_START,-INCREMENTS)) || '-' ||
                                EXTRACT (YEAR FROM ADD_MONTHS (DOWNLOAD_DATE, -INCREMENTS)),'DD-MM-YYYY')
                            ELSE
                                LAST_DAY(TO_DATE(
                                1 || '-' || EXTRACT (MONTH FROM ADD_MONTHS (DATE_START,-INCREMENTS)) || '-' ||
                                EXTRACT (YEAR FROM ADD_MONTHS (DOWNLOAD_DATE, -INCREMENTS)),'DD-MM-YYYY'))
                         END
                   END
             END;

        COMMIT;
END;