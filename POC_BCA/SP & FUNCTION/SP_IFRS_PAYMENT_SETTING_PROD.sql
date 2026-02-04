CREATE OR REPLACE PROCEDURE SP_IFRS_PAYMENT_SETTING_PROD
AS
   V_CURRDATE   DATE;
   V_PREVDATE   DATE;

/*-----------------------------------------------------------------------------------------------------
SP FEEDING DATA
CREATED BY    : WILLY
CREATED DATE  : 26-12-2018
*/-----------------------------------------------------------------------------------------------------

BEGIN

    SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;
    SELECT PREVDATE INTO V_PREVDATE FROM IFRS_PRC_DATE;

    INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_IFRS_PAYMENT_SETTING_PROD' ,'');COMMIT;

   EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_MASTER_PAYMENT_SETTING';

/*
INSERT INTO IFRS_MASTER_PAYMENT_SETTING
   SELECT *
     FROM IFRS_MASTER_PAYMENT_SETTING
    WHERE     DOWNLOAD_DATE = V_PREVDATE
          AND MASTERID NOT IN (SELECT MASTERID
                                 FROM IFRS_MASTER_PAYMENT_SETTING
                                WHERE DOWNLOAD_DATE = V_CURRDATE);

COMMIT;
*/

   INSERT INTO TMP_MASTER_PAYMENT_SETTING (DOWNLOAD_DATE,
                                           MASTERID,
                                           ACCOUNT_NUMBER,
                                           COMPONENT_TYPE,
                                           REVOLVING_FLAG
                                           )
        SELECT D.DOWNLOAD_DATE,
               D.MASTERID,
               D.ACCOUNT_NUMBER,
               D.COMPONENT_TYPE,
               E.REVOLVING_FLAG

          FROM IFRS_MASTER_PAYMENT_SETTING D
               JOIN (
               SELECT A.DOWNLOAD_DATE,
                              A.ACCOUNT_NUMBER
                         FROM IFRS_MASTER_PAYMENT_SETTING A
                              JOIN
                              (
                              SELECT DOWNLOAD_DATE,
                                        ACCOUNT_NUMBER,
                                        COMPONENT_TYPE,
                                        MAX (DATE_START) DS
                                   FROM IFRS_MASTER_PAYMENT_SETTING
                                  WHERE DOWNLOAD_DATE = V_CURRDATE
                               GROUP BY DOWNLOAD_DATE,
                                        ACCOUNT_NUMBER,
                                        COMPONENT_TYPE
                                        ) B
                                 ON (    A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
                                     AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
                                     AND A.COMPONENT_TYPE = B.COMPONENT_TYPE
                                     AND A.DATE_START = B.DS
                                     )
                     GROUP BY A.DOWNLOAD_DATE,
                              A.ACCOUNT_NUMBER
                       HAVING COUNT (B.ACCOUNT_NUMBER) = 1
                       ) E
                  ON     D.DOWNLOAD_DATE = E.DOWNLOAD_DATE
                     AND D.ACCOUNT_NUMBER = E.ACCOUNT_NUMBER
                     AND D.COMPONENT_TYPE = 1
               JOIN IFRS_MASTER_ACCOUNT E
                  ON     D.DOWNLOAD_DATE = E.DOWNLOAD_DATE
                     AND D.MASTERID = E.MASTERID
                     AND E.REVOLVING_FLAG = 0
      GROUP BY D.DOWNLOAD_DATE,
               D.MASTERID,
               D.ACCOUNT_NUMBER,
               D.COMPONENT_TYPE,
               E.REVOLVING_FLAG;
   COMMIT;


MERGE INTO TMP_MASTER_PAYMENT_SETTING C
     USING (  SELECT A.DOWNLOAD_DATE,
                     A.MASTERID,
                     A.FREQUENCY,
                     A.INCREMENTS,
                     A.AMOUNT,
                     A.DATE_START
                FROM IFRS_MASTER_PAYMENT_SETTING A
                     JOIN
                     (  SELECT DOWNLOAD_DATE, ACCOUNT_NUMBER, MAX (DATE_START) DS
                          FROM IFRS_MASTER_PAYMENT_SETTING
                         WHERE DOWNLOAD_DATE = V_CURRDATE
                      GROUP BY DOWNLOAD_DATE, ACCOUNT_NUMBER) B
                        ON (    A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
                            AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
                            AND A.DATE_START = B.DS)
            GROUP BY A.DOWNLOAD_DATE,
                     A.MASTERID,
                     A.FREQUENCY,
                     A.INCREMENTS,
                     A.AMOUNT,
                     A.DATE_START) D
        ON (C.DOWNLOAD_DATE = D.DOWNLOAD_DATE AND C.MASTERID = D.MASTERID)
WHEN MATCHED
THEN
   UPDATE SET
      C.FREQUENCY = D.FREQUENCY,
      C.INCREMENTS = D.INCREMENTS,
      C.AMOUNT = D.AMOUNT;

COMMIT;

MERGE INTO TMP_MASTER_PAYMENT_SETTING A
     USING (SELECT DOWNLOAD_DATE,
                   MASTERID,
                   LOAN_DUE_DATE,
                   INITIAL_OUTSTANDING
              FROM IFRS_MASTER_ACCOUNT) B
        ON (A.DOWNLOAD_DATE = B.DOWNLOAD_DATE AND A.MASTERID = B.MASTERID AND B.DOWNLOAD_DATE = V_CURRDATE)
WHEN MATCHED
THEN
   UPDATE SET
      A.DATE_START = B.LOAN_DUE_DATE, A.DATE_END = B.LOAN_DUE_DATE, A.AMOUNT = B.INITIAL_OUTSTANDING;

COMMIT;


UPDATE TMP_MASTER_PAYMENT_SETTING
   SET COMPONENT_TYPE = 0, TIMES_ORG = 1, PMT_DATE = EXTRACT(DAY FROM DATE_END);
COMMIT;


INSERT INTO IFRS_MASTER_PAYMENT_SETTING (DOWNLOAD_DATE,
                                         MASTERID,
                                         ACCOUNT_NUMBER,
                                         COMPONENT_TYPE,
                                         INCREMENTS,
                                         AMOUNT,
                                         FREQUENCY,
                                         DATE_START,
                                         DATE_END,
                                         PMT_DATE,
                                         TIMES_ORG)
   SELECT DOWNLOAD_DATE,
          MASTERID,
          ACCOUNT_NUMBER,
          COMPONENT_TYPE,
          INCREMENTS,
          AMOUNT,
          FREQUENCY,
          DATE_START,
          DATE_END,
          PMT_DATE,
          TIMES_ORG
     FROM TMP_MASTER_PAYMENT_SETTING
    WHERE DOWNLOAD_DATE = V_CURRDATE;

COMMIT;

    INSERT  INTO IFRS_UPDATE_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_IFRS_PAYMENT_SETTING_PROD' ,'');COMMIT;*UPDATE IFRS_MASTER_ACCOUNT
   SET NEXT_PAYMENT_DATE =
          CASE
             WHEN EXTRACT (YEAR FROM NEXT_PAYMENT_DATE) = '2999'
             THEN
                LOAN_DUE_DATE
             ELSE
                NEXT_PAYMENT_DATE
          END
          WHERE DOWNLOAD_DATE = V_CURRDATE;

COMMIT;
*/
END;