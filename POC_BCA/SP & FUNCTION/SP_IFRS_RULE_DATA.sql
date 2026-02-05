CREATE OR REPLACE PROCEDURE SP_IFRS_RULE_DATA(v_CURRDATE_PARAM DATE DEFAULT NULL)
AS
  V_CURRDATE  date;
  V_TABLE_NAME varchar2(30);
  V_STR_SQL varchar2(4000);
  V_STR_SQL_RULE varchar2(4000);
  V_RULE_ID varchar2(250);
  V_RULE_TYPE varchar2(50);

  CURSOR i IS
  SELECT
    RULE_ID,
    RULE_TYPE,
    TABLE_NAME,
    PD_RULES_QRY_RESULT
  FROM GTMP_IFRS_SCENARIO_GEN_QUERY;

BEGIN
  SELECT NVL(v_CURRDATE_PARAM, CURRDATE)
  INTO V_CURRDATE
  FROM IFRS_PRC_DATE_AMORT;

  EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_IFRS_SCENARIO_DATA';

--  DELETE FROM IFRS_SCENARIO_DATA
--  WHERE DOWNLOAD_DATE = V_CURRDATE;
--
--  COMMIT;

  OPEN i;
  FETCH i INTO V_RULE_ID, V_RULE_TYPE, V_TABLE_NAME, V_STR_SQL_RULE;

  WHILE i%FOUND
  LOOP

    V_STR_SQL := '  INSERT /*+ PARALLEL(8) */ INTO  GTMP_IFRS_SCENARIO_DATA (
                                       DOWNLOAD_DATE,
                                       RULE_ID,
                                       RULE_TYPE,
                                       MASTERID,
                                       GROUP_SEGMENT,
                                       SEGMENT,
                                       SUB_SEGMENT,
                                       RATING_CODE,
                                       DAY_PAST_DUE,
                                       BI_COLLECTABILITY,
                                       WRITEOFF_FLAG,
                                       ACCOUNT_NUMBER,
                                       ACCOUNT_STATUS,
                                       CUSTOMER_NUMBER,
                                       CUSTOMER_NAME,
                                       EXCHANGE_RATE,
                                       IMPAIRED_FLAG,
                                       OUTSTANDING,
                                       DATA_SOURCE,
                                       KEY_TMP_IMA
                                       )
      SELECT /*+ PARALLEL(8) */ DOWNLOAD_DATE,
                   ''' || V_RULE_ID || ''',
                   ''' || V_RULE_TYPE || ''',
              A.MASTERID,
              A.GROUP_SEGMENT,
              A.SEGMENT,
              A.SUB_SEGMENT,
              A.RATING_CODE,
              A.DAY_PAST_DUE,
              A.BI_COLLECTABILITY,
              A.WRITEOFF_FLAG,
              A.ACCOUNT_NUMBER,
              A.ACCOUNT_STATUS,
              A.CUSTOMER_NUMBER,
              A.CUSTOMER_NAME,
              A.EXCHANGE_RATE,
              A.IMPAIRED_FLAG,
              A.OUTSTANDING,
              A.DATA_SOURCE,
              '' '' KEY_TMP_IMA
        FROM  ' || v_TABLE_NAME || ' A '
        || CASE WHEN V_RULE_TYPE IN ('1','2','3') THEN ' JOIN IFRS_MASTER_PRODUCT_PARAM B ON TRIM(NVL(A.PRODUCT_CODE,''-'')) = B.PRD_CODE' END ||
     ' WHERE  A.DOWNLOAD_DATE =  '''
    ||V_CURRDATE
    || ''' AND ('
    || RTRIM(NVL(V_STR_SQL_RULE, '')) || ')';

    EXECUTE IMMEDIATE V_STR_SQL;

    COMMIT;

    FETCH i INTO V_RULE_ID, V_RULE_TYPE, V_TABLE_NAME, V_STR_SQL_RULE;

  END LOOP;
  CLOSE i;
END
;