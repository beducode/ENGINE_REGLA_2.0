CREATE OR REPLACE PROCEDURE SP_IFRS_RULE_DATA_AMORT
AS
  V_CURRDATE  date;
  v_TABLE_NAME varchar2(30);
  V_STR_SQL varchar2(4000);
  V_STR_SQL_RULE varchar2(4000);
  V_RULE_ID varchar2(250);

  CURSOR i IS
  SELECT
    RULE_ID,
    TABLE_NAME,
    PD_RULES_QRY_RESULT
  FROM GTMP_IFRS_SCENARIO_GEN_QUERY;

BEGIN


  SELECT
    CURRDATE INTO V_CURRDATE
  FROM IFRS_PRC_DATE_AMORT;

  EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_IFRS_SCENARIO_DATA';

--  DELETE FROM IFRS_SCENARIO_DATA
--  WHERE DOWNLOAD_DATE = V_CURRDATE;
--
--  COMMIT;

  OPEN i;
  FETCH i INTO V_RULE_ID, v_TABLE_NAME, V_STR_SQL_RULE;

  WHILE i%FOUND
  LOOP

    V_STR_SQL := '  INSERT /*+ PARALLEL(8) */ INTO  GTMP_IFRS_SCENARIO_DATA (
                                       DOWNLOAD_DATE,
                                       RULE_ID,
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
                                       KEY_TMP_IMA
                                       )
      SELECT  /*+ PARALLEL(8) */ DOWNLOAD_DATE,
                   ''' || V_RULE_ID || ''',
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
              '' '' KEY_TMP_IMA
        FROM  ' || v_TABLE_NAME || ' A
       WHERE  A.DOWNLOAD_DATE =  '''
    ||V_CURRDATE
    || ''' AND ('
    || RTRIM(NVL(V_STR_SQL_RULE, '')) || ')';

    EXECUTE IMMEDIATE V_STR_SQL;

    COMMIT;

    FETCH i INTO V_RULE_ID, v_TABLE_NAME, V_STR_SQL_RULE;

  END LOOP;
  CLOSE i;
END
;