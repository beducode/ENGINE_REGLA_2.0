CREATE OR REPLACE PROCEDURE SP_IFRS_UPDATE_SEGMENT_CHECK (v_DOWNLOADDATECUR  DATE DEFAULT ('1-JAN-1900'))
AS
  V_CURRDATE  date;
  v_TABLE_NAME varchar2(30);
  V_STR_SQL varchar2(4000);
  V_STR_SQL_RULE varchar2(4000);
  V_RULE_ID varchar2(250);
  v_Group_Segment varchar2(50);
  v_Segment varchar2(50);
  v_Sub_Segment varchar2(100);

  CURSOR i IS
  SELECT
    RULE_ID,
    TABLE_NAME,
    CONDITION,
    GROUP_SEGMENT,
    SEGMENT,
    SUB_SEGMENT
  FROM GTMP_SCENARIO_SEGMENT_GENQUERY;

BEGIN

IF v_DOWNLOADDATECUR = '1-JAN-1900'
  THEN
    SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;
  ELSE
    V_CURRDATE := v_DOWNLOADDATECUR;
  END IF;

  OPEN i;
  FETCH i INTO V_RULE_ID, v_TABLE_NAME, V_STR_SQL_RULE, V_Group_Segment, V_Segment, V_Sub_Segment;

  WHILE i%FOUND
  LOOP

    V_STR_SQL := '  INSERT INTO  GTMP_IFRS_SCENARIO_DATA (
                                       DOWNLOAD_DATE,
                                       SEGMENT,
                                       SUB_SEGMENT,
                                       MASTERID,
                                       ACCOUNT_NUMBER,
                                       CUSTOMER_NUMBER,
                                       RATING_CODE,
                                       KEY_TMP_IMA
                                       )
      SELECT  DOWNLOAD_DATE,
                   ''' || V_SEGMENT || ''',
                   ''' || V_SUB_SEGMENT || ''',
              MASTERID,
              ACCOUNT_NUMBER,
              CUSTOMER_NUMBER,
              RATING_CODE,
              '' '' KEY_TMP_IMA
        FROM  ' || v_TABLE_NAME || ' A
       WHERE  A.DOWNLOAD_DATE =  '''
    ||V_CURRDATE
    || ''' AND ('
    || RTRIM(NVL(V_STR_SQL_RULE, '')) || ')';


    EXECUTE IMMEDIATE V_STR_SQL;

    COMMIT;

    FETCH i INTO V_RULE_ID, v_TABLE_NAME, V_STR_SQL_RULE, V_Group_Segment, V_Segment, V_Sub_Segment;

  END LOOP;
  CLOSE i;
END
;