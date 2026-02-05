CREATE OR REPLACE PROCEDURE SP_IFRS_UPDATE_LIFETIME_DAY1
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


  SELECT
    CURRDATE INTO V_CURRDATE
  FROM IFRS_PRC_DATE;


  OPEN i;
  FETCH i INTO V_RULE_ID, v_TABLE_NAME, V_STR_SQL_RULE, V_Group_Segment, V_Segment, V_Sub_Segment;

  WHILE i%FOUND
  LOOP

    V_STR_SQL := '  UPDATE IFRS_MASTER_ACCOUNT_MONTHLY A
      SET  A.LIFETIME_RULE_ID =(SELECT PKID FROM ifrs_lifetime_rules_config WHERE segmentation_id = ' || V_RULE_ID || ' and LIFETIME_METHOD = 1),
            A.LIFETIME_SEGMENT =  ''' || V_Sub_Segment || '''
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