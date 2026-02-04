CREATE OR REPLACE PROCEDURE SP_IFRS_UPDATE_SEGMENT_K (V_CURRDATE  date)
AS
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
   update IFRS_MASTER_ACCOUNT_MONTHLY
    set
    segment_rule_id = 0,
    group_segment = null,
    segment = null,
    sub_segment = null,
    ccf_rule_id = 0,
    ccf_segment = null,
    lifetime_rule_id = 0,
    lifetime_segment = null,
    prepayment_rule_id = 0,
    prepayment_segment = null
    where download_date = V_CURRDATE;
    commit;

  OPEN i;
  FETCH i INTO V_RULE_ID, v_TABLE_NAME, V_STR_SQL_RULE, V_Group_Segment, V_Segment, V_Sub_Segment;

  WHILE i%FOUND
  LOOP

    V_STR_SQL := '  UPDATE IFRS_MASTER_ACCOUNT_MONTHLY A
      SET  A.SEGMENT_RULE_ID =''' || V_RULE_ID || ''',
            A.GROUP_SEGMENT = ''' || V_Group_Segment || ''',
            A.SEGMENT =  ''' || V_Segment || ''',
            A.SUB_SEGMENT =  ''' || V_Sub_Segment || '''
       WHERE A.DOWNLOAD_DATE =  '''
    ||V_CURRDATE
    || ''' AND ('
    || RTRIM(NVL(V_STR_SQL_RULE, '')) || ')';

    EXECUTE IMMEDIATE V_STR_SQL;

    COMMIT;

    FETCH i INTO V_RULE_ID, v_TABLE_NAME, V_STR_SQL_RULE, V_Group_Segment, V_Segment, V_Sub_Segment;

  END LOOP;
  CLOSE i;

    merge into ifrs_master_account_monthly a
    using IFRS_SEGMENT_MAPPING_DAY1 b
    on (a.download_date = V_CURRDATE and a.segment_rule_id = b.segment_rule_id)
    when matched then
    update set a.ccf_rule_id = b.ccf_rule_id,
    a.ccf_segment = b.ccf_segment,
    a.lifetime_rule_id = b.lifetime_rule_id,
    a.lifetime_segment = b.lifetime_segment,
    a.prepayment_rule_id = b.prepayment_rule_id,
    a.prepayment_segment = b.prepayment_segment;

    commit;
END
;