CREATE OR REPLACE PROCEDURE SP_DAY1_UPDATE_SEGMENT_IMA
as
  v_date date;
begin
    v_date := '31 JAN 2019';
    while v_date <= '31 jan 2019' loop
        update ifrs_prc_date
        set currdate = v_date;
        commit;


        update ifrs_master_account
        set
        segment_rule_id = 0,
        group_segment = null,
        segment = null,
        sub_segment = null,
        ccf_rule_id = 0,
        ccf_segment = null,
        lifetime_rule_id = 0,
        lifetime_segment = null,
        PREPAYMENT_rule_id = 0,
        prepayment_segment = null,
        ead_rule_id = 0,
        ead_segment = null,
        lgd_rule_id = 0,
        lgd_segment = null
        where download_date = v_date;
        commit;

        SP_IFRS_GENERATE_RULE_SEGMENT('PORTFOLIO_SEG','M');
        SP_IFRS_RULE_DATA_SEGMENT;

        merge into ifrs_master_account a
        using
        (select MASTERID, rule_id, b2.group_segment, b2.segment, b2.sub_segment from ifrs_scenario_data a2
        join ifrs_mstr_segment_rules_header b2
        on a2.RULE_ID = b2.PKID) b
        on (a.download_date = v_date and a.MASTERID = b.MASTERID)
        when matched then update set
        a.segment_rule_id = b.rule_id,
        a.group_segment = b.group_segment,
        a.segment = b.segment,
        a.sub_segment=b.sub_segment;

        commit;

        SP_IFRS_GENERATE_RULE_SEGMENT('CCF_SEG','M');
        SP_IFRS_RULE_DATA_SEGMENT;

        merge into ifrs_master_account a
        using
        (select a2.MASTERID, b2.sub_segment, c2.pkid rule_id from ifrs_scenario_data a2
        join ifrs_mstr_segment_rules_header b2
        on a2.RULE_ID = b2.PKID
        join ifrs_ccf_rules_config c2
        on c2.segmentation_id = a2.rule_id AND C2.AVERAGE_METHOD = 'Simple') b
        on (a.download_date = v_date and a.MASTERID = b.MASTERID)
        when matched then update set
        a.ccf_rule_id = b.rule_id,
        a.ccf_segment = b.sub_segment;

        commit;

        SP_IFRS_GENERATE_RULE_SEGMENT('PREPAYMENT_SEG','M');
        SP_IFRS_RULE_DATA_SEGMENT;

        merge into ifrs_master_account a
        using
        (select a2.MASTERID, b2.sub_segment, c2.pkid rule_id from ifrs_scenario_data a2
        join ifrs_mstr_segment_rules_header b2
        on a2.RULE_ID = b2.PKID
        join ifrs_prepayment_rules_config c2
        on c2.segmentation_id = a2.rule_id AND C2.AVERAGE_METHOD = 'Simple') b
        on (a.download_date = v_date and a.MASTERID = b.MASTERID)
        when matched then update set
        a.PREPAYMENT_rule_id = b.rule_id,
        a.PREPAYMENT_segment = b.sub_segment;

        commit;


        SP_IFRS_GENERATE_RULE_SEGMENT('LIFETIME_SEG','M');
        SP_IFRS_RULE_DATA_SEGMENT;

        merge into ifrs_master_account a
        using
        (select a2.MASTERID, b2.sub_segment, c2.pkid rule_id from ifrs_scenario_data a2
        join ifrs_mstr_segment_rules_header b2
        on a2.RULE_ID = b2.PKID
        join ifrs_lifetime_rules_config c2
        on c2.segmentation_id = a2.rule_id and c2.LIFETIME_METHOD = 1) b
        on (a.download_date = v_date and a.MASTERID = b.MASTERID)
        when matched then update set
        a.lifetime_rule_id = b.rule_id,
        a.lifetime_segment = b.sub_segment;

        commit;

        SP_IFRS_GENERATE_RULE_SEGMENT('EAD_SEG','M');
        SP_IFRS_RULE_DATA_SEGMENT;

        merge into ifrs_master_account a
        using
        (select a2.MASTERID, b2.sub_segment, c2.pkid rule_id from ifrs_scenario_data a2
        join ifrs_mstr_segment_rules_header b2
        on a2.RULE_ID = b2.PKID
        join ifrs_ead_rules_config c2
        on c2.segmentation_id = a2.rule_id) b
        on (a.download_date = v_date and a.MASTERID = b.MASTERID)
        when matched then update set
        a.ead_rule_id = b.rule_id,
        a.ead_segment = b.sub_segment;

        commit;

        SP_IFRS_GENERATE_RULE_SEGMENT('LGD_SEG','M');
        SP_IFRS_RULE_DATA_SEGMENT;

        merge into ifrs_master_account a
        using
        (select a2.MASTERID, b2.sub_segment, c2.pkid rule_id from ifrs_scenario_data a2
        join ifrs_mstr_segment_rules_header b2
        on a2.RULE_ID = b2.PKID
        join ifrs_lgd_rules_config c2
        on c2.segmentation_id = a2.rule_id) b
        on (a.download_date = v_date and a.MASTERID = b.MASTERID)
        when matched then update set
        a.lgd_rule_id = b.rule_id,
        a.lgd_segment = b.sub_segment;

        commit;

        v_date := add_months(v_date, 1);
    end loop;
end;