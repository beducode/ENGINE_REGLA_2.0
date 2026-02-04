CREATE OR REPLACE PROCEDURE SP_DAY1_UPDATE_SEGMENT_MONTHLY
as
  v_date date;
begin
    v_date := '30-APR-2013';
    while v_date <= '31-JAN-2019' loop
        update ifrs_prc_date
        set currdate = v_date;
        commit;


        update ifrs_master_account_monthly
        set
        segment_rule_id = 0,
        group_segment = null,
        segment = null,
        sub_segment = null,
        ccf_rule_id = 0,
        ccf_segment = null,
        lifetime_rule_id = 0,
        lifetime_segment = null--,
        --PREPAYMENT_rule_id = 0,
        --prepayment_segment = null,
        --ead_rule_id = 0,
        --ead_segment = null,
        --lgd_rule_id = 0,
        --lgd_segment = null
        where download_date = v_date;
        commit;

        SP_IFRS_GENERATE_RULE_SEGMENT('PORTFOLIO_SEG','M');
        SP_IFRS_RULE_DATA_SEGMENT;

        merge into ifrs_master_account_monthly a
        using
        (
            select MASTERID, rule_id, b2.group_segment, b2.segment, b2.sub_segment from ifrs_scenario_data a2
            join ifrs_mstr_segment_rules_header b2
            on a2.RULE_ID = b2.PKID
        ) b
        on (a.download_date = v_date and a.MASTERID = b.MASTERID)
        when matched then update set
        a.segment_rule_id = b.rule_id,
        a.group_segment = b.group_segment,
        a.segment = b.segment,
        a.sub_segment=b.sub_segment;

        commit;

        /*update ccf segment*/
        merge into ifrs_master_account_monthly a
        using
        (
             select
                c2.sub_segment_pf as sub_segment_pf,
                b2.sub_segment as sub_segment_ccf,
                d2.pkid as rule_id
            FROM ifrs_mstr_segment_rules_header b2
            INNER JOIN TEST_UPDATE_SEGMENT c2
            on B2.group_segment = c2.group_segment_ccf
            and B2.segment = c2.segment_ccf
            and B2.sub_segment = c2.sub_segment_ccf
            join ifrs_ccf_rules_config d2
            on d2.segmentation_id = c2.ccf_segment_id
            AND d2.AVERAGE_METHOD = 'Simple'
        ) b
        on (a.download_date = v_Date and a.sub_segment = b.sub_segment_pf)
        when matched then update set
        a.ccf_rule_id = b.rule_id,
        a.ccf_segment = b.sub_segment_pf;


        /*update lifetime segment*/
        merge into ifrs_master_account_monthly a
        using
        (
             select
                c2.sub_segment_pf as sub_segment_pf,
                b2.sub_segment as sub_segment_lt,
                d2.pkid as rule_id
            FROM ifrs_mstr_segment_rules_header b2
            INNER JOIN TEST_UPDATE_SEGMENT c2
            on B2.group_segment = c2.group_segment_lt
            and B2.segment = c2.segment_lt
            and B2.sub_segment = c2.sub_segment_lt
            join ifrs_lifetime_rules_config d2
            on d2.segmentation_id = c2.lt_segment_id
            and d2.lifetime_method = 1
        ) b
        on (a.download_date = v_Date and a.sub_segment = b.sub_segment_pf)
        when matched then update set
        a.lifetime_rule_id = b.rule_id,
        a.lifetime_segment = b.sub_segment_lt;

        v_date := add_months(v_date, 1);

    end loop;

END;