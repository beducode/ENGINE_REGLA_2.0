CREATE OR REPLACE PROCEDURE IFRS_INSERT_IMA_MONTHLY
as
  v_date date;
  V_PREV DATE;
begin
    --SP_IFRS_PD_SEQUENCE;
    v_date := '31 JAN 2012';
    V_PREV := '31 DEC 2011';
--
--
    while v_date <= '30 JUN 2020' loop

        update IFRS_DATE_DAY1
        set currdate = v_date,
            PREVDATE = V_PREV;
        commit;

    --SP_IFRS_LIFETIME;

    sp_ifrs_ccf;commit;
    --SP_IFRS_PREPAYMENT_PROCESS;
    COMMIT;
--update IFRS_MASTER_ACCOUNT_MONTHLY
--        set
--        segment_rule_id = 0,
--        group_segment = null,
--        segment = null,
--        sub_segment = null,
--        ccf_rule_id = 0,
--        ccf_segment = null,
--        lifetime_rule_id = 0,
--        lifetime_segment = null
--        where download_date = v_date
--        AND CREATEDBY <> 'DKP';
--        commit;
--
--        SP_IFRS_GENERATE_RULE_SEGMENT('PORTFOLIO_SEG','M');
--        SP_IFRS_UPDATE_SEGMENT_day1;
--
--        merge into ifrs_master_account_MONTHLY a
--        using IFRS_SEGMENT_MAPPING_DAY1 b
--        on (a.download_date = v_date and a.segment_rule_id = b.segment_rule_id)
--        when matched then
--        update set a.ccf_rule_id = b.ccf_rule_id,
--        a.ccf_segment = b.ccf_segment,
--        a.lifetime_rule_id = b.lifetime_rule_id,
--        a.lifetime_segment = b.lifetime_segment
--        WHERE CREATEDBY <> 'DKP';

          v_date := ADD_MONTHS(V_DATE,1);
          V_PREV := ADD_MONTHS(V_PREV,1);

    end loop;
end;