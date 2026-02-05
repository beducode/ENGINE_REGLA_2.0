CREATE OR REPLACE PROCEDURE IFRS_RUN_ENGINE5 (V_MASTERID varchar, v_start_date date)
as
    v_date date;
begin

    EXECUTE IMMEDIATE 'alter session enable parallel dml';

    v_date := v_start_date;
    while v_date <= to_date('30 nov 2024', 'dd mm yyyy')
        loop

            insert into /*+ PARALLEL(8) */ ifrs_master_account_to_pwc
            select /*+ PARALLEL(8) */ *
            from ifrs_master_account_acv
            where DOWNLOAD_DATE = v_date
              and MASTERID = V_MASTERID;
            commit;
            v_date := v_date + 1;
        end loop;

    --     insert into ifrs_pd_migration_detail
--         (
--             eff_date,
--             base_date,
--             pd_rule_id,
--             bucket_group,
--             pd_unique_id,
--             customer_name,
--             bucket_from,
--             bucket_to,
--             calc_amount,
--             outstanding
--         )
--         Select add_months(pool_classification_date, 12) eff_date,
--                 pool_classification_date base_date,
--                 8 pd_rule_id,
--                 'DPD5_1' bucket_group,
--                 deal_id pd_unique_id,
--                 customer_shortname,
--                 case from_pool_id when '300' then 1
--                    when '301' then 2
--                    when '302' then 3
--                    when '303' then 4
--                    when '304' then 5
--                 end bucket_from,
--                 case to_pool_id when '300' then 1
--                    when '301' then 2
--                    when '302' then 3
--                    when '303' then 4
--                    when '304' then 5
--                 end bucket_to,
--                 lcl_amount calc_amount,
--                 lcl_amount as outstanding
--                 from test_pd_card a
--                 where pool_classification_date = v_date;
--         commit;
end;