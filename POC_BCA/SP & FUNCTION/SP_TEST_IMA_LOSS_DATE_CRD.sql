CREATE OR REPLACE procedure      SP_TEST_IMA_LOSS_DATE_CRD
as
    v_date date;
begin
    v_date := '31 JAN 2008';

    while v_date <= '31 MAR 21' loop
        Merge into ifrs_master_account_monthly a
        using ifrs_ima_loss_date b
        on (a.download_date = v_date and a.account_number = b.account_number)
        when matched then
        update set
        a.reserved_date_3 = case when v_date >= last_day(b.download_date) then b.download_date else null end,
        a.reserved_amount_8 = case when v_date >= last_day(b.download_date) then b.outstanding else null end;
        commit;

        update ifrs_prc_date_k
        set currdate = v_date;
        commit;

        v_date := add_months(v_date,1);
    end loop;


--    execute immediate 'TRUNCATE TABLE test_ima_loss_date_crd';
--
--    v_date := '31 JAN 2011';
--
--    while v_date <= '30 JUN 20' loop
--        if v_date = '31 JAN 19' or v_date > '31 JUL 19' then
--            insert into test_ima_loss_date_crd
--            Select *
--            from ifrs_master_account_monthly
--            where download_date = v_date
--            and rating_code > '4'
--            and masterid not in
--            (Select masterid from test_ima_loss_date_crd);
--        else
--            insert into test_ima_loss_date_crd
--            Select *
--            from ifrs_master_account_monthly
--            where download_date = v_date
--            and reserved_amount_4 > 4
--            and masterid not in
--            (Select masterid from test_ima_loss_date_crd);
--        end if;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--
--    end loop;
end;