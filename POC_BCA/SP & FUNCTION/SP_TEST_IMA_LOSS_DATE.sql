CREATE OR REPLACE procedure      SP_TEST_IMA_LOSS_DATE
as
    v_date date;
begin
--    v_date := '31 JAN 2020';
--
--    while v_date <= '31 MAR 21' loop
--        Merge into ifrs_master_account_monthly a
--        using test_patch2 b
--        on (a.download_date = v_date and a.account_number = b.account_number)
--        when matched then
--        update set
--        a.reserved_date_3 = case when v_date >= last_day(b.npl_date) then b.npl_date else null end--,
----        a.reserved_amount_8 = case when v_date >= last_day(b.first_npl_date) then b.first_npl_os else null end
--;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--    end loop;

--    while v_date <= '31 MAR 21' loop
--        Merge into ifrs_master_account_monthly a
--        using test_patch b
--        on (a.download_date = v_date and a.account_number = b.account_number)
--        when matched then
--        update set
--        a.reserved_date_3 = case when v_date >= last_day(b.npl_date) then b.npl_date else null end--,
----        a.reserved_amount_8 = case when v_date >= last_day(b.first_npl_date) then b.first_npl_os else null end
--;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--    end loop;

--    while v_date <= '30 JUN 20' loop
--        merge into ifrs_master_account_monthly a
--        using test_patch b
--        on (a.download_date = v_date
--        and a.masterid = b.masterid
--        and a.download_date >= b.download_date)
--        when matched then update set
--        a.reserved_date_3 = null,
--        a.reserved_amount_8 = null;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--
--    end loop;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE ifrs_ima_loss_date';

    v_date := '31 JAN 2008';
    while v_date <= '31 MAR 21' loop
        insert into ifrs_ima_loss_date
        Select *
        from ifrs_master_account_monthly
        where download_date = v_date
        AND data_source = 'CRD'
        and rating_code >= '5'
        and masterid not in
        (Select masterid from ifrs_ima_loss_date);
        commit;

        update ifrs_prc_date_k
        set currdate = v_date;
        commit;

        v_date := add_months(v_date,1);

    end loop;

--    v_date := '31 JAN 11';
--    while v_date <= '31 MAR 21' loop
--        insert into ifrs_ima_loss_date
--        Select *
--        from ifrs_master_account_monthly
--        where download_date = v_date
--        AND
--        (
--            (BI_COLLECTABILITY IN ('3','4','5','C')
--            AND DATA_SOURCE = 'ILS'
--            AND PRODUCT_CODE NOT LIKE 'B%')
--        OR
--            (RATING_CODE >= '5'
--            AND DATA_SOURCE = 'CRD')
--        )
--        and masterid not in
--        (Select masterid from ifrs_ima_loss_date);
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--
--    end loop;

--    v_date := '31 OCT 2006';
--
--    while v_date <= '31 JUL 20' loop
--        Merge into ifrs_master_account_monthly a
--        using ifrs_ima_loss_amount_patch b
--        on (a.download_date = v_date and a.account_number = b.account_number)
--        when matched then
--        update set
--        a.reserved_date_3 = case when v_date >= last_day(b.npl_date) then b.npl_date else null end,
--        a.reserved_amount_8 = case when v_date >= last_day(b.npl_date) then b.total_loss_amt else null end;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--    end loop;

--    EXECUTE IMMEDIATE 'TRUNCATE TABLE ifrs_ima_loss_date';
--
--    v_date := '31 OCT 06';
--    while v_date <= '31 MAR 21' loop
--        insert into ifrs_ima_loss_date
--        Select *
--        from ifrs_master_account_monthly
--        where download_date = v_date
--        and bi_collectability in ('3','4','5','C')
--        and data_source in ('ILS','CRD')
--        and product_code not like 'B%'
--        and outstanding > 0
--        and masterid not in
--        (Select masterid from ifrs_ima_loss_date);
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--
--    end loop;

--Untuk pengecekan Account Number reuse, Account number sama, customer number/name beda
--    v_date := '31 JAN 11';
--    while v_date <= '31 DEC 20' loop
--        insert into ifrs_ima_loss_date
--        Select *
--        from ifrs_master_account_monthly
--        where download_date = v_date
--        and bi_collectability in ('3','4','5','C')
--        and account_number || customer_name not in
--        (Select account_number || customer_name from ifrs_ima_loss_date);
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--
--    end loop;

--    v_date := '31 JAN 2011';
--
--    while v_date <= '31 JUL 20' loop
--        Merge into ifrs_master_account_monthly a
--        using
--        (
--            Select * from npl_fs_all
--            where account_number in
--            (
--            '0903076965900001',
--            '0891090040500001',
--            '0906014453000001',
--            '0906014460200001',
--            '0906074224100001',
--            '0907042603700001',
--            '0906009471100001',
--            '0970091842100001',
--            '0907032284300001',
--            '0905015815000001',
--            '0908013451400001',
--            '0907059512200001',
--            '0907046954200001',
--            '0907057656000001',
--            '0906070932400001',
--            '0970095026100001',
--            '0040900492100001',
--            '0906015342300001',
--            '0001900179400001',
--            '0906073934700001',
--            '0578093669100003',
--            '0777090313500001',
--            '0970014301200001',
--            '0970023971100001',
--            '0970092499500001',
--            '0970020926900001',
--            '0908021922600001',
--            '0907057267000001',
--            '0066952525900003',
--            '0907084472600001'
--            )
--        ) b
--        on (a.download_date = v_date and a.account_number = b.account_number)
--        when matched then
--        update set
--        a.reserved_amount_8 = case when v_date >= last_day(b.loss_date) then b.loss_amount else null end;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--    end loop;

--    v_date := '31 JAN 2011';
--
--    while v_date <= '31 JUL 20' loop
--        Merge into ifrs_master_account_monthly a
--        using
--        (
--            Select account_number, min(first_npl_date) loss_date from
--            (
--            Select distinct account_number, first_npl_date from tmp_Lgd_ima
--            ) a
--            group by account_number
--            having count(*) > 1
--        ) b
--        on (a.download_date = v_date and a.account_number = b.account_number)
--        when matched then
--        update set
--        a.reserved_date_3 = case when v_date >= last_day(b.loss_date) then b.loss_date else null end;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--    end loop;


--    v_date := '31 JAN 20';
--
--    while v_date <= '31 JUL 20' loop
--        Merge into ifrs_master_account_monthly a
--        using
--        (
--            Select * from test_ima_loss_date
--            where download_date > '31 DEC 19'
--        ) b
--        on (a.download_date = v_date and a.account_number = b.account_number)
--        when matched then
--        update set
--        a.reserved_date_3 = case when v_date >= last_day(b.reserved_date_3) then b.reserved_date_3 else null end,
--        a.reserved_amount_8 = case when v_date >= last_day(b.reserved_date_3) then b.reserved_amount_8 else null end;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--    end loop;


--    v_date := '31 JAN 2011';
--
--    while v_date <= '31 JUL 20' loop
--        Merge into ifrs_master_account_monthly a
--        using
--        (
--            Select * from NPL_FS_ALL
--            where account_number not in
--            (Select account_number from test_ima_loss_date where data_source = 'ILS')
--            and loss_date > '1 JAN 11'
--        ) b
--        on (a.download_date = v_date and a.account_number = b.account_number)
--        when matched then
--        update set
--        a.reserved_date_3 = case when v_date >= last_day(b.loss_date) then b.loss_date else null end,
--        a.reserved_amount_8 = case when v_date >= last_day(b.loss_date) then b.loss_amount else null end;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--    end loop;



--    v_date := '31 JAN 2011';
--
--    while v_date <= '31 JUL 20' loop
--        Merge into ifrs_master_account_monthly a
--        using
--        (
--            Select b.*
--            from test_ima_loss_date a
--            join npl_fs_all b
--            on a.account_number = b.account_number
--            and a.download_date >= '31 JAN 20'
--        ) b
--        on (a.download_date = v_date and a.account_number = b.account_number)
--        when matched then
--        update set
--        a.reserved_date_3 = case when v_date >= last_day(b.loss_date) then b.loss_date else null end,
--        a.reserved_amount_8 = case when v_date >= last_day(b.loss_date) then b.loss_amount else null end;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--    end loop;

--    v_date := '31 JAN 2011';
--
--    while v_date <= '31 JUL 20' loop
--        Merge into ifrs_master_account_monthly a
--        using
--        (
--                Select * from npl_fs_all
--                where account_number in
--                (
--                Select account_number from test_ima_loss_date
--                where reserved_date_3 is null
--                and data_source = 'ILS'
--                )
--        ) b
--        on (a.download_date = v_date and a.account_number = b.account_number)
--        when matched then
--        update set
--        a.reserved_date_3 = case when v_date >= last_day(b.loss_date) then b.loss_date else null end,
--        a.reserved_amount_8 = case when v_date >= last_day(b.loss_date) then b.loss_amount else null end;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--    end loop;


--    merge into ifrs_master_account_monthly a
--using ifrs_master_account_monthly b
--on (a.download_date = '31 JAN 20' and b.download_date = '31 DEC 19' and a.masterid = b.masterid and b.data_source = 'ILS')
--when matched then
--update set
--    a.reserved_date_3 = b.reserved_date_3,
--    a.reserved_amount_8 = b.reserved_amount_8
--where b.reserved_date_3 is not null;
--commit;
--
--merge into ifrs_master_account_monthly a
--using ifrs_master_account_monthly b
--on (a.download_date = '29 FEB 20' and b.download_date = '31 JAN 20' and a.masterid = b.masterid and b.data_source = 'ILS')
--when matched then
--update set
--    a.reserved_date_3 = b.reserved_date_3,
--    a.reserved_amount_8 = b.reserved_amount_8
--where b.reserved_date_3 is not null;
--commit;
--
--merge into ifrs_master_account_monthly a
--using ifrs_master_account_monthly b
--on (a.download_date = '31 MAR 20' and b.download_date = '29 FEB 20' and a.masterid = b.masterid and b.data_source = 'ILS')
--when matched then
--update set
--    a.reserved_date_3 = b.reserved_date_3,
--    a.reserved_amount_8 = b.reserved_amount_8
--where b.reserved_date_3 is not null;
--commit;
--
--merge into ifrs_master_account_monthly a
--using ifrs_master_account_monthly b
--on (a.download_date = '30 APR 20' and b.download_date = '31 MAR 20' and a.masterid = b.masterid and b.data_source = 'ILS')
--when matched then
--update set
--    a.reserved_date_3 = b.reserved_date_3,
--    a.reserved_amount_8 = b.reserved_amount_8
--where b.reserved_date_3 is not null;
--commit;
--
--merge into ifrs_master_account_monthly a
--using ifrs_master_account_monthly b
--on (a.download_date = '31 MAY 20' and b.download_date = '30 APR 20' and a.masterid = b.masterid and b.data_source = 'ILS')
--when matched then
--update set
--    a.reserved_date_3 = b.reserved_date_3,
--    a.reserved_amount_8 = b.reserved_amount_8
--where b.reserved_date_3 is not null;
--commit;
--
--merge into ifrs_master_account_monthly a
--using ifrs_master_account_monthly b
--on (a.download_date = '30 JUN 20' and b.download_date = '31 MAY 20' and a.masterid = b.masterid and b.data_source = 'ILS')
--when matched then
--update set
--    a.reserved_date_3 = b.reserved_date_3,
--    a.reserved_amount_8 = b.reserved_amount_8
--where b.reserved_date_3 is not null;
--commit;
--
--merge into ifrs_master_account_monthly a
--using ifrs_master_account_monthly b
--on (a.download_date = '31 JUL 20' and b.download_date = '30 JUN 20' and a.masterid = b.masterid and b.data_source = 'ILS')
--when matched then
--update set
--    a.reserved_date_3 = b.reserved_date_3,
--    a.reserved_amount_8 = b.reserved_amount_8
--where b.reserved_date_3 is not null;
--commit;

--    v_date := '31 JAN 2011';
--
--    while v_date <= '31 JUL 20' loop
--        Merge into ifrs_master_account_monthly a
--        using
--        (
--            Select * from NPL_FS_ALL
--            where account_number not in
--            (Select account_number from test_ima_loss_date)
--            and loss_date < '31 JAN 11'
--        ) b
--        on (a.download_date = v_date and a.account_number = b.account_number)
--        when matched then
--        update set
--        a.reserved_date_3 = case when v_date >= last_day(b.loss_date) then b.loss_date else null end,
--        a.reserved_amount_8 = case when v_date >= last_day(b.loss_date) then b.loss_amount else null end;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--    end loop;

--    v_date := '31 JAN 2011';
--
--    while v_date <= '31 JUL 20' loop
--        Merge into ifrs_master_account_monthly a
--        using
--        (
--            Select a.*--, b.reserved_date_3, b.outstanding, b.reserved_amount_8, b.download_date, npl_date
--            from NPL_FS_ALL a
--            join test_ima_loss_date b
--            on (a.account_number = b.account_number)
--            and last_day(b.reserved_date_3) != last_day(a.loss_date)
--        ) b
--        on (a.download_date = v_date and a.account_number = b.account_number)
--        when matched then
--        update set
--        a.reserved_date_3 = case when v_date >= last_day(b.loss_date) then b.loss_date else null end,
--        a.reserved_amount_8 = case when v_date >= last_day(b.loss_date) then b.loss_amount else null end;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--    end loop;



--    v_date := '31 JAN 2011';
--
--    while v_date <= '31 JUL 20' loop
--        Merge into ifrs_master_account_monthly a
--        using
--        (
--            Select masterid from test_ima_loss_date
--            where reserved_date_3 < '1 OCT 2006'
--            and download_date > '31 JAN 2011'
--        ) b
--        on (a.download_date = v_date and a.masterid = b.masterid)
--        when matched then
--        update set
--        a.reserved_date_3 = null,
--        a.reserved_amount_8 = null;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--    end loop;


--    v_date := '31 JAN 2011';
--
--    while v_date <= '31 JUL 19' loop
--        update ifrs_master_account_monthly
--        set account_status = 'W'
--        where download_date = v_date
--        and bi_collectability = 'C'
--        and data_source = 'ILS';
--
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--    end loop;



--    v_date := '30 NOV 2019';
--
--    while v_date >= '31 JAN 11' loop
--        Merge into tmp_lgd_ima_20200823 a
--        using ifrs_master_account_monthly b
--        on (a.first_npl_date > v_date and b.download_date = v_date and a.account_number = b.account_number)
--        when matched then
--        update set
--            a.first_npl_os = b.outstanding
--        where a.first_npl_os = 0;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,-1);
--    end loop;



--    v_date := '31 JAN 2011';
--
--    while v_date <= '31 JUL 20' loop
--        Merge into ifrs_master_account_monthly a
--        using test_ima_loss_date_7 b
--        on (a.download_date = v_date and a.account_number = b.account_number)
--        when matched then
--        update set
--            a.reserved_amount_8 = case when v_date >= last_day(b.loss_date) then b.loss_amount else null end;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--    end loop;


--    v_date := '31 JAN 2011';
--
--    while v_date <= '31 JUL 20' loop
--        Merge into ifrs_master_account_monthly a
--        using test_ima_loss_date_6 b
--        on (a.download_date = v_date and a.account_number = b.account_number)
--        when matched then
--        update set
--        a.reserved_date_3 = null,
--        a.reserved_amount_8 = null;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--    end loop;



--    v_date := '31 JAN 2011';
--
--    while v_date <= '31 JUL 20' loop
--        Merge into ifrs_master_account_monthly a
--        using
--        (
--            Select *
--            from test_ima_loss_date
--            where download_date = '31 DEC 10'
--            and outstanding is not null
--        ) b
--        on (a.download_date = v_date and a.account_number = b.account_number)
--        when matched then
--        update set
--        a.reserved_date_3 = case when v_date >= last_day(b.reserved_date_3) then b.reserved_date_3 else null end,
--        a.reserved_amount_8 = case when v_date >= last_day(b.reserved_date_3) then b.outstanding else null end;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--    end loop;

--    v_date := '31 JAN 2011';
--
--    while v_date <= '31 JUL 20' loop
--        Merge into ifrs_master_account_monthly a
--        using test_ima_loss_date_5 b
--        on (a.download_date = v_date and a.account_number = b.account_number)
--        when matched then
--        update set
--        a.reserved_date_3 = case when v_date >= last_day(b.reserved_date_3) then b.reserved_date_3 else null end,
--        a.reserved_amount_8 = case when v_date >= last_day(b.reserved_date_3) then b.outstanding else null end;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--    end loop;



--    v_date := '31 JAN 2011';
--
--    while v_date <= '31 JUL 20' loop
--        Merge into ifrs_master_account_monthly a
--        using test_ima_loss_date_4 b
--        on (a.download_date = v_date and a.account_number = b.deal_id)
--        when matched then
--        update set
--        a.reserved_amount_8 = case when v_date >= last_day(b.loss_date) then b.LOSS_AMOUNT_IN_IDR else null end;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--    end loop;


--
--    v_date := '31 JAN 2011';
--
--    while v_date <= '31 JUL 20' loop
--        Merge into ifrs_master_account_monthly a
--        using test_ima_loss_date_3 b
--        on (a.download_date = v_date and a.account_number = b.account_number)
--        when matched then
--        update set a.reserved_date_3 = case when v_date >= last_day(b.loss_date) then b.loss_date else null end,
--        a.reserved_amount_8 = case when v_date >= last_day(b.loss_date) then b.LOSS_AMOUNT_IN_IDR else null end;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--    end loop;




--    v_date := '31 JAN 2011';
--
--    while v_date <= '31 JUL 20' loop
--        Merge into ifrs_master_account_monthly a
--        using Test_lgd_oct_2006 b
--        on (a.download_date = v_date and a.account_number = b.account_number)
--        when matched then
--        update set a.reserved_date_3 = b.loss_date;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--    end loop;


--    v_date := '31 JAN 2011';
--
--    while v_date <= '31 MAY 20' loop
--        update ifrs_master_account_monthly
--        set reserved_amount_8 = null
--        where download_date = v_date
--        and data_source = 'ILS'
--        and reserved_date_3 is null;
--
--        merge into ifrs_master_account_monthly a
--        using test_ima_loss_date b
--        on (a.download_date = v_date and a.masterid = b.masterid and a.reserved_date_3 is not null)
--        when matched then
--        update set
--        a.reserved_amount_8 = b.outstanding;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--    end loop;

    --EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_MASTER_ACCOUNT_LGD_BEF_UP';

--    v_date := '31 JAN 2011';
--
--    while v_date <= '31 MAY 20' loop
--        INSERT INTO IFRS_MASTER_ACCOUNT_LGD_BEF_UP
--        Select a.download_date, a.masterid, a.data_source, a.customer_number, a.customer_name, a.account_number, a.npl_date, a.reserved_date_3
--        From ifrs_master_account_monthly a
--        where download_date = v_date
--        and data_source in ('CBS','CRD','LIMIT');
--        commit;
--
--        update ifrs_master_account_monthly
--        set reserved_date_3 = null
--        where download_date = v_date
--        and data_source in ('CBS','CRD','LIMIT');
--        commit;
--
--        merge into ifrs_master_account_monthly a
--        using test_ima_loss_date b
--        on (a.download_date = v_date and a.account_number = b.account_number
--        and last_day(case when nvl(b.reserved_date_3, '1 JAN 1900') < b.npl_date then b.npl_date else b.reserved_date_3 end) <= v_date)
--        when matched then
--        update set
--        a.reserved_date_3 = case when nvl(b.reserved_date_3, '1 JAN 1900') < b.npl_date then b.npl_date else b.reserved_date_3 end;
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--    end loop;


--    v_date := '31 MAR 1990';
--    while v_date <= '31 DEC 19' loop
--        insert into test_ima_loss_date_FS
--        Select *
--        from T_TRN_LOAN_DEFAULT
--        where last_day(start_validity_date) = v_date
--        and default_status in ('3','4','5','C')
--        and deal_id not in
--        (Select deal_id from test_ima_loss_date_FS);
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--
--    end loop;


--    v_date := '31 JAN 11';
--    while v_date <= '31 MAY 20' loop
--        insert into test_ima_loss_date
--        Select *
--        from ifrs_master_account_monthly
--        where download_date = v_date
--        and bi_collectability in ('3','4','5','C')
--        and masterid not in
--        (Select masterid from test_ima_loss_date);
--        commit;
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--
--        v_date := add_months(v_date,1);
--
--    end loop;

--    v_date := '31 JAN 2012';
--
--    while v_date <= '30 APR 20' loop
--        insert into test_ima_loss_date
--        Select a.download_date, a.masterid, a.account_number, a.reserved_date_3, b.reserved_date_3 as next_loss_date
--        from ifrs_master_account_monthly a
--        join ifrs_master_account_monthly b
--        on a.download_date = v_date
--        and b.download_date = add_months(v_date,1)
--        and a.masterid = b.masterid
--        and nvl(a.reserved_date_3,'31 JAN 1900') != nvl(b.reserved_date_3,'31 JAN 1900')
--        and a.reserved_date_3 is not null;
--        commit;
--
--        v_date := add_months(v_date,1);
--
--        update ifrs_prc_date_k
--        set currdate = v_date;
--        commit;
--    end loop;
end;