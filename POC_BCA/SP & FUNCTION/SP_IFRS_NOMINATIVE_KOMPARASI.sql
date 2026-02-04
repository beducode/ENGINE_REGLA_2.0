CREATE OR REPLACE procedure SP_IFRS_NOMINATIVE_KOMPARASI(v_goldeb varchar2) as
    v_date date;
begin

    SELECT currdate INTO v_date FROM IFRS_PRC_DATE;

    insert into GTMP_NOMI_PREV
    SELECT *
    FROM IFRS_NOMINATIVE N
    WHERE 1 = 1
      AND N.REPORT_DATE = add_months(v_date, -1)
      and N.GOL_DEB = v_goldeb
      AND ((N.DATA_SOURCE = 'BTRD'
        AND N.ACCOUNT_STATUS = 'A'
        AND NVL(N.BI_CODE, ' ') <> '0')
        --     OR (DATA_SOURCE = 'CRD'
--         AND (ACCOUNT_STATUS = 'A' OR outstanding_on_bs_ccy > 0))
        OR (N.DATA_SOURCE = 'ILS' AND N.account_status = 'A')
        OR (N.DATA_SOURCE = 'LIMIT' AND N.account_status = 'A')
        --     OR (DATA_SOURCE = 'KTP'
--         AND ACCOUNT_STATUS = 'A'
--         AND UPPER(PRODUCT_CODE) <> 'BORROWING')
        OR (N.DATA_SOURCE = 'PBMM'
            AND N.ACCOUNT_STATUS = 'A'
            AND UPPER(N.PRODUCT_CODE) <> 'BORROWING')
        OR (N.DATA_SOURCE = 'RKN'
            AND N.ACCOUNT_STATUS = 'A'
            AND NVL(N.OUTSTANDING_PRINCIPAL_CCY, 0) >= 0))
      AND NOT EXISTS
        (SELECT 1
         FROM IFRS_NOMINATIVE L
         WHERE L.REPORT_DATE = N.REPORT_DATE
           AND L.DATA_SOURCE = 'ILS'
           AND L.ACCOUNT_STATUS = 'A'
           AND N.DATA_SOURCE = 'LIMIT'
           AND N.ACCOUNT_NUMBER = L.FACILITY_NUMBER)
      AND N.PRODUCT_CODE not like '7%'
      and N.product_code not in ('BPC', 'BSL');


    insert into GTMP_NOMI_CURR
    select  *
    FROM IFRS_NOMINATIVE N
    WHERE 1 = 1
      AND REPORT_DATE = v_date
      and GOL_DEB = v_goldeb
      AND ((DATA_SOURCE = 'BTRD'
        AND ACCOUNT_STATUS = 'A'
        AND NVL(BI_CODE, ' ') <> '0')
        --     OR (DATA_SOURCE = 'CRD'
--         AND (ACCOUNT_STATUS = 'A' OR outstanding_on_bs_ccy > 0))
        OR (DATA_SOURCE = 'ILS' AND account_status = 'A')
        OR (DATA_SOURCE = 'LIMIT' AND account_status = 'A')
        --     OR (DATA_SOURCE = 'KTP'
--         AND ACCOUNT_STATUS = 'A'
--         AND UPPER(PRODUCT_CODE) <> 'BORROWING')
        OR (DATA_SOURCE = 'PBMM'
            AND ACCOUNT_STATUS = 'A'
            AND UPPER(PRODUCT_CODE) <> 'BORROWING')
        OR (DATA_SOURCE = 'RKN'
            AND ACCOUNT_STATUS = 'A'
            AND NVL(OUTSTANDING_PRINCIPAL_CCY, 0) >= 0))
      AND NOT EXISTS
        (SELECT 1
         FROM IFRS_NOMINATIVE L
         WHERE L.REPORT_DATE = N.REPORT_DATE
           AND L.DATA_SOURCE = 'ILS'
           AND L.ACCOUNT_STATUS = 'A'
           AND N.DATA_SOURCE = 'LIMIT'
           AND N.ACCOUNT_NUMBER = L.FACILITY_NUMBER)
      AND PRODUCT_CODE not like '7%'
      and product_code not in ('BPC', 'BSL');


    insert into GTMP_CUST_NO_CMPR
    select   CUSTOMER_NUMBER
    from (select coalesce(b.CUSTOMER_NUMBER, a.CUSTOMER_NUMBER)        customer_number,
                 nvl(b.ECL_TOTAL_LCL_B, 0) - nvl(a.ECL_TOTAL_LCL_A, 0) selisih
          from (select CUSTOMER_NUMBER,
                       sum(nvl(ECL_TOTAL_LCL, 0)) ECL_TOTAL_LCL_A
                from GTMP_NOMI_PREV
                group by CUSTOMER_NUMBER) a
                   full outer join (select CUSTOMER_NUMBER,
                                           sum(nvl(ECL_TOTAL_LCL, 0)) ECL_TOTAL_LCL_B
                                    from GTMP_NOMI_CURR
                                    group by CUSTOMER_NUMBER) b
                                   on a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER)
    where selisih >= 2000000000
       or selisih <= -2000000000;


    insert into GTMP_SELISIH_CMPR
    select coalesce(b.CUSTOMER_NUMBER, a.CUSTOMER_NUMBER),
           null                                                                CUSTOMER_NAME,
           null                                                                STAGE_PAST,
           null                                                                STAGE_CURRENT,
           null                                                                RATING_PAST,
           null                                                                RATING_CURRENT,
           null                                                                CASH_FLOW_PAST,
           null                                                                CASH_FLOW_CURRENT,
           null                                                                RATING_PAST,
           null                                                                RATING_CURRENT,
           null                                                                RATING_PAST,
           null                                                                RATING_CURRENT,
           null                                                                SPECIAL_REASON_PAST,
           null                                                                SPECIAL_REASON_CURRENT,
           nvl(OUTSTANDING_ON_BS_LCL_A, 0)                                     OS_ON_PAST,
           nvl(OUTSTANDING_ON_BS_LCL_B, 0)                                     OS_ON_CURRENT,
           nvl(OUTSTANDING_ON_BS_LCL_B, 0) - nvl(OUTSTANDING_ON_BS_LCL_A, 0)   OS_ON_SELISIH,
           nvl(OUTSTANDING_OFF_BS_LCL_A, 0)                                    OS_OFF_PAST,
           nvl(OUTSTANDING_OFF_BS_LCL_B, 0)                                    OS_OFF_CURRENT,
           nvl(OUTSTANDING_OFF_BS_LCL_B, 0) - nvl(OUTSTANDING_OFF_BS_LCL_A, 0) OS_OFF_SELISIH,
           nvl(SALDO_YADIT_LCL_A, 0)                                           YADIT_PAST,
           nvl(SALDO_YADIT_LCL_B, 0)                                           YADIT_CURR,
           nvl(SALDO_YADIT_LCL_B, 0) - nvl(SALDO_YADIT_LCL_A, 0)               YADIT_SELISIH,
           nvl(ECL_TOTAL_LCL_A, 0)                                             ECL_PAST,
           nvl(ECL_TOTAL_LCL_B, 0)                                             ECL_CURR,
           nvl(ECL_TOTAL_LCL_B, 0) - nvl(ECL_TOTAL_LCL_A, 0)                   ECL_SELISIH,
           null                                                                KETERANGAN
    from (select CUSTOMER_NUMBER,
                 sum(nvl(OUTSTANDING_ON_BS_LCL, 0))  OUTSTANDING_ON_BS_LCL_A,
                 sum(nvl(OUTSTANDING_OFF_BS_LCL, 0)) OUTSTANDING_OFF_BS_LCL_A,
                 sum(nvl(SALDO_YADIT_LCL, 0))        SALDO_YADIT_LCL_A,
                 sum(nvl(ECL_TOTAL_LCL, 0))          ECL_TOTAL_LCL_A
          from GTMP_NOMI_PREV
          group by CUSTOMER_NUMBER) a
             full outer join (select CUSTOMER_NUMBER,
                                     sum(nvl(OUTSTANDING_ON_BS_LCL, 0))  OUTSTANDING_ON_BS_LCL_B,
                                     sum(nvl(OUTSTANDING_OFF_BS_LCL, 0)) OUTSTANDING_OFF_BS_LCL_B,
                                     sum(nvl(SALDO_YADIT_LCL, 0))        SALDO_YADIT_LCL_B,
                                     sum(nvl(ECL_TOTAL_LCL, 0))          ECL_TOTAL_LCL_B
                              from GTMP_NOMI_CURR
                              group by CUSTOMER_NUMBER) b
                             on a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER
    where 1 = 1
        and a.CUSTOMER_NUMBER in (select customer_number from GTMP_CUST_NO_CMPR)
       or b.CUSTOMER_NUMBER in (select customer_number from GTMP_CUST_NO_CMPR);


-- 1 Customer Number
    merge into GTMP_SELISIH_CMPR d
    using (select a.CUSTOMER_NUMBER cust_no_prev, b.CUSTOMER_NUMBER cust_no_curr
           from (select distinct CUSTOMER_NUMBER
                 from GTMP_NOMI_PREV) a
                    right join (select distinct CUSTOMER_NUMBER
                                from GTMP_NOMI_CURR) b
                               on a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER
                    join GTMP_CUST_NO_CMPR c
                         on b.CUSTOMER_NUMBER = c.CUSTOMER_NUMBER
           where a.CUSTOMER_NUMBER is null) e
    ON (d.customer_number = e.cust_no_curr)
    when matched then
        update
        set d.KETERANGAN='Customer Baru'
        where d.KETERANGAN is null;


-- 2 Write Off

    merge into GTMP_SELISIH_CMPR d
    using (select distinct b.CUSTOMER_NUMBER cust_no_curr,
                           case
                               when a.ACCOUNT_STATUS != 'W' and b.ACCOUNT_STATUS = 'W' then 'Write off'
                               end           keterangan
           from (select *
                 from IFRS_NOMINATIVE
                 where REPORT_DATE = ADD_MONTHS(v_date, -1)
                   AND PRODUCT_CODE not like '7%'
                   and product_code not in ('BPC', 'BSL')) a
                    right join (select *
                                from IFRS_NOMINATIVE
                                where REPORT_DATE = v_date
                                  AND PRODUCT_CODE not like '7%'
                                  and product_code not in ('BPC', 'BSL')) b
                               on a.MASTERID = b.MASTERID
                    join GTMP_CUST_NO_CMPR c
                         on b.CUSTOMER_NUMBER = c.CUSTOMER_NUMBER) e
    ON (d.customer_number = e.cust_no_curr)
    when matched then
        update
        set d.KETERANGAN='Write Off'
        where d.KETERANGAN is null
          and e.keterangan is not null;

-- 3 Flag H CKPN 100%


    merge into GTMP_SELISIH_CMPR d
    using (select distinct b.CUSTOMER_NUMBER cust_no_curr,
                           case
                               when nvl(a.SPECIAL_REASON, ' ') != 'CKPN 100%' and b.SPECIAL_REASON = 'CKPN 100%'
                                   then 'CKPN 100%'
                               end           keterangan
           from GTMP_NOMI_PREV a
                    right join GTMP_NOMI_CURR b
                               on a.MASTERID = b.MASTERID
                    join GTMP_CUST_NO_CMPR c
                         on b.CUSTOMER_NUMBER = c.CUSTOMER_NUMBER) e
    ON (d.customer_number = e.cust_no_curr)
    when matched then
        update
        set d.KETERANGAN='CKPN 100%'
        where d.KETERANGAN is null
          and e.keterangan is not null;

    merge into GTMP_SELISIH_CMPR d
    using (select distinct b.CUSTOMER_NUMBER cust_no_curr,
                           a.SPECIAL_REASON  spc_reason_past,
                           b.SPECIAL_REASON  spc_reason_curr
           from GTMP_NOMI_PREV a
                    right join GTMP_NOMI_CURR b
                               on a.MASTERID = b.MASTERID
                    join (select max(masterid) masterid, cust_no_curr
                          from (select distinct max(b.MASTERID)   masterid,
                                                b.CUSTOMER_NUMBER cust_no_curr,
                                                a.SPECIAL_REASON  spc_reason_past,
                                                b.SPECIAL_REASON  spc_reason_curr,
                                                case
                                                    when nvl(a.SPECIAL_REASON, 0) != 'CKPN 100%' and
                                                         b.SPECIAL_REASON = 'CKPN 100%'
                                                        then 'CKPN 100%'
                                                    end
                                from GTMP_NOMI_PREV a
                                         right join GTMP_NOMI_CURR b
                                                    on a.MASTERID = b.MASTERID
                                         join GTMP_CUST_NO_CMPR c
                                              on b.CUSTOMER_NUMBER = c.CUSTOMER_NUMBER
                                where b.SPECIAL_REASON is not null
                                group by b.CUSTOMER_NUMBER, a.SPECIAL_REASON, b.SPECIAL_REASON,
                                         case
                                             when nvl(a.SPECIAL_REASON, 0) != 'CKPN 100%' and
                                                  b.SPECIAL_REASON = 'CKPN 100%'
                                                 then 'CKPN 100%'
                                             end)
                          group by cust_no_curr) c
                         on b.MASTERID = c.masterid) e
    ON (d.customer_number = e.cust_no_curr)
    when matched then
        update
        set d.SPECIAL_REASON_PAST=e.spc_reason_past,
            d.SPECIAL_REASON_CURRENT=e.spc_reason_curr
        where d.KETERANGAN = 'CKPN 100%';


-- 4 LUNAS

    merge into GTMP_SELISIH_CMPR d
    using (select b.CUSTOMER_NUMBER
           from (select CUSTOMER_NUMBER, count(1) a_curr
                 from ((select *
                        from IFRS_NOMINATIVE
                        where REPORT_DATE = ADD_MONTHS(v_date, -1)
                          AND PRODUCT_CODE not like '7%'
                          and product_code not in ('BPC', 'BSL')
                          and GOL_DEB = v_goldeb))
                 group by CUSTOMER_NUMBER) a
                    join (select CUSTOMER_NUMBER, count(1) b_curr
                          from (select *
                                from IFRS_NOMINATIVE
                                where REPORT_DATE = v_date
                                  AND PRODUCT_CODE not like '7%'
                                  and product_code not in ('BPC', 'BSL')
                                  and GOL_DEB = v_goldeb)
                          where ACCOUNT_STATUS = 'C'
                          group by CUSTOMER_NUMBER) b
                         on a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER
           where a.a_curr = b.b_curr) e
    ON (d.customer_number = e.CUSTOMER_NUMBER)
    when matched then
        update
        set d.KETERANGAN='Customer Lunas'
        where d.KETERANGAN is null;

-- 5 2 Perubahan Assessment Imp

    merge into GTMP_SELISIH_CMPR d
    using (select distinct b.CUSTOMER_NUMBER cust_no_curr,
                           case
                               when a.ASSESSMENT_IMP != b.ASSESSMENT_IMP then 'Perubahan Assessment Imp'
                               end           keterangan
           from GTMP_NOMI_PREV a
                    right join GTMP_NOMI_CURR b
                               on a.MASTERID = b.MASTERID
                    join GTMP_CUST_NO_CMPR c
                         on b.CUSTOMER_NUMBER = c.CUSTOMER_NUMBER) e
    ON (d.customer_number = e.cust_no_curr)
    when matched then
        update
        set d.KETERANGAN=e.keterangan
        where d.KETERANGAN is null
          and e.keterangan is not null;


-- Flag BTB

    merge into GTMP_SELISIH_CMPR d
    using (select distinct b.CUSTOMER_NUMBER cust_no_curr,
                           case
                               when a.BTB_FLAG != b.BTB_FLAG then 'Perubahan Flag BTB'
                               end           keterangan
           from GTMP_NOMI_PREV a
                    right join GTMP_NOMI_CURR b
                               on a.MASTERID = b.MASTERID
                    join GTMP_CUST_NO_CMPR c
                         on b.CUSTOMER_NUMBER = c.CUSTOMER_NUMBER) e
    ON (d.customer_number = e.cust_no_curr)
    when matched then
        update
        set d.KETERANGAN=case
                             when d.keterangan = 'Perubahan Assessment Imp' then d.KETERANGAN || ', ' || e.keterangan
                             when d.KETERANGAN is not null then d.keterangan
                             else e.keterangan
            end
        where e.keterangan is not null;

-- Goldeb

    merge into GTMP_SELISIH_CMPR d
    using (select distinct b.CUSTOMER_NUMBER cust_no_curr,
                           case
                               when a.GOL_DEB != b.GOL_DEB then 'Perubahan Goldeb'
                               end           keterangan
           from (select *
                 from IFRS_NOMINATIVE
                 where REPORT_DATE = ADD_MONTHS(v_date, -1)
                   AND PRODUCT_CODE not like '7%'
                   and product_code not in ('BPC', 'BSL')) a
                    full outer join (select *
                                     from IFRS_NOMINATIVE
                                     where REPORT_DATE = v_date
                                       AND PRODUCT_CODE not like '7%'
                                       and product_code not in ('BPC', 'BSL')) b
                                    on a.MASTERID = b.MASTERID
                    join GTMP_CUST_NO_CMPR c
                         on b.CUSTOMER_NUMBER = c.CUSTOMER_NUMBER
           where a.CUSTOMER_NUMBER in (select customer_number from GTMP_CUST_NO_CMPR)
              or b.CUSTOMER_NUMBER in (select customer_number from GTMP_CUST_NO_CMPR)) e
    ON (d.customer_number = e.cust_no_curr)
    when matched then
        update
        set d.KETERANGAN=case
                             when d.keterangan = 'Perubahan Assessment Imp' or d.keterangan = 'Perubahan Flag BTB' or
                                  d.keterangan = 'Perubahan Assessment Imp, Perubahan Flag BTB'
                                 then d.KETERANGAN || ', ' || e.keterangan
                             when d.KETERANGAN = 'Customer Baru' then e.keterangan
                             when d.KETERANGAN is not null then d.keterangan
                             else e.keterangan
            end
        where e.keterangan is not null;


    merge into GTMP_SELISIH_CMPR d
    using (select coalesce(b.CUSTOMER_NUMBER, a.CUSTOMER_NUMBER)                      CUSTOMER_NUMBER,
                  nvl(OUTSTANDING_ON_BS_LCL_A, 0)                                     OS_ON_PAST,
                  nvl(OUTSTANDING_ON_BS_LCL_B, 0)                                     OS_ON_CURRENT,
                  nvl(OUTSTANDING_ON_BS_LCL_B, 0) - nvl(OUTSTANDING_ON_BS_LCL_A, 0)   OS_ON_SELISIH,
                  nvl(OUTSTANDING_OFF_BS_LCL_A, 0)                                    OS_OFF_PAST,
                  nvl(OUTSTANDING_OFF_BS_LCL_B, 0)                                    OS_OFF_CURRENT,
                  nvl(OUTSTANDING_OFF_BS_LCL_B, 0) - nvl(OUTSTANDING_OFF_BS_LCL_A, 0) OS_OFF_SELISIH,
                  nvl(SALDO_YADIT_LCL_A, 0)                                           YADIT_PAST,
                  nvl(SALDO_YADIT_LCL_B, 0)                                           YADIT_CURR,
                  nvl(SALDO_YADIT_LCL_B, 0) - nvl(SALDO_YADIT_LCL_A, 0)               YADIT_SELISIH,
                  nvl(ECL_TOTAL_LCL_A, 0)                                             ECL_PAST,
                  nvl(ECL_TOTAL_LCL_B, 0)                                             ECL_CURR,
                  nvl(ECL_TOTAL_LCL_B, 0) - nvl(ECL_TOTAL_LCL_A, 0)                   ECL_SELISIH
           from (select CUSTOMER_NUMBER,
                        sum(nvl(OUTSTANDING_ON_BS_LCL, 0))  OUTSTANDING_ON_BS_LCL_A,
                        sum(nvl(OUTSTANDING_OFF_BS_LCL, 0)) OUTSTANDING_OFF_BS_LCL_A,
                        sum(nvl(SALDO_YADIT_LCL, 0))        SALDO_YADIT_LCL_A,
                        sum(nvl(ECL_TOTAL_LCL, 0))          ECL_TOTAL_LCL_A
                 from IFRS_NOMINATIVE
                 where REPORT_DATE = ADD_MONTHS(v_date, -1)
                   AND PRODUCT_CODE not like '7%'
                   and product_code not in ('BPC', 'BSL')
                 group by CUSTOMER_NUMBER) a
                    full outer join (select CUSTOMER_NUMBER,
                                            sum(nvl(OUTSTANDING_ON_BS_LCL, 0))  OUTSTANDING_ON_BS_LCL_B,
                                            sum(nvl(OUTSTANDING_OFF_BS_LCL, 0)) OUTSTANDING_OFF_BS_LCL_B,
                                            sum(nvl(SALDO_YADIT_LCL, 0))        SALDO_YADIT_LCL_B,
                                            sum(nvl(ECL_TOTAL_LCL, 0))          ECL_TOTAL_LCL_B
                                     from IFRS_NOMINATIVE
                                     where REPORT_DATE = v_date
                                       AND PRODUCT_CODE not like '7%'
                                       and product_code not in ('BPC', 'BSL')
                                     group by CUSTOMER_NUMBER) b
                                    on a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER
           where 1 = 1
               and a.CUSTOMER_NUMBER in (select CUSTOMER_NUMBER
                                         from GTMP_SELISIH_CMPR
                                         where KETERANGAN in ('Perubahan Goldeb'))
              or b.CUSTOMER_NUMBER in (select CUSTOMER_NUMBER
                                       from GTMP_SELISIH_CMPR
                                       where KETERANGAN in ('Perubahan Goldeb'))) e
    on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
    when matched then
        update
        set d.OS_ON_PAST=e.OS_ON_PAST,
            d.OS_ON_CURRENT=e.OS_ON_CURRENT,
            d.OS_ON_SELISIH=e.OS_ON_SELISIH,
            d.OS_OFF_PAST=e.OS_OFF_PAST,
            d.OS_OFF_CURRENT=e.OS_OFF_CURRENT,
            d.OS_OFF_SELISIH=e.OS_OFF_SELISIH,
            d.YADIT_PAST=e.YADIT_PAST,
            d.YADIT_CURR=e.YADIT_CURR,
            d.YADIT_SELISIH=e.YADIT_SELISIH,
            d.ECL_PAST=e.ECL_PAST,
            d.ECL_CURR=e.ECL_CURR,
            d.ECL_SELISIH=e.ECL_SELISIH,
            d.KETERANGAN=case
                             when e.ECL_SELISIH >= 2000000000 or e.ECL_SELISIH <= -2000000000 then d.KETERANGAN
                             else '0'
                end;


    delete
    from GTMP_SELISIH_CMPR
    where KETERANGAN = '0';

-- 6 3 Perubahan Cash Flow


    merge into GTMP_SELISIH_CMPR d
    using (select distinct b.CUSTOMER_NUMBER,
                           case
                               when nvl(a.PERCENTAGE, 0) != nvl(b.PERCENTAGE, 0) and
                                    nvl(a.EXPECTED_CF_PERCENT, 0) != nvl(b.EXPECTED_CF_PERCENT, 0) and
                                    a.EXPECTED_PERIOD != b.EXPECTED_PERIOD
                                   then 'Perubahan Presentase WC, Perubahan Cash Flow, Update Target date'
                               when nvl(a.PERCENTAGE, 0) != nvl(b.PERCENTAGE, 0) and
                                    nvl(a.EXPECTED_CF_PERCENT, 0) != nvl(b.EXPECTED_CF_PERCENT, 0)
                                   then 'Perubahan Presentase WC, Perubahan Cash Flow'
                               when nvl(a.EXPECTED_CF_PERCENT, 0) != nvl(b.EXPECTED_CF_PERCENT, 0) and
                                    a.EXPECTED_PERIOD != b.EXPECTED_PERIOD
                                   then 'Perubahan Cash Flow, Update Target date'
                               when nvl(a.PERCENTAGE, 0) != nvl(b.PERCENTAGE, 0) and
                                    a.EXPECTED_PERIOD != b.EXPECTED_PERIOD
                                   then 'Perubahan Presentase WC, Update Target date'
                               when nvl(a.PERCENTAGE, 0) != nvl(b.PERCENTAGE, 0) then 'Perubahan Presentase WC'
                               when nvl(a.EXPECTED_CF_PERCENT, 0) != nvl(b.EXPECTED_CF_PERCENT, 0)
                                   then 'Perubahan Cash Flow'
                               when a.EXPECTED_PERIOD != b.EXPECTED_PERIOD then 'Update Target date'
                               end keterangan
           from (select distinct a.CUSTOMER_NUMBER, c.EXPECTED_CF_PERCENT, PERCENTAGE, c.EXPECTED_PERIOD
                 from GTMP_NOMI_PREV a
                          left join TBLU_WORSTCASE_LIST b
                                    on a.REPORT_DATE = b.DOWNLOAD_DATE and a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER
                          left join TBLU_DCF_BULK c
                                    on a.REPORT_DATE = c.EFFECTIVE_DATE and a.CUSTOMER_NUMBER = c.CUSTOMER_NUMBER) a
                    right join (select distinct a.CUSTOMER_NUMBER, c.EXPECTED_CF_PERCENT, PERCENTAGE, c.EXPECTED_PERIOD
                                from GTMP_NOMI_CURR a
                                         left join TBLU_WORSTCASE_LIST b
                                                   on a.REPORT_DATE = b.DOWNLOAD_DATE and
                                                      a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER
                                         left join TBLU_DCF_BULK c
                                                   on a.REPORT_DATE = c.EFFECTIVE_DATE and
                                                      a.CUSTOMER_NUMBER = c.CUSTOMER_NUMBER) b
                               on a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER
                    join GTMP_CUST_NO_CMPR c
                         on b.CUSTOMER_NUMBER = c.CUSTOMER_NUMBER) e
    ON (d.customer_number = e.CUSTOMER_NUMBER)
    when matched then
        update
        set d.KETERANGAN=e.keterangan
        where d.KETERANGAN is null
          and e.keterangan is not null;


    merge into GTMP_SELISIH_CMPR d
    using (select distinct coalesce(b.CUSTOMER_NUMBER, a.CUSTOMER_NUMBER) customer_number,
                           nvl(a.PERCENTAGE, 0)                           PERCENTAGE_PAST,
                           nvl(b.PERCENTAGE, 0)                           PERCENTAGE_CURRENT,
                           nvl(a.EXPECTED_CF_PERCENT, 0)                  EXPECTED_CF_PERCENT_PAST,
                           nvl(b.EXPECTED_CF_PERCENT, 0)                  EXPECTED_CF_PERCENT_CURRENT,
                           a.EXPECTED_PERIOD                              EXPECTED_PERIOD_PAST,
                           b.EXPECTED_PERIOD                              EXPECTED_PERIOD_CURRENT
           from (select distinct a.CUSTOMER_NUMBER, c.EXPECTED_CF_PERCENT, PERCENTAGE, c.EXPECTED_PERIOD
                 from GTMP_NOMI_PREV a
                          left join TBLU_WORSTCASE_LIST b
                                    on a.REPORT_DATE = b.DOWNLOAD_DATE and a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER
                          left join TBLU_DCF_BULK c
                                    on a.REPORT_DATE = c.EFFECTIVE_DATE and a.CUSTOMER_NUMBER = c.CUSTOMER_NUMBER) a
                    full outer join (select distinct a.CUSTOMER_NUMBER,
                                                     c.EXPECTED_CF_PERCENT,
                                                     PERCENTAGE,
                                                     c.EXPECTED_PERIOD
                                     from GTMP_NOMI_CURR a
                                              left join TBLU_WORSTCASE_LIST b
                                                        on a.REPORT_DATE = b.DOWNLOAD_DATE and
                                                           a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER
                                              left join TBLU_DCF_BULK c
                                                        on a.REPORT_DATE = c.EFFECTIVE_DATE and
                                                           a.CUSTOMER_NUMBER = c.CUSTOMER_NUMBER) b
                                    on a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER
           where a.CUSTOMER_NUMBER in (select customer_number from GTMP_CUST_NO_CMPR)
              or b.CUSTOMER_NUMBER in (select customer_number from GTMP_CUST_NO_CMPR)) e
    ON (d.customer_number = e.CUSTOMER_NUMBER)
    when matched then
        update
        set d.WORST_CASE_PAST=e.PERCENTAGE_PAST,
            d.WORST_CASE_CURRENT=e.PERCENTAGE_CURRENT,
            d.CASH_FLOW_PAST=e.EXPECTED_CF_PERCENT_PAST,
            d.CASH_FLOW_CURRENT=e.EXPECTED_CF_PERCENT_CURRENT,
            d.TARGET_DATE_PAST=e.EXPECTED_PERIOD_PAST,
            d.TARGET_DATE_CURRENT=e.EXPECTED_PERIOD_CURRENT;


-- Rating
    merge into GTMP_SELISIH_CMPR d
    using (select distinct a.IMP_RATING                                   rating_past,
                           b.IMP_RATING                                   rating_current,
                           coalesce(b.CUSTOMER_NUMBER, a.CUSTOMER_NUMBER) CUSTOMER_NUMBER,
                           dense_rank() over ( partition by coalesce(b.CUSTOMER_NUMBER, a.CUSTOMER_NUMBER)
                               ORDER BY CASE coalesce(b.IMP_RATING, a.IMP_RATING)
                                            WHEN 'LOSS' THEN 1
                                            WHEN 'RR10' THEN 2
                                            WHEN 'RR9' THEN 3
                                            WHEN 'RR8' THEN 4
                                            WHEN 'RR7' THEN 5
                                            WHEN 'RR6' THEN 6
                                            WHEN 'RR5' THEN 7
                                            WHEN 'RR4' THEN 8
                                            WHEN 'RR3' THEN 9
                                            WHEN 'RR2' THEN 10
                                            WHEN 'RR1' THEN 11
                                            WHEN 'BR10' THEN 13
                                            WHEN 'BR9' THEN 14
                                            WHEN 'BR8' THEN 15
                                            WHEN 'BR7' THEN 16
                                            WHEN 'BR6' THEN 17
                                            WHEN 'BR5' THEN 18
                                            WHEN 'BR4' THEN 19
                                            WHEN 'BR3' THEN 20
                                            WHEN 'BR2' THEN 21
                                            WHEN 'BR1' THEN 21
                                            WHEN '5' THEN 22
                                            WHEN '4' THEN 23
                                            WHEN '3' THEN 24
                                            WHEN '2' THEN 25
                                            WHEN '1' THEN 26
                                            ELSE 12
                                   END ) As                               priority
           from GTMP_NOMI_PREV a
                    join GTMP_NOMI_CURR b on a.MASTERID = b.MASTERID
                    join GTMP_CUST_NO_CMPR c
                         on coalesce(b.CUSTOMER_NUMBER, a.CUSTOMER_NUMBER) = c.CUSTOMER_NUMBER
           where (a.IMP_RATING is not null or b.IMP_RATING is not null)
             and a.IMP_RATING != b.IMP_RATING
             and a.DATA_SOURCE = b.DATA_SOURCE
             AND coalesce(a.DATA_SOURCE, b.DATA_SOURCE) != 'CRD') e
    on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
    when matched then
        update
        set d.RATING_PAST=e.rating_past,
            d.RATING_CURRENT=e.rating_current
        where priority = 1;


    merge into GTMP_SELISIH_CMPR d
    using (select distinct a.IMP_RATING                                   rating_past,
                           b.IMP_RATING                                   rating_current,
                           coalesce(b.CUSTOMER_NUMBER, a.CUSTOMER_NUMBER) CUSTOMER_NUMBER,
                           dense_rank() over ( partition by coalesce(b.CUSTOMER_NUMBER, a.CUSTOMER_NUMBER)
                               ORDER BY CASE coalesce(b.IMP_RATING, a.IMP_RATING)
                                            WHEN 'LOSS' THEN 1
                                            WHEN 'RR10' THEN 2
                                            WHEN 'RR9' THEN 3
                                            WHEN 'RR8' THEN 4
                                            WHEN 'RR7' THEN 5
                                            WHEN 'RR6' THEN 6
                                            WHEN 'RR5' THEN 7
                                            WHEN 'RR4' THEN 8
                                            WHEN 'RR3' THEN 9
                                            WHEN 'RR2' THEN 10
                                            WHEN 'RR1' THEN 11
                                            WHEN 'BR10' THEN 13
                                            WHEN 'BR9' THEN 14
                                            WHEN 'BR8' THEN 15
                                            WHEN 'BR7' THEN 16
                                            WHEN 'BR6' THEN 17
                                            WHEN 'BR5' THEN 18
                                            WHEN 'BR4' THEN 19
                                            WHEN 'BR3' THEN 20
                                            WHEN 'BR2' THEN 21
                                            WHEN 'BR1' THEN 21
                                            WHEN '5' THEN 22
                                            WHEN '4' THEN 23
                                            WHEN '3' THEN 24
                                            WHEN '2' THEN 25
                                            WHEN '1' THEN 26
                                            ELSE 12
                                   END ) As                               priority
           from GTMP_NOMI_PREV a
                    join GTMP_NOMI_CURR b on a.MASTERID = b.MASTERID
                    join GTMP_CUST_NO_CMPR c
                         on coalesce(b.CUSTOMER_NUMBER, a.CUSTOMER_NUMBER) = c.CUSTOMER_NUMBER
           where (a.IMP_RATING is not null or b.IMP_RATING is not null)
             and a.IMP_RATING = b.IMP_RATING
             and a.DATA_SOURCE = b.DATA_SOURCE
             AND coalesce(a.DATA_SOURCE, b.DATA_SOURCE) != 'CRD') e
    on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
    when matched then
        update
        set d.RATING_PAST=e.rating_past,
            d.RATING_CURRENT=e.rating_current
        where d.RATING_PAST is null
          and d.rating_current is null
          and e.priority = 1;

    merge into GTMP_SELISIH_CMPR d
    using (select distinct b.IMP_RATING                                   rating_current,
                           coalesce(b.CUSTOMER_NUMBER, a.CUSTOMER_NUMBER) CUSTOMER_NUMBER,
                           dense_rank() over ( partition by b.CUSTOMER_NUMBER
                               ORDER BY CASE b.IMP_RATING
                                            WHEN 'LOSS' THEN 1
                                            WHEN 'RR10' THEN 2
                                            WHEN 'RR9' THEN 3
                                            WHEN 'RR8' THEN 4
                                            WHEN 'RR7' THEN 5
                                            WHEN 'RR6' THEN 6
                                            WHEN 'RR5' THEN 7
                                            WHEN 'RR4' THEN 8
                                            WHEN 'RR3' THEN 9
                                            WHEN 'RR2' THEN 10
                                            WHEN 'RR1' THEN 11
                                            WHEN 'BR10' THEN 13
                                            WHEN 'BR9' THEN 14
                                            WHEN 'BR8' THEN 15
                                            WHEN 'BR7' THEN 16
                                            WHEN 'BR6' THEN 17
                                            WHEN 'BR5' THEN 18
                                            WHEN 'BR4' THEN 19
                                            WHEN 'BR3' THEN 20
                                            WHEN 'BR2' THEN 21
                                            WHEN 'BR1' THEN 21
                                            WHEN '5' THEN 22
                                            WHEN '4' THEN 23
                                            WHEN '3' THEN 24
                                            WHEN '2' THEN 25
                                            WHEN '1' THEN 26
                                            ELSE 12
                                   END ) As                               priority
           from GTMP_NOMI_PREV a
                    full outer join GTMP_NOMI_CURR b on a.MASTERID = b.MASTERID
                    join GTMP_CUST_NO_CMPR c
                         on coalesce(b.CUSTOMER_NUMBER, a.CUSTOMER_NUMBER) = c.CUSTOMER_NUMBER
           where (b.IMP_RATING is not null)) e
    on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
    when matched then
        update
        set d.RATING_CURRENT=e.rating_current
        where d.rating_current is null
          and e.priority = 1;

    merge into GTMP_SELISIH_CMPR d
    using (select distinct a.IMP_RATING                                   rating_past,
                           coalesce(b.CUSTOMER_NUMBER, a.CUSTOMER_NUMBER) CUSTOMER_NUMBER,
                           dense_rank() over ( partition by coalesce(b.CUSTOMER_NUMBER, a.CUSTOMER_NUMBER)
                               ORDER BY CASE coalesce(b.IMP_RATING, a.IMP_RATING)
                                            WHEN 'LOSS' THEN 1
                                            WHEN 'RR10' THEN 2
                                            WHEN 'RR9' THEN 3
                                            WHEN 'RR8' THEN 4
                                            WHEN 'RR7' THEN 5
                                            WHEN 'RR6' THEN 6
                                            WHEN 'RR5' THEN 7
                                            WHEN 'RR4' THEN 8
                                            WHEN 'RR3' THEN 9
                                            WHEN 'RR2' THEN 10
                                            WHEN 'RR1' THEN 11
                                            WHEN 'BR10' THEN 13
                                            WHEN 'BR9' THEN 14
                                            WHEN 'BR8' THEN 15
                                            WHEN 'BR7' THEN 16
                                            WHEN 'BR6' THEN 17
                                            WHEN 'BR5' THEN 18
                                            WHEN 'BR4' THEN 19
                                            WHEN 'BR3' THEN 20
                                            WHEN 'BR2' THEN 21
                                            WHEN 'BR1' THEN 21
                                            WHEN '5' THEN 22
                                            WHEN '4' THEN 23
                                            WHEN '3' THEN 24
                                            WHEN '2' THEN 25
                                            WHEN '1' THEN 26
                                            ELSE 12
                                   END ) As                               priority
           from GTMP_NOMI_PREV a
                    full outer join GTMP_NOMI_CURR b on a.MASTERID = b.MASTERID
                    join GTMP_CUST_NO_CMPR c
                         on coalesce(b.CUSTOMER_NUMBER, a.CUSTOMER_NUMBER) = c.CUSTOMER_NUMBER
           where (a.IMP_RATING is not null)) e
    on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
    when matched then
        update
        set d.RATING_PAST=e.rating_past
        where d.RATING_PAST is null
          and e.priority = 1;

    update GTMP_SELISIH_CMPR
    set KETERANGAN = 'Perubahan Rating'
    where RATING_PAST != RATING_CURRENT
      and KETERANGAN is null;

    -- 4 Stage
-----------------------
    merge into GTMP_SELISIH_CMPR d
    using (select distinct max(a.RESERVED_VARCHAR_4) stage_past,
                           a.CUSTOMER_NUMBER         CUSTOMER_NUMBER
           from GTMP_NOMI_PREV a
                    join GTMP_CUST_NO_CMPR c
                         on a.CUSTOMER_NUMBER = c.CUSTOMER_NUMBER
           where a.RESERVED_VARCHAR_4 is not null
           group by a.CUSTOMER_NUMBER) e
    on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
    when matched then
        update
        set d.STAGE_PAST=e.stage_past;

    merge into GTMP_SELISIH_CMPR d
    using (select distinct max(b.RESERVED_VARCHAR_4) stage_curr,
                           b.CUSTOMER_NUMBER         CUSTOMER_NUMBER
           from GTMP_NOMI_CURR b
                    join GTMP_CUST_NO_CMPR c
                         on b.CUSTOMER_NUMBER = c.CUSTOMER_NUMBER
           where b.RESERVED_VARCHAR_4 is not null
           group by b.CUSTOMER_NUMBER) e
    on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
    when matched then
        update
        set d.STAGE_CURRENT=e.stage_curr;


    update GTMP_SELISIH_CMPR
    set KETERANGAN = case
                         when KETERANGAN = 'Perubahan Rating' then KETERANGAN || ', Perubahan Stage'
                         when KETERANGAN is null then 'Perubahan Stage' end
    where STAGE_PAST != STAGE_CURRENT
      and (KETERANGAN is null
        OR KETERANGAN = 'Perubahan Rating');


-- 5 OS Unused Amount

    MERGE INTO GTMP_SELISIH_CMPR d
    USING (select coalesce(b.CUSTOMER_NUMBER, a.CUSTOMER_NUMBER)                      CUSTOMER_NUMBER,
                  nvl(OUTSTANDING_ON_BS_CCY_B, 0) - nvl(OUTSTANDING_ON_BS_CCY_A, 0)   OS_ON_SELISIH,
                  nvl(OUTSTANDING_OFF_BS_CCY_B, 0) - nvl(OUTSTANDING_OFF_BS_CCY_A, 0) OS_OFF_SELISIH
           from (select CUSTOMER_NUMBER,
                        sum(nvl(OUTSTANDING_ON_BS_CCY, 0))  OUTSTANDING_ON_BS_CCY_A,
                        sum(nvl(OUTSTANDING_OFF_BS_CCY, 0)) OUTSTANDING_OFF_BS_CCY_A
                 from GTMP_NOMI_PREV
                 group by CUSTOMER_NUMBER) a
                    full outer join (select CUSTOMER_NUMBER,
                                            sum(nvl(OUTSTANDING_ON_BS_CCY, 0))  OUTSTANDING_ON_BS_CCY_B,
                                            sum(nvl(OUTSTANDING_OFF_BS_CCY, 0)) OUTSTANDING_OFF_BS_CCY_B
                                     from GTMP_NOMI_CURR
                                     group by CUSTOMER_NUMBER) b
                                    on a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER
           where 1 = 1
               and a.CUSTOMER_NUMBER in (select customer_number from GTMP_CUST_NO_CMPR)
              or b.CUSTOMER_NUMBER in (select customer_number from GTMP_CUST_NO_CMPR)) e
    on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
    when matched then
        update
        set KETERANGAN = case
                             when e.OS_ON_SELISIH != 0 and e.OS_OFF_SELISIH != 0
                                 then 'Perubahan Outstanding, Perubahan Unused Amount'
                             when e.OS_ON_SELISIH != 0 then 'Perubahan Outstanding'
                             when e.OS_OFF_SELISIH != 0 then 'Perubahan Unused Amount' end
        where KETERANGAN is null;


-- 6 Kurs

    merge into GTMP_SELISIH_CMPR d
    using (select distinct b.CUSTOMER_NUMBER CUSTOMER_NUMBER,
                           case
                               when a.EXCHANGE_RATE != b.EXCHANGE_RATE
                                   then 'Perubahan Kurs'
                               end           keterangan
           from GTMP_NOMI_PREV a
                    right join GTMP_NOMI_CURR b
                               on a.MASTERID = b.MASTERID
                    join GTMP_CUST_NO_CMPR c
                         on b.CUSTOMER_NUMBER = c.CUSTOMER_NUMBER) e
    on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
    when matched then
        update
        set d.KETERANGAN='Perubahan Kurs'
        where d.KETERANGAN is null
          and e.keterangan is not null;


-- 7 Lainnya

    update GTMP_SELISIH_CMPR
    set KETERANGAN = 'Lainnya (EIR berubah, Unwinding 0, Yadit, dll.)'
    where KETERANGAN is null;

    merge into GTMP_SELISIH_CMPR d
    using (select CUSTOMER_NUMBER, max(CUSTOMER_NAME) CUSTOMER_NAME
           from GTMP_NOMI_CURR
           where CUSTOMER_NAME is not null
           group by CUSTOMER_NUMBER) e
    on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
    when matched then
        update
        set d.CUSTOMER_NAME =e.CUSTOMER_NAME;


    merge into GTMP_SELISIH_CMPR d
    using (select CUSTOMER_NUMBER, max(CUSTOMER_NAME) CUSTOMER_NAME
           from GTMP_NOMI_PREV
           where CUSTOMER_NAME is not null
           group by CUSTOMER_NUMBER) e
    on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
    when matched then
        update
        set d.CUSTOMER_NAME =e.CUSTOMER_NAME
        where d.CUSTOMER_NAME is null;


    merge into GTMP_SELISIH_CMPR d
    using (select distinct a.SPECIAL_REASON,
                           a.CUSTOMER_NUMBER CUSTOMER_NUMBER,
                           dense_rank() over ( partition by a.CUSTOMER_NUMBER
                               ORDER BY CASE a.SPECIAL_REASON
                                            WHEN 'CKPN 100%' THEN 1
                                            WHEN 'INDIVIDUAL' THEN 2
                                            WHEN 'WORSTCASE' THEN 3
                                            WHEN 'DISASTER LOAN' THEN 4
                                            WHEN 'ADJUSTMENT COVID' THEN 5
                                            WHEN 'SBLC' THEN 6
                                            WHEN 'BACK-T0-BACK, NO IMPAIRMENT' THEN 7
                                            ELSE 8
                                   END ) As  priority
           from GTMP_NOMI_PREV a
                    join GTMP_CUST_NO_CMPR c
                         on a.CUSTOMER_NUMBER = c.CUSTOMER_NUMBER
           where SPECIAL_REASON is not null) e
    on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
    when matched then
        update
        set d.SPECIAL_REASON_PAST=e.SPECIAL_REASON
        where e.priority = 1;


    merge into GTMP_SELISIH_CMPR d
    using (select distinct a.SPECIAL_REASON,
                           a.CUSTOMER_NUMBER CUSTOMER_NUMBER,
                           dense_rank() over ( partition by a.CUSTOMER_NUMBER
                               ORDER BY CASE a.SPECIAL_REASON
                                            WHEN 'CKPN 100%' THEN 1
                                            WHEN 'INDIVIDUAL' THEN 2
                                            WHEN 'WORSTCASE' THEN 3
                                            WHEN 'DISASTER LOAN' THEN 4
                                            WHEN 'ADJUSTMENT COVID' THEN 5
                                            WHEN 'SBLC' THEN 6
                                            WHEN 'BACK-T0-BACK, NO IMPAIRMENT' THEN 7
                                            ELSE 8
                                   END ) As  priority
           from GTMP_NOMI_CURR a
                    join GTMP_CUST_NO_CMPR c
                         on a.CUSTOMER_NUMBER = c.CUSTOMER_NUMBER
           where SPECIAL_REASON is not null) e
    on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
    when matched then
        update
        set d.SPECIAL_REASON_CURRENT=e.SPECIAL_REASON
        where e.priority = 1;


    delete
    from IFRS_NOMINATIVE_KOMPARASI
    where REPORT_DATE = v_date
      and SEGMENT = case when v_goldeb = 'L' then 'CORPORATE' when v_goldeb = 'M' then 'COMMERCIAL' end;

    insert into IFRS_NOMINATIVE_KOMPARASI
    select 0, v_date, case when v_goldeb = 'L' then 'CORPORATE' when v_goldeb = 'M' then 'COMMERCIAL' end, a.*
    from GTMP_SELISIH_CMPR a;

    commit;
end;