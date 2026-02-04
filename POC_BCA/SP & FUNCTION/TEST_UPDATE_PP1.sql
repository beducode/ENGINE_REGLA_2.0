CREATE OR REPLACE PROCEDURE TEST_UPDATE_PP1
as
  v_date date;
  V_PREV DATE;
begin

    EXECUTE IMMEDIATE 'alter session set temp_undo_enabled=true';
    EXECUTE IMMEDIATE 'alter session enable parallel dml';
-------------------------------------------------------------------------------- Step 1 data dari jan 20 - MAY 25

    v_date := '31-JAN-2020';
    V_PREV := '31-JAN-2019';
--
--

   DELETE /*+ PARALLEL(12) */ IFRS.IFRS_CCF_HEADER
    WHERE DOWNLOAD_DATE >= V_DATE
    AND SEGMENTATION_ID NOT IN (410,411);
    commit;

   DELETE /*+ PARALLEL(12) */ IFRS.IFRS_CCF_DETAIL_PROCESS
    WHERE DOWNLOAD_DATE >= V_DATE AND PRODUCT_CODE_L IN ('CARDS','00541');
   COMMIT;

   DELETE /*+ PARALLEL(12) */ IFRS.IFRS_CCF_DETAIL_UNPROCESS
    WHERE DOWNLOAD_DATE >= V_DATE AND PRODUCT_CODE_L IN ('CARDS','00541');
    COMMIT;

    UPDATE /*+ PARALLEL(12) */ IFRS.IFRS_CCF_DETAIL
SET SEGMENTATION_ID = case  when segmentation_id = '20476' then '217'
                            when segmentation_id = '20477' then '217'
                            when segmentation_id = '20478' then '193'
                            when segmentation_id = '20479' then '193' end,
    CCF_RULE_ID = case  when ccf_rule_id = '10146' then '23'
                        when ccf_rule_id = '10148' then '23'
                        when ccf_rule_id = '10150' then '25'
                        when ccf_rule_id = '10152' then '25' end
WHERE DOWNLOAD_DATE BETWEEN '31-OCT-2024' AND '31-MAY-2025' AND SEGMENTATION_ID IN (20476,20477,20478,20479)
AND FIRST_NPL_DATE >= '31-JAN-2020';
COMMIT;

    while v_date <= '31-MAY-2025' loop
--

insert /*+ PARALLEL(12) */ into IFRS.ifrs_ccf_detail
(download_date,current_date,account_number,customer_number,customer_name,account_status,segmentation_id,ccf_rule_id,first_npl_date,currency,os_cur,os_prev,limit_cur,limit_prev,used_amount_cur,used_amount_prev,revolving_flag_l,revolving_flag_i,product_code_l,product_code_i,os_selisih,
limit_selisih,ccf_result,createdby,createddate,createdhost)
select d.download_date,d.current_date,d.account_number,d.customer_number,d.customer_name,d.account_status,
case when d.segmentation_id = '217' and e.flag = 'T' then '20476'
     when d.segmentation_id = '217' and e.flag = 'NT' then '20477'
     when d.segmentation_id = '193' and e.flag = 'T' then '20478'
     when d.segmentation_id = '193' and e.flag = 'NT' then '20479' end segmentation_id,
case when d.segmentation_id = '217' and e.flag = 'T' then '10146'
     when d.segmentation_id = '217' and e.flag = 'NT' then '10148'
     when d.segmentation_id = '193' and e.flag = 'T' then '10150'
     when d.segmentation_id = '193' and e.flag = 'NT' then '10152' end ccf_rule_id,
d.first_npl_date, d.currency,d.os_cur,d.os_prev,d.limit_cur,d.limit_prev,d.used_amount_cur,d.used_amount_prev,d.revolving_flag_l,d.revolving_flag_i,d.product_code_l,d.product_code_i,d.os_selisih,d.limit_selisih,d.ccf_result,d.createdby,d.createddate,d.createdhost
from IFRS.IFRS_CCF_DETAIL d join (
select * from (
select a.account_number,case when b.flag is not null then b.flag when b.flag is null then 'NT' end flag from(select account_number,count(*) from IFRS.IFRS_CCF_DETAIL where download_date = V_DATE
and segmentation_id in (217,193)
group by account_number having count(*) = 1
) a left join
IFRS.ifrs_transactor_flag b
on a.account_number = b.account_number and b.download_date = V_PREV) ) e
on d.account_number = e.account_number and d.download_date = V_DATE
WHERE D.FIRST_NPL_DATE >= '31-JAN-2020';
COMMIT;

         v_date := ADD_MONTHS(V_DATE,1);
         V_PREV := ADD_MONTHS(V_PREV,1);

       update IFRS.IFRS_DATE_DAY1
       set currdate = v_date,
           PREVDATE = V_PREV;
       commit;

    end loop;

-------------------------------------------------------------------------------- Perbaikan data Jun 25

merge /*+ PARALLEL(12) */ into IFRS.ifrs_ccf_detail a
using (select account_number,reserved_varchar_30 flag, reserved_varchar_2 from ifrs_master_account_monthly where download_date = '30-jun-2024' and data_source = 'CRD')b
on (a.download_date = '30-jun-2025' and a.account_number = b.account_number)
when matched then update
set SEGMENTATION_ID = CASE WHEN B.FLAG = 'T' AND RESERVED_VARCHAR_2 = 'I' THEN 20476
                           WHEN B.FLAG = 'N' AND RESERVED_VARCHAR_2 = 'I' THEN 20477
                           WHEN B.FLAG = 'T' AND RESERVED_VARCHAR_2 = 'O' THEN 20478
                           WHEN B.FLAG = 'N' AND RESERVED_VARCHAR_2 = 'O' THEN 20479 END,
    CCF_RULE_ID = CASE WHEN B.FLAG = 'T' AND RESERVED_VARCHAR_2 = 'I' THEN 10146
                       WHEN B.FLAG = 'N' AND RESERVED_VARCHAR_2 = 'I' THEN 10148
                       WHEN B.FLAG = 'T' AND RESERVED_VARCHAR_2 = 'O' THEN 10150
                       WHEN B.FLAG = 'N' AND RESERVED_VARCHAR_2 = 'O' THEN 10152 END;

commit;

insert /*+ PARALLEL(12) */ into IFRS.ifrs_ccf_detail
(download_date,current_date,account_number,customer_number,customer_name,account_status,segmentation_id,ccf_rule_id,first_npl_date,currency,os_cur,os_prev,limit_cur,limit_prev,used_amount_cur,used_amount_prev,revolving_flag_l,revolving_flag_i,product_code_l,product_code_i,os_selisih,
limit_selisih,ccf_result,createdby,createddate,createdhost)
select d.download_date,d.current_date,d.account_number,d.customer_number,d.customer_name,d.account_status,
case when d.segmentation_id = '20476' then '217'
     when d.segmentation_id = '20477' then '217'
     when d.segmentation_id = '20478' then '193'
     when d.segmentation_id = '20479' then '193' end segmentation_id,
case when d.ccf_rule_id = '10146' then '23'
     when d.ccf_rule_id = '10148' then '23'
     when d.ccf_rule_id = '10150' then '25'
     when d.ccf_rule_id = '10152' then '25' end ccf_rule_id,
d.first_npl_date, d.currency,d.os_cur,d.os_prev,d.limit_cur,d.limit_prev,d.used_amount_cur,d.used_amount_prev,d.revolving_flag_l,d.revolving_flag_i,d.product_code_l,d.product_code_i,d.os_selisih,d.limit_selisih,d.ccf_result,d.createdby,d.createddate,d.createdhost
from IFRS.ifrs_ccf_detail d
where download_Date = '30-jun-2025'
and segmentation_id in (20476,20477,20478,20479);
commit;

end;