CREATE OR REPLACE procedure           SP_IFRS_REPORT_BIAYA_CKPN_PROCESS(V_INPUT varchar2) as
    V_STR_SQL         varchar2(4000);
    V_STR_SQL_RULE    varchar2(4000);
    V_SEGMENT         varchar2(150);
    V_CURRDATE        varchar2(20);
    V_PREVDATE        varchar2(20);
    V_REASON_VALUE_10 varchar2(50);
    V_REASON_VALUE_11 varchar2(50);
    V_REASON_VALUE_12 varchar2(50);
    V_PROCESS_NAME    VARCHAR2(300) := 'SP_IFRS_REPORT_BIAYA_CKPN_PROCESS (' || V_INPUT || ')';
begin

    --CREATED BY LEO
    --CREATED DATE 2024/12/13

    IFRS.write_log('INFO', V_PROCESS_NAME, 'Proses dimulai.');
    EXECUTE IMMEDIATE 'alter session enable parallel dml';

    select to_char(add_months(CURRDATE, -1), 'dd-mon-yyyy'), to_char(CURRDATE, 'dd-mon-yyyy')
    into V_PREVDATE,V_CURRDATE
    from IFRS_PRC_DATE;
    IFRS.write_log('DEBUG', V_PROCESS_NAME, 'Current Date: ' || V_CURRDATE || ', Prev Date: ' || V_PREVDATE);
    IFRS.SP_IFRS_GENERATE_RULE_SEGMENT('REP_SEG');

    select /*+ PARALLEL(8) */ SEGMENT, CONDITION
    INTO V_SEGMENT, V_STR_SQL_RULE
    FROM IFRS.GTMP_SCENARIO_SEGMENT_GENQUERY
    where SEGMENT = V_INPUT;

    IFRS.write_log('DEBUG', V_PROCESS_NAME, 'Rule Segment didapatkan untuk: ' || V_SEGMENT);
    -- Pastikan aturan tidak menyebabkan error
    IF V_STR_SQL_RULE IS NULL OR TRIM(V_STR_SQL_RULE) = '' THEN
        V_STR_SQL_RULE := '1=1';
    END IF;

    -- ==========================================================
    -- LANGKAH-LANGKAH DML (SATU PER SATU)
    -- ==========================================================

    -- 1. Insert awal ke IFRS.GTMP_REPORT_BIAYA_CKPN dari ECL_MOVE_VALAS
    IFRS.write_log('INFO', V_PROCESS_NAME, '1. Insert awal dari ECL_MOVE_VALAS...');
    V_STR_SQL := 'insert into /*+ PARALLEL(8) */ IFRS.GTMP_REPORT_BIAYA_CKPN
    select /*+ PARALLEL(8) */   CUSTOMER_NUMBER, null CUSTOMER_NAME, null ASS_IMP_PAST, null ASS_IMP_CURRENT, null STAGE_PAST, null STAGE_CURRENT,
           null RATING_PAST, null RATING_CURRENT, null CASH_FLOW_PAST, null CASH_FLOW_CURRENT, null WORSTCASE_PAST, null WORSTCASE_CURRENT,
           null TARGET_DATE_PAST, null TARGET_DATE_CURRENT, null SPECIAL_REASON_PAST, null SPECIAL_REASON_CURRENT, null BUCKET_PAST, null BUCKET_CURRENT,
           0 OS_ON_PAST, 0 OS_ON_CURRENT, 0 OS_ON_MOVEMENT, 0 OS_OFF_PAST, 0 OS_OFF_CURRENT, 0 OS_OFF_MOVEMENT,
           null   ECL_ON_PAST,
           null   ECL_ON_CURRENT,
           null   ECL_OFF_PAST,
           null   ECL_OFF_CURRENT,
           null   ECL_TOTAL_PAST,
           null   ECL_TOTAL_CURRENT,
           sum(nvl(case when SEQ_NO = 6 then ECL_TOTAL end, 0))         ECL_WO,
           sum(nvl(case when SEQ_NO = 7 then ECL_ON_BS end, 0))         ECL_FOREIGN_CURRENCY_EFFECT_ON,
           sum(nvl(case when SEQ_NO = 7 then ECL_OFF_BS end, 0))        ECL_FOREIGN_CURRENCY_EFFECT_OFF,
           sum(nvl(ECL_ON_BS, 0))
               - sum(nvl(case when SEQ_NO = 6 then ECL_ON_BS end, 0))
               - sum(nvl(case when SEQ_NO = 7 then ECL_ON_BS end, 0))   BIAYA_CADANGAN_BULAN_INI_ON,
           sum(nvl(ECL_OFF_BS, 0))
               - sum(nvl(case when SEQ_NO = 6 then ECL_OFF_BS end, 0))
               - sum(nvl(case when SEQ_NO = 7 then ECL_OFF_BS end, 0))  BIAYA_CADANGAN_BULAN_INI_OFF,
           sum(nvl(ECL_TOTAL, 0))
               - sum(nvl(case when SEQ_NO = 6 then ECL_TOTAL end, 0))
               - sum(nvl(case when SEQ_NO = 7 then ECL_TOTAL end, 0))   BIAYA_CADANGAN_BULAN_INI_TOTAL,
           null                                                         REASON
    from IFRS_REPORT_ECL_MOVE_VALAS_DTL A
    where REPORT_DATE =''' || V_CURRDATE || '''
    and SEQ_NO not in (0, 99) AND  ('
        || RTRIM(NVL(V_STR_SQL_RULE, '')) || ') group by CUSTOMER_NUMBER';

    EXECUTE IMMEDIATE V_STR_SQL;
    IFRS.write_log('INFO', V_PROCESS_NAME, '   -> Selesai. ' || SQL%ROWCOUNT || ' baris dimasukkan.');

    commit;

    -- 2. Insert customer yang belum ada dari IFRS_NOMINATIVE
    IFRS.write_log('INFO', V_PROCESS_NAME, '2. Insert customer baru dari IFRS_NOMINATIVE...');
    V_STR_SQL := 'insert into /*+ PARALLEL(8) */ IFRS.GTMP_REPORT_BIAYA_CKPN (CUSTOMER_NUMBER)
select CUSTOMER_NUMBER
from (select distinct CUSTOMER_NUMBER, SUB_SEGMENT
      from IFRS_NOMINATIVE partition for (''' || V_CURRDATE || ''')
      union
      select distinct CUSTOMER_NUMBER, SUB_SEGMENT
      from IFRS_NOMINATIVE partition for (''' || V_PREVDATE || ''')) A
where CUSTOMER_NUMBER not in (select CUSTOMER_NUMBER from IFRS.GTMP_REPORT_BIAYA_CKPN) and (' ||
                 RTRIM(NVL(V_STR_SQL_RULE, '')) || ')';

    EXECUTE IMMEDIATE V_STR_SQL;
    IFRS.write_log('INFO', V_PROCESS_NAME, '   -> Selesai. ' || SQL%ROWCOUNT || ' baris dimasukkan.');

    commit;

    -- 3. Isi IFRS.GTMP_NOMI_CURR_2_ALL
    IFRS.write_log('INFO', V_PROCESS_NAME, '3. Mengisi IFRS.GTMP_NOMI_CURR_2_ALL...');
    V_STR_SQL := '
            insert into /*+ PARALLEL(8) */ IFRS.GTMP_NOMI_CURR_2_ALL
select /*+ PARALLEL(8) */ *
from IFRS_NOMINATIVE partition for (''' || V_CURRDATE || ''')
where CUSTOMER_NUMBER in (select /*+ PARALLEL(8) */ distinct CUSTOMER_NUMBER
                          from IFRS.GTMP_REPORT_BIAYA_CKPN)';

    EXECUTE IMMEDIATE V_STR_SQL;
    IFRS.write_log('INFO', V_PROCESS_NAME, '   -> Selesai. ' || SQL%ROWCOUNT || ' baris dimasukkan.');

    commit;

    -- 4. Isi IFRS.GTMP_NOMI_CURR_2 dari IFRS.GTMP_NOMI_CURR_2_ALL
    IFRS.write_log('INFO', V_PROCESS_NAME, '4. Memfilter dan mengisi IFRS.GTMP_NOMI_CURR_2...');
    V_STR_SQL := '
            insert into /*+ PARALLEL(8) */ IFRS.GTMP_NOMI_CURR_2
select /*+ PARALLEL(8) */ *
from IFRS.GTMP_NOMI_CURR_2_ALL A
where 1=1 and (' || RTRIM(NVL(V_STR_SQL_RULE, '')) || ')
  AND ((A.DATA_SOURCE = ''BTRD''
    AND A.ACCOUNT_STATUS = ''A'')
    OR (A.DATA_SOURCE = ''CRD''
        AND (A.ACCOUNT_STATUS = ''A''
            OR A.outstanding_on_bs_ccy
                 > 0))
    OR (A.DATA_SOURCE = ''ILS''
        AND A.account_status = ''A'')
    OR (A.DATA_SOURCE = ''LIMIT''
        AND A.account_status = ''A'')
    OR (A.DATA_SOURCE = ''KTP''
        AND A.ACCOUNT_STATUS = ''A''
        AND UPPER(A.PRODUCT_CODE) <> ''BORROWING'')
    OR (A.DATA_SOURCE = ''PBMM''
        AND A.ACCOUNT_STATUS = ''A''
        AND UPPER(A.PRODUCT_CODE) <> ''BORROWING'')
    OR (A.DATA_SOURCE = ''RKN''
        AND A.ACCOUNT_STATUS = ''A''
        AND NVL(A.OUTSTANDING_PRINCIPAL_CCY
                , 0) >= 0))
  AND NOT EXISTS
    (SELECT 1
     FROM IFRS.GTMP_NOMI_CURR_2_ALL L
     WHERE  L.DATA_SOURCE = ''ILS''
       AND L.ACCOUNT_STATUS = ''A''
       AND A.DATA_SOURCE = ''LIMIT''
       AND A.ACCOUNT_NUMBER = L.FACILITY_NUMBER)
      AND A.PRODUCT_CODE not like ''7%''';

    EXECUTE IMMEDIATE V_STR_SQL;
    IFRS.write_log('INFO', V_PROCESS_NAME, '   -> Selesai. ' || SQL%ROWCOUNT || ' baris dimasukkan.');

    COMMIT;

    -- 5. Isi IFRS.GTMP_NOMI_PREV_2_ALL
    IFRS.write_log('INFO', V_PROCESS_NAME, '5. Mengisi IFRS.GTMP_NOMI_PREV_2_ALL...');
    V_STR_SQL := '
            insert into /*+ PARALLEL(8) */ IFRS.GTMP_NOMI_PREV_2_ALL
select /*+ PARALLEL(8) */ *
from IFRS_NOMINATIVE partition for (''' || V_PREVDATE || ''')
where CUSTOMER_NUMBER in (select /*+ PARALLEL(8) */ distinct CUSTOMER_NUMBER
                          from IFRS.GTMP_REPORT_BIAYA_CKPN)';

    EXECUTE IMMEDIATE V_STR_SQL;
    IFRS.write_log('INFO', V_PROCESS_NAME, '   -> Selesai. ' || SQL%ROWCOUNT || ' baris dimasukkan.');

    COMMIT;

    -- 6. Isi IFRS.GTMP_NOMI_PREV_2 dari IFRS.GTMP_NOMI_PREV_2_ALL
    IFRS.write_log('INFO', V_PROCESS_NAME, '6. Memfilter dan mengisi IFRS.GTMP_NOMI_PREV_2...');
    V_STR_SQL := '
            insert into /*+ PARALLEL(8) */ IFRS.GTMP_NOMI_PREV_2
select /*+ PARALLEL(8) */ *
from IFRS.GTMP_NOMI_PREV_2_ALL A
where  (' || RTRIM(NVL(V_STR_SQL_RULE, '')) || ')
  AND ((A.DATA_SOURCE = ''BTRD''
    AND A.ACCOUNT_STATUS = ''A'')
    OR (A.DATA_SOURCE = ''CRD''
        AND (A.ACCOUNT_STATUS = ''A''
            OR A.outstanding_on_bs_ccy
                 > 0))
    OR (A.DATA_SOURCE = ''ILS''
        AND A.account_status = ''A'')
    OR (A.DATA_SOURCE = ''LIMIT''
        AND A.account_status = ''A'')
    OR (A.DATA_SOURCE = ''KTP''
        AND A.ACCOUNT_STATUS = ''A''
        AND UPPER(A.PRODUCT_CODE) <> ''BORROWING'')
    OR (A.DATA_SOURCE = ''PBMM''
        AND A.ACCOUNT_STATUS = ''A''
        AND UPPER(A.PRODUCT_CODE) <> ''BORROWING'')
    OR (A.DATA_SOURCE = ''RKN''
        AND A.ACCOUNT_STATUS = ''A''
        AND NVL(A.OUTSTANDING_PRINCIPAL_CCY
                , 0) >= 0))
  AND NOT EXISTS
    (SELECT 1
     FROM IFRS.GTMP_NOMI_PREV_2_ALL L
     WHERE  L.DATA_SOURCE = ''ILS''
       AND L.ACCOUNT_STATUS = ''A''
       AND A.DATA_SOURCE = ''LIMIT''
       AND A.ACCOUNT_NUMBER = L.FACILITY_NUMBER)
      AND A.PRODUCT_CODE not like ''7%''';

    EXECUTE IMMEDIATE V_STR_SQL;
    IFRS.write_log('INFO', V_PROCESS_NAME, '   -> Selesai. ' || SQL%ROWCOUNT || ' baris dimasukkan.');
    COMMIT;

    -- 7. Delete customer yang tidak relevan
    -- delete customer number yang tidak termasuk customer number di nomi yang turun ke dkp
    IFRS.write_log('INFO', V_PROCESS_NAME, '7. Delete customer yang tidak termasuk dalam nominatif...');
    delete
    from IFRS.GTMP_REPORT_BIAYA_CKPN
    where CUSTOMER_NUMBER not in (select distinct coalesce(a.CUSTOMER_NUMBER, b.CUSTOMER_NUMBER) CUSTOMER_NUMBER
                                  from (select distinct CUSTOMER_NUMBER from IFRS.GTMP_NOMI_CURR_2) a
                                           full outer join (select distinct CUSTOMER_NUMBER from IFRS.GTMP_NOMI_PREV_2) b
                                                           on a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER)
      and nvl(BIAYA_CADANGAN_BULAN_INI_TOTAL, 0) = 0;
    IFRS.write_log('INFO', V_PROCESS_NAME, '   -> Selesai. ' || SQL%ROWCOUNT || ' baris dihapus.');

    COMMIT;

    -- 8. Delete customer dengan product code yang tidak terdaftar
    -- delete customer number yang semua deal id nya itu prd_codenya tidak terdaftar di param
    IFRS.write_log('INFO', V_PROCESS_NAME, '8. Delete customer berdasarkan IFRS_PRODUCT_PARAM...');
    delete
    from IFRS.GTMP_REPORT_BIAYA_CKPN
    where CUSTOMER_NUMBER in (select distinct a.CUSTOMER_NUMBER
                              from (select CUSTOMER_NUMBER, count(1) total_all_prd_code
                                    from (select CUSTOMER_NUMBER, PRODUCT_CODE
                                          from IFRS.GTMP_NOMI_PREV_2
                                          union all
                                          select CUSTOMER_NUMBER, PRODUCT_CODE
                                          from IFRS.GTMP_NOMI_CURR_2)
                                    group by CUSTOMER_NUMBER) a
                                       join (select CUSTOMER_NUMBER, count(1) total_filter_prd_code
                                             from (select CUSTOMER_NUMBER, PRODUCT_CODE
                                                   from IFRS.GTMP_NOMI_PREV_2
                                                   union all
                                                   select CUSTOMER_NUMBER, PRODUCT_CODE
                                                   from IFRS.GTMP_NOMI_CURR_2)
                                             WHERE PRODUCT_CODE not in (select PRD_CODE from IFRS.IFRS_PRODUCT_PARAM)
                                             group by CUSTOMER_NUMBER) b on a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER
                              where total_all_prd_code = total_filter_prd_code);
    IFRS.write_log('INFO', V_PROCESS_NAME, '   -> Selesai. ' || SQL%ROWCOUNT || ' baris dihapus.');
    COMMIT;

    -- 9. Merge OS & ECL Current
    IFRS.write_log('INFO', V_PROCESS_NAME, '9. Merge OS & ECL (Current)...');
    merge /*+ PARALLEL(8) */ into IFRS.GTMP_REPORT_BIAYA_CKPN d
    using (select /*+ PARALLEL(8) */ CUSTOMER_NUMBER                                   CUSTOMER_NUMBER,
                                     sum(nvl(case
                                                 when DATA_SOURCE = 'KTP' then P.CARRYING_AMOUNT_LCL
                                                 else P.OUTSTANDING_ON_BS_LCL end, 0)) OS_ON_CURRENT,
                                     sum(nvl(P.OUTSTANDING_OFF_BS_LCL, 0))             OS_OFF_CURRENT,
                                     sum(ECL_OFF_BS_LCL)                               ECL_OFF_BS_LCL,
                                     sum(RESERVED_AMOUNT_3)                            ECL_ON_BS_FINAL_LCL,
                                     sum(RESERVED_AMOUNT_5)                            ECL_TOTAL_FINAL_LCL
           from IFRS.GTMP_NOMI_CURR_2 P
           group by P.CUSTOMER_NUMBER) e
    on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
    when matched then
        update /*+ PARALLEL(8) */
        set d.OS_ON_CURRENT    = e.OS_ON_CURRENT,
            d.OS_OFF_CURRENT   = e.OS_OFF_CURRENT,
            d.ECL_OFF_CURRENT  = e.ECL_OFF_BS_LCL,
            d.ECL_ON_CURRENT   = e.ECL_ON_BS_FINAL_LCL,
            d.ECL_TOTAL_CURRENT=e.ECL_TOTAL_FINAL_LCL;
    IFRS.write_log('INFO', V_PROCESS_NAME, '   -> Selesai. ' || SQL%ROWCOUNT || ' baris diupdate.');
    COMMIT;

    -- 10. Merge OS & ECL Past
    IFRS.write_log('INFO', V_PROCESS_NAME, '10. Merge OS & ECL (Past)...');
    merge /*+ PARALLEL(8) */ into IFRS.GTMP_REPORT_BIAYA_CKPN d
    using (select /*+ PARALLEL(8) */ CUSTOMER_NUMBER                                   CUSTOMER_NUMBER,
                                     sum(nvl(case
                                                 when DATA_SOURCE = 'KTP' then P.CARRYING_AMOUNT_LCL
                                                 else P.OUTSTANDING_ON_BS_LCL end, 0)) OS_ON_PAST,
                                     sum(nvl(P.OUTSTANDING_OFF_BS_LCL, 0))             OS_OFF_PAST,
                                     sum(ECL_OFF_BS_LCL)                               ECL_OFF_BS_LCL,
                                     sum(RESERVED_AMOUNT_3)                            ECL_ON_BS_FINAL_LCL,
                                     sum(RESERVED_AMOUNT_5)                            ECL_TOTAL_FINAL_LCL
           from IFRS.GTMP_NOMI_PREV_2 P
           group by P.CUSTOMER_NUMBER) e
    on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
    when matched then
        update /*+ PARALLEL(8) */
        set d.OS_ON_PAST    = e.OS_ON_PAST,
            d.OS_OFF_PAST   = e.OS_OFF_PAST,
            d.ECL_OFF_PAST  = e.ECL_OFF_BS_LCL,
            d.ECL_ON_PAST   = e.ECL_ON_BS_FINAL_LCL,
            d.ECL_TOTAL_PAST=e.ECL_TOTAL_FINAL_LCL;
    IFRS.write_log('INFO', V_PROCESS_NAME, '   -> Selesai. ' || SQL%ROWCOUNT || ' baris diupdate.');
    COMMIT;

    -- 11. Update Movement
    IFRS.write_log('INFO', V_PROCESS_NAME, '11. Update OS Movement...');
    update IFRS.GTMP_REPORT_BIAYA_CKPN
    set OS_ON_MOVEMENT  = OS_ON_CURRENT - OS_ON_PAST,
        OS_OFF_MOVEMENT = OS_OFF_CURRENT - OS_OFF_PAST;
    IFRS.write_log('INFO', V_PROCESS_NAME, '   -> Selesai. ' || SQL%ROWCOUNT || ' baris diupdate.');

    COMMIT;

    -- 12. Update REASON - Baru
    IFRS.write_log('INFO', V_PROCESS_NAME, '12. Update REASON untuk Customer Baru...');
-- 1 BARU
    merge /*+ PARALLEL(8) */ into IFRS.GTMP_REPORT_BIAYA_CKPN d
    using (select /*+ PARALLEL(8) */ a.CUSTOMER_NUMBER cust_no_prev, b.CUSTOMER_NUMBER cust_no_curr
           from (select /*+ PARALLEL(8) */ distinct CUSTOMER_NUMBER
                 from IFRS.GTMP_NOMI_PREV_2) a
                    right join (select /*+ PARALLEL(8) */ distinct CUSTOMER_NUMBER
                                from IFRS.GTMP_NOMI_CURR_2) b
                               on a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER
           where a.CUSTOMER_NUMBER is null) e
    ON (d.customer_number = e.cust_no_curr)
    when matched then
        update /*+ PARALLEL(8) */
        set d.REASON=(select /*+ PARALLEL(8) */ VALUE from IFRS.IFRS_PRIORITY where TYPE = 'REASON' and PRIORITY = 1)
        where d.REASON is null;
    IFRS.write_log('INFO', V_PROCESS_NAME, '   -> Selesai. ' || SQL%ROWCOUNT || ' baris diupdate.');

    COMMIT;

    -- 13. Update REASON - Lunas/Closed (Langkah A)
    IFRS.write_log('INFO', V_PROCESS_NAME, '13. Update REASON untuk Lunas/Closed (A)...');
-- 1 LUNAS/CLOSED
    merge /*+ PARALLEL(8) */ into IFRS.GTMP_REPORT_BIAYA_CKPN d
    using (select /*+ PARALLEL(8) */ b.CUSTOMER_NUMBER
           from (select /*+ PARALLEL(8) */ CUSTOMER_NUMBER, count(1) a_curr
                 from ((select /*+ PARALLEL(8) */ *
                        from IFRS.GTMP_NOMI_PREV_2))
                 group by CUSTOMER_NUMBER) a
                    join (select /*+ PARALLEL(8) */ CUSTOMER_NUMBER, count(1) b_curr
                          from (select /*+ PARALLEL(8) */ *
                                from IFRS.GTMP_NOMI_CURR_2)
                          where ACCOUNT_STATUS = 'C'
                          group by CUSTOMER_NUMBER) b
                         on a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER
           where a.a_curr = b.b_curr) e
    ON (d.customer_number = e.CUSTOMER_NUMBER)
    when matched then
        update /*+ PARALLEL(8) */
        set d.REASON=(select /*+ PARALLEL(8) */ VALUE from IFRS.IFRS_PRIORITY where TYPE = 'REASON' and PRIORITY = 2)
        where d.REASON is null;
    IFRS.write_log('INFO', V_PROCESS_NAME, '   -> Selesai. ' || SQL%ROWCOUNT || ' baris diupdate.');
    COMMIT;

    -- 14. Update REASON - Lunas/Closed (Langkah B)
    IFRS.write_log('INFO', V_PROCESS_NAME, '14. Update REASON untuk Lunas/Closed (B)...');
    merge /*+ PARALLEL(8) */ into IFRS.GTMP_REPORT_BIAYA_CKPN d
    using (select /*+ PARALLEL(8) */ a.CUSTOMER_NUMBER cust_no_prev, b.CUSTOMER_NUMBER cust_no_curr
           from (select /*+ PARALLEL(8) */ distinct CUSTOMER_NUMBER
                 from IFRS.GTMP_NOMI_PREV_2) a
                    left join (select /*+ PARALLEL(8) */ distinct CUSTOMER_NUMBER
                               from IFRS.GTMP_NOMI_CURR_2) b
                              on a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER
           where b.CUSTOMER_NUMBER is null) e
    ON (d.customer_number = e.cust_no_prev)
    when matched then
        update /*+ PARALLEL(8) */
        set d.REASON=(select /*+ PARALLEL(8) */ VALUE from IFRS.IFRS_PRIORITY where TYPE = 'REASON' and PRIORITY = 2)
        where d.REASON is null;
    IFRS.write_log('INFO', V_PROCESS_NAME, '   -> Selesai. ' || SQL%ROWCOUNT || ' baris diupdate.');
    COMMIT;

    IF V_SEGMENT = 'CORPORATE' OR V_SEGMENT = 'COMMERCIAL'
    THEN
        IFRS.write_log('INFO', V_PROCESS_NAME, 'Memulai proses spesifik untuk segmen ' || V_SEGMENT);
        -- ASSESSMENT_IMP
--             merge /*+ PARALLEL(8) */   into IFRS.GTMP_REPORT_BIAYA_CKPN d
--             using (select /*+ PARALLEL(8) */   distinct b.CUSTOMER_NUMBER cust_no_curr,
--                                    case
--                                        when a.ASSESSMENT_IMP = 'C' AND b.ASSESSMENT_IMP = 'W'
--                                            then 'Collective to Worstcase'
--                                        when a.ASSESSMENT_IMP = 'C' AND b.ASSESSMENT_IMP = 'I'
--                                            then 'Collective to Individual'
--                                        when a.ASSESSMENT_IMP = 'W' AND b.ASSESSMENT_IMP = 'C'
--                                            then 'Worstcase to Collective'
--                                        when a.ASSESSMENT_IMP = 'W' AND b.ASSESSMENT_IMP = 'I'
--                                            then 'Worstcase to Individual'
--                                        when a.ASSESSMENT_IMP = 'I' AND b.ASSESSMENT_IMP = 'W'
--                                            then 'Individual to Worstcase'
--                                        when a.ASSESSMENT_IMP = 'I' AND b.ASSESSMENT_IMP = 'C'
--                                            then 'Individual to Collective'
--                                        end           REASON
--                    from IFRS.GTMP_NOMI_PREV_2 a
--                             right join IFRS.GTMP_NOMI_CURR_2 b
--                                        on a.MASTERID = b.MASTERID) e
--             ON (d.customer_number = e.cust_no_curr)
--             when matched then
--                 update /*+ PARALLEL(8) */
--                 set d.REASON=e.REASON
--                 where d.REASON is null
--                   and e.REASON is not null;
--
--             commit;

        -- 15. Merge Assessment Imp Current
        IFRS.write_log('INFO', V_PROCESS_NAME, '15. Merge Assessment Imp (Current)...');
        -- 2 C to Worstcase
        merge /*+ PARALLEL(8) */ into IFRS.GTMP_REPORT_BIAYA_CKPN d
        using (select /*+ PARALLEL(8) */ distinct a.ASSESSMENT_IMP,
                                                  a.CUSTOMER_NUMBER          CUSTOMER_NUMBER,
                                                  dense_rank() over ( partition by a.CUSTOMER_NUMBER
                                                      ORDER BY PRIORITY ) As priority
               from IFRS.GTMP_NOMI_CURR_2 a
                        join IFRS.IFRS_PRIORITY b
                             on a.ASSESSMENT_IMP = b.VALUE
               where a.ASSESSMENT_IMP is not null
                 and TYPE = 'ASSESSMENT_IMP') e
        on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
        when matched then
            update /*+ PARALLEL(8) */
            set d.ASS_IMP_CURRENT=e.ASSESSMENT_IMP
            where e.priority = 1;
        IFRS.write_log('INFO', V_PROCESS_NAME, '   -> Selesai. ' || SQL%ROWCOUNT || ' baris diupdate.');
        COMMIT;

        -- 16. Merge Assessment Imp Past
        IFRS.write_log('INFO', V_PROCESS_NAME, '16. Merge Assessment Imp (Past)...');
        merge /*+ PARALLEL(8) */ into IFRS.GTMP_REPORT_BIAYA_CKPN d
        using (select /*+ PARALLEL(8) */ distinct a.ASSESSMENT_IMP,
                                                  a.CUSTOMER_NUMBER          CUSTOMER_NUMBER,
                                                  dense_rank() over ( partition by a.CUSTOMER_NUMBER
                                                      ORDER BY PRIORITY ) As priority
               from IFRS.GTMP_NOMI_PREV_2 a
                        join IFRS.IFRS_PRIORITY b
                             on a.ASSESSMENT_IMP = b.VALUE
               where a.ASSESSMENT_IMP is not null
                 and TYPE = 'ASSESSMENT_IMP') e
        on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
        when matched then
            update /*+ PARALLEL(8) */
            set d.ASS_IMP_PAST=e.ASSESSMENT_IMP
            where e.priority = 1;
        IFRS.write_log('INFO', V_PROCESS_NAME, '   -> Selesai. ' || SQL%ROWCOUNT || ' baris diupdate.');
        COMMIT;

        -- 17. Update REASON - Perubahan Assessment
        IFRS.write_log('INFO', V_PROCESS_NAME, '17. Update REASON untuk Perubahan Assessment...');
        update /*+ PARALLEL(8) */ IFRS.GTMP_REPORT_BIAYA_CKPN
        set REASON = case
                         when ASS_IMP_PAST = 'C' AND ASS_IMP_CURRENT = 'W'
                             then (select /*+ PARALLEL(8) */ VALUE
                                   from IFRS.IFRS_PRIORITY
                                   where TYPE = 'REASON'
                                     and PRIORITY = 3)
                         when ASS_IMP_PAST = 'C' AND ASS_IMP_CURRENT = 'I'
                             then (select /*+ PARALLEL(8) */ VALUE
                                   from IFRS.IFRS_PRIORITY
                                   where TYPE = 'REASON'
                                     and PRIORITY = 4)
                         when ASS_IMP_PAST = 'W' AND ASS_IMP_CURRENT = 'C'
                             then (select /*+ PARALLEL(8) */ VALUE
                                   from IFRS.IFRS_PRIORITY
                                   where TYPE = 'REASON'
                                     and PRIORITY = 5)
                         when ASS_IMP_PAST = 'W' AND ASS_IMP_CURRENT = 'I'
                             then (select /*+ PARALLEL(8) */ VALUE
                                   from IFRS.IFRS_PRIORITY
                                   where TYPE = 'REASON'
                                     and PRIORITY = 6)
                         when ASS_IMP_PAST = 'I' AND ASS_IMP_CURRENT = 'C'
                             then (select /*+ PARALLEL(8) */ VALUE
                                   from IFRS.IFRS_PRIORITY
                                   where TYPE = 'REASON'
                                     and PRIORITY = 7)
                         when ASS_IMP_PAST = 'I' AND ASS_IMP_CURRENT = 'W'
                             then (select /*+ PARALLEL(8) */ VALUE
                                   from IFRS.IFRS_PRIORITY
                                   where TYPE = 'REASON'
                                     and PRIORITY = 8)
            end
        where REASON is null;
        IFRS.write_log('INFO', V_PROCESS_NAME, '   -> Selesai. ' || SQL%ROWCOUNT || ' baris diupdate.');
        COMMIT;

        -- 18. Merge Perubahan Cash Flow
        IFRS.write_log('INFO', V_PROCESS_NAME, '18. Merge data Cash Flow & Worst Case...');
        -- 3 Perubahan Cash Flow
        merge /*+ PARALLEL(8) */ into IFRS.GTMP_REPORT_BIAYA_CKPN d
        using (select /*+ PARALLEL(8) */ distinct coalesce(b.CUSTOMER_NUMBER, a.CUSTOMER_NUMBER) customer_number,
                                                  nvl(WORST_CASE_PAST, 0)                        PERCENTAGE_PAST,
                                                  nvl(WORST_CASE_CURRENT, 0)                     PERCENTAGE_CURRENT,
                                                  nvl(CASH_FLOW_PAST, 0)                         EXPECTED_CF_PERCENT_PAST,
                                                  nvl(CASH_FLOW_CURRENT, 0)                      EXPECTED_CF_PERCENT_CURRENT,
                                                  TARGET_DATE_PAST                               EXPECTED_PERIOD_PAST,
                                                  TARGET_DATE_CURRENT                            EXPECTED_PERIOD_CURRENT
               from (select /*+ PARALLEL(8) */ distinct a.CUSTOMER_NUMBER,
                                                        c.EXPECTED_CF_PERCENT CASH_FLOW_PAST,
                                                        PERCENTAGE            WORST_CASE_PAST,
                                                        c.EXPECTED_PERIOD     TARGET_DATE_PAST
                     from IFRS.GTMP_NOMI_PREV_2 a
                              left join TBLU_WORSTCASE_LIST b
                                        on a.REPORT_DATE = b.DOWNLOAD_DATE and
                                           a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER
                              left join TBLU_DCF_BULK c
                                        on a.REPORT_DATE = c.EFFECTIVE_DATE and
                                           a.CUSTOMER_NUMBER = c.CUSTOMER_NUMBER) a
                        full outer join (select /*+ PARALLEL(8) */ distinct a.CUSTOMER_NUMBER,
                                                                            c.EXPECTED_CF_PERCENT CASH_FLOW_CURRENT,
                                                                            PERCENTAGE            WORST_CASE_CURRENT,
                                                                            c.EXPECTED_PERIOD     TARGET_DATE_CURRENT
                                         from IFRS.GTMP_NOMI_CURR_2 a
                                                  left join TBLU_WORSTCASE_LIST b
                                                            on a.REPORT_DATE = b.DOWNLOAD_DATE and
                                                               a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER
                                                  left join TBLU_DCF_BULK c
                                                            on a.REPORT_DATE = c.EFFECTIVE_DATE and
                                                               a.CUSTOMER_NUMBER = c.CUSTOMER_NUMBER) b
                                        on a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER) e
        ON (d.customer_number = e.CUSTOMER_NUMBER)
        when matched then
            update /*+ PARALLEL(8) */
            set d.WORST_CASE_PAST=e.PERCENTAGE_PAST,
                d.WORST_CASE_CURRENT=e.PERCENTAGE_CURRENT,
                d.CASH_FLOW_PAST=e.EXPECTED_CF_PERCENT_PAST,
                d.CASH_FLOW_CURRENT=e.EXPECTED_CF_PERCENT_CURRENT,
                d.TARGET_DATE_PAST=e.EXPECTED_PERIOD_PAST,
                d.TARGET_DATE_CURRENT=e.EXPECTED_PERIOD_CURRENT;
        IFRS.write_log('INFO', V_PROCESS_NAME, '   -> Selesai. ' || SQL%ROWCOUNT || ' baris diupdate.');
        COMMIT;

        -- 19. Update REASON - Perubahan Cash Flow
        IFRS.write_log('INFO', V_PROCESS_NAME, '19. Update REASON untuk Perubahan Cash Flow...');
        update /*+ PARALLEL(8) */ IFRS.GTMP_REPORT_BIAYA_CKPN
        set REASON = case
                         when ASS_IMP_PAST = 'W' and ASS_IMP_CURRENT = 'W' and
                              nvl(WORST_CASE_PAST, 0) != nvl(WORST_CASE_CURRENT, 0)
                             then (select /*+ PARALLEL(8) */ VALUE
                                   from IFRS.IFRS_PRIORITY
                                   where TYPE = 'REASON'
                                     and PRIORITY = 9)
                         when ASS_IMP_PAST = 'I' and ASS_IMP_CURRENT = 'I' and
                              nvl(CASH_FLOW_PAST, 0) != nvl(CASH_FLOW_CURRENT, 0)
                             then (select /*+ PARALLEL(8) */ VALUE
                                   from IFRS.IFRS_PRIORITY
                                   where TYPE = 'REASON'
                                     and PRIORITY = 9)
            end
        where REASON is null;
        IFRS.write_log('INFO', V_PROCESS_NAME, '   -> Selesai. ' || SQL%ROWCOUNT || ' baris diupdate.');
        COMMIT;

    END IF;-- Akhir blok IF

    -- 20. Update REASON - Perubahan Status CKPN
    --Perubahan Status CKPN 365
    IFRS.write_log('INFO', V_PROCESS_NAME, '20. Update REASON untuk Perubahan Status CKPN...');
    -- (Logika untuk mengambil v_reason_value_10 dan MERGE)
    -- 1. Ambil nilai REASON sekali saja dan simpan di variabel
    BEGIN
    SELECT VALUE
    INTO v_reason_value_10
    FROM IFRS.IFRS_PRIORITY
    WHERE TYPE = 'REASON'
      AND PRIORITY = 10;
    EXCEPTION
    WHEN NO_DATA_FOUND THEN
        -- Handle jika query IFRS.IFRS_PRIORITY tidak mengembalikan baris
                IFRS.write_log('ERROR', 'NO_DATA_FOUND', 'Konfigurasi REASON tidak ditemukan untuk PRIORITY=10 di tabel IFRS.IFRS_PRIORITY. Proses dihentikan.', SQLCODE);
        ROLLBACK;
    END;


    -- 2. Gunakan variabel tersebut di dalam MERGE statement
    MERGE /*+ PARALLEL(8) */ INTO IFRS.GTMP_REPORT_BIAYA_CKPN d
    USING (WITH
               -- Langkah 1: Temukan semua pasangan MASTERID yang memenuhi kriteria perubahan
               changed_masterids AS (SELECT a.customer_number,
                                            b.masterid,
                                            a.special_reason AS special_reason_past,
                                            b.special_reason AS special_reason_current
                                     FROM IFRS.GTMP_NOMI_PREV_2 a
                                              JOIN
                                          IFRS.GTMP_NOMI_CURR_2 b ON a.masterid = b.masterid
                                     WHERE NVL(a.special_reason, '0') != NVL(b.special_reason, '0')
                                       AND (
                                         (a.special_reason IN ('CKPN 100%', 'CKPN 365') AND
                                          NVL(b.special_reason, '0') NOT IN ('CKPN 100%', 'CKPN 365'))
                                             OR
                                         (NVL(a.special_reason, '0') NOT IN ('CKPN 100%', 'CKPN 365') AND
                                          b.special_reason IN ('CKPN 100%', 'CKPN 365'))
                                             OR
                                         (a.special_reason = 'CKPN 365' AND b.special_reason = 'CKPN 100%')
                                         )),
               -- Langkah 2: Beri peringkat pada setiap perubahan di dalam satu CUSTOMER_NUMBER
               ranked_changes AS (SELECT c.customer_number,
                                         c.special_reason_past,
                                         c.special_reason_current,
                                         -- Di sinilah kita mendefinisikan "aturan tie-breaker"
                                         -- Pilih satu MASTERID yang "paling penting" untuk setiap CUSTOMER_NUMBER
                                         ROW_NUMBER() OVER (
                                             PARTITION BY c.customer_number
                                             ORDER BY
                                                 -- Aturan Prioritas:
                                                 -- 1. Perubahan yang melibatkan 'CKPN 100%' lebih diutamakan
                                                 CASE
                                                     WHEN c.special_reason_past = 'CKPN 100%' OR
                                                          c.special_reason_current = 'CKPN 100%' THEN 1
                                                     ELSE 2 END ASC,
                                                 -- 2. Jika sama-sama penting, ambil berdasarkan masterid (agar hasilnya konsisten)
                                                 c.masterid ASC
                                             ) AS rn
                                  FROM changed_masterids c)
           -- Langkah 3: Pilih hanya data dari MASTERID dengan peringkat #1
           SELECT customer_number,
                  special_reason_past,
                  special_reason_current
           FROM ranked_changes
           WHERE rn = 1 -- Ini memastikan satu baris unik per CUSTOMER_NUMBER
    ) e
    ON (d.customer_number = e.customer_number)
    WHEN MATCHED THEN
        UPDATE
        SET d.special_reason_past    = e.special_reason_past,
            d.special_reason_current = e.special_reason_current,
            d.reason                 = v_reason_value_10
        WHERE d.reason IS NULL;
    IFRS.write_log('INFO', V_PROCESS_NAME, '   -> Selesai. ' || SQL%ROWCOUNT || ' baris diupdate.');
    COMMIT;


        -- Atau tindakan lain yang sesuai


--     merge into /*+ PARALLEL(8) */ IFRS.GTMP_REPORT_BIAYA_CKPN d
--     using (
--             select a.CUSTOMER_NUMBER,max(b.MASTERID),min(a.SPECIAL_REASON) SPECIAL_REASON_PAST,min(b.SPECIAL_REASON) SPECIAL_REASON_CURRENT
--             from IFRS.GTMP_NOMI_PREV_2 a
--             join IFRS.GTMP_NOMI_CURR_2 b
--             on a.MASTERID=b.MASTERID
--             where a.SPECIAL_REASON!=nvl(b.SPECIAL_REASON,'0') and
--                   a.SPECIAL_REASON in ('CKPN 100%','CKPN 365') and
--                   nvl(b.SPECIAL_REASON,'0') not in ('CKPN 100%','CKPN 365')
--             group by a.CUSTOMER_NUMBER) e
--                 on (d.CUSTOMER_NUMBER=e.CUSTOMER_NUMBER)
--     when matched then
--         update /*+ PARALLEL(8) */
--         set d.SPECIAL_REASON_PAST=e.SPECIAL_REASON_PAST,
--             d.SPECIAL_REASON_CURRENT=e.SPECIAL_REASON_CURRENT,
--             d.REASON=(select /*+ PARALLEL(8) */ VALUE
--                                    from IFRS.IFRS_PRIORITY
--                                    where TYPE = 'REASON'
--                                      and PRIORITY = 10)
--     where d.REASON is null;
--
--     commit;
--
--     merge into /*+ PARALLEL(8) */ IFRS.GTMP_REPORT_BIAYA_CKPN d
--     using (
--             select a.CUSTOMER_NUMBER,max(b.MASTERID),min(a.SPECIAL_REASON) SPECIAL_REASON_PAST,min(b.SPECIAL_REASON) SPECIAL_REASON_CURRENT
--             from IFRS.GTMP_NOMI_PREV_2 a
--             join IFRS.GTMP_NOMI_CURR_2 b
--             on a.MASTERID=b.MASTERID
--             where nvl(a.SPECIAL_REASON,'0')!=b.SPECIAL_REASON and
--                   nvl(a.SPECIAL_REASON,'0') not in ('CKPN 100%','CKPN 365') and
--                   b.SPECIAL_REASON in ('CKPN 100%','CKPN 365')
--             group by a.CUSTOMER_NUMBER) e
--                 on (d.CUSTOMER_NUMBER=e.CUSTOMER_NUMBER)
--     when matched then
--         update /*+ PARALLEL(8) */
--         set d.SPECIAL_REASON_PAST=e.SPECIAL_REASON_PAST,
--             d.SPECIAL_REASON_CURRENT=e.SPECIAL_REASON_CURRENT,
--             d.REASON=(select /*+ PARALLEL(8) */ VALUE
--                                    from IFRS.IFRS_PRIORITY
--                                    where TYPE = 'REASON'
--                                      and PRIORITY = 10)
--     where d.REASON is null;
--
--     commit;
--
--     merge into /*+ PARALLEL(8) */ IFRS.GTMP_REPORT_BIAYA_CKPN d
--     using (
--             select a.CUSTOMER_NUMBER,max(b.MASTERID),min(a.SPECIAL_REASON) SPECIAL_REASON_PAST,min(b.SPECIAL_REASON) SPECIAL_REASON_CURRENT
--             from IFRS.GTMP_NOMI_PREV_2 a
--             join IFRS.GTMP_NOMI_CURR_2 b
--             on a.MASTERID=b.MASTERID
--             where a.SPECIAL_REASON!=b.SPECIAL_REASON and
--                   a.SPECIAL_REASON ='CKPN 365' and
--                   b.SPECIAL_REASON ='CKPN 100%'
--             group by a.CUSTOMER_NUMBER) e
--                 on (d.CUSTOMER_NUMBER=e.CUSTOMER_NUMBER)
--     when matched then
--         update /*+ PARALLEL(8) */
--         set d.SPECIAL_REASON_PAST=e.SPECIAL_REASON_PAST,
--             d.SPECIAL_REASON_CURRENT=e.SPECIAL_REASON_CURRENT,
--             d.REASON=(select /*+ PARALLEL(8) */ VALUE
--                                    from IFRS.IFRS_PRIORITY
--                                    where TYPE = 'REASON'
--                                      and PRIORITY = 10)
--     where d.REASON is null;
--
--     commit;


        -- 21. Update REASON - Perubahan Stage
        -- 4 Stage
        IFRS.write_log('INFO', V_PROCESS_NAME, '21. Update REASON untuk Perubahan Stage...');
        -- (Logika untuk mengambil v_reason_value_11, v_reason_value_12, dan MERGE)
        BEGIN
            SELECT VALUE INTO v_reason_value_11 FROM IFRS.IFRS_PRIORITY WHERE TYPE = 'REASON' AND PRIORITY = 11;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                IFRS.write_log('ERROR', 'NO_DATA_FOUND', 'Konfigurasi REASON tidak ditemukan untuk PRIORITY=11 di tabel IFRS.IFRS_PRIORITY. Proses dihentikan.', SQLCODE);
                RAISE; -- Melemparkan kembali error untuk menghentikan proses
        END;

        BEGIN
            SELECT VALUE INTO v_reason_value_12 FROM IFRS.IFRS_PRIORITY WHERE TYPE = 'REASON' AND PRIORITY = 12;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                IFRS.write_log('ERROR', 'NO_DATA_FOUND', 'Konfigurasi REASON tidak ditemukan untuk PRIORITY=12 di tabel IFRS.IFRS_PRIORITY. Proses dihentikan.', SQLCODE);
                RAISE; -- Melemparkan kembali error untuk menghentikan proses
        END;

        MERGE /*+ PARALLEL(8) */ INTO IFRS.GTMP_REPORT_BIAYA_CKPN d
        USING (WITH
                   -- Langkah 1: Hitung max stage untuk periode lampau
                   stage_past_source AS (SELECT customer_number,
                                                MAX(stage) AS stage_past
                                         FROM IFRS.GTMP_NOMI_PREV_2
                                         WHERE stage IS NOT NULL
                                         GROUP BY customer_number),
                   -- Langkah 2: Hitung max stage untuk periode kini
                   stage_current_source AS (SELECT customer_number,
                                                   MAX(stage) AS stage_curr
                                            FROM IFRS.GTMP_NOMI_CURR_2
                                            WHERE stage IS NOT NULL
                                            GROUP BY customer_number)
               -- Langkah 3: Gabungkan keduanya dan hitung REASON
               SELECT
                   -- Gunakan COALESCE untuk mendapatkan customer_number dari sisi mana pun yang tidak NULL
                   COALESCE(p.customer_number, c.customer_number) AS customer_number,
                   p.stage_past,
                   c.stage_curr,
                   -- Logika CASE untuk menentukan REASON, sama seperti di script asli
                   CASE
                       WHEN p.stage_past < c.stage_curr
                           THEN v_reason_value_11
                       WHEN p.stage_past > c.stage_curr
                           THEN v_reason_value_12
                       ELSE NULL
                       END                                        AS calculated_reason
               FROM stage_past_source p
                        -- FULL OUTER JOIN memastikan customer yang hanya ada di satu periode tetap diproses
                        FULL OUTER JOIN stage_current_source c ON p.customer_number = c.customer_number) e
        ON (d.customer_number = e.customer_number)
        WHEN MATCHED THEN
            UPDATE
            SET
                -- Update STAGE_PAST dan STAGE_CURRENT tanpa syarat seperti di skrip asli
                d.stage_past    = e.stage_past,
                d.stage_current = e.stage_curr,
                -- Hanya update REASON jika REASON saat ini masih NULL
                d.reason        = COALESCE(d.reason, e.calculated_reason)
            WHERE
                -- Kondisi ini meniru 'WHERE STAGE_PAST != STAGE_CURRENT' dari UPDATE asli.
                -- NVL digunakan untuk menangani kasus di mana salah satu stage adalah NULL.
                NVL(e.stage_past, -1) != NVL(e.stage_curr, -1);

        IFRS.write_log('INFO', V_PROCESS_NAME, '   -> Selesai. ' || SQL%ROWCOUNT || ' baris diupdate.');
        COMMIT;

        --         merge /*+ PARALLEL(8) */ into IFRS.GTMP_REPORT_BIAYA_CKPN d
--         using (select /*+ PARALLEL(8) */ distinct max(a.STAGE)      stage_past,
--                                                   a.CUSTOMER_NUMBER CUSTOMER_NUMBER
--                from IFRS.GTMP_NOMI_PREV_2 a
--                where a.STAGE is not null
--                group by a.CUSTOMER_NUMBER) e
--         on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
--         when matched then
--             update /*+ PARALLEL(8) */
--             set d.STAGE_PAST=e.stage_past;
--
--         commit;
--
--         merge /*+ PARALLEL(8) */ into IFRS.GTMP_REPORT_BIAYA_CKPN d
--         using (select /*+ PARALLEL(8) */ distinct max(b.STAGE)      stage_curr,
--                                                   b.CUSTOMER_NUMBER CUSTOMER_NUMBER
--                from IFRS.GTMP_NOMI_CURR_2 b
--                where b.STAGE is not null
--                group by b.CUSTOMER_NUMBER) e
--         on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
--         when matched then
--             update /*+ PARALLEL(8) */
--             set d.STAGE_CURRENT=e.stage_curr;
--
--         commit;
--
--         update /*+ PARALLEL(8) */ IFRS.GTMP_REPORT_BIAYA_CKPN
--         set REASON = case
--                          when STAGE_PAST < STAGE_CURRENT
--                              then (select /*+ PARALLEL(8) */ VALUE
--                                    from IFRS.IFRS_PRIORITY
--                                    where TYPE = 'REASON'
--                                      and PRIORITY = 11)
--                          when STAGE_PAST > STAGE_CURRENT
--                              then (select /*+ PARALLEL(8) */ VALUE
--                                    from IFRS.IFRS_PRIORITY
--                                    where TYPE = 'REASON'
--                                      and PRIORITY = 12)
--             end
--         where STAGE_PAST != STAGE_CURRENT
--           and REASON is null;
--
--         commit;

        --BUCKET

        IF V_SEGMENT = 'CORPORATE' OR V_SEGMENT = 'COMMERCIAL' OR V_SEGMENT = 'SME'
        THEN
        IFRS.write_log('INFO', V_PROCESS_NAME, 'Memulai proses spesifik untuk segmen ' || V_SEGMENT);
--             MERGE /*+ PARALLEL(8) */ INTO IFRS.GTMP_REPORT_BIAYA_CKPN d
--             USING (WITH
--                        -- Langkah 0 (BARU): Ambil daftar customer yang menjadi target kita.
--                        target_customers AS (SELECT /*+ MATERIALIZE */ DISTINCT customer_number
--                                             FROM IFRS.GTMP_REPORT_BIAYA_CKPN),
--                        -- Langkah 1: Proses SUMBER 1 (GTMP) secara terpisah
--                        gtemp_ratings AS (SELECT r.customer_number,
--                                                 MAX(CASE WHEN r.period = 'PAST' THEN r.rating END)    AS rating_past,
--                                                 MAX(CASE WHEN r.period = 'CURRENT' THEN r.rating END) AS rating_current
--                                          FROM (SELECT customer_number,
--                                                       period,
--                                                       rating,
--                                                       ROW_NUMBER() OVER (PARTITION BY customer_number, period ORDER BY p.priority ASC) as rn
--                                                FROM (SELECT customer_number,
--                                                             imp_rating AS rating,
--                                                             a.rating_code,
--                                                             'PAST'     AS period
--                                                      FROM IFRS.GTMP_NOMI_PREV_2 a
--                                                      WHERE a.report_date = V_PREVDATE
--                                                      UNION ALL
--                                                      SELECT customer_number,
--                                                             imp_rating AS rating,
--                                                             a.rating_code,
--                                                             'CURRENT'  AS period
--                                                      FROM IFRS.GTMP_NOMI_CURR_2 a
--                                                      WHERE a.report_date = V_CURRDATE) raw_gtemp
--                                                         JOIN IFRS.IFRS_PRIORITY p ON raw_gtemp.rating_code = p.value AND p.type = 'RATING'
--                                                -- Filter penting: hanya proses rating yang tidak NULL
--                                                WHERE raw_gtemp.rating IS NOT NULL
--                                                  AND raw_gtemp.rating_code IS NOT NULL) r
--                                          WHERE r.rn = 1
--                                          GROUP BY r.customer_number),
--                        -- Langkah 2: Proses SUMBER 2 (IFRS) secara terpisah, dengan filter target customer
--                        ifrs_ratings AS (SELECT r.customer_number,
--                                                MAX(CASE WHEN r.period = 'PAST' THEN r.rating END)    AS rating_past,
--                                                MAX(CASE WHEN r.period = 'CURRENT' THEN r.rating END) AS rating_current
--                                         FROM (SELECT customer_number,
--                                                      period,
--                                                      rating,
--                                                      ROW_NUMBER() OVER (PARTITION BY customer_number, period ORDER BY p.priority ASC) as rn
--                                               FROM (SELECT a.customer_number,
--                                                            a.rating_code AS rating,
--                                                            a.rating_code,
--                                                            'PAST'        AS period
--                                                     FROM IFRS_MASTER_ACCOUNT_MONTHLY a
--                                                     WHERE a.download_date = V_PREVDATE
--                                                       AND a.customer_number IN (SELECT customer_number FROM target_customers)
--                                                     UNION ALL
--                                                     SELECT a.customer_number,
--                                                            a.rating_code AS rating,
--                                                            a.rating_code,
--                                                            'CURRENT'     AS period
--                                                     FROM IFRS_MASTER_ACCOUNT_MONTHLY a
--                                                     WHERE a.download_date = V_CURRDATE
--                                                       AND a.customer_number IN (SELECT customer_number FROM target_customers)) raw_ifrs
--                                                        JOIN IFRS.IFRS_PRIORITY p ON raw_ifrs.rating_code = p.value AND p.type = 'RATING'
--                                               WHERE raw_ifrs.rating IS NOT NULL
--                                                 AND raw_ifrs.rating_code IS NOT NULL) r
--                                         WHERE r.rn = 1
--                                         GROUP BY r.customer_number),
--                        -- Langkah 3: Gabungkan dengan logika fallback
--                        final_source_data AS (SELECT c.customer_number,
--                                                     COALESCE(g.rating_past, i.rating_past)       AS rating_past,
--                                                     COALESCE(g.rating_current, i.rating_current) AS rating_current
--                                              FROM target_customers c
--                                                       LEFT JOIN gtemp_ratings g ON c.customer_number = g.customer_number
--                                                       LEFT JOIN ifrs_ratings i ON c.customer_number = i.customer_number),
--                        -- Langkah 4 & 5
--                        priority_lookup AS (SELECT value AS rating_code, priority
--                                            FROM IFRS.IFRS_PRIORITY
--                                            WHERE type = 'RATING'),
--                        reason_lookup AS (SELECT priority, value AS reason_text FROM IFRS.IFRS_PRIORITY WHERE type = 'REASON')
--                    SELECT fsd.customer_number,
--                           fsd.rating_past,
--                           fsd.rating_current,
--                           CASE
--                               WHEN (fsd.rating_past IN ('RR9', 'RR10', 'LOSS') AND
--                                     fsd.rating_current IN ('RR9', 'RR10', 'LOSS')) THEN NULL
--                               WHEN p_past.priority > p_curr.priority AND fsd.rating_current IN ('RR9', 'RR10', 'LOSS')
--                                   THEN (SELECT reason_text FROM reason_lookup WHERE priority = 13)
--                               WHEN p_curr.priority > p_past.priority AND fsd.rating_past IN ('RR9', 'RR10', 'LOSS')
--                                   THEN (SELECT reason_text FROM reason_lookup WHERE priority = 14)
--                               WHEN p_past.priority > p_curr.priority AND fsd.rating_past != fsd.rating_current
--                                   THEN (SELECT reason_text FROM reason_lookup WHERE priority = 15)
--                               WHEN p_past.priority < p_curr.priority AND fsd.rating_past != fsd.rating_current
--                                   THEN (SELECT reason_text FROM reason_lookup WHERE priority = 16)
--                               ELSE NULL
--                               END AS calculated_reason
--                    FROM final_source_data fsd
--                             LEFT JOIN priority_lookup p_past ON fsd.rating_past = p_past.rating_code
--                             LEFT JOIN priority_lookup p_curr ON fsd.rating_current = p_curr.rating_code) e
--             ON (d.customer_number = e.customer_number)
--             WHEN MATCHED THEN
--                 UPDATE
--                 SET d.rating_past    = COALESCE(d.rating_past, e.rating_past),
--                     d.rating_current = COALESCE(d.rating_current, e.rating_current),
--                     d.reason         = COALESCE(d.reason, e.calculated_reason),
--                     d.bucket_past    = COALESCE(d.bucket_past, e.rating_past),
--                     d.bucket_current = COALESCE(d.bucket_current, e.rating_current)
--                 WHERE d.rating_past IS NULL
--                    OR d.rating_current IS NULL
--                    OR d.reason IS NULL;
--
--             COMMIT;


            merge /*+ PARALLEL(8) */ into IFRS.GTMP_REPORT_BIAYA_CKPN d
            using (select /*+ PARALLEL(8) */ distinct a.IMP_RATING               rating_past,
                                                      a.CUSTOMER_NUMBER          CUSTOMER_NUMBER,
                                                      dense_rank() over ( partition by a.CUSTOMER_NUMBER
                                                          ORDER BY priority ) As priority
                   from IFRS.GTMP_NOMI_PREV_2 a
                            join IFRS.IFRS_PRIORITY c on a.RATING_CODE = c.VALUE
                   where a.REPORT_DATE = V_PREVDATE
                     and c.TYPE = 'RATING') e
            on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
            when matched then
                update /*+ PARALLEL(8) */
                set d.RATING_PAST=e.rating_past
                where d.RATING_PAST is null
                  and e.priority = 1;

            commit;

            merge /*+ PARALLEL(8) */ into IFRS.GTMP_REPORT_BIAYA_CKPN d
            using (select /*+ PARALLEL(8) */ distinct a.IMP_RATING               rating_CURRENT,
                                                      a.CUSTOMER_NUMBER          CUSTOMER_NUMBER,
                                                      dense_rank() over ( partition by a.CUSTOMER_NUMBER
                                                          ORDER BY priority ) As priority
                   from IFRS.GTMP_NOMI_CURR_2 a
                            join IFRS.IFRS_PRIORITY c on a.RATING_CODE = c.VALUE
                   where a.REPORT_DATE = V_CURRDATE
                     and c.TYPE = 'RATING') e
            on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
            when matched then
                update /*+ PARALLEL(8) */
                set d.rating_CURRENT=e.rating_CURRENT
                where d.rating_CURRENT is null
                  and e.priority = 1;

            commit;

            merge /*+ PARALLEL(8) */ into IFRS.GTMP_REPORT_BIAYA_CKPN d
            using (select /*+ PARALLEL(8) */ distinct a.RATING_CODE              rating_past,
                                                      a.CUSTOMER_NUMBER          CUSTOMER_NUMBER,
                                                      dense_rank() over ( partition by a.CUSTOMER_NUMBER
                                                          ORDER BY priority ) As priority
                   from IFRS.IFRS_MASTER_ACCOUNT_MONTHLY a
                            join IFRS.IFRS_PRIORITY c on a.RATING_CODE = c.VALUE
                   where a.DOWNLOAD_DATE = V_PREVDATE
                     and c.TYPE = 'RATING') e
            on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
            when matched then
                update /*+ PARALLEL(8) */
                set d.RATING_PAST=e.rating_past
                where d.RATING_PAST is null
                  and e.priority = 1;

            commit;

            merge /*+ PARALLEL(8) */ into IFRS.GTMP_REPORT_BIAYA_CKPN d
            using (select /*+ PARALLEL(8) */ distinct a.RATING_CODE              rating_CURRENT,
                                                      a.CUSTOMER_NUMBER          CUSTOMER_NUMBER,
                                                      dense_rank() over ( partition by a.CUSTOMER_NUMBER
                                                          ORDER BY priority ) As priority
                   from IFRS.IFRS_MASTER_ACCOUNT_MONTHLY a
                            join IFRS.IFRS_PRIORITY c on a.RATING_CODE = c.VALUE
                   where a.DOWNLOAD_DATE = V_CURRDATE
                     and c.TYPE = 'RATING') e
            on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
            when matched then
                update /*+ PARALLEL(8) */
                set d.rating_CURRENT=e.rating_CURRENT
                where d.rating_CURRENT is null
                  and e.priority = 1;

            commit;

            --diamankan dulu yang rating past rating current RR9 RR10 LOSS biar gk masuk perhitungan dibawahnya
            update IFRS.GTMP_REPORT_BIAYA_CKPN
            set REASON = case
                             when (RATING_CURRENT = 'RR9' OR RATING_CURRENT = 'RR10' OR RATING_CURRENT = 'LOSS') and
                                  (RATING_PAST = 'RR9' OR RATING_PAST = 'RR10' OR RATING_PAST = 'LOSS')
                                 then '0' end
            where REASON is null;

            commit;

            update /*+ PARALLEL(8) */ IFRS.GTMP_REPORT_BIAYA_CKPN
            set REASON = case
                             when RATING_CURRENT = 'RR9' OR RATING_CURRENT = 'RR10' OR RATING_CURRENT = 'LOSS'
                                 then
                                 case
                                     when (select /*+ PARALLEL(8) */ PRIORITY
                                           from IFRS.IFRS_PRIORITY
                                           where TYPE = 'RATING'
                                             and VALUE = RATING_PAST) >
                                          (select /*+ PARALLEL(8) */ PRIORITY
                                           from IFRS.IFRS_PRIORITY
                                           where TYPE = 'RATING'
                                             and VALUE = RATING_CURRENT)
                                         then (select /*+ PARALLEL(8) */ VALUE
                                               from IFRS.IFRS_PRIORITY
                                               where TYPE = 'REASON'
                                                 and PRIORITY = 13)
                                     end
                             when RATING_PAST = 'RR9' OR RATING_PAST = 'RR10' OR RATING_PAST = 'LOSS'
                                 then
                                 case
                                     when (select /*+ PARALLEL(8) */ PRIORITY
                                           from IFRS.IFRS_PRIORITY
                                           where TYPE = 'RATING'
                                             and VALUE = RATING_CURRENT) >
                                          (select /*+ PARALLEL(8) */ PRIORITY
                                           from IFRS.IFRS_PRIORITY
                                           where TYPE = 'RATING'
                                             and VALUE = RATING_PAST)
                                         then (select /*+ PARALLEL(8) */ VALUE
                                               from IFRS.IFRS_PRIORITY
                                               where TYPE = 'REASON'
                                                 and PRIORITY = 14)
                                     end
                             when RATING_PAST != RATING_CURRENT
                                 then case
                                          when (select /*+ PARALLEL(8) */ PRIORITY
                                                from IFRS.IFRS_PRIORITY
                                                where TYPE = 'RATING'
                                                  and VALUE = RATING_PAST) > (select /*+ PARALLEL(8) */ PRIORITY
                                                                              from IFRS.IFRS_PRIORITY
                                                                              where TYPE = 'RATING'
                                                                                and VALUE = RATING_CURRENT)
                                              then (select /*+ PARALLEL(8) */ VALUE
                                                    from IFRS.IFRS_PRIORITY
                                                    where TYPE = 'REASON'
                                                      and PRIORITY = 15)
                                          when (select /*+ PARALLEL(8) */ PRIORITY
                                                from IFRS.IFRS_PRIORITY
                                                where TYPE = 'RATING'
                                                  and VALUE = RATING_PAST) < (select /*+ PARALLEL(8) */ PRIORITY
                                                                              from IFRS.IFRS_PRIORITY
                                                                              where TYPE = 'RATING'
                                                                                and VALUE = RATING_CURRENT)
                                              then (select /*+ PARALLEL(8) */ VALUE
                                                    from IFRS.IFRS_PRIORITY
                                                    where TYPE = 'REASON'
                                                      and PRIORITY = 16)
                                 end
                end
            where REASON is null;

            commit;

            update IFRS.GTMP_REPORT_BIAYA_CKPN
            set REASON=null
            where REASON = '0';

            update /*+ PARALLEL(8) */ IFRS.GTMP_REPORT_BIAYA_CKPN
            set BUCKET_PAST    = RATING_PAST,
                BUCKET_CURRENT = RATING_CURRENT;
            commit;

        ELSE
	        begin
	            -- Logging: Menandai awal proses
	            IFRS.write_log('INFO', V_PROCESS_NAME, 'Langkah 1: Memulai MERGE untuk BUCKET_CURRENT.');
	            IFRS.write_log('DEBUG', V_PROCESS_NAME, 'Current Date: ' || V_CURRDATE || ', Previous Date: ' || V_PREVDATE);
	            V_STR_SQL := 'merge /*+ PARALLEL(8) */ into IFRS.GTMP_REPORT_BIAYA_CKPN a
	                using (select /*+ PARALLEL(8) */ distinct CUSTOMER_NUMBER, max(BUCKET_ID||BUCKET_GROUP) BUCKET_ID
	                       from IFRS_MASTER_ACCOUNT_MONTHLY partition for (''' || V_CURRDATE || ''') A
	                       where (' || RTRIM(NVL(V_STR_SQL_RULE, '1=1')) || ') and BUCKET_GROUP in (''DPD5_1'',''DPD4_1'',''DLQ5_1'') and MASTERID in ( select masterid from IFRS.GTMP_NOMI_CURR_2 )
	                             group by CUSTOMER_NUMBER) b
	                on (a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER)
	                when matched then
	                    update /*+ PARALLEL(8) */ set a.BUCKET_CURRENT = b.BUCKET_ID';

	            -- Opsi: Log query lengkap untuk debugging
	            -- IFRS.write_log('DEBUG', V_PROCESS_NAME, 'SQL: ' || V_STR_SQL);

	            EXECUTE IMMEDIATE V_STR_SQL;
	            IFRS.write_log('INFO', V_PROCESS_NAME, 'Langkah 1 selesai. ' || SQL%ROWCOUNT || ' baris diupdate untuk BUCKET_CURRENT.');

	            COMMIT;
	            IFRS.write_log('INFO', V_PROCESS_NAME, 'Commit setelah Langkah 1.');

	            --------------------------------------------------------------------------------
	            -- Langkah 2: MERGE untuk BUCKET_PAST
	            --------------------------------------------------------------------------------
	            IFRS.write_log('INFO', V_PROCESS_NAME, 'Langkah 2: Memulai MERGE untuk BUCKET_PAST.');

	            V_STR_SQL := 'merge /*+ PARALLEL(8) */ into IFRS.GTMP_REPORT_BIAYA_CKPN a
	                using (select /*+ PARALLEL(8) */ distinct CUSTOMER_NUMBER,  max(BUCKET_ID||BUCKET_GROUP) BUCKET_ID
	                       from IFRS_MASTER_ACCOUNT_MONTHLY partition for (''' || V_PREVDATE || ''') A
	                       where (' || RTRIM(NVL(V_STR_SQL_RULE, '1=1')) || ') and BUCKET_GROUP in (''DPD5_1'',''DPD4_1'',''DLQ5_1'') and MASTERID in ( select masterid from IFRS.GTMP_NOMI_PREV_2 )
	                             group by CUSTOMER_NUMBER) b
	                on (a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER)
	                when matched then
	                    update /*+ PARALLEL(8) */ set a.BUCKET_PAST = b.BUCKET_ID';

	            EXECUTE IMMEDIATE V_STR_SQL;
	            IFRS.write_log('INFO', V_PROCESS_NAME, 'Langkah 2 selesai. ' || SQL%ROWCOUNT || ' baris diupdate untuk BUCKET_PAST.');

	            COMMIT;
	            IFRS.write_log('INFO', V_PROCESS_NAME, 'Commit setelah Langkah 2.');

	            --------------------------------------------------------------------------------
	            -- Langkah 3: UPDATE REASON untuk bucket yang memburuk
	            --------------------------------------------------------------------------------
	            IFRS.write_log('INFO', V_PROCESS_NAME, 'Langkah 3: Memulai UPDATE REASON untuk bucket yang memburuk.');

	            update IFRS.GTMP_REPORT_BIAYA_CKPN
	            set REASON = (select VALUE from IFRS.IFRS_PRIORITY where TYPE = 'REASON' and PRIORITY = 17)
	            where REASON is null
	              and substr(BUCKET_PAST, 0, 1) < substr(BUCKET_CURRENT, 0, 1);
	            IFRS.write_log('INFO', V_PROCESS_NAME, 'Langkah 3 selesai. ' || SQL%ROWCOUNT || ' baris diupdate untuk REASON (memburuk).');

	            COMMIT;
	            IFRS.write_log('INFO', V_PROCESS_NAME, 'Commit setelah Langkah 3.');

	            --------------------------------------------------------------------------------
	            -- Langkah 4: UPDATE REASON untuk bucket yang membaik
	            --------------------------------------------------------------------------------
	            IFRS.write_log('INFO', V_PROCESS_NAME, 'Langkah 4: Memulai UPDATE REASON untuk bucket yang membaik.');

	            update IFRS.GTMP_REPORT_BIAYA_CKPN
	            set REASON = (select VALUE from IFRS.IFRS_PRIORITY where TYPE = 'REASON' and PRIORITY = 18)
	            where REASON is null
	              and substr(BUCKET_PAST, 0, 1) > substr(BUCKET_CURRENT, 0, 1);
	            IFRS.write_log('INFO', V_PROCESS_NAME, 'Langkah 4 selesai. ' || SQL%ROWCOUNT || ' baris diupdate untuk REASON (membaik).');

	            COMMIT;
	            IFRS.write_log('INFO', V_PROCESS_NAME, 'Commit setelah Langkah 4.');

	            --------------------------------------------------------------------------------
	            -- Langkah 5: UPDATE BUCKET menjadi teks deskriptif
	            --------------------------------------------------------------------------------
	            IFRS.write_log('INFO', V_PROCESS_NAME, 'Langkah 5: Memulai UPDATE untuk mengubah BUCKET menjadi teks deskriptif.');

	            update IFRS.GTMP_REPORT_BIAYA_CKPN
	            set BUCKET_PAST    = case
	                                     when substr(BUCKET_PAST, 0, 1) = '1' then '0 days overdue'
	                                     when substr(BUCKET_PAST, 0, 1) = '2' then '1-30 days overdue'
	                                     when substr(BUCKET_PAST, 0, 1) = '3' then '31-60 days overdue'
	                                     when substr(BUCKET_PAST, 0, 1) = '4' then case when BUCKET_PAST = '4DPD4_1' then '>60 days overdue' else '61-90 days overdue' end
	                                     when substr(BUCKET_PAST, 0, 1) = '5' then '>90 days overdue'
	                                 end,
	                BUCKET_CURRENT = case
	                                     when substr(BUCKET_CURRENT, 0, 1) = '1' then '0 days overdue'
	                                     when substr(BUCKET_CURRENT, 0, 1) = '2' then '1-30 days overdue'
	                                     when substr(BUCKET_CURRENT, 0, 1) = '3' then '31-60 days overdue'
	                                     when substr(BUCKET_CURRENT, 0, 1) = '4' then case when BUCKET_CURRENT = '4DPD4_1' then '>60 days overdue' else '61-90 days overdue' end
	                                     when substr(BUCKET_CURRENT, 0, 1) = '5' then '>90 days overdue'
	                                 end;

	            IFRS.write_log('INFO', V_PROCESS_NAME, 'Langkah 5 selesai. ' || SQL%ROWCOUNT || ' baris diupdate untuk teks deskripsi BUCKET.');

	            COMMIT;
	            IFRS.write_log('INFO', V_PROCESS_NAME, 'Commit setelah Langkah 5.');

	            -- Log akhir proses
	            IFRS.write_log('INFO', V_PROCESS_NAME, 'Proses berhasil diselesaikan.');

	        EXCEPTION
	            WHEN OTHERS THEN
	                -- Jika terjadi error di mana pun dalam blok BEGIN di atas
	                ROLLBACK; -- Batalkan semua perubahan yang belum di-commit
	                IFRS.write_log('ERROR', V_PROCESS_NAME, 'Terjadi kesalahan. Proses dibatalkan. Error: ' || SQLCODE || ' - ' || SQLERRM);
	                -- Melempar kembali error agar proses pemanggil tahu ada masalah
	                RAISE;

	        END;

        END IF;
        -- 22. Update REASON - Perubahan Exposure
        IFRS.write_log('INFO', V_PROCESS_NAME, '22. Update REASON untuk Perubahan Exposure...');
        -- Penambahan/Pengurangan exposure
        merge /*+ PARALLEL(8) */ INTO IFRS.GTMP_REPORT_BIAYA_CKPN d
        USING (select /*+ PARALLEL(8) */ coalesce(b.CUSTOMER_NUMBER, a.CUSTOMER_NUMBER) CUSTOMER_NUMBER,
                                         case
                                             --Penambahan Eksposure
                                             when a.EAD_AMOUNT_LCL < b.EAD_AMOUNT_LCL
                                                 then (select /*+ PARALLEL(8) */ VALUE
                                                       from IFRS.IFRS_PRIORITY
                                                       where TYPE = 'REASON'
                                                         and PRIORITY = 19)

                                             when a.EAD_AMOUNT_LCL > b.EAD_AMOUNT_LCL
                                                 then (select /*+ PARALLEL(8) */ VALUE
                                                       from IFRS.IFRS_PRIORITY
                                                       where TYPE = 'REASON'
                                                         and PRIORITY = 20)
                                             end                                        REASON
               from (select /*+ PARALLEL(8) */ CUSTOMER_NUMBER,
                                               sum(EAD_AMOUNT_LCL) EAD_AMOUNT_LCL
                     from IFRS.GTMP_NOMI_PREV_2
                     group by CUSTOMER_NUMBER) a
                        full outer join (select /*+ PARALLEL(8) */ CUSTOMER_NUMBER,
                                                                   sum(EAD_AMOUNT_LCL) EAD_AMOUNT_LCL
                                         from IFRS.GTMP_NOMI_CURR_2
                                         group by CUSTOMER_NUMBER) b
                                        on a.CUSTOMER_NUMBER = b.CUSTOMER_NUMBER) e
        on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
        when matched then
            update /*+ PARALLEL(8) */
            set d.REASON = e.REASON
            where d.REASON is null;

        IFRS.write_log('INFO', V_PROCESS_NAME, '   -> Selesai. ' || SQL%ROWCOUNT || ' baris diupdate.');
        COMMIT;


        -- 23. Update REASON - Lainnya
            IFRS.write_log('INFO', V_PROCESS_NAME, '23. Update REASON untuk kategori Lainnya...');
        -- 7 Lainnya

        update /*+ PARALLEL(8) */ IFRS.GTMP_REPORT_BIAYA_CKPN
        set REASON = (select /*+ PARALLEL(8) */ VALUE from IFRS.IFRS_PRIORITY where TYPE = 'REASON' and PRIORITY = 21)
        where REASON is null;
        IFRS.write_log('INFO', V_PROCESS_NAME, '   -> Selesai. ' || SQL%ROWCOUNT || ' baris diupdate.');
            COMMIT;


        -- 24. Finalisasi (Update Nama, Special Reason, dll.)
        IFRS.write_log('INFO', V_PROCESS_NAME, '24. Finalisasi data (Nama, Special Reason)...');
        merge /*+ PARALLEL(8) */ into IFRS.GTMP_REPORT_BIAYA_CKPN d
        using (select /*+ PARALLEL(8) */ CUSTOMER_NUMBER, max(CUSTOMER_NAME) CUSTOMER_NAME
               from IFRS.GTMP_NOMI_CURR_2
               where CUSTOMER_NAME is not null
               group by CUSTOMER_NUMBER) e
        on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
        when matched then
            update /*+ PARALLEL(8) */
            set d.CUSTOMER_NAME =e.CUSTOMER_NAME;

        commit;

        merge /*+ PARALLEL(8) */ into IFRS.GTMP_REPORT_BIAYA_CKPN d
        using (select /*+ PARALLEL(8) */ CUSTOMER_NUMBER, max(CUSTOMER_NAME) CUSTOMER_NAME
               from IFRS.GTMP_NOMI_PREV_2
               where CUSTOMER_NAME is not null
               group by CUSTOMER_NUMBER) e
        on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
        when matched then
            update /*+ PARALLEL(8) */
            set d.CUSTOMER_NAME =e.CUSTOMER_NAME
            where d.CUSTOMER_NAME is null;

        commit;

        merge /*+ PARALLEL(8) */ into IFRS.GTMP_REPORT_BIAYA_CKPN d
        using (select /*+ PARALLEL(8) */ distinct a.SPECIAL_REASON,
                                                  a.CUSTOMER_NUMBER          CUSTOMER_NUMBER,
                                                  dense_rank() over ( partition by a.CUSTOMER_NUMBER
                                                      ORDER BY PRIORITY ) As priority
               from IFRS.GTMP_NOMI_PREV_2 a
                        join IFRS.IFRS_PRIORITY b
                             on a.SPECIAL_REASON = b.VALUE
               where a.SPECIAL_REASON is not null
                 and TYPE = 'SPECIAL_REASON') e
        on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
        when matched then
            update /*+ PARALLEL(8) */
            set d.SPECIAL_REASON_PAST=e.SPECIAL_REASON
            where e.priority = 1
              and d.SPECIAL_REASON_PAST is null
              and REASON != (select /*+ PARALLEL(8) */ VALUE
                             from IFRS.IFRS_PRIORITY
                             where TYPE = 'REASON'
                               and PRIORITY = 10);

        commit;

        merge /*+ PARALLEL(8) */ into IFRS.GTMP_REPORT_BIAYA_CKPN d
        using (select /*+ PARALLEL(8) */ distinct a.SPECIAL_REASON,
                                                  a.CUSTOMER_NUMBER          CUSTOMER_NUMBER,
                                                  dense_rank() over ( partition by a.CUSTOMER_NUMBER
                                                      ORDER BY PRIORITY ) As priority
               from IFRS.GTMP_NOMI_CURR_2 a
                        join IFRS.IFRS_PRIORITY b
                             on a.SPECIAL_REASON = b.VALUE
               where a.SPECIAL_REASON is not null
                 and TYPE = 'SPECIAL_REASON') e
        on (d.CUSTOMER_NUMBER = e.CUSTOMER_NUMBER)
        when matched then
            update /*+ PARALLEL(8) */
            set d.SPECIAL_REASON_CURRENT=e.SPECIAL_REASON
            where e.priority = 1
              and d.SPECIAL_REASON_CURRENT is null
              and REASON != (select /*+ PARALLEL(8) */ VALUE
                             from IFRS.IFRS_PRIORITY
                             where TYPE = 'REASON'
                               and PRIORITY = 10);

        commit;

        -- ==========================================================
        -- LANGKAH AKHIR: PINDAHKAN KE TABEL FINAL
        -- ==========================================================
        IFRS.write_log('INFO', V_PROCESS_NAME, 'Memulai pemindahan ke tabel final...');
        delete /*+ PARALLEL(8) */
        from IFRS.IFRS_REPORT_BIAYA_CKPN
        where REPORT_DATE = (select /*+ PARALLEL(8) */ CURRDATE from IFRS.IFRS_PRC_DATE)
          and REPORT_SEGMENT = V_SEGMENT;

        commit;

        insert into /*+ PARALLEL(8) */ IFRS.IFRS_REPORT_BIAYA_CKPN
        select /*+ PARALLEL(8) */ to_date(V_CURRDATE, 'dd-mon-yyyy'), V_SEGMENT, A.*
        from IFRS.GTMP_REPORT_BIAYA_CKPN A;

        commit;

        delete /*+ PARALLEL(8) */ IFRS.IFRS_REPORT_BIAYA_CKPN_TOP10
        where REPORT_DATE = V_CURRDATE
          and REPORT_SEGMENT = V_SEGMENT;

        commit;

        insert into /*+ PARALLEL(8) */ IFRS.IFRS_REPORT_BIAYA_CKPN_TOP10
        select /*+ PARALLEL(8) */ *
        from IFRS.IFRS_REPORT_BIAYA_CKPN
        where REPORT_DATE = V_CURRDATE
          and REPORT_SEGMENT = V_SEGMENT
        order by abs(nvl(BIAYA_CADANGAN_BULAN_INI_TOTAL, 0)) desc
            fetch first 10 rows only;

        commit;

        delete /*+ PARALLEL(8) */ IFRS.IFRS_REPORT_BIAYA_CKPN_SUMM
        where REPORT_DATE = V_CURRDATE
          and REPORT_SEGMENT = V_SEGMENT;

        commit;

        insert into /*+ PARALLEL(8) */ IFRS.IFRS_REPORT_BIAYA_CKPN_SUMM
        select /*+ PARALLEL(8) */ report_date,
                                  REPORT_SEGMENT,
                                  REASON,
                                  sum(BIAYA_CADANGAN_BULAN_INI_TOTAL) BIAYA_CADANGAN_BULAN_INI_TOTAL
        from IFRS.IFRS_REPORT_BIAYA_CKPN
        where report_date = V_CURRDATE
          and REPORT_SEGMENT = V_SEGMENT
        group by report_date, REPORT_SEGMENT, REASON
        union all
        select /*+ PARALLEL(8) */ report_date, REPORT_SEGMENT, 'TOTAL', sum(BIAYA_CADANGAN_BULAN_INI_TOTAL)
        from IFRS.IFRS_REPORT_BIAYA_CKPN
        where report_date = V_CURRDATE
          and REPORT_SEGMENT = V_SEGMENT
        group by report_date, REPORT_SEGMENT;

        commit;

        IFRS.write_log('INFO', V_PROCESS_NAME, 'Proses selesai dengan sukses.');
-- Blok EXCEPTION untuk BEGIN utama
        EXCEPTION
        WHEN OTHERS THEN
        IFRS.write_log('ERROR', V_PROCESS_NAME, 'Proses gagal. Melakukan ROLLBACK. Error: ' || SQLCODE || ' - ' || SQLERRM, SQLCODE);
        ROLLBACK;
        RAISE;
    -- Lanjutkan melempar error agar proses pemanggil tahu ada masalah

-- Akhiri blok utama
end;