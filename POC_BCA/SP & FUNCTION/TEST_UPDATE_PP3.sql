CREATE OR REPLACE PROCEDURE TEST_UPDATE_PP3
as
  v_date date;
  V_PREV DATE;
begin

-------------------------------------------------------------------------------- Perbaikan CCF Process - Header jan 20 - jun 25
    EXECUTE IMMEDIATE 'alter session set temp_undo_enabled=true';
    EXECUTE IMMEDIATE 'alter session enable parallel dml';

    v_date := '31-JAN-2023';
    V_PREV := '31-dec-2022';
--
--

update IFRS.ifrs_prepayment_detail
set segmentation_id = case when segmentation_id = '544' then '543'
                           when segmentation_id = '546' then '545'
                           when segmentation_id = '548' then '547'
                           when segmentation_id = '550' then '549'
                      end,
     prepayment_segment = case when segmentation_id = '544' then 'PREPAYMENT KKB AUTOMOBIL NON REV BARU'
                               when segmentation_id = '546' then 'PREPAYMENT KKB AUTOMOBIL NON REV BEKAS'
                               when segmentation_id = '548' then 'PREPAYMENT KKB RODA 2 NON REV BARU'
                               when segmentation_id = '550' then 'PREPAYMENT KKB RODA 2 NON REV BEKAS'
                          else prepayment_segment end
    where segmentation_id in ('544','546','548','550')
    and download_Date = '31-jan-2023';
    commit;

    delete ifrs.ifrs_prepayment_header where download_date >= '31-jan-2023';commit;

    while v_date <= '30-jun-2025' loop
--

   INSERT INTO IFRS.IFRS_PREPAYMENT_HEADER (DOWNLOAD_DATE,
                                       SEGMENTATION_ID,
                                       SEGMENTATION_NAME,
                                       PREPAYMENT_RULE_ID,
                                       PREPAYMENT_RULE_NAME,
                                       AVERAGE_SMM,
                                       PREPAYMENT_RATE,
                                       DURATION)
        SELECT V_DATE DOWNLOAD_DATE,
               A.SEGMENTATION_ID,
               NVL (C.SEGMENT, '-') SEGMENTATION_NAME,
               NVL (D.PKID, 0) PREPAYMENT_RULE_ID,
               NVL (D.PREPAYMENT_RULE_NAME, '-') PREPAYMENT_RULE_NAME,
               AVG (A.SMM) AVERAGE_SMM,
               ROUND (
                  1
                  - POWER (
                       (1 - AVG (A.SMM)),
                       CASE
                          WHEN A.INCREMENTS = 1 THEN 12
                          WHEN A.INCREMENTS = 3 THEN 4
                          WHEN A.INCREMENTS = 6 THEN 2
                          WHEN A.INCREMENTS = 12 THEN 1
                       END),
                  4)
                  PREPAYMENT_RATE,
               CASE
                  WHEN A.INCREMENTS = 1 THEN 12
                  WHEN A.INCREMENTS = 3 THEN 4
                  WHEN A.INCREMENTS = 6 THEN 2
                  WHEN A.INCREMENTS = 12 THEN 1
               END
                  DURATION
          FROM IFRS.IFRS_PREPAYMENT_DETAIL A
               LEFT JOIN IFRS.IFRS_MSTR_SEGMENT_RULES_HEADER C
                  ON A.SEGMENTATION_ID = C.PKID
               LEFT JOIN IFRS.IFRS_PREPAYMENT_RULES_CONFIG D
                  ON C.PKID = D.SEGMENTATION_ID
         WHERE     A.DOWNLOAD_DATE <= v_date
               AND D.AVERAGE_METHOD = 'Simple'
               AND A.SMM >= 0
               AND A.DOWNLOAD_dATE <= v_date
               AND A.DURATION = 12
      GROUP BY A.SEGMENTATION_ID,
               C.SEGMENT,
               D.PKID,
               D.PREPAYMENT_RULE_NAME,
               A.INCREMENTS;

   COMMIT;

            v_date := ADD_MONTHS(V_DATE,1);
         V_PREV := ADD_MONTHS(V_PREV,1);

       update IFRS.IFRS_DATE_DAY1
       set currdate = v_date,
           PREVDATE = V_PREV;
       commit;

    end loop;

end;