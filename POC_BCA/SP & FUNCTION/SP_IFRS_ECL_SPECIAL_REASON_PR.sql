CREATE OR REPLACE procedure             SP_IFRS_ECL_SPECIAL_REASON_PR(v_ECLID NUMBER DEFAULT(0), v_DOWNLOADDATE DATE DEFAULT('1-JAN-1900'))
AS
    V_DATE_CKPN365 DATE;
BEGIN
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS.GTMP_IFRS_MASTER_ACCOUNT';

    INSERT INTO IFRS.GTMP_IFRS_MASTER_ACCOUNT
    (
        PKID,
        DOWNLOAD_DATE,
        MASTERID,
        MASTER_ACCOUNT_CODE,
        CUSTOMER_NUMBER,
        ACCOUNT_NUMBER,
        CR_STAGE,
        OUTSTANDING,
        RESERVED_VARCHAR_1
    )
   SELECT A.PKID,
        A.DOWNLOAD_DATE,
        A.MASTERID,
        ' ' MASTER_ACCOUNT_CODE,
        A.CUSTOMER_NUMBER,
        A.ACCOUNT_NUMBER,
        A.CR_STAGE,
        A.OUTSTANDING,
        CASE
            WHEN NVL(BTB_FLAG, 0) = 1          THEN 'BACK-T0-BACK, NO IMPAIRMENT' --NOTE! RULE PRIORITY IS IMPORTANT.
            WHEN NVL(A.RESERVED_FLAG_6,0) = 1  THEN 'CKPN 100%'
        END AS SPECIAL_REASON
    FROM IFRS.IFRS_MASTER_ACCOUNT A
    WHERE A.DOWNLOAD_DATE = v_DOWNLOADDATE
    AND (A.ACCOUNT_STATUS = 'A' OR (A.DATA_SOURCE = 'CRD' AND A.ACCOUNT_STATUS = 'C' AND A.OUTSTANDING > 0))
    AND A.DATA_SOURCE IN ('ILS', 'BTRD', 'KTP', 'CRD','LIMIT')
    AND 1= (
        CASE WHEN NVL(BTB_FLAG, 0) = 1 OR NVL(A.RESERVED_FLAG_6,0) = 1 THEN 1
             ELSE 0
        END
    );

    COMMIT;

    DELETE IFRS.IFRS_ECL_RESULT_DETAIL_CALC_PR
    WHERE DOWNLOAD_DATE = v_DOWNLOADDATE
    AND ECL_MODEL_ID = v_ECLID
    AND MASTERID IN
    (SELECT MASTERID FROM IFRS.GTMP_IFRS_MASTER_ACCOUNT)
    AND COUNTER_PAYSCHD > 1;

    COMMIT;

    MERGE INTO IFRS.IFRS_ECL_RESULT_DETAIL_CALC_PR A
    USING IFRS.GTMP_IFRS_MASTER_ACCOUNT B
    ON (A.DOWNLOAD_DATE = v_DOWNLOADDATE
    AND A.ECL_MODEL_ID = v_ECLID
    AND A.MASTERID = B.MASTERID)
    WHEN MATCHED THEN
    UPDATE SET
        A.PD_RATE = 0,
        A.LGD_RATE = 1,
        A.DISCOUNT_RATE = 1,
        A.ECL_AMOUNT = 0
    WHERE B.RESERVED_VARCHAR_1 = 'BACK-T0-BACK, NO IMPAIRMENT';

    COMMIT;

    MERGE INTO IFRS.IFRS_ECL_RESULT_DETAIL_CALC_PR A
    USING IFRS.GTMP_IFRS_MASTER_ACCOUNT B
    ON (A.DOWNLOAD_DATE = v_DOWNLOADDATE
    AND A.ECL_MODEL_ID = v_ECLID
    AND A.MASTERID = B.MASTERID)
    WHEN MATCHED THEN
    UPDATE SET
        A.PD_RATE = 1,
        A.LGD_RATE = 1,
        A.DISCOUNT_RATE = 1,
        A.ECL_AMOUNT = A.EAD_AMOUNT
    WHERE B.RESERVED_VARCHAR_1 = 'CKPN 100%';

    COMMIT;

    -- LEO update detail to detail_pr
    MERGE INTO IFRS.IFRS_ECL_RESULT_DETAIL_PR A
    USING
    (   SELECT A2.MASTERID, B2.ECL_AMOUNT,
               A2.RESERVED_VARCHAR_1 AS SPECIAL_REASON
        FROM IFRS.GTMP_IFRS_MASTER_ACCOUNT A2
        JOIN IFRS.IFRS_ECL_RESULT_DETAIL_CALC_PR B2
        ON B2.DOWNLOAD_DATE = v_DOWNLOADDATE
        AND B2.ECL_MODEL_ID = v_ECLID
        AND A2.MASTERID = B2.MASTERID
    ) B
    ON (A.DOWNLOAD_DATE = v_DOWNLOADDATE
    AND A.ECL_MODEL_ID = v_ECLID
    AND A.MASTERID = B.MASTERID)
    WHEN MATCHED THEN
    UPDATE SET
        A.ECL_AMOUNT = B.ECL_AMOUNT,
        A.SPECIAL_REASON = B.SPECIAL_REASON;

    COMMIT;
         ---------------------------------------------------------------------------
/* CKPN 365                                                              */
---------------------------------------------------------------------------
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS.tmp_ima_ckpn365';
-- LEO 600094647 - CKPN 100% PAYLATER & Rekon REGLA vs TB
--insert populasi ckpn365 ke table tmp
    INSERT INTO IFRS.tmp_ima_ckpn365
    WITH ima AS (SELECT A.PKID,
                 A.DOWNLOAD_DATE,
                 A.MASTERID,
                 A.CUSTOMER_NUMBER,
                 A.ACCOUNT_NUMBER,
                 A.DATA_SOURCE,
                 A.ACCOUNT_STATUS,
                 A.CR_STAGE,
                 A.OUTSTANDING,
                 a.SEGMENT,
                 a.PRODUCT_CODE,
                 a.DAY_PAST_DUE,
                 A.RATING_CODE DELINQUENCY
          FROM IFRS.IFRS_MASTER_ACCOUNT A
          WHERE A.DOWNLOAD_DATE = v_DOWNLOADDATE
            AND (A.ACCOUNT_STATUS = 'A' OR
                 (A.DATA_SOURCE = 'CRD' AND A.ACCOUNT_STATUS = 'C' AND A.OUTSTANDING > 0))
            AND A.DATA_SOURCE IN ('ILS', 'BTRD', 'KTP', 'CRD', 'LIMIT')
            AND NVL(A.RESERVED_VARCHAR_9, ' ') NOT LIKE '%H%'),

         param AS (SELECT distinct a.OPTION_GROUPING,
                                a.SEGMENT,
                                a.PRODUCT_CODE,
                                nvl(a.DPD, 0)     as DPD,
                                a.DLQ,
                                nvl(a.CKPN365, 0) as CKPN365
                         FROM IFRS.TBLU_CKPN365_PARAM a
                         WHERE a.DOWNLOAD_DATE =(select max(DOWNLOAD_DATE) DOWNLOAD_DATE from IFRS.TBLU_CKPN365_PARAM)
                         ),

         ranked_join AS (SELECT ima.PKID,
                                ima.DOWNLOAD_DATE,
                                ima.MASTERID,
                                ima.CUSTOMER_NUMBER,
                                ima.ACCOUNT_NUMBER,
                                ima.DATA_SOURCE,
                                ima.ACCOUNT_STATUS,
                                ima.PRODUCT_CODE,
                                ima.SEGMENT,
                                ima.CR_STAGE,
                                ima.OUTSTANDING,
                                ima.DAY_PAST_DUE,
                                ima.DELINQUENCY,
                                param.CKPN365,
                                param.DPD,
                                param.DLQ,
                                param.OPTION_GROUPING,
                                ROW_NUMBER() OVER (
                                    PARTITION BY ima.PKID
                                    ORDER BY
                                        CASE
                                            WHEN nvl(param.SEGMENT,'-') <> '-' AND nvl(param.PRODUCT_CODE,'-') <> '-' THEN 1
                                            WHEN nvl(param.SEGMENT,'-') <> '-' AND nvl(param.PRODUCT_CODE,'-') = '-' THEN 2
                                            WHEN nvl(param.SEGMENT,'-') = '-' AND  nvl(param.PRODUCT_CODE,'-') <> '-' THEN 3
                                            END
                                    ) as match_rank
                         FROM ima
                                  INNER JOIN param ON (param.SEGMENT = ima.SEGMENT OR nvl(param.SEGMENT,'-') = '-')
                             AND (param.PRODUCT_CODE = ima.PRODUCT_CODE OR nvl(param.PRODUCT_CODE,'-') = '-'))

    SELECT pkid,
           DOWNLOAD_DATE,
           MASTERID,
           CUSTOMER_NUMBER,
           ACCOUNT_NUMBER,
           DATA_SOURCE,
           ACCOUNT_STATUS,
           SEGMENT,
           PRODUCT_CODE,
           CR_STAGE,
           OUTSTANDING,
           CASE WHEN CKPN365 = 1 THEN 'CKPN 100%' ELSE 'CKPN 365' END      AS SPECIAL_REASON,
           OPTION_GROUPING,
           DAY_PAST_DUE,
           CASE WHEN OPTION_GROUPING = 'DLQ' then DELINQUENCY else null end as DELINQUENCY,
           CKPN365
    FROM ranked_join
    WHERE match_rank = 1
      AND (
        -- Kondisi 1: Jika grouping berdasarkan DLQ
        (OPTION_GROUPING = 'DLQ' AND DELINQUENCY >= DLQ)
            OR
            -- Kondisi 2: Jika grouping berdasarkan DPD (atau jika OPTION_GROUPING kosong/lainnya)
        (NVL(OPTION_GROUPING, 'DPD') = 'DPD' AND DAY_PAST_DUE >= DPD)
        );
    COMMIT;
-- end leo 600094647 - CKPN 100% PAYLATER & Rekon REGLA vs TB
    -- ANGEL
--insert populasi ckpn365 ke table tmp
--     INSERT INTO IFRS.tmp_ima_ckpn365
--     select pkid,
--            DOWNLOAD_DATE,
--            MASTERID,
--            MASTER_ACCOUNT_CODE,
--            CUSTOMER_NUMBER,
--            ACCOUNT_NUMBER,
--            CR_STAGE,
--            OUTSTANDING,
--            case when param.CKPN365 = 1 then 'CKPN 100%' else 'CKPN 365' end SPECIAL_REASON,
--            DAY_PAST_DUE,
--            CKPN365
--     from (SELECT A.PKID,
--                  A.DOWNLOAD_DATE,
--                  A.MASTERID,
--                  ' ' MASTER_ACCOUNT_CODE,
--                  A.CUSTOMER_NUMBER,
--                  A.ACCOUNT_NUMBER,
--                  A.CR_STAGE,
--                  A.OUTSTANDING,
-- --                  CASE
-- --                      WHEN NVL(BTB_FLAG, 0) = 1
-- --                          THEN 'BACK-T0-BACK, NO IMPAIRMENT' --NOTE! RULE PRIORITY IS IMPORTANT.
-- --                      WHEN NVL(A.RESERVED_FLAG_6, 0) = 1 THEN 'CKPN 100%'
-- --                      END AS SPECIAL_REASON,
--                  a.SEGMENT,
--                  a.DAY_PAST_DUE
--           FROM IFRS.IFRS_MASTER_ACCOUNT A
--           WHERE A.DOWNLOAD_DATE = v_DOWNLOADDATE
--             AND (A.ACCOUNT_STATUS = 'A' OR
--                  (A.DATA_SOURCE = 'CRD' AND A.ACCOUNT_STATUS = 'C' AND A.OUTSTANDING > 0))
--             AND A.DATA_SOURCE IN ('ILS', 'BTRD', 'KTP', 'CRD', 'LIMIT')
-- --             AND NVL(A.RESERVED_FLAG_6, 0) = 1
--             AND NVL(A.RESERVED_VARCHAR_9, ' ') NOT LIKE '%H%') ima
--              join (SELECT SEGMENT,
--                           nvl(DPD,0) dpd,
--                           nvl(CKPN365,0) ckpn365
--                    FROM (SELECT a.*,
--                                 ROW_NUMBER() OVER (PARTITION BY SEGMENT ORDER BY DOWNLOAD_DATE desc ,EFFECTIVE_DATE DESC, CREATEDDATE DESC) as rn
--                          FROM IFRS.TBLU_CKPN365_PARAM a
--                          where DOWNLOAD_DATE <= v_DOWNLOADDATE
--                            and EFFECTIVE_DATE <= v_DOWNLOADDATE
--                            and CREATEDDATE <= v_DOWNLOADDATE
--                         ) sub
--                    WHERE rn = 1
--                      and EFFECTIVE_DATE is not null
--                      and SEGMENT is not null
--                      and dpd is not null
--                      and ckpn365 is not null) param on ima.segment = param.segment
--     where ima.DAY_PAST_DUE >= param.DPD;
--
--     COMMIT;

    -- END ANGEL

    -- LEO 600084564 -- dibawah ini logic agar bisa exclude akun 365 sejak desember 2024

    V_DATE_CKPN365:='31-dec-2024';
    -- LEO
    update IFRS.tmp_ima_ckpn365
    set CKPN365 = 1,
        SPECIAL_REASON='CKPN 100%'
    where MASTERID in (
        select MASTERID
        from IFRS.TMP_CKPN_365_100
        where DOWNLOAD_DATE between V_DATE_CKPN365 and add_months(v_DOWNLOADDATE,-1) and DAY_PAST_DUE>=365
        group by MASTERID having count(1)=months_between(last_day(v_DOWNLOADDATE),last_day(V_DATE_CKPN365))
    );

    commit;
    delete from IFRS.TMP_CKPN_365_100 where DOWNLOAD_DATE=v_DOWNLOADDATE;

    commit;

    insert into  /*+ PARALLEL(8) */ IFRS.TMP_CKPN_365_100
    select /*+ PARALLEL(8) */ DOWNLOAD_DATE,MASTERID,DAY_PAST_DUE
    from IFRS.IFRS_MASTER_ACCOUNT
    where DOWNLOAD_DATE=v_DOWNLOADDATE and SEGMENT in ('SME','KUK','KPR') and DAY_PAST_DUE>=365;

    commit;
    -- END LEO

    MERGE INTO IFRS.IFRS_ECL_RESULT_DETAIL_CALC_PR A
    USING IFRS.tmp_ima_ckpn365 B
    ON (A.DOWNLOAD_DATE = v_DOWNLOADDATE
        AND A.ECL_MODEL_ID = v_ECLID
        AND A.MASTERID = B.MASTERID)
    WHEN MATCHED THEN
        UPDATE
        SET A.PD_RATE       = 1,
            A.LGD_RATE      = 1,
            A.DISCOUNT_RATE = 1,
            A.ECL_AMOUNT    = A.EAD_AMOUNT * B.CKPN365
        WHERE B.SPECIAL_REASON in ('CKPN 365', 'CKPN 100%');

    COMMIT;

    MERGE INTO IFRS.IFRS_ECL_RESULT_DETAIL_PR A
    USING
        (SELECT A2.MASTERID,
                B2.ECL_AMOUNT,
                A2.SPECIAL_REASON AS SPECIAL_REASON
         FROM IFRS.tmp_ima_ckpn365 A2
                  JOIN IFRS.IFRS_ECL_RESULT_DETAIL_CALC_PR B2
                       ON B2.DOWNLOAD_DATE = v_DOWNLOADDATE
                           AND B2.ECL_MODEL_ID = v_ECLID
                           AND A2.MASTERID = B2.MASTERID) B
    ON (A.DOWNLOAD_DATE = v_DOWNLOADDATE
        AND A.ECL_MODEL_ID = v_ECLID
        AND A.MASTERID = B.MASTERID)
    WHEN MATCHED THEN
        UPDATE
        SET A.ECL_AMOUNT     = B.ECL_AMOUNT,
            A.SPECIAL_REASON = B.SPECIAL_REASON;

    COMMIT;
    ---------------------------------------------------------------------------
/* CKPN 365                                                              */
---------------------------------------------------------------------------
END;