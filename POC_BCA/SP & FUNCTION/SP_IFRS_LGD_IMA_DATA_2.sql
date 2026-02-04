CREATE OR REPLACE PROCEDURE SP_IFRS_LGD_IMA_DATA_2
AS
    v_MIN_DATE DATE;
    v_MAX_DATE DATE;
BEGIN
    DBMS_STATS.UNLOCK_TABLE_STATS ( OWNNAME=>'IFRS', TABNAME=>'TMP_LGD_IMA');

    DBMS_STATS.GATHER_TABLE_STATS ( OWNNAME=>'IFRS', TABNAME=>'TMP_LGD_IMA',DEGREE=>2);
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LGD_IMA';
    --EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_LGD_WRITEOFF_RECOVERY';

--    SELECT MAX(DOWNLOAD_DATE), MIN(RESERVED_DATE_3)
--    INTO v_MAX_DATE, v_MIN_DATE
--    FROM TEST_LGD_21;

--    v_MIN_DATE := '31 OCT 2006';
--    v_MAX_DATE := '30 JUN 2020';
--
--    WHILE v_MIN_DATE <= v_MAX_DATE LOOP
--        SP_IFRS_INSERT_GTMP_FROM_IMA_M(v_MIN_DATE, 'ILS');
--
--        INSERT INTO TMP_LGD_IMA
--        (
--            PROCESS_DATE,
--            PROCESS_SOURCE,
--            UPLOAD_SOURCE,
--            DOWNLOAD_DATE,
--            CUSTOMER_NUMBER,
--            CUSTOMER_NAME,
--            FACILITY_NUMBER,
--            ACCOUNT_NUMBER,
--            OUTSTANDING,
--            CURRENCY,
--            ACCOUNT_STATUS,
--            BI_COLLECTABILITY,
--            PRODUCT_CODE,
--            FIRST_NPL_DATE,
--            FIRST_NPL_OS,
--            INTEREST_RATE,
--            EIR,
--            GROUP_SEGMENT,
--            SEGMENT,
--            SUB_SEGMENT,
--            LGD_RULE_ID,
--            LGD_SEGMENT,
--            SEGMENT_RULE_ID,
--            SPECIAL_REASON
--        )
--        SELECT
--            v_MAX_DATE PROCESS_DATE,
--            'IMA' PROCESS_SOURCE,
--            'IMA' UPLOAD_SOURCE,
--            A.DOWNLOAD_DATE,
--            A.CUSTOMER_NUMBER,
--            A.CUSTOMER_NAME,
--            A.FACILITY_NUMBER,
--            A.ACCOUNT_NUMBER,
--            A.OUTSTANDING,
--            A.CURRENCY,
--            A.ACCOUNT_STATUS,
--            A.BI_COLLECTABILITY,
--            A.PRODUCT_CODE,
--            A.RESERVED_DATE_3,
--            A.RESERVED_AMOUNT_8,
--            A.INTEREST_RATE,
--            A.EIR,
--            A.GROUP_SEGMENT,
--            A.SEGMENT,
--            A.SUB_SEGMENT,
--            A.LGD_RULE_ID,
--            A.LGD_SEGMENT,
--            A.SEGMENT_RULE_ID,
--            A.RESERVED_VARCHAR_9
--        FROM GTMP_IFRS_MASTER_ACCOUNT A
--        WHERE RESERVED_DATE_3 IS NOT NULL
--        AND RESERVED_DATE_3 >= '31 OCT 2006'
--        AND ACCOUNT_NUMBER NOT IN
--        (SELECT DEAL_ID FROM T_EXCLUDED_LOANS_LGD_BCA);
--
--        COMMIT;
--
--        update ifrs_prc_date_k
--        set currdate = v_min_date;
--        commit;
--
--        v_MIN_DATE := ADD_MONTHS(v_MIN_DATE, 1);
--    END LOOP;

    DELETE TMP_LGD_IMA
    WHERE FIRST_NPL_OS = 0;
    COMMIT;

    DELETE TMP_LGD_IMA
    WHERE PKID IN
    (
        SELECT A.PKID
        FROM TMP_LGD_IMA A
        JOIN
        (
            SELECT A2.ACCOUNT_NUMBER,
                MIN(DOWNLOAD_DATE) MIN_DOWNLOAD_DATE
            FROM TMP_LGD_IMA A2
            JOIN
            (
                SELECT A3.ACCOUNT_NUMBER FROM TMP_LGD_IMA A3
                JOIN
                (
                    SELECT ACCOUNT_NUMBER,
                        MAX(DOWNLOAD_DATE) MAX_DOWNLOAD_DATE
                    FROM TMP_LGD_IMA
                    GROUP BY ACCOUNT_NUMBER
                ) B3
                ON A3.DOWNLOAD_DATE = B3.MAX_DOWNLOAD_DATE
                AND A3.ACCOUNT_NUMBER = B3.ACCOUNT_NUMBER
                AND A3.OUTSTANDING > 0
            ) B2
            ON A2.ACCOUNT_NUMBER = B2.ACCOUNT_NUMBER
            AND A2.SPECIAL_REASON LIKE '%J%'
            GROUP BY A2.ACCOUNT_NUMBER
        ) B
        ON A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER
        AND A.DOWNLOAD_DATE > B.MIN_DOWNLOAD_DATE
    );
    COMMIT;

    MERGE INTO TMP_LGD_IMA A
    USING
    (
        SELECT A2.* FROM
        (
            SELECT ACCOUNT_NUMBER, MIN(DOWNLOAD_DATE) DOWNLOAD_DATE
            FROM TMP_LGD_IMA
            WHERE OUTSTANDING = 0
            GROUP BY ACCOUNT_NUMBER
        ) A2
        JOIN
        (
            SELECT A3.ACCOUNT_NUMBER
            FROM TMP_LGD_IMA A3
            JOIN
            (
             SELECT ACCOUNT_NUMBER, MAX(PKID) PKID
             FROM TMP_LGD_IMA
             GROUP BY ACCOUNT_NUMBER
            ) B3
            ON A3.ACCOUNT_NUMBER = B3.ACCOUNT_NUMBER
            AND A3.PKID = B3.PKID
            AND A3.OUTSTANDING = 0
        ) B2
        ON A2.ACCOUNT_NUMBER = B2.ACCOUNT_NUMBER
    ) B
    ON (A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER)
    WHEN MATCHED THEN
    UPDATE SET
        A.CLOSED_DATE = B.DOWNLOAD_DATE,
        A.LGD_FLAG = 'N';
    COMMIT;

    MERGE INTO TMP_LGD_IMA A
    USING
    (
        SELECT ACCOUNT_NUMBER, DOWNLOAD_DATE
        FROM TMP_LGD_IMA
        WHERE SPECIAL_REASON LIKE '%J%'
        AND CLOSED_DATE IS NULL
    ) B
    ON (A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER)
    WHEN MATCHED THEN
    UPDATE SET
        A.CLOSED_DATE = B.DOWNLOAD_DATE,
        A.LGD_FLAG = 'J';
    COMMIT;

    MERGE INTO TMP_LGD_IMA A
    USING
    (
        SELECT ACCOUNT_NUMBER, MAX(MIN_DOWNLOAD_DATE) DOWNLOAD_DATE
        FROM
        (
            SELECT DISTINCT ACCOUNT_NUMBER, OUTSTANDING, MIN(DOWNLOAD_DATE) MIN_DOWNLOAD_DATE
            FROM TMP_LGD_IMA
            WHERE ACCOUNT_NUMBER IN
            (
                SELECT DISTINCT ACCOUNT_NUMBER
                FROM TMP_LGD_IMA
                WHERE CLOSED_DATE IS NULL
                AND PRODUCT_CODE NOT IN ('304','315','310','311','312','313','314','316','320','321')
                AND ACCOUNT_STATUS = 'W'
            )
            GROUP BY ACCOUNT_NUMBER, OUTSTANDING
        )
        GROUP BY ACCOUNT_NUMBER
    ) B
    ON (A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER)
    WHEN MATCHED THEN
    UPDATE SET
        A.CLOSED_DATE = B.DOWNLOAD_DATE,
        A.LGD_FLAG = 'L';
    COMMIT;


    MERGE INTO TMP_LGD_IMA A
    USING
    (
        SELECT C2.ACCOUNT_NUMBER, C2.DOWNLOAD_DATE
        FROM TMP_LGD_IMA A2
        JOIN
        (
        SELECT ACCOUNT_NUMBER, MIN(DOWNLOAD_DATE) MIN_DOWNLOAD_DATE
        FROM TMP_LGD_IMA
        WHERE PRODUCT_CODE IN ('312','314')
        AND ACCOUNT_STATUS = 'W'
        AND CLOSED_DATE IS NULL
        GROUP BY ACCOUNT_NUMBER
        ) B2
        ON A2.DOWNLOAD_DATE = B2.MIN_DOWNLOAD_DATE
        AND A2.ACCOUNT_NUMBER = B2.ACCOUNT_NUMBER
        JOIN TMP_LGD_IMA C2
        ON ADD_MONTHS(B2.MIN_DOWNLOAD_DATE,12) = C2.DOWNLOAD_DATE
        AND A2.ACCOUNT_NUMBER = C2.ACCOUNT_NUMBER
        JOIN
        (
            SELECT A3.ACCOUNT_NUMBER, A3.OUTSTANDING
            FROM TMP_LGD_IMA A3
            JOIN
            (
                SELECT ACCOUNT_NUMBER, MAX(DOWNLOAD_DATE) MAX_DOWNLOAD_DATE
                FROM TMP_LGD_IMA
                WHERE PRODUCT_CODE IN ('312','314')
                AND ACCOUNT_STATUS = 'W'
                AND CLOSED_DATE IS NULL
                GROUP BY ACCOUNT_NUMBER
            ) B3
            ON A3.DOWNLOAD_DATE = B3.MAX_DOWNLOAD_DATE
            AND A3.ACCOUNT_NUMBER = B3.ACCOUNT_NUMBER
        ) D2
        ON A2.ACCOUNT_NUMBER = D2.ACCOUNT_NUMBER
        AND A2.OUTSTANDING = D2.OUTSTANDING
    ) B
    ON (A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER)
    WHEN MATCHED THEN
    UPDATE SET
        A.CLOSED_DATE = CASE WHEN B.DOWNLOAD_DATE < TO_DATE('30 JUN 2014','DD MON YYYY') THEN TO_DATE('30 JUN 2014','DD MON YYYY') ELSE B.DOWNLOAD_DATE END,
        A.LGD_FLAG = 'C1';
    COMMIT;

    MERGE INTO TMP_LGD_IMA A
    USING
    (
        SELECT C2.ACCOUNT_NUMBER, MAX(C2.DOWNLOAD_DATE) DOWNLOAD_DATE
        FROM TMP_LGD_IMA A2
        JOIN
        (
            SELECT ACCOUNT_NUMBER, MIN(DOWNLOAD_DATE) MIN_DOWNLOAD_DATE
            FROM TMP_LGD_IMA
            WHERE PRODUCT_CODE IN ('312','314')
            AND ACCOUNT_STATUS = 'W'
            AND CLOSED_DATE IS NULL
            GROUP BY ACCOUNT_NUMBER
        ) B2
        ON A2.DOWNLOAD_DATE = B2.MIN_DOWNLOAD_DATE
        AND A2.ACCOUNT_NUMBER = B2.ACCOUNT_NUMBER
        JOIN TMP_LGD_IMA C2
        ON ADD_MONTHS(B2.MIN_DOWNLOAD_DATE,12) <= C2.DOWNLOAD_DATE
        AND A2.ACCOUNT_NUMBER = C2.ACCOUNT_NUMBER
        JOIN
        (
            SELECT ACCOUNT_NUMBER, OUTSTANDING, MIN(DOWNLOAD_DATE) MIN_DOWNLOAD_DATE
            FROM TMP_LGD_IMA
            WHERE PRODUCT_CODE IN ('312','314')
            AND ACCOUNT_STATUS = 'W'
            AND CLOSED_DATE IS NULL
            GROUP BY ACCOUNT_NUMBER, OUTSTANDING
        ) D2
        ON A2.ACCOUNT_NUMBER = D2.ACCOUNT_NUMBER
        AND A2.OUTSTANDING != D2.OUTSTANDING
        AND C2.DOWNLOAD_DATE = CASE WHEN D2.MIN_DOWNLOAD_DATE < ADD_MONTHS(B2.MIN_DOWNLOAD_DATE,12) THEN C2.DOWNLOAD_DATE ELSE D2.MIN_DOWNLOAD_DATE END
        GROUP BY C2.ACCOUNT_NUMBER
    ) B
    ON (A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER)
    WHEN MATCHED THEN
    UPDATE SET
        A.CLOSED_DATE = CASE WHEN B.DOWNLOAD_DATE < TO_DATE('30 JUN 2014','DD MON YYYY') THEN TO_DATE('30 JUN 2014','DD MON YYYY') ELSE B.DOWNLOAD_DATE END,
        A.LGD_FLAG = 'C2';
    COMMIT;

    DELETE TMP_LGD_IMA
    WHERE LGD_FLAG IS NULL;
    COMMIT;

    MERGE INTO TMP_LGD_IMA A
    USING
    (
        SELECT
            A2.PKID,
            CASE WHEN B2.CHARGEOFF_AMOUNT > 0 THEN
                CASE WHEN B2.OS_BEFORE_CHARGEOFF - B2.CHARGEOFF_AMOUNT > 0 THEN B2.OS_BEFORE_CHARGEOFF - B2.CHARGEOFF_AMOUNT ELSE 0 END
            WHEN C2.FIRST_NPL_OS - SUM(B2.RECOVERY_AMOUNT) OVER(PARTITION BY B2.ACCOUNT_NUMBER ORDER BY B2.PKID)> 0 THEN
                B2.RECOVERY_AMOUNT
            ELSE
                CASE WHEN C2.FIRST_NPL_OS + B2.RECOVERY_AMOUNT - SUM(B2.RECOVERY_AMOUNT) OVER(PARTITION BY B2.ACCOUNT_NUMBER ORDER BY B2.PKID) > 0 THEN
                    C2.FIRST_NPL_OS + B2.RECOVERY_AMOUNT - SUM(B2.RECOVERY_AMOUNT) OVER(PARTITION BY B2.ACCOUNT_NUMBER ORDER BY B2.PKID)
                ELSE
                    0
                END
            END RECOVERY_AMOUNT
        FROM TMP_LGD_IMA A2
        JOIN
        (
            SELECT A3.PKID,
                A3.FACILITY_NUMBER,
                A3.ACCOUNT_NUMBER,
                CASE WHEN LAG(A3.OUTSTANDING) OVER(PARTITION BY A3.ACCOUNT_NUMBER ORDER BY A3.PKID) - A3.OUTSTANDING > 0 THEN
                    LAG(A3.OUTSTANDING) OVER(PARTITION BY A3.ACCOUNT_NUMBER ORDER BY A3.PKID) - A3.OUTSTANDING
                ELSE
                    0
                END RECOVERY_AMOUNT,
                NVL(B3.OS_BEFORE_CHARGEOFF,0) OS_BEFORE_CHARGEOFF,
                NVL(B3.CHARGEOFF_AMOUNT,0) CHARGEOFF_AMOUNT
            FROM TMP_LGD_IMA A3
            LEFT JOIN
            (
                SELECT A4.ACCOUNT_NUMBER,
                A4.OUTSTANDING AS OS_BEFORE_CHARGEOFF,
                VALUATION_DATE CHARGEOFF_DATE,
                B4.AMOUNT CHARGEOFF_AMOUNT
                FROM TMP_LGD_IMA A4
                JOIN
                (
                    SELECT A5.ACCOUNT_NUMBER,
                        MAX(DOWNLOAD_DATE) DOWNLOAD_DATE,
                        B5.VALUATION_DATE,
                        B5.AMOUNT
                    FROM TMP_LGD_IMA A5
                    JOIN
                    (
                        SELECT DISTINCT ACCOUNT_NUMBER,
                            LAST_DAY(VALUATION_DATE) VALUATION_DATE,
                            SUM(AMOUNT) AMOUNT
                        FROM
                        (
                            SELECT DISTINCT ACCOUNT_NUMBER, VALUATION_DATE, AMOUNT
                            FROM IFRS_MASTER_VALUATION
                            WHERE TRX_CODE = 'CHARGEOFF'
                        )
                        GROUP BY ACCOUNT_NUMBER,
                            LAST_DAY(VALUATION_DATE)
                        HAVING SUM(AMOUNT) > 0
                    ) B5
                    ON A5.DOWNLOAD_DATE <= B5.VALUATION_DATE
                    AND A5.ACCOUNT_NUMBER = B5.ACCOUNT_NUMBER
                    AND A5.OUTSTANDING > 0
                    GROUP BY A5.ACCOUNT_NUMBER,
                        B5.VALUATION_DATE,
                        B5.AMOUNT
                ) B4
                ON A4.ACCOUNT_NUMBER = B4.ACCOUNT_NUMBER
                AND A4.DOWNLOAD_DATE = B4.DOWNLOAD_DATE
            ) B3
            ON A3.ACCOUNT_NUMBER = B3.ACCOUNT_NUMBER
            AND A3.DOWNLOAD_DATE = B3.CHARGEOFF_DATE
        ) B2
        ON A2.PKID = B2.PKID
        JOIN
        (
            SELECT A3.*
            FROM TMP_LGD_IMA A3
            JOIN
            (
                SELECT ACCOUNT_NUMBER,
                    MIN(DOWNLOAD_DATE) MIN_DOWNLOAD_DATE
                FROM TMP_LGD_IMA
                GROUP BY ACCOUNT_NUMBER
            ) B3
            ON A3.DOWNLOAD_DATE = B3.MIN_DOWNLOAD_DATE
            AND A3.ACCOUNT_NUMBER = B3.ACCOUNT_NUMBER
        ) C2
        ON A2.ACCOUNT_NUMBER = C2.ACCOUNT_NUMBER
    ) B
    ON (A.PKID = B.PKID)
    WHEN MATCHED THEN
    UPDATE SET A.RECOVERY_AMOUNT = B.RECOVERY_AMOUNT;

    COMMIT;

    MERGE INTO TMP_LGD_IMA A
    USING
    (
        SELECT ACCOUNT_NUMBER, MAX(DOWNLOAD_DATE) DOWNLOAD_DATE
        FROM TMP_LGD_IMA
        WHERE DOWNLOAD_DATE > CLOSED_DATE
        AND RECOVERY_AMOUNT > 0
        GROUP BY ACCOUNT_NUMBER
    ) B
    ON (A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER)
    WHEN MATCHED THEN
    UPDATE SET
        A.CLOSED_DATE = B.DOWNLOAD_DATE;
    COMMIT;

    UPDATE TMP_LGD_IMA
    SET INTEREST_RATE = ROUND(INTEREST_RATE,2) / 100;
    COMMIT;

    MERGE INTO TMP_LGD_IMA A
    USING
    (
        SELECT A2.ACCOUNT_NUMBER,
            A2.INTEREST_RATE,
            A2.SEGMENT_RULE_ID
        FROM TMP_LGD_IMA A2
        JOIN
        (
            SELECT ACCOUNT_NUMBER, MIN(DOWNLOAD_DATE) MIN_DOWNLOAD_DATE
            FROM TMP_LGD_IMA
            GROUP BY ACCOUNT_NUMBER
        ) B2
        ON A2.DOWNLOAD_DATE = B2.MIN_DOWNLOAD_DATE
        AND A2.ACCOUNT_NUMBER = B2.ACCOUNT_NUMBER
    ) B
    ON (A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER)
    WHEN MATCHED THEN
    UPDATE SET
        A.INTEREST_RATE = B.INTEREST_RATE;--,
--        A.SEGMENT_RULE_ID = B.SEGMENT_RULE_ID;
    COMMIT;

    MERGE INTO TMP_LGD_IMA A
    USING
    (
        SELECT A2.ACCOUNT_NUMBER,
            A2.CUSTOMER_NUMBER
        FROM TMP_LGD_IMA A2
        JOIN
        (
            SELECT ACCOUNT_NUMBER, MAX(DOWNLOAD_DATE) MAX_DOWNLOAD_DATE
            FROM TMP_LGD_IMA
            GROUP BY ACCOUNT_NUMBER
        ) B2
        ON A2.DOWNLOAD_DATE = B2.MAX_DOWNLOAD_DATE
        AND A2.ACCOUNT_NUMBER = B2.ACCOUNT_NUMBER
    ) B
    ON (A.ACCOUNT_NUMBER = B.ACCOUNT_NUMBER)
    WHEN MATCHED THEN
    UPDATE SET
        A.CUSTOMER_NUMBER = B.CUSTOMER_NUMBER;
    COMMIT;

--Raphael:
--di FS cara nentuin segmentasi begini... gw lupa dulu udah pernah gw bilang belum...
--
--jika product type bukan kepala 3 atau bukan 610, tapi   segment terakhir saat close adalah 'X' maka  segmentasi menggunakan 1 bulan sebelum tutup
--
--jika  segment terakhir sebelum close adalah S,M,L,K dan tanggal close > 30-juni-2015 maka segmentasi menggunakan 1 bulan sebelum tutup
--
--selain kondisi tersebut segmentasi yang digunaakan adalah segmentasi saat close date

--    Select distinct account_number
--From tmp_lgd_ima
--where account_number in
--(
--Select account_number from
--(
--Select distinct account_number, product_code, segment_rule_id from tmp_lgd_ima
--) group by account_number having count(*) > 1
--)
--and closed_date >= '30 JUN 19'


--merge into tmp_lgd_ima a
--using
--(
--select a.account_number, a.segment_rule_id closed_segment, b.segment_rule_id prev_segment
--from tmp_lgd_ima a
--join tmp_lgd_ima b
--on a.account_number = b.account_number
--and a.download_date = a.closed_date
--and b.download_date = add_months(a.closed_date, -1)
--and a.account_number in
--(
--'0231900047400001',
--'00619008661000031214',
--'09880247988000010515',
--'00039067686000490118',
--'04379001073001011214',
--'0588590033700001',
--'0846090277100001',
--'0113900055100001',
--'00019077146000040514',
--'00619008164000031215',
--'00039067686000480118',
--'09880185125000010214',
--'09880371514000010717',
--'09880392881000011017',
--'09880428001000010218',
--'0001907714600003',
--'09880270157000010915',
--'02859001046000010815',
--'02639001401000041214',
--'0383900042200001',
--'0027900665000011',
--'01169000334000031016',
--'00019077294000100118',
--'0822090429700001',
--'00039067686000050616',
--'04379001073001020618',
--'05460919683000010617',
--'0794090052700001',
--'09880269779000010915',
--'0604090099100001',
--'0027900972100001',
--'0027900665000027',
--'0116900033400001',
--'0487901003600001',
--'09880294188000010216',
--'07920900920000040214',
--'02859001046000050416',
--'09880389600000030917',
--'09880461769000010518',
--'0058900051100001',
--'0034900032800001',
--'09880247988000030515',
--'00039067686000060616',
--'05460932213003840817',
--'09880269779000041018',
--'0296900066500001',
--'09880246591000010515',
--'0898590001500001',
--'0426900549900001',
--'00039067686000040616',
--'00039067686000470118',
--'00039067686000510418',
--'00039067686000520418',
--'00489006270000040417',
--'00039067686000460118',
--'09880237257000010315',
--'02859001046000060917',
--'0116900087300001',
--'00039067686000010616',
--'00039067686000500318',
--'0437900107300001',
--'0988017074800001',
--'05460932213003031215',
--'0263900140100001',
--'09880296628000030419',
--'09880461769001010518',
--'09880211789000010814'
--)
--) b
--on (a.account_Number = b.account_number)
--when matched then
--update set
--a.segment_rule_id = b.prev_segment


--    merge into tmp_lgd_ima a
--using
--(
--     Select a.*, loss_date, discount_rate
--from (Select distinct account_number, interest_rate, first_npl_date from tmp_lgd_ima) a
--join
--(Select a.*
--from T_RPT_LGD_LOAN_BCA a
--    join T_LOSS_EVENT b
--    on a.deal_id = trim(b.loss_event_external_reference)) b
--on a.account_number = b.deal_id
--and a.interest_rate != b.discount_rate
--and last_day(loss_date) < '31 JAN 2011'
--) b
--on (a.account_number = b.account_number)
--when matched then
--update set a.interest_rate = b.discount_rate;
--commit;

END;