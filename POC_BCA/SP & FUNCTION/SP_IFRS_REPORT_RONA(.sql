CREATE OR REPLACE PROCEDURE SP_IFRS_REPORT_RONA(
    V_CURR DATE DEFAULT '1-JAN-1900',
    V_PREV DATE DEFAULT '1-JAN-1900')
AS
BEGIN

    DBMS_STATS.UNLOCK_TABLE_STATS(OWNNAME => 'IFRS',
                                  TABNAME => 'TMP_SUBTOTAL_RONA');
    DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => 'IFRS',
                                  TABNAME => 'TMP_SUBTOTAL_RONA',
                                  DEGREE => 2);


    DECLARE
        COUNTER   NUMBER;
        RE1       VARCHAR2(5);
        RE2       VARCHAR2(5);
        RE3       VARCHAR2(5);
        LAP       VARCHAR2(10);
        TGL       DATE;
        CBG       VARCHAR2(50);
        HAL       NUMBER;
        MAXPID    NUMBER;
        BR        VARCHAR2(5);
        BR2       VARCHAR2(5);
        PRODUCT   VARCHAR2(10);
        CCY       VARCHAR2(3);
        SOURCE    VARCHAR2(10);
        PID       NUMBER;
        TGL_CETAK DATE;
    BEGIN
        IF V_CURR = '1-JAN-1900'
        THEN
            SELECT CURRDATE INTO TGL FROM IFRS_PRC_DATE;
        ELSE
            TGL := V_CURR;
        END IF;

        TGL_CETAK := TRUNC(SYSDATE);

        COUNTER := 1;
        RE1 := '1B';
        RE2 := '6B';
        RE3 := '2T';
        LAP := 'R-77061';
        --TGL     := '31-MAR-2020';
        CBG := 'KCU - PANGKAL PINANG';
        HAL := 1;
        PID := 1;
        BR2 := ' ';


        EXECUTE IMMEDIATE 'TRUNCATE TABLE GTMP_NOMINATIVE_RONA';

        INSERT INTO GTMP_NOMINATIVE_RONA (REPORT_DATE,
                                          BRANCH_CODE,
                                          DATA_SOURCE,
                                          ACCOUNT_STATUS,
                                          PRODUCT_CODE,
                                          BI_CODE,
                                          NOREK_LBU,
                                          BI_COLLECTABILITY,
                                          STAGE,
                                          CURRENCY,
                                          OUTSTANDING_OFF_BS_CCY,
                                          ECL_OFF_BS_CCY,
                                          PRINCIPAL_AMOUNT_CCY,
                                          OUTSTANDING_PRINCIPAL_CCY,
                                          OUTSTANDING_ON_BS_CCY,
                                          RESERVED_AMOUNT_2,
                                          ACCOUNT_NUMBER,
                                          FACILITY_NUMBER)
        SELECT REPORT_DATE,
               BRANCH_CODE,
               DATA_SOURCE,
               ACCOUNT_STATUS,
               CASE
                   WHEN DATA_SOURCE IN ('ILS', 'LIMIT') THEN PRODUCT_CODE
                   ELSE PRODUCT_CODE_GL
                   END,
               BI_CODE,
               NOREK_LBU,
               BI_COLLECTABILITY,
               STAGE,
               CURRENCY,
               OUTSTANDING_OFF_BS_CCY,
               ECL_OFF_BS_CCY,
               PRINCIPAL_AMOUNT_CCY,
               OUTSTANDING_PRINCIPAL_CCY,
               OUTSTANDING_ON_BS_CCY,
               RESERVED_AMOUNT_2,
               ACCOUNT_NUMBER,
               FACILITY_NUMBER
        FROM IFRS_NOMINATIVE
        WHERE REPORT_DATE = TGL
          AND (DATA_SOURCE NOT IN ('ILS', 'LIMIT')
            OR (DATA_SOURCE IN ('ILS', 'LIMIT')
                AND PRODUCT_CODE NOT LIKE '7%')); -- Exclude Product Code 7XX dari laporan R 77061;

        COMMIT;

        EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_REPORT_RONA';

        EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_SUBTOTAL_RONA';

        --BEGIN
        --EXECUTE IMMEDIATE 'DROP SEQUENCE SEQ_IFRS_REPORT_RONA';
        --EXECUTE IMMEDIATE 'CREATE SEQUENCE SEQ_IFRS_REPORT_RONA START WITH 1';
        --END;

        /*
            INSERT INTO TMP_BRANCH_LIST
              (BRANCH_CODE)
              SELECT DISTINCT BRANCH_NUM
                FROM IFRS_MASTER_BRANCH A
               WHERE DOWNLOAD_DATE = '31-JAN-2020'
                 AND NOT EXISTS (SELECT 1
                        FROM TMP_BRANCH_LIST B
                       WHERE A.BRANCH_NUM = B.BRANCH_CODE)
               ORDER BY BRANCH_NUM;
            COMMIT;
        */

        --- INSERT SUBTOTAL PER PRODUCT CODE ILS/GL
        INSERT INTO TMP_SUBTOTAL_RONA (SUBTOTAL_SOURCE,
                                       DATA_SOURCE,
                                       BRANCH_CODE,
                                       PRODUCT_CODE,
                                       CURRENCY,
                                       OUTSTANDING_OFF_BS_CCY,
                                       ECL_OFF_BS_CCY,
                                       OUTSTANDING,
                                       RESERVED_AMOUNT_2)
        SELECT 'SUB_PRODUCT' SUBTOTAL_SOURCE,
               DATA_SOURCE,
               BRANCH_CODE,
               PRODUCT_CODE,
               CURRENCY,
               CASE
                   WHEN DATA_SOURCE IN ('KTP', 'RKN') THEN 0
                   ELSE SUM(NVL(OUTSTANDING_OFF_BS_CCY, 0))
                   END       OUTSTANDING_OFF_BS_CCY,
               SUM(NVL(ECL_OFF_BS_CCY, 0)),
               CASE
                   WHEN DATA_SOURCE = 'KTP'
                       THEN
                       SUM(NVL(PRINCIPAL_AMOUNT_CCY, 0))
                   WHEN DATA_SOURCE = 'RKN'
                       THEN
                       SUM(NVL(OUTSTANDING_PRINCIPAL_CCY, 0))
                   ELSE
                       SUM(NVL(OUTSTANDING_ON_BS_CCY, 0))
                   END       OUTSTANDING,
               SUM(NVL(RESERVED_AMOUNT_2, 0))
        FROM GTMP_NOMINATIVE_RONA
             --         WHERE DATA_SOURCE IN ('ILS', 'LIMIT')
        GROUP BY 'SUB_PRODUCT',
                 DATA_SOURCE,
                 BRANCH_CODE,
                 PRODUCT_CODE,
                 CURRENCY;

        COMMIT;

        --- INSERT SUBTOTAL PER MU
        INSERT INTO TMP_SUBTOTAL_RONA (SUBTOTAL_SOURCE,
                                       DATA_SOURCE,
                                       BRANCH_CODE,
                                       PRODUCT_CODE,
                                       CURRENCY,
                                       OUTSTANDING_OFF_BS_CCY,
                                       ECL_OFF_BS_CCY,
                                       OUTSTANDING,
                                       RESERVED_AMOUNT_2)
        SELECT 'SUB_MU',
               DATA_SOURCE,
               BRANCH_CODE,
               ' ',
               CURRENCY,
               CASE
                   WHEN DATA_SOURCE IN ('KTP', 'RKN') THEN 0
                   ELSE SUM(NVL(OUTSTANDING_OFF_BS_CCY, 0))
                   END OUTSTANDING_OFF_BS_CCY,
               SUM(NVL(ECL_OFF_BS_CCY, 0)),
               CASE
                   WHEN DATA_SOURCE = 'KTP'
                       THEN
                       SUM(NVL(PRINCIPAL_AMOUNT_CCY, 0))
                   WHEN DATA_SOURCE = 'RKN'
                       THEN
                       SUM(NVL(OUTSTANDING_PRINCIPAL_CCY, 0))
                   ELSE
                       SUM(NVL(OUTSTANDING_ON_BS_CCY, 0))
                   END OUTSTANDING,
               SUM(NVL(RESERVED_AMOUNT_2, 0))
        FROM GTMP_NOMINATIVE_RONA
             --         WHERE DATA_SOURCE IN ('ILS', 'LIMIT')
        GROUP BY 'SUB_MU',
                 DATA_SOURCE,
                 BRANCH_CODE,
                 CURRENCY;

        COMMIT;

        INSERT INTO TMP_BRANCH_LIST (BRANCH_CODE,
                                     BRANCH_NAME,
                                     KCU_CODE,
                                     KCU_NAME)
        SELECT DISTINCT BRANCH_NUM,
                        BRANCH_NAME,
                        MAIN_BRANCH_CD,
                        (SELECT DISTINCT BRANCH_NAME
                         FROM IFRS_MASTER_BRANCH KCU
                         WHERE KCU.BRANCH_NUM = KCP.MAIN_BRANCH_CD
                           AND KCU.DOWNLOAD_DATE = TGL) KCU_NAME
        FROM IFRS_MASTER_BRANCH KCP
        WHERE KCP.DOWNLOAD_DATE = TGL
          AND NOT EXISTS
            (SELECT 1
             FROM TMP_BRANCH_LIST B
             WHERE KCP.BRANCH_NUM = B.BRANCH_CODE);

        COMMIT;

        SELECT MIN(PKID), MAX(PKID) PKID
        INTO PID, MAXPID
        FROM TMP_BRANCH_LIST;

        --where branch_code in ('0001','0005');

        --BR      := (SELECT BRANCH_CODE FROM TMP_BRANCH_LIST WHERE PKID = ID);
        --         SELECT BRANCH_CODE INTO BR2 FROM TMP_BRANCH_LIST WHERE PKID = PID ;
        --and branch_code in ('0001','0005') ;

        DBMS_STATS.UNLOCK_TABLE_STATS(OWNNAME => 'IFRS',
                                      TABNAME => 'GTMP_NOMINATIVE_RONA');
        DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => 'IFRS',
                                      TABNAME => 'GTMP_NOMINATIVE_RONA',
                                      DEGREE => 2);
        DBMS_STATS.UNLOCK_TABLE_STATS(OWNNAME => 'IFRS',
                                      TABNAME => 'TMP_DATA_SOURCE');
        DBMS_STATS.GATHER_TABLE_STATS(OWNNAME => 'IFRS',
                                      TABNAME => 'TMP_DATA_SOURCE',
                                      DEGREE => 2);
        WHILE PID <= MAXPID
            LOOP
                -- 10 LOOP


                BEGIN
                    SELECT BRANCH_CODE, BRANCH_NAME
                    INTO BR, CBG
                    FROM TMP_BRANCH_LIST
                    WHERE PKID = PID;
                    --and branch_code in ('0001','0005');
                EXCEPTION
                    WHEN NO_DATA_FOUND
                        THEN
                            BR := '';
                END;


                FOR REC
                    IN ( SELECT PRODUCT_CODE,
                                CURRENCY,
                                A.DATA_SOURCE,
                                RPAD(' ', 1)
                                    || RPAD(A.DATA_SOURCE, 5)
                                    || RPAD(' ', 1)
                                    || LPAD(NOREK_LBU, 25)
                                    || RPAD(' ', 1)
                                    || RPAD(' ', 9)
                                    || --                            CASE
                                --                                WHEN A.DATA_SOURCE IN ('ILS', 'LIMIT') THEN RPAD(A.PRODUCT_CODE, 3)
                                --                                ELSE RPAD(' ', 4) END ||
                                --                            RPAD(' ', 1) ||
                                --                            CASE
                                --                                WHEN A.DATA_SOURCE NOT IN ('ILS', 'LIMIT') THEN RPAD(A.PRODUCT_CODE, 3)
                                --                                ELSE RPAD(' ', 4) END ||
                                RPAD(' ', 1)
                                    || LPAD(NVL(A.BI_COLLECTABILITY, ' '), 3)
                                    || RPAD(' ', 1)
                                    || LPAD(NVL(A.STAGE, ' '), 3)
                                    || RPAD(' ', 1)
                                    || RPAD(A.CURRENCY, 3)
                                    || RPAD(' ', 1)
                                    || CASE
                                           WHEN A.DATA_SOURCE IN ('KTP', 'RKN')
                                               THEN
                                               --                                    LPAD(TRIM(TO_CHAR(ROUND('0.00', 2), '999,999,999,999,999.99')), 22)
                                               LPAD('0.00', 22)
                                           ELSE
                                               CASE
                                                   WHEN NVL(A.OUTSTANDING_OFF_BS_CCY,
                                                            0) =
                                                        0
                                                       THEN
                                                       LPAD('0.00', 22)
                                                   ELSE
                                                       LPAD(
                                                               TRIM(
                                                                       TO_CHAR(
                                                                               ROUND(
                                                                                       A.OUTSTANDING_OFF_BS_CCY,
                                                                                       2),
                                                                               '999,999,999,999,999.99')),
                                                               22)
                                                   END
                                    END
                                    || RPAD(' ', 1)
                                    || CASE
                                           WHEN NVL(A.ECL_OFF_BS_CCY, 0) = 0
                                               THEN
                                               LPAD('0.00', 22)
                                           ELSE
                                               LPAD(
                                                       TRIM(
                                                               TO_CHAR(
                                                                       ROUND(A.ECL_OFF_BS_CCY, 2),
                                                                       '999,999,999,999,999.99')),
                                                       22)
                                    END
                                    || RPAD(' ', 1)
                                    || CASE
                                           WHEN A.DATA_SOURCE = 'KTP'
                                               THEN
                                               CASE
                                                   WHEN NVL(A.PRINCIPAL_AMOUNT_CCY,
                                                            0) =
                                                        0
                                                       THEN
                                                       LPAD('0.00', 22)
                                                   ELSE
                                                       LPAD(
                                                               TRIM(
                                                                       TO_CHAR(
                                                                               ROUND(
                                                                                       A.PRINCIPAL_AMOUNT_CCY,
                                                                                       2),
                                                                               '999,999,999,999,999.99')),
                                                               22)
                                                   END
                                           WHEN A.DATA_SOURCE = 'RKN'
                                               THEN
                                               CASE
                                                   WHEN NVL(
                                                                A.OUTSTANDING_PRINCIPAL_CCY,
                                                                0) =
                                                        0
                                                       THEN
                                                       LPAD('0.00', 22)
                                                   ELSE
                                                       LPAD(
                                                               TRIM(
                                                                       TO_CHAR(
                                                                               ROUND(
                                                                                       A.OUTSTANDING_PRINCIPAL_CCY,
                                                                                       2),
                                                                               '999,999,999,999,999.99')),
                                                               22)
                                                   END
                                           ELSE
                                               CASE
                                                   WHEN NVL(A.OUTSTANDING_ON_BS_CCY,
                                                            0) =
                                                        0
                                                       THEN
                                                       LPAD('0.00', 22)
                                                   ELSE
                                                       LPAD(
                                                               TRIM(
                                                                       TO_CHAR(
                                                                               ROUND(
                                                                                       A.OUTSTANDING_ON_BS_CCY,
                                                                                       2),
                                                                               '999,999,999,999,999.99')),
                                                               22)
                                                   END
                                    END
                                    || RPAD(' ', 1)
                                    || CASE
                                           WHEN NVL(A.RESERVED_AMOUNT_2, 0) = 0
                                               THEN
                                               LPAD('0.00', 22)
                                           ELSE
                                               LPAD(
                                                       TRIM(
                                                               TO_CHAR(
                                                                       ROUND(A.RESERVED_AMOUNT_2,
                                                                             2),
                                                                       '999,999,999,999,999.99')),
                                                       22)
                                    END HEADER
                         FROM GTMP_NOMINATIVE_RONA A
                                  JOIN TMP_DATA_SOURCE B
                                       ON A.DATA_SOURCE = B.DATA_SOURCE
                         WHERE REPORT_DATE = TGL
                           AND BRANCH_CODE = BR
                           AND ((A.DATA_SOURCE = 'CRD'
                             AND (A.ACCOUNT_STATUS = 'A'
                                 OR A.OUTSTANDING_ON_BS_CCY > 0))
                             OR (A.DATA_SOURCE = 'KTP'
                                 AND A.ACCOUNT_STATUS = 'A'
                                 AND A.PRODUCT_CODE <> 'BORROWING')
                             OR (A.DATA_SOURCE = 'BTRD'
                                 AND A.ACCOUNT_STATUS = 'A'
                                 AND NVL(A.BI_CODE, ' ') <> '0')
                             OR (A.DATA_SOURCE = 'RKN'
                                 AND A.ACCOUNT_STATUS = 'A'
                                 AND NVL(OUTSTANDING_PRINCIPAL_CCY, 0) >=
                                     0)
                             OR (A.DATA_SOURCE = 'ILS'
                                 AND A.ACCOUNT_STATUS = 'A')
                             OR (A.DATA_SOURCE = 'LIMIT'
                                 AND A.ACCOUNT_STATUS = 'A')
                             OR (A.DATA_SOURCE = 'PBMM'
                                 AND A.ACCOUNT_STATUS = 'A') --AND A.RESERVED_AMOUNT_5 <> 0)
                             )
                           AND NOT EXISTS
                             (SELECT 1
                              FROM GTMP_NOMINATIVE_RONA L
                              WHERE L.REPORT_DATE = A.REPORT_DATE
                                AND L.DATA_SOURCE = 'ILS'
                                AND L.ACCOUNT_STATUS = 'A'
                                AND A.DATA_SOURCE = 'LIMIT'
                                AND A.ACCOUNT_NUMBER =
                                    L.FACILITY_NUMBER)
                         ORDER BY B.PKID,
                                  A.DATA_SOURCE,
                                  A.PRODUCT_CODE,
                                  a.CURRENCY,
                                  A.BI_COLLECTABILITY,
                                  A.NOREK_LBU/*
                                                SELECT
                                                            LPAD(A.DATA_SOURCE,8) ||
                                                            RPAD(' ',2) ||
                                                            LPAD(NOREK_LBU,25) ||
                                                            RPAD(' ',2) ||
                                                            LPAD(A.BI_COLLECTABILITY,3) ||
                                                            RPAD(' ',4) ||
                                                            LPAD(NVL(A.STAGE,' '),3) ||
                                                            RPAD(' ',4) ||
                                                            LPAD(A.CURRENCY,4) ||
                                                            RPAD(' ',2) ||
                                                            LPAD(ROUND(A.OUTSTANDING_OFF_BS_CCY,2),16) ||
                                                            RPAD(' ',2) ||
                                                            LPAD(ROUND(A.ECL_OFF_BS_CCY,2),16) ||
                                                            RPAD(' ',2) ||
                                                            LPAD(ROUND(A.OUTSTANDING_PRINCIPAL_CCY,2),16) ||
                                                            RPAD(' ',2) ||
                                                            LPAD(ROUND(A.RESERVED_AMOUNT_2,2),14)HEADER
                                                FROM IFRS_NOMINATIVE A
                                                JOIN TMP_DATA_SOURCE B
                                                ON A.DATA_SOURCE = B.DATA_SOURCE
                                                WHERE REPORT_DATE = '31-JAN-2020' AND BRANCH_CODE = BR ORDER BY B.PKID
                                        */
                    )
                    LOOP
                        --dbms_output.PUT_LINE(LPAD(COUNTER, 5)||'|'||REC.HEADER);

                        IF (COUNTER = 1 OR BR2 <> BR)
                        THEN
                            -- SET PRODUCT, CCY  TEMP
                            IF BR2 <> BR
                            THEN
                                PRODUCT := REC.PRODUCT_CODE;
                                CCY := REC.CURRENCY;
                                SOURCE := REC.DATA_SOURCE;
                            END IF;

                            -------------------------------
                            COUNTER := 1;
                            HAL := CASE WHEN BR2 <> BR THEN 1 ELSE HAL END;
                            BR2 := BR;

                            INSERT INTO IFRS_REPORT_RONA (RONA)
                            VALUES ('1'
                                || RPAD('RETENSI', 7)
                                || ':'
                                || LPAD(RE1, 2, 0)
                                || '/'
                                || LPAD(RE2, 2, 0)
                                || '/'
                                || LPAD(RE3, 2, 0)
                                || RPAD(' ', 37)
                                || RPAD(
                                            'LAPORAN CADANGAN ASET KEUANGAN DAN KOMITMEN',
                                            43)
                                || RPAD(' ', 33)
                                || RPAD('FREKUENSI', 9)
                                || ':'
                                || LPAD('BULANAN', 10));

                            COMMIT;

                            --RPAD(' ',27)||
                            --RPAD('FREKUENSI',9) ||':'||LPAD('BULANAN',9)); COMMIT;


                            INSERT INTO IFRS_REPORT_RONA (RONA)
                            VALUES (' '
                                || RPAD('LAPORAN', 7)
                                || ':'
                                || RPAD(LAP, 8)
                                || RPAD(' ', 113)
                                || RPAD('TANGGAL', 9)
                                || ':'
                                || LPAD(
                                            TO_CHAR(TGL_CETAK,
                                                    'DD-MM-RRRR'),
                                            10));

                            COMMIT;

                            --RPAD(' ',97)||
                            --RPAD('TANGGAL',9) ||':'||LPAD(TGL,9)); COMMIT;


                            INSERT INTO IFRS_REPORT_RONA (RONA)
                            VALUES (' '
                                || RPAD('CABANG', 7)
                                || ':'
                                || RPAD(BR || ' - ' || CBG, 50)
                                || RPAD(' ', 71)
                                || RPAD('HALAMAN', 9)
                                || ':'
                                || LPAD(HAL, 10));

                            COMMIT;

                            --RPAD(' ',55)||
                            --RPAD('HALAMAN',9) ||':'||LPAD(HAL,9)); COMMIT;


                            INSERT INTO IFRS_REPORT_RONA (RONA)
                            VALUES (RPAD(' ', 150, '='));

                            COMMIT;

                            --HEADER ROW 1
                            INSERT INTO IFRS_REPORT_RONA (RONA)
                            VALUES (' '
                                || RPAD(' ', 36)
                                || RPAD('KODE', 4)
                                || RPAD(' ', 1)
                                || RPAD('KODE', 4)
                                || RPAD(' ', 18)
                                || RPAD('JUMLAH', 6)
                                || RPAD(' ', 16)
                                || RPAD('JUMLAH CADANGAN', 15)
                                || RPAD(' ', 27)
                                || RPAD('JUMLAH', 22));

                            COMMIT;

                            --HEADER ROW 2
                            INSERT INTO IFRS_REPORT_RONA (RONA)
                            VALUES (' '
                                || LPAD('NO', 3)
                                || LPAD(' ', 1)
                                || RPAD('APL', 5)
                                || RPAD(' ', 1)
                                || LPAD('NOMOR REKENING', 16)
                                || LPAD(' ', 10)
                                || RPAD('ILS', 4)
                                || LPAD(' ', 1)
                                || RPAD('GL', 4)
                                || LPAD(' ', 1)
                                || LPAD('KOL', 3)
                                || LPAD(' ', 1)
                                || LPAD('STG', 3)
                                || LPAD(' ', 1)
                                || RPAD('MU', 3)
                                || LPAD(' ', 6)
                                || RPAD('KELONGGARAN TARIK', 21)
                                || RPAD(' ', 1)
                                || RPAD('KELONGGARAN TARIK', 20)
                                || RPAD(' ', 1)
                                || RPAD('OUTSTANDING', 20)
                                || RPAD(' ', 1)
                                || RPAD('CADANGAN PINJAMAN', 22));

                            COMMIT;

                            /*
                                    --HEADER ROW 3
                                    INSERT INTO IFRS_REPORT_RONA(RONA)
                                    VALUES( ' '||
                                            RPAD(' ',3) ||

                                            RPAD(' ',2) ||
                                            RPAD(' ',8) ||
                                            RPAD(' ',1) ||
                                            RPAD(' ',24) ||
                                            RPAD(' ',1) ||
                                            RPAD(' ',5) ||
                                            RPAD(' ',1) ||
                                            RPAD(' ',5) ||
                                            RPAD(' ',1) ||
                                            RPAD(' ',4) ||
                                            RPAD(' ',1) ||
                                            RPAD(' ',22) ||
                                            RPAD(' ',1) ||
                                            RPAD(' ',22) ||
                                            RPAD(' ',1) ||
                                            RPAD(' ',22) ||
                                            RPAD(' ',1) ||
                                            RPAD(' ',22)
                                            ); COMMIT;
                            */

                            INSERT INTO IFRS_REPORT_RONA (RONA)
                            VALUES (RPAD(' ', 150, '-'));

                            COMMIT;
                        END IF;

                        --PRINT SUBTOTAL PER KODE
                        IF REC.PRODUCT_CODE <> PRODUCT OR REC.CURRENCY <> CCY
                        THEN
                            INSERT INTO IFRS_REPORT_RONA (RONA)
                            SELECT RPAD(' ', 14)
                                       || LPAD(
                                               'SUB TOTAL PER KODE '
                                               || CASE
                                                      WHEN SOURCE IN ('ILS', 'LIMIT')
                                                          THEN
                                                          'ILS'
                                                      ELSE
                                                          'GL'
                                                   END,
                                               22)
                                       || RPAD(' ', 1)
                                       || CASE
                                              WHEN SOURCE IN ('ILS', 'LIMIT')
                                                  THEN
                                                  LPAD(PRODUCT, 3)
                                              ELSE
                                                  LPAD(' ', 3)
                                       END
                                       || RPAD(' ', 2)
                                       || CASE
                                              WHEN SOURCE NOT IN ('ILS', 'LIMIT')
                                                  THEN
                                                  LPAD(PRODUCT, 3)
                                              ELSE
                                                  LPAD(' ', 3)
                                       END
                                       || LPAD(' ', 10)
                                       || LPAD(CCY, 3)
                                       || RPAD(' ', 1)
                                       || CASE
                                              WHEN NVL(OUTSTANDING_OFF_BS_CCY, 0) =
                                                   0
                                                  THEN
                                                  LPAD('0.00', 22)
                                              ELSE
                                                  LPAD(
                                                          TRIM(
                                                                  TO_CHAR(
                                                                          ROUND(
                                                                                  OUTSTANDING_OFF_BS_CCY,
                                                                                  2),
                                                                          '999,999,999,999,999.99')),
                                                          22)
                                       END
                                       || RPAD(' ', 1)
                                       || CASE
                                              WHEN NVL(ECL_OFF_BS_CCY, 0) = 0
                                                  THEN
                                                  LPAD('0.00', 22)
                                              ELSE
                                                  LPAD(
                                                          TRIM(
                                                                  TO_CHAR(
                                                                          ROUND(ECL_OFF_BS_CCY,
                                                                                2),
                                                                          '999,999,999,999,999.99')),
                                                          22)
                                       END
                                       || RPAD(' ', 1)
                                       || CASE
                                              WHEN NVL(OUTSTANDING, 0) = 0
                                                  THEN
                                                  LPAD('0.00', 22)
                                              ELSE
                                                  LPAD(
                                                          TRIM(
                                                                  TO_CHAR(
                                                                          ROUND(OUTSTANDING, 2),
                                                                          '999,999,999,999,999.99')),
                                                          22)
                                       END
                                       || RPAD(' ', 1)
                                       || CASE
                                              WHEN NVL(RESERVED_AMOUNT_2, 0) = 0
                                                  THEN
                                                  LPAD('0.00', 22)
                                              ELSE
                                                  LPAD(
                                                          TRIM(
                                                                  TO_CHAR(
                                                                          ROUND(
                                                                                  RESERVED_AMOUNT_2,
                                                                                  2),
                                                                          '999,999,999,999,999.99')),
                                                          22)
                                       END HEADER
                            FROM TMP_SUBTOTAL_RONA T
                            WHERE DATA_SOURCE = SOURCE
                              AND PRODUCT_CODE = PRODUCT
                              AND CURRENCY = CCY
                              AND BRANCH_CODE = BR
                              AND SUBTOTAL_SOURCE = 'SUB_PRODUCT';

                            COMMIT;

                            INSERT INTO IFRS_REPORT_RONA (RONA)
                            VALUES (RPAD(' ', 150));

                            COMMIT;

                            PRODUCT := REC.PRODUCT_CODE;
                            CCY := REC.CURRENCY;
                        END IF;

                        --PRINT SUBTOTAL PER CCY
                        IF REC.DATA_SOURCE <> SOURCE
                        THEN
                            --LOOPING SUBTOTAL PER CCY
                            FOR SUBS
                                IN ( SELECT RPAD(' ', 20)
                                                || LPAD('SUB TOTAL PER MU', 16)
                                                || RPAD(' ', 19)
                                                || LPAD(CURRENCY, 3)
                                                || RPAD(' ', 1)
                                                || CASE
                                                       WHEN NVL(OUTSTANDING_OFF_BS_CCY,
                                                                0) =
                                                            0
                                                           THEN
                                                           LPAD('0.00', 22)
                                                       ELSE
                                                           LPAD(
                                                                   TRIM(
                                                                           TO_CHAR(
                                                                                   ROUND(
                                                                                           OUTSTANDING_OFF_BS_CCY,
                                                                                           2),
                                                                                   '999,999,999,999,999.99')),
                                                                   22)
                                                END
                                                || RPAD(' ', 1)
                                                || CASE
                                                       WHEN NVL(ECL_OFF_BS_CCY, 0) = 0
                                                           THEN
                                                           LPAD('0.00', 22)
                                                       ELSE
                                                           LPAD(
                                                                   TRIM(
                                                                           TO_CHAR(
                                                                                   ROUND(
                                                                                           ECL_OFF_BS_CCY,
                                                                                           2),
                                                                                   '999,999,999,999,999.99')),
                                                                   22)
                                                END
                                                || RPAD(' ', 1)
                                                || CASE
                                                       WHEN NVL(OUTSTANDING, 0) = 0
                                                           THEN
                                                           LPAD('0.00', 22)
                                                       ELSE
                                                           LPAD(
                                                                   TRIM(
                                                                           TO_CHAR(
                                                                                   ROUND(OUTSTANDING,
                                                                                         2),
                                                                                   '999,999,999,999,999.99')),
                                                                   22)
                                                END
                                                || RPAD(' ', 1)
                                                || CASE
                                                       WHEN NVL(RESERVED_AMOUNT_2, 0) = 0
                                                           THEN
                                                           LPAD('0.00', 22)
                                                       ELSE
                                                           LPAD(
                                                                   TRIM(
                                                                           TO_CHAR(
                                                                                   ROUND(
                                                                                           RESERVED_AMOUNT_2,
                                                                                           2),
                                                                                   '999,999,999,999,999.99')),
                                                                   22)
                                                END HEADER
                                     FROM TMP_SUBTOTAL_RONA
                                     WHERE DATA_SOURCE = SOURCE
                                       AND BRANCH_CODE = BR
                                       AND SUBTOTAL_SOURCE = 'SUB_MU'
                                     ORDER BY CURRENCY)
                                LOOP
                                    INSERT INTO IFRS_REPORT_RONA (RONA)
                                    VALUES (SUBS.HEADER);

                                    COMMIT;
                                END LOOP;

                            INSERT INTO IFRS_REPORT_RONA (RONA)
                            VALUES (RPAD(' ', 150));

                            COMMIT;

                            PRODUCT := REC.PRODUCT_CODE;
                            CCY := REC.CURRENCY;
                            SOURCE := REC.DATA_SOURCE;
                        END IF;

                        --CONTINUE WHEN COUNTER = 0;
                        INSERT INTO IFRS_REPORT_RONA (RONA)
                        VALUES (' ' || LPAD(COUNTER, 3) || REC.HEADER);

                        COMMIT;

                        COUNTER := COUNTER + 1;
                        COUNTER := CASE WHEN COUNTER = 51 THEN 1 ELSE COUNTER END;
                        HAL := CASE WHEN COUNTER = 1 THEN HAL + 1 ELSE HAL END;
                    END LOOP;

                --PRINT SUBTOTAL PER KODE
                INSERT INTO IFRS_REPORT_RONA (RONA)
                SELECT RPAD(' ', 14)
                           || LPAD(
                                   'SUB TOTAL PER KODE '
                                   || CASE
                                          WHEN SOURCE IN ('ILS', 'LIMIT')
                                              THEN
                                              'ILS'
                                          ELSE
                                              'GL'
                                       END,
                                   22)
                           || RPAD(' ', 1)
                           || CASE
                                  WHEN SOURCE IN ('ILS', 'LIMIT')
                                      THEN
                                      LPAD(PRODUCT, 3)
                                  ELSE
                                      LPAD(' ', 3)
                           END
                           || RPAD(' ', 2)
                           || CASE
                                  WHEN SOURCE NOT IN ('ILS', 'LIMIT')
                                      THEN
                                      LPAD(PRODUCT, 3)
                                  ELSE
                                      LPAD(' ', 3)
                           END
                           || LPAD(' ', 10)
                           || LPAD(CCY, 3)
                           || RPAD(' ', 1)
                           || CASE
                                  WHEN NVL(OUTSTANDING_OFF_BS_CCY, 0) = 0
                                      THEN
                                      LPAD('0.00', 22)
                                  ELSE
                                      LPAD(
                                              TRIM(
                                                      TO_CHAR(
                                                              ROUND(OUTSTANDING_OFF_BS_CCY,
                                                                    2),
                                                              '999,999,999,999,999.99')),
                                              22)
                           END
                           || RPAD(' ', 1)
                           || CASE
                                  WHEN NVL(ECL_OFF_BS_CCY, 0) = 0
                                      THEN
                                      LPAD('0.00', 22)
                                  ELSE
                                      LPAD(
                                              TRIM(
                                                      TO_CHAR(ROUND(ECL_OFF_BS_CCY, 2),
                                                              '999,999,999,999,999.99')),
                                              22)
                           END
                           || RPAD(' ', 1)
                           || CASE
                                  WHEN NVL(OUTSTANDING, 0) = 0
                                      THEN
                                      LPAD('0.00', 22)
                                  ELSE
                                      LPAD(
                                              TRIM(
                                                      TO_CHAR(ROUND(OUTSTANDING, 2),
                                                              '999,999,999,999,999.99')),
                                              22)
                           END
                           || RPAD(' ', 1)
                           || CASE
                                  WHEN NVL(RESERVED_AMOUNT_2, 0) = 0
                                      THEN
                                      LPAD('0.00', 22)
                                  ELSE
                                      LPAD(
                                              TRIM(
                                                      TO_CHAR(
                                                              ROUND(RESERVED_AMOUNT_2, 2),
                                                              '999,999,999,999,999.99')),
                                              22)
                           END HEADER
                FROM TMP_SUBTOTAL_RONA T
                WHERE DATA_SOURCE = SOURCE
                  AND PRODUCT_CODE = PRODUCT
                  AND CURRENCY = CCY
                  AND BRANCH_CODE = BR
                  AND SUBTOTAL_SOURCE = 'SUB_PRODUCT';

                COMMIT;

                --
                --                 INSERT INTO IFRS_REPORT_RONA(RONA)
                --                 VALUES (RPAD(' ', 150));
                --                 COMMIT;

                --PRINT SUBTOTAL PER CCY
                --LOOPING SUBTOTAL PER CCY
                FOR SUBS
                    IN ( SELECT RPAD(' ', 20)
                                    || LPAD('SUB TOTAL PER MU', 16)
                                    || RPAD(' ', 19)
                                    || LPAD(CURRENCY, 3)
                                    || RPAD(' ', 1)
                                    || CASE
                                           WHEN NVL(OUTSTANDING_OFF_BS_CCY, 0) = 0
                                               THEN
                                               LPAD('0.00', 22)
                                           ELSE
                                               LPAD(
                                                       TRIM(
                                                               TO_CHAR(
                                                                       ROUND(
                                                                               OUTSTANDING_OFF_BS_CCY,
                                                                               2),
                                                                       '999,999,999,999,999.99')),
                                                       22)
                                    END
                                    || RPAD(' ', 1)
                                    || CASE
                                           WHEN NVL(ECL_OFF_BS_CCY, 0) = 0
                                               THEN
                                               LPAD('0.00', 22)
                                           ELSE
                                               LPAD(
                                                       TRIM(
                                                               TO_CHAR(
                                                                       ROUND(ECL_OFF_BS_CCY, 2),
                                                                       '999,999,999,999,999.99')),
                                                       22)
                                    END
                                    || RPAD(' ', 1)
                                    || CASE
                                           WHEN NVL(OUTSTANDING, 0) = 0
                                               THEN
                                               LPAD('0.00', 22)
                                           ELSE
                                               LPAD(
                                                       TRIM(
                                                               TO_CHAR(
                                                                       ROUND(OUTSTANDING, 2),
                                                                       '999,999,999,999,999.99')),
                                                       22)
                                    END
                                    || RPAD(' ', 1)
                                    || CASE
                                           WHEN NVL(RESERVED_AMOUNT_2, 0) = 0
                                               THEN
                                               LPAD('0.00', 22)
                                           ELSE
                                               LPAD(
                                                       TRIM(
                                                               TO_CHAR(
                                                                       ROUND(RESERVED_AMOUNT_2,
                                                                             2),
                                                                       '999,999,999,999,999.99')),
                                                       22)
                                    END HEADER
                         FROM TMP_SUBTOTAL_RONA
                         WHERE DATA_SOURCE = SOURCE
                           AND BRANCH_CODE = BR
                           AND SUBTOTAL_SOURCE = 'SUB_MU'
                         ORDER BY CURRENCY)
                    LOOP
                        INSERT INTO IFRS_REPORT_RONA (RONA)
                        VALUES (SUBS.HEADER);

                        COMMIT;
                    END LOOP;

                --
                --                 INSERT INTO IFRS_REPORT_RONA(RONA)
                --                 VALUES (RPAD(' ', 150));
                --                 COMMIT;

                PID := PID + 1;
            END LOOP;
    END;
END;