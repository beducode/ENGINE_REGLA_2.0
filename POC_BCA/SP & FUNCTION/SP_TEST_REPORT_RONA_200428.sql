CREATE OR REPLACE PROCEDURE SP_TEST_REPORT_RONA_200428
(
  V_CURR DATE DEFAULT '1-JAN-1900'
)
authid current_user
AS
BEGIN

    DECLARE COUNTER NUMBER;
            RE1 VARCHAR2(5);
            RE2 VARCHAR2(5);
            RE3 VARCHAR2(5);
            LAP VARCHAR2(10);
            TGL DATE;
            CBG VARCHAR2(50);
            HAL NUMBER;
            MAXPID NUMBER;
            BR VARCHAR2(5);
            BR2 VARCHAR2(5);
            PID NUMBER;
    BEGIN

    IF V_CURR = '1-JAN-1900' THEN
        SELECT CURRDATE INTO TGL FROM IFRS_PRC_DATE;
    ELSE
        TGL := V_CURR;
    END IF;



    COUNTER :=  1;
    RE1     := '1B';
    RE2     := '6B';
    RE3     := '2T';
    LAP     := 'R-77061';
    --TGL     := '31-MAR-2020';
    CBG     := 'KCU - PANGKAL PINANG';
    HAL     := 1;
    PID     := 1;



    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_REPORT_RONA';
    BEGIN
    EXECUTE IMMEDIATE 'DROP SEQUENCE SEQ_IFRS_REPORT_RONA';
    EXECUTE IMMEDIATE 'CREATE SEQUENCE SEQ_IFRS_REPORT_RONA START WITH 1';
    END;


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

INSERT INTO TMP_BRANCH_LIST
  (BRANCH_CODE,
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
     AND NOT EXISTS (SELECT 1
            FROM TMP_BRANCH_LIST B
           WHERE KCP.BRANCH_NUM = B.BRANCH_CODE);
COMMIT;





    SELECT MIN(PKID), MAX(PKID)PKID INTO PID, MAXPID FROM TMP_BRANCH_LIST ;
    --where branch_code in ('0001','0005');

    --BR      := (SELECT BRANCH_CODE FROM TMP_BRANCH_LIST WHERE PKID = ID);
        SELECT BRANCH_CODE INTO BR2 FROM TMP_BRANCH_LIST WHERE PKID = PID ;
        --and branch_code in ('0001','0005') ;

        WHILE PID <= MAXPID LOOP  -- 10 LOOP


        begin
        SELECT BRANCH_CODE  , BRANCH_NAME INTO BR , CBG FROM TMP_BRANCH_LIST WHERE PKID = PID;
        --and branch_code in ('0001','0005');
        exception when no_data_found then
        BR:= '';
        end;


        FOR REC IN (

 SELECT LPAD(A.DATA_SOURCE, 8) ||
        RPAD(' ', 2) ||
        LPAD(NOREK_LBU, 25) ||
        RPAD(' ', 2) ||
        LPAD(A.BI_COLLECTABILITY,3) ||
        RPAD(' ', 4) ||
        LPAD(NVL(A.STAGE, ' '), 3) ||
        RPAD(' ', 4) ||
        LPAD(A.CURRENCY, 4) ||
        RPAD(' ', 2) ||

        CASE
          WHEN A.DATA_SOURCE IN ('KTP', 'RKN') THEN
           LPAD(ROUND(0, 2), 16)
          ELSE
           LPAD(ROUND(A.OUTSTANDING_OFF_BS_CCY, 2), 16)
        END ||

        RPAD(' ', 2) ||
        LPAD(ROUND(A.ECL_OFF_BS_CCY, 2), 16) ||
        RPAD(' ', 2) ||

        CASE
          WHEN A.DATA_SOURCE = 'KTP' THEN
           LPAD(ROUND(A.PRINCIPAL_AMOUNT_CCY, 2), 16)

          WHEN A.DATA_SOURCE = 'RKN' THEN
           LPAD(ROUND(A.OUTSTANDING_PRINCIPAL_CCY, 2), 16)

          ELSE
           LPAD(ROUND(A.OUTSTANDING_ON_BS_CCY, 2), 16)
        END ||

        RPAD(' ', 2) ||
        LPAD(ROUND(A.RESERVED_AMOUNT_2, 2), 14) HEADER
   FROM IFRS_NOMINATIVE A
   JOIN TMP_DATA_SOURCE B
     ON A.DATA_SOURCE = B.DATA_SOURCE
  WHERE REPORT_DATE = TGL
    AND BRANCH_CODE = BR
    AND ((A.DATA_SOURCE = 'CRD' AND (A.ACCOUNT_STATUS = 'A' OR A.OUTSTANDING_ON_BS_CCY > 0)) OR
         (A.DATA_SOURCE = 'KTP' AND A.ACCOUNT_STATUS = 'A' AND A.PRODUCT_CODE <> 'BORROWING' ) OR
         (A.DATA_SOURCE = 'BTRD' AND A.ACCOUNT_STATUS = 'A' AND NVL(A.BI_CODE, ' ') <> '0' ) OR
         (A.DATA_SOURCE = 'RKN' AND A.ACCOUNT_STATUS = 'A' AND NVL(OUTSTANDING_PRINCIPAL_CCY,0) >=0 ) OR
         (A.DATA_SOURCE = 'ILS' AND A.ACCOUNT_STATUS = 'A' ) OR
         (A.DATA_SOURCE = 'LIMIT' AND A.ACCOUNT_STATUS = 'A') --AND A.RESERVED_AMOUNT_5 <> 0)
        )

  ORDER BY B.PKID

/*
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

        IF (COUNTER = 1 OR BR2 <> BR) THEN
        COUNTER := 1;
        HAL     := CASE WHEN BR2 <> BR THEN 1 ELSE HAL END;
        BR2     := BR;
        INSERT INTO IFRS_REPORT_RONA(RONA)
        VALUES( '1' || RPAD('RETENSI',7) || ':' || LPAD(RE1,2,0) || '/' || LPAD(RE2,2,0) || '/' || LPAD(RE3,2,0) ||
                RPAD(' ',27)||
                RPAD('LAPORAN CADANGAN ASET KEUANGAN DAN KOMITMEN',43)||
                RPAD(' ',26)||
                RPAD('FREKUENSI',9) ||':'||LPAD('BULANAN',10)); COMMIT;
                --RPAD(' ',27)||
                --RPAD('FREKUENSI',9) ||':'||LPAD('BULANAN',9)); COMMIT;



        INSERT INTO IFRS_REPORT_RONA(RONA)
        VALUES( ' ' || RPAD('LAPORAN',7) || ':' || RPAD(LAP,8)||
                RPAD(' ',96)||
                RPAD('TANGGAL',9) ||':'||LPAD(TO_CHAR(TGL,'DD-MM-RRRR'),10)); COMMIT;
                --RPAD(' ',97)||
                --RPAD('TANGGAL',9) ||':'||LPAD(TGL,9)); COMMIT;





        INSERT INTO IFRS_REPORT_RONA(RONA)
        VALUES( ' ' || RPAD('CABANG',7) || ':' || RPAD(BR||' - '||CBG,50) ||
                RPAD(' ',54)||
                RPAD('HALAMAN',9) ||':'||LPAD(HAL,10)); COMMIT;
                --RPAD(' ',55)||
                --RPAD('HALAMAN',9) ||':'||LPAD(HAL,9)); COMMIT;





        INSERT INTO IFRS_REPORT_RONA(RONA)
        VALUES(RPAD(' ',133,'=')); COMMIT;

        --HEADER ROW 1
        INSERT INTO IFRS_REPORT_RONA(RONA)
        VALUES( ' '||
                RPAD(' ',5) ||
                RPAD(' ',2) ||
                RPAD(' ',8) ||
                RPAD(' ',2) ||
                RPAD(' ',25) ||
                RPAD(' ',2) ||
                RPAD(' ',5) ||
                RPAD(' ',2) ||
                RPAD(' ',5) ||
                RPAD(' ',2) ||
                RPAD('MATA',4) ||
                RPAD(' ',2) ||
                RPAD('JUMLAH',16) ||
                RPAD(' ',2) ||
                RPAD('JUMLAH CADANGAN',16) ||
                RPAD(' ',2) ||
                RPAD(' ',16) ||
                RPAD(' ',2) ||
                RPAD('JUMLAH',14)
                ); COMMIT;

        --HEADER ROW 2
        INSERT INTO IFRS_REPORT_RONA(RONA)
        VALUES( ' '||
                LPAD('NO',5) ||
                LPAD(' ',2) ||
                LPAD('APLIKASI',8) ||
                LPAD(' ',2) ||
                LPAD('NOMOR REKENING',25) ||
                LPAD(' ',2) ||
                LPAD('KOLEK',5) ||
                LPAD(' ',2) ||
                LPAD('STAGE',5) ||
                LPAD(' ',2) ||
                LPAD('UANG',4) ||
                LPAD(' ',2) ||
                RPAD('KELONGGARAN',16) ||
                LPAD(' ',2) ||
                RPAD('KELONGGARAN',16) ||
                LPAD(' ',2) ||
                RPAD('OUTSTANDING',16) ||
                LPAD(' ',2) ||
                RPAD('CADANGAN',14)
                ); COMMIT;

        --HEADER ROW 3
        INSERT INTO IFRS_REPORT_RONA(RONA)
        VALUES( ' '||
                RPAD(' ',5) ||
                RPAD(' ',2) ||
                RPAD(' ',8) ||
                RPAD(' ',2) ||
                RPAD(' ',25) ||
                RPAD(' ',2) ||
                RPAD(' ',5) ||
                RPAD(' ',2) ||
                RPAD(' ',5) ||
                RPAD(' ',2) ||
                RPAD(' ',4) ||
                RPAD(' ',2) ||
                RPAD('TARIK',16) ||
                RPAD(' ',2) ||
                RPAD('TARIK',16) ||
                RPAD(' ',2) ||
                RPAD(' ',16) ||
                RPAD(' ',2) ||
                RPAD('PINJAMAN',14)
                ); COMMIT;

        INSERT INTO IFRS_REPORT_RONA(RONA)
        VALUES(RPAD(' ',133,'-')); COMMIT;
        END IF ;



    --CONTINUE WHEN COUNTER = 0;
    INSERT INTO IFRS_REPORT_RONA(RONA)
    VALUES (' '||LPAD(COUNTER, 5)||LPAD(' ',2)||REC.HEADER);COMMIT;

    COUNTER := COUNTER+1;
    COUNTER := CASE WHEN COUNTER = 50 THEN 1 ELSE COUNTER END;
    HAL     := CASE WHEN COUNTER = 1 THEN HAL+1 ELSE HAL END;

    END LOOP;

    PID     := PID+1;

    END LOOP;
    END;
    END;