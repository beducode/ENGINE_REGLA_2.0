CREATE OR REPLACE PROCEDURE SP_IFRS_PAYM_CORE_PROCESS_NOP
AS
  V_CURRDATE DATE ;
  V_PREVDATE DATE ;
  V_PARAM_CALC_TO_LASTPAYMENT NUMBER(10) ;
  V_PARAM_CUT_INT_1ST_PAYMENT NUMBER(10);

BEGIN

    -- bypass SP_IFRS_PAYM_CORE_PROCESS use this sp

    SELECT  MAX(CURRDATE), MAX(PREVDATE)
    INTO V_CURRDATE, V_PREVDATE
    FROM    IFRS_PRC_DATE_AMORT;




    BEGIN
      SELECT  CASE WHEN COMMONUSAGE = 'Y' THEN 1 ELSE 0 END
    INTO V_PARAM_CALC_TO_LASTPAYMENT
    FROM    TBLM_COMMONCODEHEADER
    WHERE   COMMONCODE = 'SCM005';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        V_PARAM_CALC_TO_LASTPAYMENT := 0;
    END;


    BEGIN
      SELECT  CASE WHEN COMMONUSAGE = 'Y' THEN 1 ELSE 0 END
      INTO V_PARAM_CUT_INT_1ST_PAYMENT
      FROM    TBLM_COMMONCODEHEADER
      WHERE   COMMONCODE = 'SCM006';
    EXCEPTION
      WHEN NO_DATA_FOUND THEN
        V_PARAM_CUT_INT_1ST_PAYMENT := 0;
    END;

   -- prorate int amount from paym schd core so it match with goalseek which using i_days
   -- param_cut_int_1st_payment := 0 ==> disable interest  : 1==> enable interest

    V_PARAM_CUT_INT_1ST_PAYMENT := V_PARAM_CUT_INT_1ST_PAYMENT;

   -- calc ecf back to last payment date
   -- param_calc_to_lastpayment := 0 ==> disable calc to last payment date  : 1==> enable calc to last payment date

    V_PARAM_CALC_TO_LASTPAYMENT := V_PARAM_CALC_TO_LASTPAYMENT;

   --CALC_FROM_LASTPAYMDATE
   --PARAM_CUT_INT

    INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_IFRS_PAYM_CORE_PROCESS' ,'' );


/* Remarks dulu 20160524
    -- fill prev pmt date
    TRUNCATE TABLE psak_gs_date1

    --ARIES case when amount pmt_os karena funding tidak mempunyai principal
    INSERT INTO psak_gs_date1 (masterid, pmt_date, pmt_os)
    SELECT masterid,
           MIN (pmt_date),
           CASE WHEN SUM (COALESCE (prn_amt, 0)) > 0  THEN SUM (COALESCE (prn_amt, 0))
                ELSE MAX (os_prn_prev) END
    FROM IFRS_PAYM_CORE
    GROUP BY masterid;


    INSERT  INTO IFRS_GS_DATE1
    ( MASTERID ,
      PMT_DATE ,
      PMT_OS
    )
    SELECT  A.MASTERID ,
            MIN(A.PMT_DATE) ,
            CASE WHEN B.FLAG_AL = 'A' THEN CASE WHEN SUM(COALESCE(PRN_AMT, 0)) > 0 THEN SUM(COALESCE(PRN_AMT, 0)) END
                 WHEN B.FLAG_AL = 'L' AND COMPOUND_TYPE IN ( '0', '2' ) THEN MAX(OS_PRN_PREV)
                 WHEN B.FLAG_AL = 'L' AND COMPOUND_TYPE IN ( '1', '3' ) THEN MIN(OS_PRN_PREV)
                 END AS AMOUNT
    FROM IFRS_PAYM_CORE A
    JOIN IFRS_IMA_AMORT_CURR B ON A.MASTERID = B.MASTERID
    GROUP BY A.MASTERID ,
             B.MASTERID ,
             B.FLAG_AL ,
             B.COMPOUND_TYPE;



    -- update prn os on min date
    MERGE INTO  IFRS_PAYM_CORE A
    USING  IFRS_GS_DATE1 B
    ON   (B.MASTERID = A.MASTERID
    AND B.PMT_DATE = A.PMT_DATE)
    WHEN MATCHED THEN
    UPDATE SET
    A.OS_PRN = B.PMT_OS ,
    A.OS_PRN_PREV = B.PMT_OS ,
    A.PREV_PMT_DATE = A.PMT_DATE;


    WHILE 1 = 1
    LOOP
        SELECT  COUNT(*) INTO V_VCNT
        FROM    IFRS_PAYM_CORE
        WHERE   PREV_PMT_DATE IS NULL;

        IF V_VCNT <= 0
          THEN BREAK;
        END IF;

        EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_GS_DATE2'  ;

        INSERT  INTO IFRS_GS_DATE2
        ( MASTERID ,
          PMT_DATE
        )
        SELECT  MASTERID ,
                MIN(PMT_DATE)
        FROM    IFRS_PAYM_CORE
        WHERE   PREV_PMT_DATE IS NULL
        GROUP BY MASTERID;


        EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_GS_DATE21' ;

        INSERT  INTO IFRS_GS_DATE21
        ( MASTERID ,
          PMT_DATE ,
          PREV_DATE ,
          PREV_OS
        )
        SELECT  A.MASTERID ,
                A.PMT_DATE ,
                B.PMT_DATE AS PREV_DATE ,
                B.PMT_OS AS PREV_OS
        FROM IFRS_GS_DATE2 A
        JOIN IFRS_GS_DATE1 B ON B.MASTERID = A.MASTERID;


        MERGE INTO IFRS_PAYM_CORE A
        USING IFRS_GS_DATE21 B
        ON( A.MASTERID = B.MASTERID
            AND A.PMT_DATE = B.PMT_DATE
          )
        WHEN MATCHED THEN
        UPDATE
        SET PREV_PMT_DATE = B.PREV_DATE ,
            OS_PRN_PREV = B.PREV_OS ,
            OS_PRN = B.PREV_OS - PRN_AMT;



        EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_GS_DATE1'  ;

        INSERT  INTO IFRS_GS_DATE1
        ( MASTERID ,
          PMT_DATE ,
          PMT_OS
        )
        SELECT  MASTERID ,
                MAX(PMT_DATE) ,
                MIN(COALESCE(OS_PRN, 0))
        FROM    IFRS_PAYM_CORE
        WHERE   PREV_PMT_DATE IS NOT NULL
        GROUP BY MASTERID;

    END LOOP; --while


   --emi update i_days

    UPDATE  IFRS_PAYM_CORE
    SET     I_DAYS = FN_CNT_DAYS_30_360(PREV_PMT_DATE, PMT_DATE)
    WHERE   ICC = '6';



    IF V_PARAM_CUT_INT_1ST_PAYMENT != 0
        THEN
            UPDATE (SELECT A.INT_AMT AS INT_AMT,B.NEW_INT_AMT AS NEW_INT_AMT
                    FROM IFRS_PAYM_CORE A
                    JOIN ( SELECT A.MASTERID ,
                                  A.PREV_PMT_DATE ,
                                  A.PMT_DATE ,
                                  A.I_DAYS ,
                                  1.00 / CASE WHEN ICC IN ( '1', '6' ) THEN 36000.00-- ironport calc new int as intererst calc code 5 nov 2015
                                              WHEN ICC IN ( '2', '3' ) THEN 36500.00
                                              ELSE 36000.00
                                              END * D.INT_RATE * A.I_DAYS * A.OS_PRN_PREV AS NEW_INT_AMT
                           FROM IFRS_PAYM_CORE A
                           JOIN ( SELECT MASTERID ,
                                         MIN(PMT_DATE) MIN_DATE
                                  FROM     IFRS_PAYM_CORE
                                  WHERE    PMT_DATE <> PREV_PMT_DATE
                                  GROUP BY MASTERID
                                ) B ON B.MASTERID = A.MASTERID
                                    AND B.MIN_DATE = A.PMT_DATE
                           JOIN ( SELECT MASTERID ,
                                         MIN(ROUND(INT_AMT/ OS_PRN_PREV
                                         * CASE WHEN ICC IN ('1', '6' ) THEN 36000.00 -- ironport calc new int as intererst calc code 5 nov 2015
                                                WHEN ICC IN ('2', '3' ) THEN 36500.00
                                                ELSE 36000.00
                                                END / I_DAYS, 2)) AS INT_RATE
                                  FROM     IFRS_PAYM_CORE
                                  WHERE    PMT_DATE <> PREV_PMT_DATE
                                  AND INT_AMT <> 0
                                  GROUP BY MASTERID
                                 )  D ON D.MASTERID = A.MASTERID
                           ON A.PMT_DATE <> A.PREV_PMT_DATE
                          ) B
                    ON   B.MASTERID = A.MASTERID
                    AND A.PREV_PMT_DATE = B.PREV_PMT_DATE
                    AND A.PMT_DATE = B.PMT_DATE
                  )
            SET INT_AMT = NEW_INT_AMT;

        END IF;


   --calc back to last payment date

    IF V_PARAM_CALC_TO_LASTPAYMENT != 0
    THEN

      --calc by interest --reverse engineering
          merge into IFRS_PAYM_CORE a
          using (
              select a.MASTERID,a.PREV_PMT_DATE,a.PMT_DATE
              ,(pmt_date-round(a.int_amt/(1.00/36500.00*d.INT_RATE*a.OS_PRN_PREV),0)) last_date
              from IFRS_PAYM_CORE a
              join (
                  select masterid,min(pmt_date) min_date from IFRS_PAYM_CORE where PMT_DATE<>prev_pmt_date group by masterid
              ) b ON b.masterid=a.masterid and b.min_date=a.pmt_date
              join (
              select masterid,min(round(int_amt/os_prn_prev*36500/i_days,2)) as int_rate from IFRS_PAYM_CORE
                  where PMT_DATE<>prev_pmt_date and INT_AMT<>0
                  group by masterid
              ) d on d.masterid=a.masterid
              where a.PMT_DATE<>a.prev_pmt_date
          ) b on (b.masterid=a.masterid and a.pmt_date=b.pmt_date)
          when matched then
          update set a.prev_pmt_date = b.last_date;
          COMMIT;


        UPDATE (SELECT A.PREV_PMT_DATE AS PREV_PMT_DATE
                      ,B.LAST_PAYMENT_DATE_ASSIGN AS LAST_PAYMENT_DATE_ASSIGN
                FROM IFRS_PAYM_CORE
                JOIN ( SELECT A.MASTERID ,
                              A.PREV_PMT_DATE ,
                              A.PMT_DATE , -- (pmt_date-round(a.int_amt/(1.00/36500.00*d.INT_RATE*a.OS_PRN_PREV),0)) last_date
                              E.LAST_PAYMENT_DATE_ASSIGN
                       FROM IFRS_PAYM_CORE A
                       JOIN ( SELECT MASTERID ,MIN(PMT_DATE) MIN_DATE
                              FROM IFRS_PAYM_CORE
                              WHERE PMT_DATE <> PREV_PMT_DATE
                              GROUP BY MASTERID
                            ) B ON B.MASTERID = A.MASTERID AND B.MIN_DATE = A.PMT_DATE
                       JOIN ( SELECT MASTERID
                                    ,MIN(ROUND(INT_AMT / OS_PRN_PREV * CASE WHEN ICC IN ('1', '6' ) THEN 36000.00-- ironport calc new int as intererst calc code 5 nov 2015
                                                                            WHEN ICC IN ('2', '3' ) THEN 36500.00
                                                                            ELSE 36000.00
                                                                            END / I_DAYS, 2)) AS INT_RATE
                              FROM     IFRS_PAYM_CORE
                              WHERE    PMT_DATE <> PREV_PMT_DATE
                              AND INT_AMT <> 0
                              GROUP BY MASTERID
                            ) D ON D.MASTERID = A.MASTERID
                       JOIN ( SELECT   MASTER_ACCOUNT_ID ,LAST_PAYMENT_DATE_ASSIGN FROM TT_PSAK_LAST_PAYM_DATE ) E ON A.MASTERID = E.MASTER_ACCOUNT_ID
                              WHERE     A.PMT_DATE <> A.PREV_PMT_DATE
                            ) B
                       ON b.masterid = A.masterid
                      AND A.pmt_date = b.pmt_date
               )
        SET     PREV_PMT_DATE = LAST_PAYMENT_DATE_ASSIGN ;


      --only process last_payment_date_assign <> min pmt_date 20160428

        EXECUTE IMMEDIATE 'TRUNCATE TABLE PSAK_TMP_T1'  ;



        INSERT  INTO PSAK_TMP_T1 ( MASTERID)
        SELECT DISTINCT A.MASTERID
        FROM    IFRS_PAYM_CORE A
        JOIN TT_PSAK_LAST_PAYM_DATE B ON A.MASTERID = B.MASTER_ACCOUNT_ID
        WHERE   A.PREV_PMT_DATE = A.PMT_DATE
        AND B.LAST_PAYMENT_DATE_ASSIGN <> A.PMT_DATE;

        --20160428

        UPDATE (SELECT A.PREV_PMT_DATE AS PREV_PMT_DATE,
                       B.MIN_PREV_DATE AS MIN_PREV_DATE,
                       A.PMT_DATE AS PMT_DATE,
                       B.MIN_PREV_DATE AS MIN_PREV_DATE
                FROM IFRS_PAYM_CORE A
                JOIN ( SELECT MASTERID ,
                       MIN(PREV_PMT_DATE) MIN_PREV_DATE
                       FROM IFRS_PAYM_CORE
                       WHERE PMT_DATE <> PREV_PMT_DATE
                       AND MASTERID IN ( SELECT MASTERID FROM PSAK_TMP_T1 ) --adding bcoz grace period prn and int is zero
                       GROUP BY  MASTERID
                     ) B
                ON B.MASTERID = A.MASTERID
                WHERE A.I_DAYS = 0
                AND A.PRN_AMT = 0
                AND A.INT_AMT = 0
               )
        SET PREV_PMT_DATE = MIN_PREV_DATE ,
            PMT_DATE = MIN_PREV_DATE;


        --adding case if grace period.. 20160428
        UPDATE (SELECT A.I_DAYS AS I_DAYS
                      ,A.GRACE_DATE AS GRACE_DATE
                      ,A.PMT_DATE AS PMT_DATE
                      ,B.ICC AS ICC
                      ,B.PREV_PMT_DATE AS PREV_PMT_DATE
                      ,B.PMT_DATE AS PMT_DATE
                FROM IFRS_PAYM_CORE A
                JOIN  ( SELECT MASTERID ,
                               MIN(PMT_DATE) MIN_PMT_DATE ,
                               MIN(PREV_PMT_DATE) MIN_PREV_DATE
                        FROM IFRS_PAYM_CORE
                        WHERE PMT_DATE <> PREV_PMT_DATE
                        GROUP BY  MASTERID
                      ) B
                ON B.MASTERID = A.MASTERID
                WHERE A.PREV_PMT_DATE = B.MIN_PREV_DATE
                AND A.PMT_DATE = B.MIN_PMT_DATE
               )
        SET I_DAYS = CASE WHEN GRACE_DATE >= PMT_DATE AND GRACE_DATE IS NOT NULL THEN 0--bibd for grace period
                          ELSE CASE WHEN ICC = '6' THEN FN_CNT_DAYS_30_360(PREV_PMT_DATE,PMT_DATE)
                                    ELSE (PMT_DATE - PREV_PMT_DATE  )
                                    END
                          END ;

        --20160428

        --begin : if schd count <=2 then can not use above logic
        -- use loan start date from IMA_AMORT_CURR
        --update 1st row

        /*
        merge into IFRS_PAYM_CORE a
        using (
        select masterid,loan_start_date from IMA_AMORT_CURR
        where masterid in (
           select masterid from IFRS_PAYM_CORE
           group by masterid having count(*)<=2
        )
        -- rules to update : currdate no more than 30 days from loan_start_date
        and (DOWNLOAD_DATE-loan_start_date)<30
        ) b on (b.masterid=a.masterid and a.i_days=0 and a.prn_amt=0 and a.int_amt=0)
        when matched then
        update set a.prev_pmt_date = b.loan_start_date, a.pmt_date=b.loan_start_date;

        COMMIT;

        --update 2nd row
        merge into IFRS_PAYM_CORE a
        using (
        select masterid,loan_start_date from IMA_AMORT_CURR
        where masterid in (
             select masterid from IFRS_PAYM_CORE
             group by masterid having count(*)<=2
        )
        -- rules to update : currdate no more than 30 days from loan_start_date
        and (DOWNLOAD_DATE-loan_start_date)<30
        ) b on (b.masterid=a.masterid and a.prev_pmt_date<>a.pmt_date)
        when matched then
        update set a.loan_start_date = b.loan_start_date;
        commit;

        update IFRS_PAYM_CORE a
        set a.prev_pmt_date  =a.loan_start_date
        where loan_start_date is not null;
        commit;

        merge into IFRS_PAYM_CORE a
        using (
        select masterid,loan_start_date from IMA_AMORT_CURR
        where masterid in (
             select masterid from IFRS_PAYM_CORE
             group by masterid having count(*)<=2
        )
        -- rules to update : currdate no more than 30 days from loan_start_date
        and (DOWNLOAD_DATE-loan_start_date)<30
        ) b on (b.masterid=a.masterid and a.prev_pmt_date<>a.pmt_date)
        when matched then
        update
        set
        a.i_days=case when a.icc='6' then FN_CNT_DAYS_30_360(a.prev_pmt_date,a.pmt_date) else a.pmt_date-a.loan_start_date end;
        COMMIT;

        --end : if schd count <=2 then can not use above logic

    END IF;



   -- merge for staff loan for fix 30 days / normalize npv by 30 days for icbc
    UPDATE (SELECT A.I_DAYS AS I_DAYS
            FROM IFRS_PAYM_CORE A
            JOIN ( SELECT MASTERID FROM IFRS_IMA_AMORT_CURR
                   WHERE STAFF_LOAN_FLAG = 'Y'
                   AND EIRECF = 'Y'
                  ) B
            ON A.MASTERID = B.MASTERID
            AND A.PREV_PMT_DATE <> A.PMT_DATE
           )
    SET I_DAYS = 30 ;



    REMARKS DULU 20160524
    -- prepare for loop

   /*REMARKS DULU 20160524
    TRUNCATE TABLE IFRS_PAYM_CORE_date

    INSERT  INTO IFRS_PAYM_CORE_date
            ( masterid ,
              dtmin ,
              dtmax ,
              target_int_amt
            )
            SELECT  masterid ,
                    MIN(pmt_date) ,
                    MAX(pmt_date) ,
                    SUM(int_amt)
            FROM    IFRS_PAYM_CORE
            GROUP BY masterid

   -- init value

    UPDATE  IFRS_PAYM_CORE_DATE
    SET     eir = NULL ,
            next_eir = NULL ,
            final_eir = NULL

    REMARKS DULU 20160524*/


    UPDATE  IFRS_PAYM_CORE
    SET     EIR1 = 12 ,
            EIR2 = 12.1;


    COMMIT;

    INSERT  INTO IFRS_ACCT_EIR_PAYM
    ( MASTERID ,
      DOWNLOAD_DATE ,
      N_LOAN_AMT ,
      N_INT_RATE ,
      STARTAMORTDATE ,
      ENDAMORTDATE ,
      GRACEDATE ,
      ISGRACE ,
      PREV_PMT_DATE ,
      PMT_DATE ,
      I_DAYS ,
      COUNTER ,
      N_OSPRN_PREV ,
      N_INSTALLMENT ,
      N_PRN_PAYMENT ,
      N_INT_PAYMENT ,
      N_INT_PAYMENT_ORG ,
      N_OSPRN ,
      DISB_PERCENTAGE ,
      DISB_AMOUNT ,
      PLAFOND ,
      PERIOD ,
      INTCALCCODE ,
      PAYMENTCODE ,
      PAYMENTTERM
    )
    SELECT  A.MASTERID ,
            V_CURRDATE AS DOWNLOAD_DATE ,
            MAX(A.OS_PRN_PREV) OVER (PARTITION BY A.MASTERID) ,
            --b.INTEREST_RATE, remarks for multitier 20160428
            A.INT_RATE , --for multitier --20160428
            V_CURRDATE AS STARTAMORTDATE ,
            MAX(A.PMT_DATE) OVER (PARTITION BY A.MASTERID) ,
            NVL(A.GRACE_DATE, V_CURRDATE) AS GRACEDATE ,
            CASE WHEN A.GRACE_DATE IS NOT NULL THEN 'Y' ELSE 'N' END AS ISGRACE ,
            A.PREV_PMT_DATE ,
            A.PMT_DATE ,
            A.I_DAYS ,
            A.COUNTER ,
            A.OS_PRN_PREV ,
            ( A.PRN_AMT + A.INT_AMT ) AS INSTALLMENT ,
            A.PRN_AMT ,
            A.INT_AMT ,
            A.INT_AMT ,
            A.OS_PRN ,
            A.DISB_PERCENTAGE ,
            A.DISB_AMOUNT ,
            A.PLAFOND ,
            MAX(A.COUNTER) OVER (PARTITION BY A.MASTERID) ,
            ICC AS INTCALCCODE ,
            '-' PAYMENTCODE ,
            '-' PAYMENTTERM
    FROM    IFRS_PAYM_CORE A
    LEFT JOIN IFRS_IMA_AMORT_CURR B ON B.MASTERID = A.MASTERID  ;

    COMMIT;
    INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_PAYM_CORE_PROCESS' ,'INSERT TO EIR PAYM' );

    COMMIT;

   --update end amort date

   /*REMARKS DULU 20160524
    UPDATE  dbo.IFRS_ACCT_EIR_PAYM
    SET     endamortdate = b.dtmax
    FROM    IFRS_PAYM_CORE_date b
    WHERE   b.masterid = dbo.IFRS_ACCT_EIR_PAYM.masterid


    INSERT  INTO IFRS_AMORT_LOG
            ( DOWNLOAD_DATE ,
              DTM ,
              OPS ,
              PROCNAME ,
              REMARK
            )
    VALUES  ( @v_currdate ,
              CURRENT_TIMESTAMP ,
              'DEBUG' ,
              'SP_IFRS_PAYM_CORE_PROCESS' ,
              'UPD END AMORT DT'
            )


   --update period

    UPDATE  dbo.IFRS_ACCT_EIR_PAYM
    SET     period = b.cnt - 1 ,
            n_loan_amt = b.osprn
    FROM    ( SELECT    masterid ,
                        COUNT(*) cnt ,
                        MAX(os_prn_prev) osprn
              FROM      IFRS_PAYM_CORE
              GROUP BY  masterid
            ) b
    WHERE   ( b.masterid = dbo.IFRS_ACCT_EIR_PAYM.masterid )



    INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'DEBUG' ,'SP_IFRS_PAYM_CORE_PROCESS' ,'UPD PERIOD' );

    REMARKS DULU 20160524*/

    INSERT  INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES  ( V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_IFRS_PAYM_CORE_PROCESS' ,'' );

    COMMIT;

END;