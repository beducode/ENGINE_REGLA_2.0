CREATE OR REPLACE PROCEDURE SP_IFRS_PD_LGD_BACK_TEST(V_EFF_DATE DATE)
AS
    V_MAXDATE DATE;
    V_SPNAME VARCHAR2(100);
BEGIN
    /*=======================================================================================
      Initial Update First NPL Date and First NPL OS
      =======================================================================================*/
    SELECT ADD_MONTHS(MAX(DOWNLOAD_DATE),1)
    INTO V_MAXDATE
    FROM IFRS_LGD_FIRST_NPL_DATE;

    WHILE V_MAXDATE <= V_EFF_DATE LOOP
        V_SPNAME := 'SP_IFRS_LGD_FIRST_NPL_DATE('''|| TO_CHAR(V_MAXDATE, 'dd-mon-yyyy')  ||''')';
        SP_IFRS_EXEC_AND_LOG_PROCESS(V_SPNAME,'IMP','Y');
        V_MAXDATE := ADD_MONTHS(V_MAXDATE,1);
    END LOOP;

    /*=======================================================================================
      PD Calculation
      =======================================================================================*/
    SELECT ADD_MONTHS(MAX(EFF_DATE),1)
    INTO V_MAXDATE
    FROM IFRS_PD_MIG_ODR;

    WHILE V_MAXDATE <= V_EFF_DATE LOOP
        V_SPNAME := 'SP_IFRS_PD_SEQUENCE('''|| TO_CHAR(V_MAXDATE, 'dd-mon-yyyy')  ||''')';
        SP_IFRS_EXEC_AND_LOG_PROCESS(V_SPNAME,'IMP','Y');
        V_MAXDATE := ADD_MONTHS(V_MAXDATE,1);
    END LOOP;


    /*=======================================================================================
      LGD Calculation
      =======================================================================================*/
    V_SPNAME := 'SP_IFRS_LGD_SEQUENCE('''|| TO_CHAR(V_EFF_DATE, 'dd-mon-yyyy')  ||''')';
    SP_IFRS_EXEC_AND_LOG_PROCESS(V_SPNAME,'IMP','Y');
END;