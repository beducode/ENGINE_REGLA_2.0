CREATE OR REPLACE PROCEDURE      IFRS_RUN_ENGINE
AS
    V_EFF_DATE DATE;
    V_MAXDATE  DATE;
    V_SPNAME   VARCHAR2(1000);
BEGIN
    V_EFF_DATE := '30-JUN-2025';

IF EXTRACT (MONTH FROM V_EFF_DATE) = 6
    THEN
        /*=======================================================================================
          Calculate PD Vasicek for external rating
          =======================================================================================*/
        V_SPNAME :='SP_IFRS_PD_VAS('''|| TO_CHAR (V_EFF_DATE, 'dd-mon-yyyy')|| ''')';
        SP_IFRS_EXEC_AND_LOG_PROCESS (V_SPNAME, 'IMP', 'Y');

        V_SPNAME :='SP_IFRS_PD_VAS_CORRELATION('''|| TO_CHAR (V_EFF_DATE, 'dd-mon-yyyy')|| ''')';
        SP_IFRS_EXEC_AND_LOG_PROCESS (V_SPNAME, 'IMP', 'Y');

        V_SPNAME :='SP_IFRS_PD_VAS_Z_SCORE('''|| TO_CHAR (V_EFF_DATE, 'dd-mon-yyyy')|| ''')';
        SP_IFRS_EXEC_AND_LOG_PROCESS (V_SPNAME, 'IMP', 'Y');

        V_SPNAME :='SP_IFRS_PD_VAS_PIT('''|| TO_CHAR (V_EFF_DATE, 'dd-mon-yyyy')|| ''')';
        SP_IFRS_EXEC_AND_LOG_PROCESS (V_SPNAME, 'IMP', 'Y');

        V_SPNAME :='SP_IFRS_PD_VAS_CUMULATIVE('''|| TO_CHAR (V_EFF_DATE, 'dd-mon-yyyy')|| ''')';
        SP_IFRS_EXEC_AND_LOG_PROCESS (V_SPNAME, 'IMP', 'Y');

        V_SPNAME :='SP_IFRS_PD_VAS_MARGINAL('''|| TO_CHAR (V_EFF_DATE, 'dd-mon-yyyy')|| ''')';
        SP_IFRS_EXEC_AND_LOG_PROCESS (V_SPNAME, 'IMP', 'Y');
    END IF;
END;