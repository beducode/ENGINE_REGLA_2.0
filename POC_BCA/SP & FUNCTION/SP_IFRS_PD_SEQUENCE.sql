CREATE OR REPLACE PROCEDURE SP_IFRS_PD_SEQUENCE (V_EFF_DATE DATE)
AS
    V_SPNAME   VARCHAR2 (100);
BEGIN
    /*=======================================================================================
      Calculate PD Migration Analysis Method
      =======================================================================================*/

    V_SPNAME := 'SP_IFRS_GENERATE_RULE_SEGMENT(''PD_SEG'',''M'')';
    SP_IFRS_EXEC_AND_LOG_PROCESS (V_SPNAME, 'IMP', 'Y');

    V_SPNAME :='SP_IFRS_RULE_DATA_PD('''|| TO_CHAR (V_EFF_DATE, 'dd-mon-yyyy')|| ''')';
    SP_IFRS_EXEC_AND_LOG_PROCESS (V_SPNAME, 'IMP', 'Y');

    V_SPNAME :='SP_IFRS_PD_TM_SCENARIO_DATA('''|| TO_CHAR (V_EFF_DATE, 'dd-mon-yyyy')|| ''')';
    SP_IFRS_EXEC_AND_LOG_PROCESS (V_SPNAME, 'IMP', 'Y');


    V_SPNAME :='SP_IFRS_PD_MIG_ENR('''|| TO_CHAR (V_EFF_DATE, 'dd-mon-yyyy')|| ''')';
    SP_IFRS_EXEC_AND_LOG_PROCESS (V_SPNAME, 'IMP', 'Y');

    V_SPNAME :='SP_IFRS_PD_MIG_FLOWRATE('''|| TO_CHAR (V_EFF_DATE, 'dd-mon-yyyy')|| ''')';
    SP_IFRS_EXEC_AND_LOG_PROCESS (V_SPNAME, 'IMP', 'Y');

    V_SPNAME :='SP_IFRS_PD_MIG_FLOW_TO_LOSS('''|| TO_CHAR (V_EFF_DATE, 'dd-mon-yyyy')|| ''')';
    SP_IFRS_EXEC_AND_LOG_PROCESS (V_SPNAME, 'IMP', 'Y');

    V_SPNAME :='SP_IFRS_PD_MIG_TTC('''|| TO_CHAR (V_EFF_DATE, 'dd-mon-yyyy')|| ''')';
    SP_IFRS_EXEC_AND_LOG_PROCESS (V_SPNAME, 'IMP', 'Y');

    V_SPNAME :='SP_IFRS_PD_MIG_ODR('''|| TO_CHAR (V_EFF_DATE, 'dd-mon-yyyy')|| ''')';
    SP_IFRS_EXEC_AND_LOG_PROCESS (V_SPNAME, 'IMP', 'Y');

    V_SPNAME :='SP_IFRS_PD_MIG_LOGIT_ODR('''|| TO_CHAR (V_EFF_DATE, 'dd-mon-yyyy')|| ''')';
    SP_IFRS_EXEC_AND_LOG_PROCESS (V_SPNAME, 'IMP', 'Y');


    /*=======================================================================================
      Generate PD Rule ID for PD Include Report
      =======================================================================================*/
    V_SPNAME :='SP_IFRS_RULE_DATA_SEGMENT('''|| TO_CHAR (V_EFF_DATE, 'dd-mon-yyyy')|| ''')';
    SP_IFRS_EXEC_AND_LOG_PROCESS (V_SPNAME, 'IMP', 'Y');

    UPDATE IFRS_MASTER_ACCOUNT_MONTHLY
       SET PD_RULE_ID = 0, PD_SEGMENT = NULL
     WHERE DOWNLOAD_DATE = V_EFF_DATE;

    COMMIT;

    MERGE /*+ PARALLEL(8) */ INTO IFRS_MASTER_ACCOUNT_MONTHLY A
         USING ( /*SELECT A.*, B.PKID PD_RULE_ID
                   FROM GTMP_IFRS_SCENARIO_DATA  A
                        JOIN IFRS_PD_RULES_CONFIG B
                            ON     A.RULE_ID = B.SEGMENTATION_ID
                               AND B.PD_METHOD = 'MIG'*/
                 SELECT   DOWNLOAD_DATE,
                         MASTERID,
                         SUB_SEGMENT,
                         PD_RULE_ID
                    FROM (SELECT A.DOWNLOAD_DATE, A.MASTERID,A.SUB_SEGMENT, B.PKID PD_RULE_ID,
                    ROW_NUMBER() OVER (PARTITION BY A.MASTERID ORDER BY B.PKID DESC) RN
                            FROM    GTMP_IFRS_SCENARIO_DATA A
                                 JOIN
                                    IFRS_PD_RULES_CONFIG B
                                 ON A.RULE_ID = B.SEGMENTATION_ID
                                    AND B.PD_METHOD = 'MIG') C
                    WHERE RN = 1
                ) B
            ON (A.DOWNLOAD_DATE = V_EFF_DATE AND A.MASTERID = B.MASTERID)
    WHEN MATCHED
    THEN
       UPDATE SET A.PD_RULE_ID = B.PD_RULE_ID, A.PD_SEGMENT = B.SUB_SEGMENT;

    COMMIT;

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