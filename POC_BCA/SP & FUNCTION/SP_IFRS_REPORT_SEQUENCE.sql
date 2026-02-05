CREATE OR REPLACE PROCEDURE SP_IFRS_REPORT_SEQUENCE (v_DOWNLOADDATECUR  DATE DEFAULT ('1-JAN-1900'),
                                               v_DOWNLOADDATEPREV DATE DEFAULT ('1-JAN-1900'))
AS
  V_CURRDATE DATE;
  V_PREVDATE DATE;
  V_SPNAME   VARCHAR2(200);

BEGIN

IF v_DOWNLOADDATECUR = '1-JAN-1900'
  THEN
    SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;
  ELSE
    V_CURRDATE := v_DOWNLOADDATECUR;
  END IF;

  IF v_DOWNLOADDATEPREV = '1-JAN-1900'
  THEN
    SELECT PREVDATE INTO V_PREVDATE FROM IFRS_PRC_DATE;
  ELSE
    V_PREVDATE := v_DOWNLOADDATEPREV;
  END IF;


--V_SPNAME := 'SP_IFRS_NOMINATIVE (' || '''' ||TO_CHAR(V_CURRDATE, 'dd-mon-yyyy') || '''' || ',' || '''' || TO_CHAR(V_PREVDATE, 'dd-mon-yyyy') || '''' || ')';
--SP_IFRS_EXEC_AND_LOG_PROCESS(V_SPNAME, 'REPORT', 'Y');

--V_SPNAME := 'SP_IFRS_IMPC_JOURNAL_DATA (' || '''' ||TO_CHAR(V_CURRDATE, 'dd-mon-yyyy') || '''' || ',' || '''' || TO_CHAR(V_PREVDATE, 'dd-mon-yyyy') || '''' || ')';
--SP_IFRS_EXEC_AND_LOG_PROCESS(V_SPNAME, 'REPORT', 'Y');
--
--V_SPNAME := 'SP_IFRS_IMPI_JOURNAL_DATA (' || '''' ||TO_CHAR(V_CURRDATE, 'dd-mon-yyyy') || '''' || ',' || '''' || TO_CHAR(V_PREVDATE, 'dd-mon-yyyy') || '''' || ')';
--SP_IFRS_EXEC_AND_LOG_PROCESS(V_SPNAME, 'REPORT', 'Y');

--V_SPNAME := 'SP_IFRS_ACCT_AMORT_RPT_REKON (' || '''' ||TO_CHAR(V_CURRDATE, 'dd-mon-yyyy') || '''' || ',' || '''' || TO_CHAR(V_PREVDATE, 'dd-mon-yyyy') || '''' || ')';
--SP_IFRS_EXEC_AND_LOG_PROCESS(V_SPNAME, 'REPORT', 'Y');
--
--V_SPNAME := 'SP_IFRS_INSERT_GL_OUTBOUND (' || '''' ||TO_CHAR(V_CURRDATE, 'dd-mon-yyyy') || '''' || ',' || '''' || TO_CHAR(V_PREVDATE, 'dd-mon-yyyy') || '''' || ')';
--SP_IFRS_EXEC_AND_LOG_PROCESS(V_SPNAME, 'REPORT', 'Y');
--
--V_SPNAME := 'SP_IFRS_REPORT_RONA (' || '''' ||TO_CHAR(V_CURRDATE, 'dd-mon-yyyy') || '''' || ')';
--SP_IFRS_EXEC_AND_LOG_PROCESS(V_SPNAME, 'REPORT', 'Y');

--V_SPNAME := 'SP_IFRS_REPORT_ECL_MOVEMENT (' || '''' ||TO_CHAR(V_CURRDATE, 'dd-mon-yyyy') || '''' || ',' || '''' || TO_CHAR(V_CURRDATE, 'dd-mon-yyyy') || '''' || ')';
--SP_IFRS_EXEC_AND_LOG_PROCESS(V_SPNAME, 'REPORT', 'Y');
--
--V_SPNAME := 'SP_IFRS_REPORT_ECL_MOVE_VALAS (' || '''' ||TO_CHAR(V_CURRDATE, 'dd-mon-yyyy') || '''' || ',' || '''' || TO_CHAR(V_PREVDATE, 'dd-mon-yyyy') || '''' || ')';
--SP_IFRS_EXEC_AND_LOG_PROCESS(V_SPNAME, 'REPORT', 'Y');
--
--
--V_SPNAME := 'SP_IFRS_NOMINATIVE_LBU (' || '''' ||TO_CHAR(V_CURRDATE, 'dd-mon-yyyy') || '''' || ',' || '''' || TO_CHAR(V_PREVDATE, 'dd-mon-yyyy') || '''' || ')';
--SP_IFRS_EXEC_AND_LOG_PROCESS(V_SPNAME, 'REPORT', 'Y');


V_SPNAME := 'SP_IFRS_RPT_NOMINATIVE_CRD (' || '''' ||TO_CHAR(V_CURRDATE, 'dd-mon-yyyy') || '''' || ')';
SP_IFRS_EXEC_AND_LOG_PROCESS(V_SPNAME, 'REPORT', 'Y');


V_SPNAME := 'SP_IFRS_GEN_REPORT_SEQUENCE (' || '''' ||TO_CHAR(V_CURRDATE, 'dd-mon-yyyy') || '''' || ',' || '''' || TO_CHAR(V_PREVDATE, 'dd-mon-yyyy') || '''' || ')';
SP_IFRS_EXEC_AND_LOG_PROCESS(V_SPNAME, 'REPORT', 'Y');

END;