CREATE OR REPLACE PROCEDURE SP_IFRS_LI_EXEC_AND_LOG (
   P_SP_NAME       VARCHAR2,
   P_PRC_NAME      VARCHAR2 DEFAULT 'AMT',
   EXECUTE_FLAG    CHAR DEFAULT 'Y')
IS
   V_COUNTER               INT;
   V_ERRN                  NUMBER;
   V_ERRM                  VARCHAR2 (255);
   V_CURRDATE              DATE;
   V_MAX_COUNTER           INT := 0;
   V_PREVDATE              DATE;
   V_SESSIONID             VARCHAR2 (50);
   V_COUNT                 INT;
   V_STR_SQL               VARCHAR (50);
   V_MINSTARTDATESESSION   TIMESTAMP;
BEGIN
    /******************************************************************************
    01. DECLARE VARIABLE
    *******************************************************************************/
      SELECT CURRDATE, PREVDATE, SESSIONID
      INTO V_CURRDATE, V_PREVDATE, V_SESSIONID
      FROM IFRS_LI_PRC_DATE_AMORT;

      SELECT SESSIONID
      INTO V_SESSIONID
      FROM IFRS_LI_PRC_DATE_AMORT;

      SELECT NVL(MAX(COUNTER),0),NVL(MIN(START_DATE),SYSDATE)
      INTO V_COUNTER,V_MINSTARTDATESESSION
      FROM IFRS_STATISTIC A
      WHERE PRC_NAME = P_PRC_NAME AND DOWNLOAD_DATE = V_CURRDATE;


      V_COUNTER := V_COUNTER + 1;
      COMMIT;
    /******************************************************************************
    02. UPDATE PRC DATE STATUS
    *******************************************************************************/
      UPDATE IFRS_LI_PRC_DATE_AMORT
      SET BATCH_STATUS = 'Running..'
        , REMARK = P_SP_NAME;
      COMMIT;

    /******************************************************************************
    03. INSERT INTO STATISTIC
    *******************************************************************************/
      INSERT INTO IFRS_STATISTIC (
      DOWNLOAD_DATE,
      SP_NAME,
      START_DATE,
      ISCOMPLETE,
      COUNTER,
      PRC_NAME,
      SESSIONID,
      REMARK
      )
      SELECT CURRDATE,
             P_SP_NAME,
             SYSDATE,
             'N',
             V_COUNTER,
             P_PRC_NAME,
             V_SESSIONID,
                'Running..'
      FROM IFRS_LI_PRC_DATE_AMORT;
      COMMIT;
    /******************************************************************************
    04.  SET VARIABLE AND EXECUTE SP
    *******************************************************************************/

      IF EXECUTE_FLAG = 'Y'
        THEN

        V_STR_SQL:='BEGIN ' || P_SP_NAME ||' ; END;';
        dbms_output.put_line(V_STR_SQL);
        EXECUTE IMMEDIATE V_STR_SQL;

     --  V_STR_SQL := ' BEGIN  ' || P_SP_NAME || '; END;';
     --  EXECUTE IMMEDIATE ('EXECUTE ' || V_STR_SQL || ' ;');

      END IF;
    /******************************************************************************
    05. UPDATE STATISTIC
    *******************************************************************************/
      UPDATE IFRS_STATISTIC A
      SET A.END_DATE = SYSDATE,
          ISCOMPLETE = 'Y',
          PRC_PROCESS_TIME =
          REPLACE (REPLACE (SYSDATE - START_DATE, '.000000000', ''),'000000000',''),
          REMARK = 'Successed'
      WHERE     A.DOWNLOAD_DATE = V_CURRDATE
      AND A.SP_NAME = P_SP_NAME
      AND PRC_NAME = P_PRC_NAME;

      COMMIT;

    /******************************************************************************
    06. UPDATE PROCESS TIME
    *******************************************************************************/
      UPDATE IFRS_STATISTIC
      SET SESSION_PROCESS_TIME =REPLACE (REPLACE (SYSDATE - V_MINSTARTDATESESSION,'.000000000',''),'000000000','')
      WHERE DOWNLOAD_DATE = V_CURRDATE AND SESSIONID = V_SESSIONID;

      COMMIT;
    /******************************************************************************
    07. UPDATE STATUS FINISHED
    *******************************************************************************/

      UPDATE IFRS_LI_PRC_DATE_AMORT
      SET BATCH_STATUS = 'Finished',
          REMARK = 'Execute ' || P_SP_NAME || ' is successed';

    /******************************************************************************
    08. EXCEPTION
    *******************************************************************************/
      EXCEPTION
        WHEN OTHERS
        THEN
          V_ERRM := SQLERRM;
          V_ERRN := SQLCODE;
          UPDATE IFRS_STATISTIC A
          SET A.END_DATE = SYSDATE,
              ISCOMPLETE = 'N',
              REMARK = 'Error - ' || V_ERRN || ' ' || V_ERRM
          WHERE     A.DOWNLOAD_DATE = V_CURRDATE
          AND A.SP_NAME = P_SP_NAME
          AND PRC_NAME = P_PRC_NAME;


          UPDATE IFRS_LI_PRC_DATE_AMORT
          SET RUNNING_FLAG_FROM_DW = 'N',
              BATCH_STATUS = 'Error!! ',
              REMARK = V_ERRN || ' ' || V_ERRM || ' (' || P_PRC_NAME || ')';

          UPDATE IFRS_PRC_DATE
          SET BATCH_STATUS = 'Error!! ',
              REMARK = V_ERRN || ' ' || V_ERRM || ' (' || P_PRC_NAME || ')';
          COMMIT;
          --         sp_psak_drop_psak_batch_job;

      --    SP_PSAK_SENT_EMAIL ('TS-ERROR', V_ERRM);
          RAISE_APPLICATION_ERROR (V_ERRN, V_ERRM);

END;