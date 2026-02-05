CREATE OR REPLACE PROCEDURE SP_IFRS_SYNC_JOURNAL_PARAM
IS

    V_CURRDATE DATE ;
    V_PREVDATE DATE;

BEGIN
    /******************************************************************************
    01. DECLARE VARIABLE
    *******************************************************************************/
    SELECT  MAX(CURRDATE), MAX(PREVDATE)
    INTO V_CURRDATE, V_PREVDATE
    FROM    IFRS_PRC_DATE_AMORT;


    INSERT INTO IFRS_AMORT_LOG( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES( V_CURRDATE ,SYSTIMESTAMP ,'START' ,'SP_IFRS_SYNC_JOURNAL_PARAM' ,'');

    COMMIT;

    /******************************************************************************
    02. UPDATE FOR TRX_TYPE DEBET CHANGE TO DB
    *******************************************************************************/
    UPDATE  IFRS_MASTER_JOURNAL_PARAM
    SET     DRCR = 'DB'
    WHERE   SUBSTR(DRCR, 1, 1) = 'D';

    COMMIT;

    /******************************************************************************
    03. INSERT JOURNAL PARAM
    *******************************************************************************/
    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_JOURNAL_PARAM';

    INSERT  INTO IFRS_JOURNAL_PARAM
    ( JOURNALCODE ,
    GL_CONSTNAME ,
    TRX_CODE,
    COSTCENTER ,
    CCY ,
    FLAG_CF ,
    DRCR ,
    GLNO ,
    --seq ,
    JOURNAL_DESC ,
    GL_INTERNAL_CODE --,
    --PORTION
    )
    SELECT  JOURNALCODE ,
            GL_CONSTNAME ,
            TRX_CODE,
            '000' AS COST_CENTER ,
            CCY ,
            FLAG_CF ,
            SUBSTR(DRCR, 1, 1) ,
            GL_NO ,
            --psak_cost_code ,
            JOURNAL_DESC,
            GL_INTERNAL_CODE --,
            --NVL(PORTION, 100)
    FROM    IFRS_MASTER_JOURNAL_PARAM
	WHERE INST_CLS_VALUE = 'A';

    INSERT INTO IFRS_AMORT_LOG ( DOWNLOAD_DATE ,DTM ,OPS ,PROCNAME ,REMARK)
    VALUES( V_CURRDATE ,SYSTIMESTAMP ,'END' ,'SP_IFRS_SYNC_JOURNAL_PARAM' ,'');

    COMMIT;

END;