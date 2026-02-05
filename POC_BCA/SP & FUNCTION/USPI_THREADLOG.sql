CREATE OR REPLACE PROCEDURE  USPI_THREADLOG
(
    v_LogId IN VARCHAR2,
    v_JobId IN NUMBER,
    v_Status IN VARCHAR2,
    v_Description IN VARCHAR2
)
AS
BEGIN

    INSERT INTO TBLT_APPLICATIONLOG
    (
        THREADID,
        JOBID,
        MESSAGE,
        START_TIME,
        STATUS
    )
    VALUES
    (
        v_LogId,
        v_JobId,
        v_Description,
        SYSDATE(),
        v_Status
    );

    COMMIT;

END;