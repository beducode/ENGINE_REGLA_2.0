CREATE OR REPLACE PROCEDURE USPU_UPDATEJOBH
(
    v_PKID IN NUMBER,
    v_Status IN VARCHAR2,
    v_Message IN VARCHAR2,
    v_UpdatedBy IN VARCHAR2,
    v_ThreadId In VARCHAR2
)
AS
BEGIN

    UPDATE TBLT_SERVICEJOBSHEADER
    SET
        STATUS      = v_Status,
        MESSAGE     = v_Message,
        UPDATEDBY   = v_UpdatedBy,
        UPDATEDDATE = SYSDATE()
    WHERE PKID = v_PKID;

    COMMIT;

    IF v_Status = 'Failed' or v_Status = 'Success'  THEN
        UPDATE TBLT_APPLICATIONLOG
        SET STATUS = v_Status,
        MESSAGE = v_Message,
        FINISH_TIME = SYSDATE()
        WHERE THREADID = v_ThreadId;

    END IF;

    COMMIT;

END;