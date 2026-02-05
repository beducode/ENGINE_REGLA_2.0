CREATE OR REPLACE PROCEDURE USPU_UPDATEEMAILOUTSTATUS
(
    V_PKID NUMBER default 0,
    V_Status VARCHAR2,
    V_Message VARCHAR2,
    V_Sentdate DATE default null
)
AS
BEGIN
    UPDATE TBLT_EMAILOUT
    SET STATUS = V_Status, MESSAGE = V_Message, sentdate = V_Sentdate
    WHERE PKID = V_PKID;
END;