CREATE OR REPLACE PROCEDURE USPS_UPLOADREJECT
(
    V_UploadID number default 0,
    V_UpdatedBy VARCHAR2 default ' ',
    V_UpdatedHost varchar2 default ' '
)
AS
 v_Query VARCHAR2(5000);
BEGIN

    UPDATE TBLT_UPLOAD_POOL
    SET STATUS = 'REJECTED',
        UPDATEDBY = V_UpdatedBy ,
        UPDATEDHOST =  V_UpdatedHost,
        APPROVEDBY = V_UpdatedBy,
        APPROVEDDATE = sysdate,
        UPDATEDDATE = sysdate
    WHERE PKID = V_UploadID;

    commit;

END;