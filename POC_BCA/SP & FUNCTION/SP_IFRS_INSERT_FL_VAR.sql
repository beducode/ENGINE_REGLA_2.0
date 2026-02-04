CREATE OR REPLACE PROCEDURE SP_IFRS_INSERT_FL_VAR(v_uploadId number)
AS
BEGIN
        DELETE FROM IFRS_MACRO_ECONOMIC
        WHERE ME_CODE IN
        (
            SELECT DISTINCT ME_CODE FROM TBLU_MACRO_ECONOMIC
            WHERE UPLOADID = v_uploadId
        );

        COMMIT;

        INSERT INTO IFRS_MACRO_ECONOMIC
        (
            HEADERID,
            UPLOADID,
             ME_CODE,
            ME_PERIOD,
            ME_VAL,
            CREATEDBY,
            CREATEDDATE,
            CREATEDHOST
        )
        SELECT B.PKID,
            A.UPLOADID,
            UPPER(A.ME_CODE),
            A.ME_PERIOD,
            A.ME_VAL,
            A.UPLOADBY,
            A.UPLOADDATE,
            A.UPLOADHOST
        FROM TBLU_MACRO_ECONOMIC A
        JOIN TBLM_MACRO_ECONOMIC B
        ON A.ME_CODE = B.ME_CODE;

        COMMIT;
END;