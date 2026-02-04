CREATE OR REPLACE PROCEDURE  USPS_R_VARCLUS_CLUSTER_SUMM (
    v_MODEL_ID NUMBER,
    Cur_out OUT SYS_REFCURSOR
)
AS
    v_status NUMBER(10);
BEGIN
    SELECT STATUS
    INTO v_status
    FROM IFRS_FL_MODEL_VAR_PEN
    WHERE PKID = v_MODEL_ID;

        IF v_status = 1 THEN
            OPEN Cur_out FOR
            SELECT A.PKID,
            A.MODEL_ID,
            A.CLUS_NO,
            ROUND(A.NO_OF_MEMBERS,6) NO_OF_MEMBERS,
            ROUND(A.VAR_EXPL,6) VAR_EXPL,
            ROUND(A.PROP_EXPL,6) PROP_EXPL,
            ROUND(A.SECOND_EIGV,6) SECOND_EIGV
            FROM R_VARCLUS_CLUSTER_SUMM A
            WHERE MODEL_ID = v_MODEL_ID;
        ELSE
            OPEN Cur_out FOR
            SELECT A.PKID,
            A.MODEL_ID,
            A.CLUS_NO,
            ROUND(A.NO_OF_MEMBERS,6) NO_OF_MEMBERS,
            ROUND(A.VAR_EXPL,6) VAR_EXPL,
            ROUND(A.PROP_EXPL,6) PROP_EXPL,
            ROUND(A.SECOND_EIGV,6) SECOND_EIGV
            FROM R_VARCLUS_CLUSTER_SUMM_PEN A
            WHERE MODEL_ID = v_MODEL_ID;
    END IF;
END;