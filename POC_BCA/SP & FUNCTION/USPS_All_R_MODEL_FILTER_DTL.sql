CREATE OR REPLACE PROCEDURE  USPS_All_R_MODEL_FILTER_DTL (
    v_MODEL_ID    NUMBER,
    Cur_out OUT SYS_REFCURSOR
)
AS
    v_status NUMBER(10);
BEGIN
    SELECT STATUS
    INTO v_status
    FROM IFRS_FL_MODEL_VAR_PEN
    WHERE PKID = v_Model_ID;

    IF v_status = 1 THEN
        OPEN Cur_out FOR
        SELECT A.PKID,
            A.MODEL_ID,
            A.CORRELATION_VAL,
            A.VARIABLENAME,
            A.EXPECTED_TREND,
            A.IN_SAMPLE_SET,
            A.CORR_SIGN,
            A.CORR_GT_FLG,
            A.PASS,
            A.VARIABLE_SELECTION,
            A.LOOKUPINITIAL,
            A.TRANSFORM_C,
            A.TRANSFORM_M,
            A.SUM_TRANSFORM,
            A.ABS_CORRELATION,
            A.ADJ_CORRELATION,
            A.ADF_STATISTIC_NC,
            A.ADF_PVALUE_NC,
            A.ADF_STATISTIC_C,
            A.ADF_PVALUE_C,
            A.ADF_STATISTIC_CT,
            A.ADF_PVALUE_CT,
            A.CREATEDBY,
            A.CREATEDDATE,
            A.CREATEDHOST,
            A.UPDATEDBY,
            A.UPDATEDDATE,
            A.UPDATEDHOST
        FROM R_MODEL_FILTER_DTL A
        WHERE A.MODEL_ID = v_MODEL_ID;
    ELSE
        OPEN Cur_out FOR
        SELECT A.PKID,
            A.MODEL_ID,
            A.CORRELATION_VAL,
            A.VARIABLENAME,
            A.EXPECTED_TREND,
            A.IN_SAMPLE_SET,
            A.CORR_SIGN,
            A.CORR_GT_FLG,
            A.PASS,
            A.VARIABLE_SELECTION,
            A.LOOKUPINITIAL,
            A.TRANSFORM_C,
            A.TRANSFORM_M,
            A.SUM_TRANSFORM,
            A.ABS_CORRELATION,
            A.ADJ_CORRELATION,
            A.ADF_STATISTIC_NC,
            A.ADF_PVALUE_NC,
            A.ADF_STATISTIC_C,
            A.ADF_PVALUE_C,
            A.ADF_STATISTIC_CT,
            A.ADF_PVALUE_CT,
            A.CREATEDBY,
            A.CREATEDDATE,
            A.CREATEDHOST,
            A.UPDATEDBY,
            A.UPDATEDDATE,
            A.UPDATEDHOST
        FROM R_MODEL_FILTER_DTL_PEN A
        WHERE A.MODEL_ID = v_MODEL_ID;
    END IF;

END;