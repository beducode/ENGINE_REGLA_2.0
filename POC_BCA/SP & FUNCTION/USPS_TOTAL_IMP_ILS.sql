CREATE OR REPLACE PROCEDURE USPS_TOTAL_IMP_ILS
(
    v_downloadDate DATE,
    v_sortParameter VARCHAR2,
    Cur_out OUT SYS_REFCURSOR
)
AS
    v_Query VARCHAR2(4000);
    v_DiffDate NUMBER;
BEGIN

   SELECT TO_NUMBER(MONTHS_BETWEEN (A.CURRDATE, TO_DATE(TO_CHAR(v_downloadDate,'dd MON yyyy')))) INTO v_DiffDate FROM IFRS_PRC_DATE A;
    IF(v_DiffDate <= 3) THEN
        v_Query :=  'SELECT
                    C.SUB_SEGMENT "ImpairmentSegment",
                    C.GOL_DEB "GolonganDebitur",
                    C.STAGE "Stage",
                    C.BTB_FLAG "BackToBackFlag",
                    C.ASSESSMENT_IMP "ImpairedFlag",
                    C.BUCKET_NAME "BucketName",
                    D.BRANCH_NUM || ''_'' || D.BRANCH_NAME "BranchCode",
                    SUM(ROUND(C.OUTSTANDING_PRINCIPAL_CCY, 6)) "Outstanding",
                    SUM((CASE WHEN C.STAGE = ''1'' THEN ROUND(C.STAGE_1_CCY, 6)
                        ELSE 0 END)) AS "ECL12Months" ,
                    SUM((CASE WHEN C.STAGE = ''2'' THEN ROUND(C.STAGE_2_CCY, 6)
                        ELSE 0 END)) AS "ECLLifetimeStage2" ,
                    SUM((CASE WHEN C.STAGE = ''3'' THEN ROUND(C.STAGE_3_CCY, 6)
                        ELSE 0 END)) AS "ECLLifetimeStage3" ,
                    SUM((CASE WHEN C.ASSESSMENT_IMP = ''I'' THEN ROUND(C.ECL_INDIVIDUAL_CCY, 6)
                        ELSE 0 END)) AS "ECLIndividual" ,
                    SUM((CASE WHEN C.ASSESSMENT_IMP = ''C'' THEN ROUND(C.ECL_COLLECTIVE_CCY, 6)
                        ELSE 0 END)) AS "ECLCollective" ,
                    SUM((CASE WHEN C.ASSESSMENT_IMP = ''W'' THEN ROUND(C.ECL_WORSTCASE_CCY, 6)
                        ELSE 0 END)) AS "ECLWorstCase" ,
                    SUM(ROUND(C.CARRYING_AMOUNT_CCY, 6)) "CarryingValue"
                    FROM IFRS_NOMINATIVE C
                    JOIN IFRS_MASTER_BRANCH D
                    ON C.BRANCH_CODE = D.BRANCH_NUM
                    AND C.REPORT_DATE = ''' || TO_CHAR(v_downloadDate) || '''
                    AND C.DATA_SOURCE = ''ILS''
                    GROUP BY C.SUB_SEGMENT, C.GOL_DEB, C.STAGE,
                    C.BTB_FLAG, C.ASSESSMENT_IMP, C.BUCKET_NAME,
                    D.BRANCH_NUM || ''_'' || D.BRANCH_NAME';
    ELSE

        v_Query :=  'SELECT
                    C.SUB_SEGMENT "ImpairmentSegment",
                    C.GOL_DEB "GolonganDebitur",
                    C.STAGE "Stage",
                    C.BTB_FLAG "BackToBackFlag",
                    C.ASSESSMENT_IMP "ImpairedFlag",
                    C.BUCKET_NAME "BucketName",
                    D.BRANCH_NUM || ''_'' || D.BRANCH_NAME "BranchCode",
                    SUM(ROUND(C.OUTSTANDING_PRINCIPAL_CCY, 6)) "Outstanding",
                    SUM((CASE WHEN C.STAGE = ''1'' THEN ROUND(C.STAGE_1_CCY, 6)
                        ELSE 0 END)) AS "ECL12Months" ,
                    SUM((CASE WHEN C.STAGE = ''2'' THEN ROUND(C.STAGE_2_CCY, 6)
                        ELSE 0 END)) AS "ECLLifetimeStage2" ,
                    SUM((CASE WHEN C.STAGE = ''3'' THEN ROUND(C.STAGE_3_CCY, 6)
                        ELSE 0 END)) AS "ECLLifetimeStage3" ,
                    SUM((CASE WHEN C.ASSESSMENT_IMP = ''I'' THEN ROUND(C.ECL_INDIVIDUAL_CCY, 6)
                        ELSE 0 END)) AS "ECLIndividual" ,
                    SUM((CASE WHEN C.ASSESSMENT_IMP = ''C'' THEN ROUND(C.ECL_COLLECTIVE_CCY, 6)
                        ELSE 0 END)) AS "ECLCollective" ,
                    SUM((CASE WHEN C.ASSESSMENT_IMP = ''W'' THEN ROUND(C.ECL_WORSTCASE_CCY, 6)
                        ELSE 0 END)) AS "ECLWorstCase" ,
                    SUM(ROUND(C.CARRYING_AMOUNT_CCY, 6)) "CarryingValue"
                    FROM IFRS_NOMINATIVE_ACV C
                    JOIN IFRS_MASTER_BRANCH D
                    ON C.BRANCH_CODE = D.BRANCH_NUM
                    AND C.REPORT_DATE = ''' || TO_CHAR(v_downloadDate) || '''
                    AND C.DATA_SOURCE = ''ILS''
                    GROUP BY C.SUB_SEGMENT, C.GOL_DEB, C.STAGE,
                    C.BTB_FLAG, C.ASSESSMENT_IMP, C.BUCKET_NAME,
                    D.BRANCH_NUM || ''_'' || D.BRANCH_NAME';
    END IF;
   OPEN Cur_out FOR v_query;

END;