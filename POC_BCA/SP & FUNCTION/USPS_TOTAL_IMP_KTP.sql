CREATE OR REPLACE PROCEDURE USPS_TOTAL_IMP_KTP
(
    v_downloadDate DATE,
    v_sortParameter VARCHAR2,
    Cur_out OUT SYS_REFCURSOR
)
AS
    v_Query VARCHAR2(4000);
    v_sortParameter2 VARCHAR2(2000);
    v_DiffDate NUMBER;
    BEGIN

    SELECT TO_NUMBER(MONTHS_BETWEEN (A.CURRDATE, TO_DATE(TO_CHAR(v_downloadDate,'dd MON yyyy')))) INTO v_DiffDate FROM IFRS_PRC_DATE A;

        IF v_sortParameter IS NULL THEN
            v_sortParameter2 := '"ImpairmentSegment"';
        ELSE
            v_sortParameter2 := v_sortParameter;
        END IF;

        IF(v_DiffDate <= 3) THEN
            v_Query := 'SELECT
                        C.SUB_SEGMENT "ImpairmentSegment",
                        C.STAGE "Stage",
                        C.RATING_CODE "Rating",
                        D.BRANCH_NUM || ''_'' || D.BRANCH_NAME "BranchCode",
                        ROUND(C.OUTSTANDING_PRINCIPAL_CCY, 6) "Outstanding",
                        (CASE WHEN C.STAGE = ''1'' THEN ROUND(C.STAGE_1_CCY, 6)
                            ELSE 0 END) AS "ECL12Months" ,
                        (CASE WHEN C.STAGE = ''2'' THEN ROUND(C.STAGE_2_CCY, 6)
                            ELSE 0 END) AS "ECLLifetimeStage2" ,
                        (CASE WHEN C.STAGE = ''3'' THEN ROUND(C.STAGE_3_CCY, 6)
                            ELSE 0 END) AS "ECLLifetimeStage3" ,
                        (CASE WHEN C.ASSESSMENT_IMP = ''I'' THEN ROUND(C.ECL_INDIVIDUAL_CCY, 6)
                            ELSE 0 END) AS "ECLIndividual" ,
                        (CASE WHEN C.ASSESSMENT_IMP = ''C'' THEN ROUND(C.ECL_COLLECTIVE_CCY, 6)
                            ELSE 0 END) AS "ECLCollective" ,
                        (CASE WHEN C.ASSESSMENT_IMP = ''W'' THEN ROUND(C.ECL_WORSTCASE_CCY, 6)
                            ELSE 0 END) AS "ECLWorstCase" ,
                        ROUND(C.CARRYING_AMOUNT_CCY, 6) "CarryingValue"
                        FROM IFRS_NOMINATIVE C
                        JOIN IFRS_MASTER_BRANCH D
                        ON C.BRANCH_CODE = D.BRANCH_NUM
                        AND D.DOWNLOAD_DATE = ''' || CASE WHEN TO_CHAR(v_downloadDate , 'DAY') = 'SUNDAY   'THEN TO_CHAR(TO_DATE(v_downloadDate, 'yyyy-mm-dd') - 2)
                        WHEN TO_CHAR(v_downloadDate , 'DAY') = 'SATURDAY ' THEN TO_CHAR(TO_DATE(v_downloadDate, 'yyyy-mm-dd') - 1)  ELSE TO_CHAR(v_downloadDate) END  || '''
                        AND C.REPORT_DATE = ''' || TO_CHAR(v_downloadDate) || '''
                        AND C.DATA_SOURCE IN (''PBMM'',''KTP'')
                        ORDER BY ' || v_sortParameter2 || '';
        ELSE
            v_Query := 'SELECT
                        C.SUB_SEGMENT "ImpairmentSegment",
                        C.STAGE "Stage",
                        C.RATING_CODE "Rating",
                        D.BRANCH_NUM || ''_'' || D.BRANCH_NAME "BranchCode",
                        ROUND(C.OUTSTANDING_PRINCIPAL_CCY, 6) "Outstanding",
                        (CASE WHEN C.STAGE = ''1'' THEN ROUND(C.STAGE_1_CCY, 6)
                            ELSE 0 END) AS "ECL12Months" ,
                        (CASE WHEN C.STAGE = ''2'' THEN ROUND(C.STAGE_2_CCY, 6)
                            ELSE 0 END) AS "ECLLifetimeStage2" ,
                        (CASE WHEN C.STAGE = ''3'' THEN ROUND(C.STAGE_3_CCY, 6)
                            ELSE 0 END) AS "ECLLifetimeStage3" ,
                        (CASE WHEN C.ASSESSMENT_IMP = ''I'' THEN ROUND(C.ECL_INDIVIDUAL_CCY, 6)
                            ELSE 0 END) AS "ECLIndividual" ,
                        (CASE WHEN C.ASSESSMENT_IMP = ''C'' THEN ROUND(C.ECL_COLLECTIVE_CCY, 6)
                            ELSE 0 END) AS "ECLCollective" ,
                        (CASE WHEN C.ASSESSMENT_IMP = ''W'' THEN ROUND(C.ECL_WORSTCASE_CCY, 6)
                            ELSE 0 END) AS "ECLWorstCase" ,
                        ROUND(C.CARRYING_AMOUNT_CCY, 6) "CarryingValue"
                        FROM IFRS_NOMINATIVE_ACV C
                        JOIN IFRS_MASTER_BRANCH D
                        ON C.BRANCH_CODE = D.BRANCH_NUM
                        AND D.DOWNLOAD_DATE = ''' || CASE WHEN TO_CHAR(v_downloadDate , 'DAY') = 'SUNDAY   'THEN TO_CHAR(TO_DATE(v_downloadDate, 'yyyy-mm-dd') - 2)
                        WHEN TO_CHAR(v_downloadDate , 'DAY') = 'SATURDAY ' THEN TO_CHAR(TO_DATE(v_downloadDate, 'yyyy-mm-dd') - 1)  ELSE TO_CHAR(v_downloadDate) END  || '''
                        AND C.REPORT_DATE = ''' || TO_CHAR(v_downloadDate) || '''
                        AND C.DATA_SOURCE IN (''PBMM'',''KTP'')
                        ORDER BY ' || v_sortParameter2 || '';
        END IF;

   OPEN Cur_out FOR v_query;

END;