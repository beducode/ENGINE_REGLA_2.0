CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_ECL_MODEL_DETAIL_PD (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_ECL_MODEL_DETAIL_PD';
    V_OWNER       VARCHAR2(30);
    V_CURRDATE      DATE;

    -- TABLE NAMES (UNQUALIFIED PARTS)
    V_TABLEINSERT1  VARCHAR2(100);


    -- MISC
    V_RETURNROWS2   NUMBER := 0;
    V_TABLEDEST     VARCHAR2(100);
    V_COLUMNDEST    VARCHAR2(100);
    V_OPERATION     VARCHAR2(100);
    V_RUNID        VARCHAR2(30);

    -- RESULT QUERY
    V_QUERYS        CLOB;
BEGIN

    ----------------------------------------------------------------
    -- GET OWNER
    ----------------------------------------------------------------
    SELECT USERNAME INTO V_OWNER FROM USER_USERS;

    ----------------------------------------------------------------
    -- INSERT VCURRDATE DETERMINATION IF NULL
    ----------------------------------------------------------------
    IF P_DOWNLOAD_DATE IS NULL THEN
        BEGIN
            SELECT CURRDATE INTO V_CURRDATE FROM IFRS9_BCA.IFRS_PRC_DATE;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE_APPLICATION_ERROR(-20010, 'IFRS_PRC_DATE has no CURRDATE row');
        END;
    ELSE
        V_CURRDATE := P_DOWNLOAD_DATE;
    END IF;

    V_RUNID := NVL(P_RUNID, 'P_00000_0000');

    ----------------------------------------------------------------
    -- TABLE DETERMINATION
    ----------------------------------------------------------------
    V_TABLEINSERT1 := 'GTMP_IFRS_ECL_MODEL_DETAIL_PD';


    IFRS9_BCA.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;
     
    
    ---- PUT YOUR MAIN CODE HERE (E.G. MERGE STATEMENT)
    -----------------------------------------------------------------
    
	MERGE INTO IFRS9_BCA.GTMP_IFRS_ECL_MODEL_DETAIL_PD a
    USING (
        SELECT
            SYSCODE_PD,
            ECL_MODEL_ID,
            PF_SEGMENT_ID,
            PD_MODEL_ID,
            FL_MODEL_ID,
            PD_EFF_PERIOD,
            PD_DATE
        FROM (
            SELECT
                A."code_pd_configuration" AS SYSCODE_PD,

                CAST(B.PKID AS NUMBER(22)) AS ECL_MODEL_ID,

                CAST(C.PKID AS NUMBER(22)) AS PF_SEGMENT_ID,

                TO_NUMBER(
                    TRIM(TO_CHAR(A."workflow_header_draft_pkid"))
                    DEFAULT 0 ON CONVERSION ERROR
                ) AS PD_MODEL_ID,

                CAST(NULL AS NUMBER(22)) AS FL_MODEL_ID,

                CASE
                    WHEN REGEXP_LIKE(
                        SUBSTR(TRIM(TO_CHAR(A."effective_date")), 1, 10),
                        '^[1-9][0-9]{3}-[0-9]{2}-[0-9]{2}$'
                    )
                    THEN SUBSTR(
                        TRIM(TO_CHAR(A."effective_date")),
                        1,
                        10
                    )
                    ELSE NULL
                END AS PD_EFF_PERIOD,

                CASE
                    WHEN REGEXP_LIKE(
                        SUBSTR(TRIM(TO_CHAR(A."pd_date")), 1, 10),
                        '^[1-9][0-9]{3}-[0-9]{2}-[0-9]{2}$'
                    )
                    THEN TO_DATE(
                        SUBSTR(TRIM(TO_CHAR(A."pd_date")), 1, 10),
                        'YYYY-MM-DD'
                    )
                    ELSE NULL
                END AS PD_DATE,

                ROW_NUMBER() OVER (
                    PARTITION BY B.PKID, C.PKID
                    ORDER BY TO_CHAR(A."pkid") DESC
                ) rn

            FROM "NTT_IMPAIRMENT2"."EclPdModel"@DBCONFIGLINK A

            INNER JOIN "Segmentation"@DBCONFIGLINK seg
                ON TRIM(TO_CHAR(A."code_segmentation"))
                = TRIM(TO_CHAR(seg."syscode_segmentation"))

            INNER JOIN IFRS9_BCA.IFRS_SEGMENTATION_MAPPING C
                ON TRIM(
                    TO_CHAR(
                        CASE
                            WHEN seg."level" = 1 THEN C.SYSCODE_SEGMENTATION_LV1
                            WHEN seg."level" = 2 THEN C.SYSCODE_SEGMENTATION_LV2
                            WHEN seg."level" = 3 THEN C.SYSCODE_SEGMENTATION_LV3
                        END
                    )
                ) = TRIM(TO_CHAR(A."code_segmentation"))

            INNER JOIN IFRS9_BCA.GTMP_IFRS_ECL_MODEL_HEADER B
                ON TRIM(TO_CHAR(A."syscode_ecl_configuration"))
                = TRIM(TO_CHAR(B.SYSCODE_ECL))
        )
        WHERE rn = 1
    ) b
    ON (
        a.ECL_MODEL_ID = b.ECL_MODEL_ID
        AND a.PF_SEGMENT_ID = b.PF_SEGMENT_ID
    )

    WHEN MATCHED THEN
        UPDATE SET
            a.SYSCODE_PD    = b.SYSCODE_PD,
            a.PD_MODEL_ID   = b.PD_MODEL_ID,
            a.FL_MODEL_ID   = b.FL_MODEL_ID,
            a.PD_EFF_PERIOD = b.PD_EFF_PERIOD,
            a.PD_DATE       = b.PD_DATE,
            a.UPDATEDBY     = 'GTMP_IFRS_ECL_MODEL_DETAIL_PD',
            a.UPDATEDDATE   = SYSTIMESTAMP,
            a.UPDATEDHOST   = 'LOCALHOST'

    WHEN NOT MATCHED THEN
        INSERT (
            SYSCODE_PD,
            ECL_MODEL_ID,
            PD_MODEL_ID,
            PF_SEGMENT_ID,
            FL_MODEL_ID,
            PD_EFF_PERIOD,
            PD_DATE,
            CREATEDBY,
            CREATEDDATE,
            CREATEDHOST
        )
        VALUES (
            b.SYSCODE_PD,
            b.ECL_MODEL_ID,
            b.PD_MODEL_ID,
            b.PF_SEGMENT_ID,
            b.FL_MODEL_ID,
            b.PD_EFF_PERIOD,
            b.PD_DATE,
            'SYSTEM',
            SYSTIMESTAMP,
            'LOCALHOST'
        );
	        
	COMMIT;

    ------------------------------------------------------------------

    DBMS_OUTPUT.PUT_LINE('PROCEDURE ' || V_SP_NAME || ' EXECUTED SUCCESSFULLY.');

    ----------------------------------------------------------------
    -- LOG: CALL EXEC_AND_LOG (ASSUMED SIGNATURE)
    ----------------------------------------------------------------
    V_TABLEDEST := V_OWNER || '.' || V_TABLEINSERT1;
    V_COLUMNDEST := '-';
    V_OPERATION := 'INSERT';

    IFRS9_BCA.SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SP_NAME, V_OPERATION, NVL(V_RETURNROWS2,0), V_RUNID);
    COMMIT;

    ----------------------------------------------------------------
    -- RESULT PREVIEW
    ----------------------------------------------------------------
    V_QUERYS := 'SELECT * FROM ' || V_OWNER || '.' || V_TABLEINSERT1;

    IFRS9_BCA.SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SP_NAME, NVL(V_RETURNROWS2,0), V_RUNID);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;

        RAISE_APPLICATION_ERROR(-20001,'ERROR IN ' || V_SP_NAME || ' : ' || SQLERRM);
END;