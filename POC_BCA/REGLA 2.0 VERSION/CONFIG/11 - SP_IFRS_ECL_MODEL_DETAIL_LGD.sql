CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_ECL_MODEL_DETAIL_LGD (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_ECL_MODEL_DETAIL_LGD';
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
    V_TABLEINSERT1 := 'GTMP_IFRS_ECL_MODEL_DETAIL_LGD';


    IFRS9_BCA.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;
     
    
    ---- PUT YOUR MAIN CODE HERE (E.G. MERGE STATEMENT)
    -----------------------------------------------------------------
    
	MERGE INTO IFRS9_BCA.GTMP_IFRS_ECL_MODEL_DETAIL_LGD a
    USING (
        SELECT 
            A."code_lgd_configuration" AS SYSCODE_LGD,
            B.PKID AS ECL_MODEL_ID,
            C.PKID AS PF_SEGMENT_ID,
            NULL   AS FL_MODEL_ID,
            D.PKID AS LGD_MODEL_ID,
            TO_CHAR(A."effective_date",'YYYYMM') AS LGD_EFF_PERIOD,
            NVL(A."is_override_lgd",0) AS FL_FLAG
        FROM "NTT_IMPAIRMENT2"."EclLgdModel"@DBCONFIGLINK A
        INNER JOIN "Segmentation"@DBCONFIGLINK seg 
            ON A."code_segmentation" = seg."syscode_segmentation"
        INNER JOIN IFRS_SEGMENTATION_MAPPING C 
            ON CASE 
                WHEN seg."level" = 1 THEN C.SYSCODE_SEGMENTATION_LV1 
                WHEN seg."level" = 2 THEN C.SYSCODE_SEGMENTATION_LV2 
                WHEN seg."level" = 3 THEN C.SYSCODE_SEGMENTATION_LV3 
            END = A."code_segmentation"
        INNER JOIN GTMP_IFRS_ECL_MODEL_HEADER B 
            ON A."syscode_ecl_configuration" = B.SYSCODE_ECL
        INNER JOIN GTMP_IFRS_LGD_RULES_CONFIG D 
            ON A."code_lgd_configuration" = D.SYSCODE_LGD
    ) b
    ON (
        a.SYSCODE_LGD = b.SYSCODE_LGD
        AND a.ECL_MODEL_ID = b.ECL_MODEL_ID
        AND a.PF_SEGMENT_ID = b.PF_SEGMENT_ID
    )

    WHEN MATCHED THEN
        UPDATE SET  
            a.LGD_MODEL_ID   = b.LGD_MODEL_ID,
            a.FL_MODEL_ID    = b.FL_MODEL_ID,
            a.LGD_EFF_PERIOD = b.LGD_EFF_PERIOD,
            a.FL_FLAG        = b.FL_FLAG,
            a.UPDATEDBY      = 'SP_IFRS_ECL_CONFIG_MAPPING_2',
            a.UPDATEDDATE    = SYSTIMESTAMP,
            a.UPDATEDHOST    = 'LOCALHOST'

    WHEN NOT MATCHED THEN
        INSERT (
            SYSCODE_LGD,
            ECL_MODEL_ID,
            PF_SEGMENT_ID,
            FL_MODEL_ID,
            LGD_MODEL_ID,
            LGD_EFF_PERIOD,
            FL_FLAG,
            CREATEDBY,
            CREATEDDATE,
            CREATEDHOST
        )
        VALUES (
            b.SYSCODE_LGD,
            b.ECL_MODEL_ID,
            b.PF_SEGMENT_ID,
            b.FL_MODEL_ID,
            b.LGD_MODEL_ID,
            b.LGD_EFF_PERIOD,
            b.FL_FLAG,
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