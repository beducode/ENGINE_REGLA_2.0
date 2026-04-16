CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_BUCKET_DETAIL (
    P_RUNID         IN VARCHAR2 DEFAULT 'SP_IFRS_EIL_MODEL_DETAIL_PD',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_EIL_MODEL_DETAIL_PD';
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
            SELECT CURRDATE INTO V_CURRDATE FROM PSAK413.IFRS_PRC_DATE;
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
    V_TABLEINSERT1 := 'IFRS_EIL_MODEL_DETAIL_PD';


    PSAK413.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;
     
    
    ---- PUT YOUR MAIN CODE HERE (E.G. MERGE STATEMENT)
    -----------------------------------------------------------------
    
	MERGE INTO IFRS_EIL_MODEL_DETAIL_PD a
			USING (
                SELECT *
                FROM (
                    SELECT
                        A."pkid",
                        B.PKID AS EIL_MODEL_ID,
                        C.PKID AS SEGMENTATION_ID,
                        D.PKID AS PD_RULE_ID,
                        NVL(A."workflow_header_draft_pkid",0) AS PD_MODEL_ID,
                        A."effective_date",
                        A."pd_date",
                        A."is_override_pd",
                        A."is_deleted",

                        ROW_NUMBER() OVER (
                            PARTITION BY B.PKID, C.PKID
                            ORDER BY A."pkid"
                        ) rn
                    FROM "EilPdModel"@DBCONFIGLINK A
                    INNER JOIN "Segmentation"@DBCONFIGLINK seg 
                        ON A."code_segmentation" = seg."syscode_segmentation"
                    INNER JOIN IFRS_SEGMENTATION_MAPPING C 
                        ON CASE 
                            WHEN seg."level" = 1 THEN C.SYSCODE_SEGMENTATION_LV1 
                            WHEN seg."level" = 2 THEN C.SYSCODE_SEGMENTATION_LV2 
                            WHEN seg."level" = 3 THEN C.SYSCODE_SEGMENTATION_LV3 
                        END = A."code_segmentation"
                    INNER JOIN IFRS_EIL_MODEL_HEADER B 
                        ON A."syscode_eil_configuration" = B.SYSCODE_EIL
                    INNER JOIN IFRS_PD_RULES_CONFIG D 
                        ON A."code_pd_configuration" = D.SYSCODE_PD
                )
                WHERE rn = 1
            ) b
			ON ( a.EIL_MODEL_ID = b.EIL_MODEL_ID AND A.SEGMENTATION_ID = B.SEGMENTATION_ID)  
			
			WHEN MATCHED THEN
				UPDATE SET  
					a.IS_OVERRIDE			 = b."is_override_pd",
					a.UPDATEDBY              = 'SP_IFRS_EIL_CONFIG_MAPPING_3',
					a.UPDATEDDATE            = sysdate,
					a.IS_DELETE              = b."is_deleted",
					a.PD_MODEL_ID			 = b.PD_MODEL_ID
		
			WHEN NOT MATCHED THEN
				INSERT (
					EIL_MODEL_ID, SEGMENTATION_ID,
					PD_RULE_ID,PD_MODEL_ID,EFF_DATE_OPTION,EFF_DATE ,IS_OVERRIDE,IS_DELETE,  
					UPDATEDBY, UPDATEDDATE
				)
				VALUES (
					b.EIL_MODEL_ID, b.SEGMENTATION_ID,
					b.PD_RULE_ID,b.PD_MODEL_ID,b."effective_date",b."pd_date",b."is_override_pd",nvl(b."is_deleted",0), 'SP_IFRS_EIL_CONFIG_MAPPING_3', sysdate 
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

    PSAK413.SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SP_NAME, V_OPERATION, NVL(V_RETURNROWS2,0), V_RUNID);
    COMMIT;

    ----------------------------------------------------------------
    -- RESULT PREVIEW
    ----------------------------------------------------------------
    V_QUERYS := 'SELECT * FROM ' || V_OWNER || '.' || V_TABLEINSERT1;

    PSAK413.SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SP_NAME, NVL(V_RETURNROWS2,0), V_RUNID);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;

        RAISE_APPLICATION_ERROR(-20001,'ERROR IN ' || V_SP_NAME || ' : ' || SQLERRM);
END;