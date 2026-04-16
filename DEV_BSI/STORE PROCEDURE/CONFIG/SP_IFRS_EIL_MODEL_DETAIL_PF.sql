CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_EIL_MODEL_DETAIL_PF (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_EIL_MODEL_DETAIL_PF';
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
    V_TABLEINSERT1 := 'IFRS_EIL_MODEL_DETAIL_PF';


    PSAK413.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;
     
    
    ---- PUT YOUR MAIN CODE HERE (E.G. MERGE STATEMENT)
    -----------------------------------------------------------------
    
	MERGE INTO IFRS_EIL_MODEL_DETAIL_PF a
			USING (
			
				selecT A."pkid",
					B.PKID AS EIL_MODEL_ID ,C.PKID AS SEGMENTATION_ID,
					D.PKID AS LT_RULE_ID, A."is_deleted"
					from "EilPortfolio"@DBCONFIGLINK A
					INNER JOIN "Segmentation"@DBCONFIGLINK seg ON A."code_segmentation" = seg."syscode_segmentation"
					INNER JOIN IFRS_SEGMENTATION_MAPPING C on CASE WHEN seg."level" = 1 THEN C.SYSCODE_SEGMENTATION_LV1 
																WHEN seg."level" = 2 THEN C.SYSCODE_SEGMENTATION_LV2 
																WHEN seg."level" = 3 THEN C.SYSCODE_SEGMENTATION_LV3 END = A."code_segmentation"
					INNER JOIN IFRS_EIL_MODEL_HEADER B on A."syscode_eil_configuration" = B.SYSCODE_EIL
					LEFT  JOIN IFRS_LIFETIME_RULES_CONFIG D on C.PKID  = D.SEGMENTATION_ID
					
				) b
				ON ( a.EIL_MODEL_ID = b.EIL_MODEL_ID AND A.SEGMENTATION_ID = B.SEGMENTATION_ID) 
				WHEN MATCHED THEN
					UPDATE SET  
						a.UPDATEDBY              = 'SP_IFRS_EIL_CONFIG_MAPPING_4',
						a.UPDATEDDATE            = sysdate,
						a.IS_DELETE              = b."is_deleted"
				WHEN NOT MATCHED THEN
					INSERT (
						EIL_MODEL_ID, SEGMENTATION_ID,
						LT_RULE_ID, IS_DELETE,
						UPDATEDBY, UPDATEDDATE
					)
					VALUES (
						b.EIL_MODEL_ID, b.SEGMENTATION_ID,
						b.LT_RULE_ID,b."is_deleted", 'SP_IFRS_EIL_CONFIG_MAPPING_4', sysdate 
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