CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_SYNC_PARAM (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_SYNC_PARAM';
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
    V_TABLEINSERT1 := 'SEGMENTATIONMAPPING';


    PSAK413.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;
     
    
    ---- PUT YOUR MAIN CODE HERE (E.G. MERGE STATEMENT)
    -----------------------------------------------------------------
    
	EXECUTE IMMEDIATE 'TRUNCATE TABLE SEGMENTATIONMAPPING';
	
	INSERT INTO SEGMENTATIONMAPPING 
	(
		"pkid"
		,"syscode_segmentation_lv1"
		,"segment_name_lv1"
		,"syscode_segmentation_lv2"
		,"segment_name_lv2"
		,"syscode_segmentation_lv3"
		,"segment_name_lv3"
		,"syscode_segmentation_lv4"
		,"segment_name_lv4"
		,"syscode_segmentation_lv5"
		,"segment_name_lv5"
		,"syscode_segmentation_lv6"
		,"segment_name_lv6"
		,"syscode_segmentation_lv7"
		,"segment_name_lv7"
		,"syscode_segmentation_lv8"
		,"segment_name_lv8"
		,"syscode_segmentation_lv9"
		,"segment_name_lv9"
		,"syscode_segmentation_lv10"
		,"segment_name_lv10"
		,"merge_sql_conditions"
		,"created_by"
		,"created_date"
		,"created_host"
		,"updated_by"
		,"updated_date"
		,"updated_host"
		,"is_deleted"
		,"deleted_by"
		,"deleted_date"
		,"deleted_host"

	)	
	SELECT
		"pkid"
		,"syscode_segmentation_lv1"
		,"segment_name_lv1"
		,"syscode_segmentation_lv2"
		,"segment_name_lv2"
		,"syscode_segmentation_lv3"
		,"segment_name_lv3"
		,"syscode_segmentation_lv4"
		,"segment_name_lv4"
		,"syscode_segmentation_lv5"
		,"segment_name_lv5"
		,"syscode_segmentation_lv6"
		,"segment_name_lv6"
		,"syscode_segmentation_lv7"
		,"segment_name_lv7"
		,"syscode_segmentation_lv8"
		,"segment_name_lv8"
		,"syscode_segmentation_lv9"
		,"segment_name_lv9"
		,"syscode_segmentation_lv10"
		,"segment_name_lv10"
		,"merge_sql_conditions"
		,"created_by"
		,"created_date"
		,"created_host"
		,"updated_by"
		,"updated_date"
		,"updated_host"
		,"is_deleted"
		,"deleted_by"
		,"deleted_date"
		,"deleted_host"
	FROM "SegmentationMapping"@DBCONFIGLINK;
	
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