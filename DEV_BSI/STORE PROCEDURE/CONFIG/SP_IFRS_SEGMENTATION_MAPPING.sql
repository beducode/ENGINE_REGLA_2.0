CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_SEGMENTATION_MAPPING (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_SEGMENTATION_MAPPING';
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
    V_MAXLV         NUMBER(2,0);
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
    V_TABLEINSERT1 := 'IFRS_SEGMENTATION_MAPPING';


    PSAK413.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;
     
    
    ---- PUT YOUR MAIN CODE HERE (E.G. MERGE STATEMENT)
    -----------------------------------------------------------------
    
        SELECT TO_NUMBER("value")
        into V_MAXLV
        FROM "MstConfigParams"@DBCONFIGLINK
        WHERE "api_project_name" = 'PSAK413_Impairment' AND "name" = 'MaxSegmentLevelPsak413' ;
        
        MERGE INTO IFRS_SEGMENTATION_MAPPING a
        USING (
                
        selecT "pkid",
            "syscode_segmentation_lv1", "segment_name_lv1",
            "syscode_segmentation_lv2", "segment_name_lv2",
            "syscode_segmentation_lv3", "segment_name_lv3",
            "syscode_segmentation_lv4", "segment_name_lv4",
            "syscode_segmentation_lv5", "segment_name_lv5",
            "syscode_segmentation_lv6", "segment_name_lv6",
            "syscode_segmentation_lv7", "segment_name_lv7",
            "syscode_segmentation_lv8", "segment_name_lv8",
            "syscode_segmentation_lv9", "segment_name_lv9",
            "syscode_segmentation_lv10", "segment_name_lv10",
            "is_deleted","merge_sql_conditions",
            CASE WHEN V_MAXLV = 1 THEN "syscode_segmentation_lv1"
                WHEN V_MAXLV = 2 THEN "syscode_segmentation_lv2"
                WHEN V_MAXLV = 3 THEN "syscode_segmentation_lv3"
                WHEN V_MAXLV = 4 THEN "syscode_segmentation_lv4"
                WHEN V_MAXLV = 5 THEN "syscode_segmentation_lv5"
                WHEN V_MAXLV = 6 THEN "syscode_segmentation_lv6"
                WHEN V_MAXLV = 7 THEN "syscode_segmentation_lv7"
                WHEN V_MAXLV = 8 THEN "syscode_segmentation_lv8"
                WHEN V_MAXLV = 9 THEN "syscode_segmentation_lv9"
                WHEN V_MAXLV = 10 THEN "syscode_segmentation_lv10"
            END AS "syscode_segmentation",
            CASE WHEN "segment_type" = 'ALL' THEN 'PORTFOLIO'
            ELSE 
            "segment_type"
            END AS "segment_type"
        from "SegmentationMapping"@DBCONFIGLINK a 
        where "pkid" in (
            select max("pkid")
            from "SegmentationMapping"@DBCONFIGLINK 
            group by "syscode_segmentation_lv1",
                nvl("syscode_segmentation_lv2",' '),
                nvl("syscode_segmentation_lv3",' '),
                nvl("syscode_segmentation_lv4",' '),
                nvl("syscode_segmentation_lv5",' '),
                nvl("syscode_segmentation_lv6",' '),
                nvl("syscode_segmentation_lv7",' '),
                nvl("syscode_segmentation_lv8",' '),
                nvl("syscode_segmentation_lv9",' '),
                nvl("syscode_segmentation_lv10", ' '),
                "segment_type")
        ) b
        ON (a.SYSCODE_SEGMENTATION_LV1 = b."syscode_segmentation_lv1"
            and nvl(a.SYSCODE_SEGMENTATION_LV2,' ') = nvl(b."syscode_segmentation_lv2",' ')
            and nvl(a.SYSCODE_SEGMENTATION_LV3,' ') = nvl(b."syscode_segmentation_lv3",' ')
            and nvl(a.SYSCODE_SEGMENTATION_LV4,' ') = nvl(b."syscode_segmentation_lv4",' ')
            and nvl(a.SYSCODE_SEGMENTATION_LV5,' ') = nvl(b."syscode_segmentation_lv5",' ')
            and nvl(a.SYSCODE_SEGMENTATION_LV6,' ') = nvl(b."syscode_segmentation_lv6",' ')
            and nvl(a.SYSCODE_SEGMENTATION_LV7,' ') = nvl(b."syscode_segmentation_lv7",' ')
            and nvl(a.SYSCODE_SEGMENTATION_LV8,' ') = nvl(b."syscode_segmentation_lv8",' ')
            and nvl(a.SYSCODE_SEGMENTATION_LV9,' ') = nvl(b."syscode_segmentation_lv9",' ')
            and nvl(a.SYSCODE_SEGMENTATION_LV10,' ') = nvl(b."syscode_segmentation_lv10" ,' ')
            )  
        
        WHEN MATCHED THEN
            UPDATE SET  
                a.MERGE_SQL_CONDITIONS    = b."merge_sql_conditions",
                a.UPDATED_BY              = 'SP_IFRS_SEGMENTATION_MAPPING',
                a.UPDATED_DATE            = sysdate,
                a.IS_DELETED              = b."is_deleted"
    
        WHEN NOT MATCHED THEN
            INSERT (
                SYSCODE_SEGMENTATION_LV1, SEGMENT_NAME_LV1,
                SYSCODE_SEGMENTATION_LV2, SEGMENT_NAME_LV2,
                SYSCODE_SEGMENTATION_LV3, SEGMENT_NAME_LV3,
                SYSCODE_SEGMENTATION_LV4, SEGMENT_NAME_LV4,
                SYSCODE_SEGMENTATION_LV5, SEGMENT_NAME_LV5,
                SYSCODE_SEGMENTATION_LV6, SEGMENT_NAME_LV6,
                SYSCODE_SEGMENTATION_LV7, SEGMENT_NAME_LV7,
                SYSCODE_SEGMENTATION_LV8, SEGMENT_NAME_LV8,
                SYSCODE_SEGMENTATION_LV9, SEGMENT_NAME_LV9,
                SYSCODE_SEGMENTATION_LV10, SEGMENT_NAME_LV10,
                MERGE_SQL_CONDITIONS,
                CREATED_BY, CREATED_DATE, 
                IS_DELETED,
                SYSCODE_SEGMENTATION, SEGMENT_TYPE
            )
            VALUES (
                b."syscode_segmentation_lv1", b."segment_name_lv1",
                b."syscode_segmentation_lv2", b."segment_name_lv2",
                b."syscode_segmentation_lv3", b."segment_name_lv3",
                b."syscode_segmentation_lv4", b."segment_name_lv4",
                b."syscode_segmentation_lv5", b."segment_name_lv5",
                b."syscode_segmentation_lv6", b."segment_name_lv6",
                b."syscode_segmentation_lv7", b."segment_name_lv7",
                b."syscode_segmentation_lv8", b."segment_name_lv8",
                b."syscode_segmentation_lv9", b."segment_name_lv9",
                b."syscode_segmentation_lv10", b."segment_name_lv10",
                b."merge_sql_conditions",
                'SP_IFRS_SEGMENTATION_MAPPING', sysdate, 
                nvl(b."is_deleted",0),
                b."syscode_segmentation",nvl(b."segment_type",'PORTFOLIO')
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