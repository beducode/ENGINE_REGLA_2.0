CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_MSTR_SEGMENT_RULES_HEADER (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_MSTR_SEGMENT_RULES_HEADER';
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
    V_TABLEINSERT1 := 'GTMP_IFRS_MSTR_SEGMENT_RULES_HEADER';


    IFRS9_BCA.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;
     
    
    ---- PUT YOUR MAIN CODE HERE (E.G. MERGE STATEMENT)
    -----------------------------------------------------------------
    
    MERGE INTO IFRS9_BCA.GTMP_IFRS_MSTR_SEGMENT_RULES_HEADER a
    USING (
        SELECT * 
        FROM (
            SELECT
                PKID,
                SYSCODE_SEGMENTATION,
                SEGMENT_NAME_LV1        AS GROUP_SEGMENT,
                SEGMENT_NAME_LV2        AS SEGMENT,
                SEGMENT_NAME_LV3        AS SUB_SEGMENT,
                SEGMENT_TYPE,
                MERGE_SQL_CONDITIONS    AS SQL_CONDITIONS,
                1                       AS IS_NEW,
                NVL(IS_DELETED,0)       AS IS_DELETED,
                ROW_NUMBER() OVER (
                    PARTITION BY SYSCODE_SEGMENTATION 
                    ORDER BY PKID DESC
                ) AS RN
            FROM IFRS_SEGMENTATION_MAPPING
        ) x
        WHERE x.RN = 1
    ) b
    ON (a.SYSCODE_SEGMENTATION = b.SYSCODE_SEGMENTATION)

    WHEN MATCHED THEN
        UPDATE SET   
            a.GROUP_SEGMENT   = b.GROUP_SEGMENT,
            a.SEGMENT         = b.SEGMENT,
            a.SUB_SEGMENT     = b.SUB_SEGMENT,
            a.SEGMENT_TYPE    = b.SEGMENT_TYPE,
            a.SQL_CONDITIONS  = b.SQL_CONDITIONS,
            a.IS_NEW          = b.IS_NEW,
            a.IS_DELETED      = b.IS_DELETED,
            a.UPDATEDBY       = 'SP_IFRS_MSTR_SEGMENT_RULES_HEADER',
            a.UPDATEDDATE     = SYSDATE,
            a.UPDATEDHOST     = 'LOCALHOST'

    WHEN NOT MATCHED THEN
        INSERT (
            SYSCODE_SEGMENTATION,
            GROUP_SEGMENT,
            SEGMENT,
            SUB_SEGMENT,
            SEGMENT_TYPE,
            SQL_CONDITIONS,
            IS_NEW,
            IS_DELETED,
            CREATEDBY,
            CREATEDDATE,
            CREATEDHOST
        )
        VALUES (
            b.SYSCODE_SEGMENTATION,
            b.GROUP_SEGMENT,
            b.SEGMENT,
            b.SUB_SEGMENT,
            b.SEGMENT_TYPE,
            b.SQL_CONDITIONS,
            1,
            b.IS_DELETED,
            'SYSTEM',
            SYSDATE,
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