CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_BUCKET_HEADER (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_BUCKET_HEADER';
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
    V_TABLEINSERT1 := 'GTMP_IFRS_BUCKET_HEADER';


    IFRS9_BCA.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;
     
    
    ---- PUT YOUR MAIN CODE HERE (E.G. MERGE STATEMENT)
    -----------------------------------------------------------------
    
    MERGE INTO IFRS9_BCA.GTMP_IFRS_BUCKET_HEADER a
    USING (
        SELECT * 
        FROM (
            SELECT 
                "pkid"                 AS PKID,
                "syscode_group_bucket" AS SYSCODE_GROUP_BUCKET,
                "bucket_group_name"    AS BUCKET_GROUP,
                "unique_code"          AS BUCKET_DESCRIPTION,
                "bucket_type"          AS OPTION_GROUPING,
                "is_deleted"           AS IS_DELETED,
                'SYSTEM'               AS CREATEDBY,
                SYSDATE                AS CREATEDDATE,
                'LOCALHOST'            AS CREATEDHOST,
                ROW_NUMBER() OVER (
                    PARTITION BY "syscode_group_bucket" 
                    ORDER BY "pkid" DESC
                ) AS RN
            FROM "NTT_IMPAIRMENT2"."GroupBucket"@DBCONFIGLINK
        ) x
        WHERE x.RN = 1
    ) b
    ON (a.SYSCODE_GROUP_BUCKET = b.SYSCODE_GROUP_BUCKET)

    WHEN MATCHED THEN
        UPDATE SET   
            a.BUCKET_GROUP        = b.BUCKET_GROUP,
            a.BUCKET_DESCRIPTION  = b.BUCKET_DESCRIPTION,
            a.OPTION_GROUPING     = b.OPTION_GROUPING,
            a.IS_DELETED          = NVL(b.IS_DELETED,0),
            a.UPDATEDBY           = 'SP_IFRS_BUCKET_HEADER',
            a.UPDATEDDATE         = SYSDATE,
            a.UPDATEDHOST         = 'LOCALHOST'

    WHEN NOT MATCHED THEN
        INSERT (
            SYSCODE_GROUP_BUCKET,
            BUCKET_GROUP,
            BUCKET_DESCRIPTION,
            OPTION_GROUPING,
            IS_DELETED,
            CREATEDBY,
            CREATEDDATE,
            CREATEDHOST
        )
        VALUES (
            b.SYSCODE_GROUP_BUCKET,
            b.BUCKET_GROUP,
            b.BUCKET_DESCRIPTION,
            b.OPTION_GROUPING,
            NVL(b.IS_DELETED,0),
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