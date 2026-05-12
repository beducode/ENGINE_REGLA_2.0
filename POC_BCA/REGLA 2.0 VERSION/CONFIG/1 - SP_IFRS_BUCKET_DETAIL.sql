CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_BUCKET_DETAIL (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_BUCKET_DETAIL';
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
    V_TABLEINSERT1 := 'GTMP_IFRS_BUCKET_DETAIL';


    IFRS9_BCA.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;
     
    
    MERGE INTO IFRS9_BCA.GTMP_IFRS_BUCKET_DETAIL a
    USING (
        SELECT * 
        FROM (
            SELECT 
                b."unique_code" AS BUCKET_GROUP,

                CASE 
                    WHEN REGEXP_LIKE(a."bucket_id", '^\d+$') 
                    THEN TO_NUMBER(a."bucket_id") 
                END AS BUCKET_ID,

                a."bucket_name" AS BUCKET_NAME,

                CASE 
                    WHEN REGEXP_LIKE(a."range_from_days", '^\d+$') 
                    THEN TO_NUMBER(a."range_from_days") 
                END AS RANGE_START,

                CASE 
                    WHEN REGEXP_LIKE(a."range_to_days", '^\d+$') 
                    THEN TO_NUMBER(a."range_to_days") 
                END AS RANGE_END,

                CASE 
                    WHEN REGEXP_LIKE(a."rating_value", '^\d+(\.\d+)?$') 
                    THEN TO_NUMBER(a."rating_value") 
                END AS FLAG_DEFAULT,

                CASE 
                    WHEN REGEXP_LIKE(a."bi_collectability_value", '^\d+(\.\d+)?$') 
                    THEN TO_NUMBER(a."bi_collectability_value") 
                END AS PD_DEFAULT,

                NULL AS IMPAIRMENT_BUCKET,

                NVL(a."is_deleted",0) AS IS_DELETED,

                ROW_NUMBER() OVER (
                    PARTITION BY b."unique_code", a."bucket_id" 
                    ORDER BY a."pkid" DESC
                ) RN

            FROM "NTT_IMPAIRMENT2"."GroupBucketDetail"@DBCONFIGLINK a
            JOIN "NTT_IMPAIRMENT2"."GroupBucket"@DBCONFIGLINK b 
                ON a."syscode_group_bucket" = b."syscode_group_bucket"
            WHERE a."bucket_id" IS NOT NULL
        ) x
        WHERE x.RN = 1
    ) b
    ON (
        a.BUCKET_GROUP = b.BUCKET_GROUP 
        AND a.BUCKET_ID = b.BUCKET_ID
    )

    WHEN MATCHED THEN
        UPDATE SET
            a.BUCKET_NAME        = b.BUCKET_NAME,
            a.RANGE_START        = b.RANGE_START,
            a.RANGE_END          = b.RANGE_END,
            a.FLAG_DEFAULT       = b.FLAG_DEFAULT,
            a.PD_DEFAULT         = b.PD_DEFAULT,
            a.IMPAIRMENT_BUCKET  = b.IMPAIRMENT_BUCKET,
            a.UPDATEDBY          = 'SP_IFRS_BUCKET_DETAIL',
            a.UPDATEDDATE        = SYSDATE,
            a.UPDATEDHOST        = 'LOCALHOST'

    WHEN NOT MATCHED THEN
        INSERT (
            BUCKET_GROUP,
            BUCKET_ID,
            BUCKET_NAME,
            RANGE_START,
            RANGE_END,
            FLAG_DEFAULT,
            PD_DEFAULT,
            IMPAIRMENT_BUCKET,
            CREATEDBY,
            CREATEDDATE,
            CREATEDHOST
        )
        VALUES (
            b.BUCKET_GROUP,
            b.BUCKET_ID,
            b.BUCKET_NAME,
            b.RANGE_START,
            b.RANGE_END,
            b.FLAG_DEFAULT,
            b.PD_DEFAULT,
            b.IMPAIRMENT_BUCKET,
            'SYSTEM',
            SYSDATE,
            'LOCALHOST'
        );
        
    COMMIT;

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
