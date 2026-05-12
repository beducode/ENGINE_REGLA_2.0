CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_PD_RULES_CONFIG (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_PD_RULES_CONFIG';
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
    V_TABLEINSERT1 := 'GTMP_IFRS_PD_RULES_CONFIG';


    IFRS9_BCA.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;
     
    
    ---- PUT YOUR MAIN CODE HERE (E.G. MERGE STATEMENT)
    -----------------------------------------------------------------
    
	MERGE INTO IFRS9_BCA.GTMP_IFRS_PD_RULES_CONFIG a
    USING (
        SELECT * 
        FROM (
            SELECT 
                a."syscode_pd_config"       AS SYSCODE_PD,
                a."pd_name"                 AS PD_RULE_NAME,
                b.pkid                      AS SEGMENTATION_ID,
                a."pd_method"               AS PD_METHOD,
                a."calculation_method"      AS CALC_METHOD,
                a."historical_data"         AS HISTORICAL_DATA,
                a."expected_lifetime"       AS EXPECTED_LIFE,
                c.BUCKET_GROUP              AS BUCKET_GROUP,
                a."windows_moving"          AS INCREMENT_PERIOD,
                a."is_deleted"              AS IS_DELETED,
                ROW_NUMBER() OVER (
                    PARTITION BY a."syscode_pd_config" 
                    ORDER BY a."pkid" DESC
                ) AS RN
            FROM "NTT_IMPAIRMENT2"."PdConfiguration"@DBCONFIGLINK a
            JOIN IFRS_SEGMENTATION_MAPPING b 
                ON a."segment_code" = b.SYSCODE_SEGMENTATION 
            JOIN GTMP_IFRS_BUCKET_HEADER c 
                ON a."bucket_code" = c.SYSCODE_GROUP_BUCKET 
        ) x
        WHERE x.RN = 1
    ) b
    ON (a.SYSCODE_PD = b.SYSCODE_PD)

    WHEN MATCHED THEN
        UPDATE SET   
            a.PD_RULE_NAME       = b.PD_RULE_NAME,
            a.SEGMENTATION_ID    = b.SEGMENTATION_ID,
            a.PD_METHOD          = b.PD_METHOD,
            a.CALC_METHOD        = b.CALC_METHOD,
            a.HISTORICAL_DATA    = b.HISTORICAL_DATA,
            a.EXPECTED_LIFE      = b.EXPECTED_LIFE,
            a.BUCKET_GROUP       = b.BUCKET_GROUP,
            a.INCREMENT_PERIOD   = b.INCREMENT_PERIOD,
            a.IS_DELETED         = NVL(b.IS_DELETED,0),
            a.UPDATEDBY          = 'SP_IFRS_PD_RULES_CONFIG',
            a.UPDATEDDATE        = SYSDATE,
            a.UPDATEDHOST        = 'LOCALHOST'

    WHEN NOT MATCHED THEN
        INSERT (
            SYSCODE_PD,
            PD_RULE_NAME,
            SEGMENTATION_ID,
            PD_METHOD,
            CALC_METHOD,
            HISTORICAL_DATA,
            EXPECTED_LIFE,
            BUCKET_GROUP,
            INCREMENT_PERIOD,
            ACTIVE_FLAG,
            IS_NEW,
            IS_DELETED,
            CREATEDBY,
            CREATEDDATE,
            CREATEDHOST
        )
        VALUES (
            b.SYSCODE_PD,
            b.PD_RULE_NAME,
            b.SEGMENTATION_ID,
            b.PD_METHOD,
            b.CALC_METHOD,
            b.HISTORICAL_DATA,
            b.EXPECTED_LIFE,
            b.BUCKET_GROUP,
            b.INCREMENT_PERIOD,
            1,                  -- ACTIVE_FLAG default
            1,                  -- IS_NEW default
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