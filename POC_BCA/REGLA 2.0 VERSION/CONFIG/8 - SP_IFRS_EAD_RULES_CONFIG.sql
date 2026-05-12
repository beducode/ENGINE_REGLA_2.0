CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_EAD_RULES_CONFIG (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_EAD_RULES_CONFIG';
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
    V_TABLEINSERT1 := 'GTMP_IFRS_EAD_RULES_CONFIG';


    IFRS9_BCA.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;
     
    
    ---- PUT YOUR MAIN CODE HERE (E.G. MERGE STATEMENT)
    -----------------------------------------------------------------
    
	MERGE INTO IFRS9_BCA.GTMP_IFRS_EAD_RULES_CONFIG a
    USING (
        SELECT * 
        FROM (
            SELECT 
                a."ead_name"                     AS EAD_RULE_NAME,
                a."syscode_ead_config"          AS SYSCODE_EAD,
                B.PKID                          AS SEGMENTATION_ID,
                a."balance_source"              AS EAD_BALANCE,
                -- mapping baru (sementara default/null)
                NULL                            AS CCF_FLAG,
                NULL                            AS CCF_RULES_ID,
                NULL                            AS PREPAYMENT_FLAG,
                NULL                            AS PREPAYMENT_RULES_ID,
                1                               AS ACTIVE_FLAG,
                NVL(a."is_deleted",0)           AS IS_DELETE,
                ROW_NUMBER() OVER (
                    PARTITION BY a."syscode_ead_config" 
                    ORDER BY a."pkid" DESC
                ) RN
            FROM "NTT_IMPAIRMENT2"."EadConfiguration"@DBCONFIGLINK a 
            JOIN IFRS_SEGMENTATION_MAPPING B 
                ON a."segment_code" = b.SYSCODE_SEGMENTATION
        ) x 
        WHERE x.RN = 1
    ) b
    ON (a.SYSCODE_EAD = b.SYSCODE_EAD)

    WHEN MATCHED THEN
        UPDATE SET   
            a.EAD_RULE_NAME        = b.EAD_RULE_NAME,
            a.SEGMENTATION_ID      = b.SEGMENTATION_ID,
            a.EAD_BALANCE          = b.EAD_BALANCE,
            a.CCF_FLAG             = b.CCF_FLAG,
            a.CCF_RULES_ID         = b.CCF_RULES_ID,
            a.PREPAYMENT_FLAG      = b.PREPAYMENT_FLAG,
            a.PREPAYMENT_RULES_ID  = b.PREPAYMENT_RULES_ID,
            a.ACTIVE_FLAG          = b.ACTIVE_FLAG,
            a.IS_DELETE            = b.IS_DELETE,
            a.UPDATEDBY            = 'SP_IFRS_EAD_RULES_CONFIG',
            a.UPDATEDDATE          = SYSTIMESTAMP,
            a.UPDATEDHOST          = 'LOCALHOST'

    WHEN NOT MATCHED THEN
        INSERT (
            SYSCODE_EAD,
            EAD_RULE_NAME,
            SEGMENTATION_ID,
            EAD_BALANCE,
            CCF_FLAG,
            CCF_RULES_ID,
            PREPAYMENT_FLAG,
            PREPAYMENT_RULES_ID,
            ACTIVE_FLAG,
            IS_DELETE,
            CREATEDBY,
            CREATEDDATE,
            CREATEDHOST
        )
        VALUES (
            b.SYSCODE_EAD,
            b.EAD_RULE_NAME,
            b.SEGMENTATION_ID,
            b.EAD_BALANCE,
            b.CCF_FLAG,
            b.CCF_RULES_ID,
            b.PREPAYMENT_FLAG,
            b.PREPAYMENT_RULES_ID,
            1,
            b.IS_DELETE,
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