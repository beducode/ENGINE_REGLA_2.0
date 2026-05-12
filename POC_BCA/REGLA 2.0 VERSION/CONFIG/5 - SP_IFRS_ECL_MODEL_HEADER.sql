CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_ECL_MODEL_HEADER (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_ECL_MODEL_HEADER';
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
    V_TABLEINSERT1 := 'GTMP_IFRS_ECL_MODEL_HEADER';


    IFRS9_BCA.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;
     
    
    ---- PUT YOUR MAIN CODE HERE (E.G. MERGE STATEMENT)
    -----------------------------------------------------------------
    MERGE INTO IFRS9_BCA.GTMP_IFRS_ECL_MODEL_HEADER a
    USING (
        SELECT * 
        FROM (
            SELECT 
                "pkid"                          AS PKID,
                "syscode_ecl_configuration"     AS SYSCODE_ECL,
                "ecl_configuration_name"        AS ECL_MODEL_NAME,
                "effective_start_date"          AS EFFECTIVE_DATE,
                "is_active"                     AS ACTIVE_FLAG,
                "is_eom"                        AS RUN_STATUS,
                ROW_NUMBER() OVER (
                    PARTITION BY "syscode_ecl_configuration" 
                    ORDER BY "pkid" DESC
                ) rn
            FROM "NTT_IMPAIRMENT2"."EclConfiguration"@DBCONFIGLINK
        ) x
        WHERE x.rn = 1
    ) b
    ON (a.SYSCODE_ECL = b.SYSCODE_ECL)

    WHEN MATCHED THEN
        UPDATE SET  
            a.ECL_MODEL_NAME   = b.ECL_MODEL_NAME,
            a.EFFECTIVE_DATE   = b.EFFECTIVE_DATE,
            a.ACTIVE_FLAG      = b.ACTIVE_FLAG,
            a.RUN_STATUS       = TO_CHAR(b.RUN_STATUS),
            a.UPDATEDBY        = 'SP_IFRS_ECL_MODEL_HEADER',
            a.UPDATEDDATE      = SYSTIMESTAMP,
            a.UPDATEDHOST      = 'LOCALHOST'

    WHEN NOT MATCHED THEN
        INSERT (
            SYSCODE_ECL,
            ECL_MODEL_NAME,
            EFFECTIVE_DATE,
            ACTIVE_FLAG,
            RUN_STATUS,
            CREATEDBY,
            CREATEDDATE,
            CREATEDHOST
        )
        VALUES (
            b.SYSCODE_ECL,
            b.ECL_MODEL_NAME,
            b.EFFECTIVE_DATE,
            b.ACTIVE_FLAG,
            TO_CHAR(b.RUN_STATUS),
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