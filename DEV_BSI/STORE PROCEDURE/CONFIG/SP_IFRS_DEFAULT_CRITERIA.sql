CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_DEFAULT_CRITERIA (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_DEFAULT_CRITERIA';
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
    V_TABLEINSERT1 := 'IFRS_DEFAULT_CRITERIA';


    PSAK413.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;
     
    
    ---- PUT YOUR MAIN CODE HERE (E.G. MERGE STATEMENT)
    -----------------------------------------------------------------
    
    MERGE INTO IFRS_DEFAULT_CRITERIA a
        USING (
        
        SELECT * 
        FROM (
        selecT "pkid", "syscode_default_criteria", "default_criteria_name",  
            "sql_conditions", "source_table_conditions", "is_deleted",
            ROW_NUMBER() OVER (PARTITION BY "syscode_default_criteria" ORDER BY "pkid" DESC ) rn 
        from "DefaultCriteria"@DBCONFIGLINK) A 
        WHERE RN = 1 
        ) b
        ON (a.SYSCODE_DEFAULT_CRITERIA = b."syscode_default_criteria"
            )  
        
        WHEN MATCHED THEN
            UPDATE SET  
                a.sql_conditions          = b."sql_conditions",
                a.UPDATED_BY              = 'SP_IFRS_DEFAULT_CRITERIA',
                a.UPDATED_DATE            = sysdate,
                a.IS_DELETED              = b."is_deleted"
    
        WHEN NOT MATCHED THEN
            INSERT (
                SYSCODE_DEFAULT_CRITERIA, DEFAULT_CRITERIA_NAME,
                SQL_CONDITIONS, SOURCE_TABLE_CONDITIONS,
                IS_DELETED, 
                CREATED_BY, CREATED_DATE 
                )
            VALUES (
                b."syscode_default_criteria", b."default_criteria_name",
                b."sql_conditions", b."source_table_conditions",
                nvl(b."is_deleted", 0),
                'SP_IFRS_DEFAULT_CRITERIA', sysdate
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