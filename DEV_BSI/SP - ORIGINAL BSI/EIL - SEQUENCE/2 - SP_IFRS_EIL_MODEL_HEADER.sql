CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_EIL_MODEL_HEADER_DEV (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_SYSCODE       IN VARCHAR2 DEFAULT '0',
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_EIL_MODEL_HEADER_DEV';
    
    V_CURRDATE      DATE;

    -- TABLE NAMES (UNQUALIFIED PARTS)
    V_TABLEINSERT1  VARCHAR2(100);

    -- MISC
    V_RETURNROWS2   NUMBER := 0;
    V_TABLEDEST     VARCHAR2(100);
    V_COLUMNDEST    VARCHAR2(100);
    V_OPERATION     VARCHAR2(100);

    -- RESULT QUERY
    V_TAB_OWNER CONSTANT VARCHAR2(30) := 'PSAK413';
    V_QUERYS        CLOB;
BEGIN

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

    ----------------------------------------------------------------
    -- TABLE DETERMINATION
    ----------------------------------------------------------------
    V_TABLEINSERT1 := 'IFRS_EIL_MODEL_HEADER';


    -------- RECORD RUN_ID --------
    PSAK413.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
	COMMIT;
    -------- RECORD RUN_ID --------
     
    
    ---- PUT YOUR MAIN CODE HERE (E.G. MERGE STATEMENT)
    -----------------------------------------------------------------
    
    MERGE INTO IFRS_EIL_MODEL_HEADER a
    USING (

    SELECT * FROM (
    SELECT "pkid",
        "syscode_eil_configuration", "eil_configuration_name",
        "effective_start_date","description", "is_active",
        "is_eom", "is_deleted",ROW_NUMBER() OVER (PARTITION BY "syscode_eil_configuration" ORDER BY "pkid" DESC ) rn 
    FROM "EilConfiguration"@DBCONFIGLINK ) x 
    WHERE x.rn = 1
    ) b
    ON ( a.SYSCODE_EIL = b."syscode_eil_configuration" )  

    WHEN MATCHED THEN
    UPDATE SET  
        a.DATA_DATE    			  = b."effective_start_date",
        a.ACTIVE_STATUS			  = b."is_active",
        a.RUN_STATUS			  = b."is_eom",
        a.UPDATEDBY               = 'SP_IFRS_EIL_MODEL_HEADER',
        a.UPDATEDDATE             = SYSDATE,
        a.IS_DELETE               = b."is_deleted",
        A.IS_EOM = B."is_eom"

    WHEN NOT MATCHED THEN
    INSERT (
        SYSCODE_EIL, EIL_MODEL_NAME,
        DATA_DATE, ACTIVE_STATUS, is_eom,
        IS_DELETE, CREATEDBY,  
        UPDATEDBY, UPDATEDDATE
    )
    VALUES (
        b."syscode_eil_configuration", b."eil_configuration_name",
        b."effective_start_date",b."is_active",nvl(b."is_eom",0) ,nvl(b."is_deleted",0), 'SP_IFRS_EIL_MODEL_HEADER', sysdate , sysdate
    );

    COMMIT;
    ------------------------------------------------------------------

    DBMS_OUTPUT.PUT_LINE('PROCEDURE ' || V_SP_NAME || ' EXECUTED SUCCESSFULLY.');

    -------- ====== LOG ======
    V_TABLEDEST := V_TAB_OWNER || '.' || V_TABLEINSERT1;
    V_COLUMNDEST := '-';
    V_OPERATION := 'INSERT';
    
    PSAK413.SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SP_NAME, V_OPERATION, NVL(V_RETURNROWS2,0), P_RUNID);
	COMMIT;
    -------- ====== LOG ======

    -------- ====== RESULT ======
    V_QUERYS := 'SELECT * FROM ' || V_TAB_OWNER || '.' || V_TABLEINSERT1;
    PSAK413.SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SP_NAME, NVL(V_RETURNROWS2,0), P_RUNID);
	COMMIT;
    -------- ====== RESULT ======

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;

        RAISE_APPLICATION_ERROR(-20001,'ERROR IN ' || V_SP_NAME || ' : ' || SQLERRM);
END;