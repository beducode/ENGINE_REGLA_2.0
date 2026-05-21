CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_STAGE_CONFIG (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_STAGE_CONFIG';
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
    V_TABLEINSERT1 := 'IFRS_STAGE_CONFIG';


    PSAK413.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;
     
    
    ---- PUT YOUR MAIN CODE HERE (E.G. MERGE STATEMENT)
    -----------------------------------------------------------------
    
	MERGE INTO IFRS_STAGE_CONFIG a
			USING (
			
				SELECT b."pkid",x."syscode_staging" AS syscode_header, b."syscode_staging" AS syscode_detail,x."staging_name" AS group_name,
						b."staging_name" AS detail_name,TO_CHAR(b."sql_conditions") AS sql_conditions,
						b."is_deleted", b."is_sicr", b."is_publish"
				FROM (
					selecT A."pkid", A."syscode_staging",
					A."staging_name" 
					from "Staging"@DBCONFIGLINK A
					WHERE "level" = 1 AND "is_publish"= 1 ) x 
					LEFT JOIN "Staging"@DBCONFIGLINK b ON x."syscode_staging" = b."parent_code_level_0" AND b."level" = 2
				
			) b
			ON ( a.SYSCODE_STAGE_HEADER = b.syscode_header 
				AND a.SYSCODE_STAGE_DETAIL = b.syscode_detail ) 
			WHEN MATCHED THEN
				UPDATE SET  
					a.UPDATEDBY              = 'SP_IFRS_STAGE_CONFIG',
					a.UPDATEDDATE            = sysdate,
					a.IS_DELETE              = b."is_deleted",
					a.IS_SICR 				 = b."is_sicr",
					a.SQL_CONDITIONS		 = b.sql_conditions
			WHEN NOT MATCHED THEN
				INSERT (
					SYSCODE_STAGE_HEADER, SYSCODE_STAGE_DETAIL, STAGE_HEADER, STAGE_DETAIL,
					SQL_CONDITIONS, IS_DELETE,IS_SICR , UPDATEDBY, UPDATEDDATE
				)
				VALUES (
					b.syscode_header, b.syscode_detail,
					b.group_name,b.detail_name,b.sql_conditions, b."is_deleted",b."is_sicr" , 'SP_IFRS_STAGE_CONFIG', sysdate 
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