CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_LIFETIME_RULES_CONFIG (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_LIFETIME_RULES_CONFIG';
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
    V_TABLEINSERT1 := 'IFRS_LIFETIME_RULES_CONFIG';


    PSAK413.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;
     
    
    ---- PUT YOUR MAIN CODE HERE (E.G. MERGE STATEMENT)
    -----------------------------------------------------------------
    
    MERGE INTO IFRS_LIFETIME_RULES_CONFIG a
    USING (
    
    	 SELECT * 
	 FROM (
	 

		SELECT a."lifetime_name",A."syscode_lifetime_config" ,
			B.PKID AS SEGMENTATION_ID,A."method", 
			a."start_historical_date", a."calculation_method", a."historical_month",
			NVL(A."is_deleted",0) AS IS_DELETED,
			ROW_NUMBER() OVER (PARTITION BY a."syscode_lifetime_config" ORDER BY a."pkid" desc) RN
		FROM "LifetimeConfiguration"@DBCONFIGLINK a 
			JOIN IFRS_SEGMENTATION_MAPPING b ON a."segment_code" = b.SYSCODE_SEGMENTATION  ) x 
			WHERE x.rn = 1 
			) b
			ON (a.SYSCODE_Lifetime = b."syscode_lifetime_config" )  
			WHEN MATCHED THEN
				UPDATE SET   
					a.UPDATED_BY              = 'SP_IFRS_LIFETIME_RULES_CONFIG',
					a.UPDATED_DATE            = sysdate,
					a.IS_DELETED              = B.IS_DELETED
			
			WHEN NOT MATCHED THEN
				INSERT (
					LIFETIME_RULE_NAME,
					SYSCODE_LIFETIME,
					SEGMENTATION_ID,
					LIFETIME_METHOD,
					START_HISTORICAL_DATE,
					CALCULATION_METHOD,
					HISTORICAL_MONTH,
					IS_DELETED,
					CREATED_BY,
					CREATED_DATE
					)
				VALUES (
					b."lifetime_name",b."syscode_lifetime_config" ,
				b.SEGMENTATION_ID,b."method", 
			b."start_historical_date", b."calculation_method", b."historical_month",   B.IS_DELETED,
				'SP_IFRS_LIFETIME_RULES_CONFIG',sysdate
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