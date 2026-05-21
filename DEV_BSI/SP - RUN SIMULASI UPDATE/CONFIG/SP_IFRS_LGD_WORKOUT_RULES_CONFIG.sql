CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_LGD_WORKOUT_RULES_CONFIG (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_LGD_WORKOUT_RULES_CONFIG';
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
    V_TABLEINSERT1 := 'IFRS_LGD_WORKOUT_RULES_CONFIG';


    PSAK413.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;
     
    
    ---- PUT YOUR MAIN CODE HERE (E.G. MERGE STATEMENT)
    -----------------------------------------------------------------
    
	MERGE INTO IFRS_LGD_WORKOUT_RULES_CONFIG a
		USING (
		
			SELECT * 
		FROM (
		

	SELECT a."workout_period_configuration_name",A."syscode_workout_period_config" ,
		B.PKID AS SEGMENTATION_ID, 
		a."start_period",  a."historical_month",
		NVL(A."is_deleted",0) AS IS_DELETED,
		ROW_NUMBER() OVER (PARTITION BY a."syscode_workout_period_config" ORDER BY a."pkid" desc) RN
		FROM "NTT_PSAK413_IMPAIRMENT"."WorkoutPeriodConfiguration"@DBCONFIGLINK a 
		JOIN IFRS_SEGMENTATION_MAPPING b ON a."segment_code" = b.SYSCODE_SEGMENTATION  ) x 
		WHERE x.rn = 1 
		) b
		ON (a.SYSCODE_workout = b."syscode_workout_period_config" )  
		WHEN MATCHED THEN
			UPDATE SET   
				a.UPDATED_BY              = 'SP_IFRS_LGD_WORKOUT_RULES_CONFIG',
				a.UPDATED_DATE            = sysdate,
				a.IS_DELETED              = B.IS_DELETED
		
		WHEN NOT MATCHED THEN
			INSERT ( 
				syscode_workout,
				workout_rule_name,
				segmentation_id,
				historical_data,
				start_period,
				is_deleted,
				created_by ,
				created_date
			
				)
			VALUES (
				b."syscode_workout_period_config",b."workout_period_configuration_name" ,
			b.SEGMENTATION_ID, 
		b."historical_month", b."start_period",     B.IS_DELETED,
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