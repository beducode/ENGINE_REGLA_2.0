CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_EAD_RULES_CONFIG (
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
    V_TABLEINSERT1 := 'IFRS_EAD_RULES_CONFIG';


    PSAK413.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;
     
    
    ---- PUT YOUR MAIN CODE HERE (E.G. MERGE STATEMENT)
    -----------------------------------------------------------------
    
	MERGE INTO IFRS_EAD_RULES_CONFIG a
		USING (
		
			SELECT * 
			FROM (
			
					SELECT a."ead_name",A."syscode_ead_config" ,
						B.PKID AS SEGMENTATION_ID, A."balance_source",  
						NVL(C.PKID,0) AS LIFETIME_RULE_ID,
						a."is_ead_parameter_lifetime", A."include_accrued_interest", A."include_unamortized_fee_cost", a."include_interest_margin_past_due",
						NVL(A."is_deleted",0) AS IS_DELETE,
						ROW_NUMBER() OVER (PARTITION BY a."syscode_ead_config" ORDER BY a."pkid" desc) RN
						FROM "EadConfiguration"@DBCONFIGLINK a 
						JOIN IFRS_SEGMENTATION_MAPPING B ON a."segment_code" = b.SYSCODE_SEGMENTATION
						LEFT JOIN IFRS_LIFETIME_RULES_CONFIG C ON a."lifetime_code" = C.SYSCODE_LIFETIME
							) x 
			WHERE x.rn = 1 
		) b
		ON (a.SYSCODE_EAD = b."syscode_ead_config" )  
		WHEN MATCHED THEN
			UPDATE SET   
				a.UPDATEDBY              = 'SP_IFRS_EAD_RULES_CONFIG',
				a.UPDATEDDATE            = sysdate,
				a.IS_DELETE              = B.IS_DELETE,
				a.EAD_BALANCE			 = B."balance_source",
				A.UNAMORTIZED_FEE_COST_FLAG = B."include_unamortized_fee_cost",
				A.MARGIN_ACCRUED_FLAG	 = B."include_accrued_interest",
				A.MARGIN_PAST_DUE_FLAG	 = B."include_interest_margin_past_due"
		
		WHEN NOT MATCHED THEN
			INSERT (
				EAD_RULE_NAME,
				SYSCODE_EAD,
				SEGMENTATION_ID,
				EAD_BALANCE,
				LIFETIME_RULE_ID,
				LIFETIME_FLAG,
				UNAMORTIZED_FEE_COST_FLAG,
				MARGIN_ACCRUED_FLAG,
				MARGIN_PAST_DUE_FLAG,
				IS_DELETE,
				CREATEDBY,
				CREATEDDATE
				)
			VALUES (
				b."ead_name",b."syscode_ead_config" ,
			b.SEGMENTATION_ID,b."balance_source", 
			b.LIFETIME_RULE_ID, b."is_ead_parameter_lifetime",B."include_unamortized_fee_cost",B."include_accrued_interest",B."include_interest_margin_past_due", B.IS_DELETE,
			'SP_IFRS_EAD_RULES_CONFIG',sysdate
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