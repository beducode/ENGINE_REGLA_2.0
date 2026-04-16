CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_PD_SYNC_TERM_STRUCTURE_FL (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE,
    P_SYSCODE       IN VARCHAR2 DEFAULT '0',
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS

    V_STR_QUERY     CLOB;
    V_QUERYS        CLOB;
    V_TAB_OWNER     CONSTANT VARCHAR2(30) := 'PSAK413';

    -- MISC
    V_RETURNROWS    NUMBER;
    V_RETURNROWS2   NUMBER;
    V_TABLEDEST     VARCHAR2(100);
    V_COLUMNDEST    VARCHAR2(100);
    V_OPERATION     VARCHAR2(100);
    
    V_RULE_ID        VARCHAR2(250);
    V_STAGE_DETAIL     VARCHAR2(100);
    V_IS_SICR  		 NUMBER;
    V_CONDITION      VARCHAR2(4000);
    V_CURRDATE       DATE;
    V_TABLENAME      VARCHAR2(100);
    V_SP_NAME        VARCHAR2(100);

    V_IFRS_PD_TERM_STRUCTURE  VARCHAR2(100);
    V_TABLEPDCONFIG VARCHAR2(100);
BEGIN
    V_SP_NAME := 'SP_IFRS_PD_SYNC_TERM_STRUCTURE_FL';
    ----------------------------------------------------------------
    -- INIT
    ----------------------------------------------------------------
    IF P_DOWNLOAD_DATE IS NULL THEN
        SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;
    ELSE
        V_CURRDATE := P_DOWNLOAD_DATE;
    END IF;

    IF P_PRC = 'S' THEN
        V_IFRS_PD_TERM_STRUCTURE := 'IFRS_PD_TERM_STRUCTURE_' || P_RUNID;
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG_' || P_RUNID;
    ELSE
        V_IFRS_PD_TERM_STRUCTURE := 'IFRS_PD_TERM_STRUCTURE';
        V_TABLEPDCONFIG := 'IFRS_PD_RULES_CONFIG';
    END IF;

    PSAK413.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, 0, SYSDATE);
    COMMIT;
   
    V_STR_QUERY := 'DELETE FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_TERM_STRUCTURE || ' A
        WHERE A.EFF_DATE  = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''', ''YYYY-MM-DD'') 
          AND A.PD_METHOD = ''MAA''
          AND A.MODEL_ID  <> 0';

  	EXECUTE IMMEDIATE V_STR_QUERY;
    COMMIT;

    ----------------------------------------------------------------
    -- INSERT TERM STRUCTURE MODEL
    ----------------------------------------------------------------
    V_STR_QUERY :=
    'INSERT INTO ' || V_TAB_OWNER || '.' || V_IFRS_PD_TERM_STRUCTURE || ' (EFF_DATE, SCENARIO_NO, PD_RULE_ID, MODEL_ID,
    BUCKET_GROUP, BUCKET_ID, FL_SEQ, FL_YEAR, FL_MONTH, PD, PD_OVERRIDE, PD_METHOD, CREATEDBY, CREATEDDATE)
        SELECT "effective_date" AS EFF_DATE,0 AS SCENARIO_NO,"pd_rule_id" as PD_RULE_ID, "workflow_header_draft_pkid" as MODEL_ID,
			 B.BUCKET_GROUP , "bucket" BUCKET_ID, "fl_seq" as FL_SEQ,"year" as FL_YEAR,"month" as FL_MONTH,
			 "weighted" AS PD,A."weighted" AS PD_OVERRIDE,B.PD_METHOD , ''SP_IFRS_SYNC_TERM_STRUCTURE_FL'' as CREATEDBY, SYSDATE AS CREATEDDATE
			FROM "NTT_RISK_MODELLING"."PyWeightingScenarioInterpolasi"@DBCONFIGLINK A
			INNER JOIN ' || V_TAB_OWNER || '.' || V_TABLEPDCONFIG || ' B ON A."pd_rule_id" = B.PKID 
			WHERE A."effective_date" = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') 
			AND A."is_selected" = 0 AND A."flag" = ''MPD'' AND NVL(A."workflow_header_draft_pkid",0) <> 0 
            AND B.PD_METHOD = ''MAA'' AND (  
                UPPER(TRIM(B.SYSCODE_PD)) IN ( 
                SELECT UPPER(TRIM(REGEXP_SUBSTR(:1, ''[^;]+'', 1, LEVEL)))
                FROM DUAL
                CONNECT BY REGEXP_SUBSTR(:2, ''[^;]+'', 1, LEVEL) IS NOT NULL
                )
                OR :3 = ''0'' 
            )';

    EXECUTE IMMEDIATE V_STR_QUERY USING P_SYSCODE, P_SYSCODE, P_SYSCODE;
    COMMIT;

    V_TABLEDEST := V_TAB_OWNER || '.' || V_IFRS_PD_TERM_STRUCTURE;
    V_COLUMNDEST := '-';
    V_OPERATION := 'INSERT';
 
    PSAK413.SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SP_NAME, V_OPERATION, NVL(V_RETURNROWS2,0), P_RUNID);
    COMMIT;


    ----------------------------------------------------------------
    -- RESULT
    ----------------------------------------------------------------
    V_QUERYS := 'SELECT * FROM ' || V_TAB_OWNER || '.' || V_IFRS_PD_TERM_STRUCTURE;

    PSAK413.SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SP_NAME, NVL(V_RETURNROWS2,0), P_RUNID);
    COMMIT;  
	
   
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;

        RAISE_APPLICATION_ERROR(-20001,'ERROR IN ' || V_SP_NAME || ' : ' || SQLERRM);
END;