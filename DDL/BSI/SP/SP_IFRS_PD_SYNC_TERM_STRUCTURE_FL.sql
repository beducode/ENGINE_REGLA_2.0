CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_PD_SYNC_TERM_STRUCTURE_FL (
	p_DOWNLOAD_DATE IN DATE
)
AS
    v_RULE_ID        VARCHAR2(250);
    v_STAGE_DETAIL     VARCHAR2(100);
    v_STR_SQL        VARCHAR2(4000);
    v_IS_SICR  		 NUMBER;
    v_CONDITION      VARCHAR2(4000);
    v_CURRDATE       DATE;
    v_TABLENAME      VARCHAR2(100);
BEGIN
    ----------------------------------------------------------------
    -- INIT
    ----------------------------------------------------------------
    IF p_DOWNLOAD_DATE IS NULL THEN
        SELECT CURRDATE INTO v_CURRDATE FROM IFRS_PRC_DATE;
    ELSE
        v_CURRDATE := p_DOWNLOAD_DATE;
    END IF;

    v_TABLENAME := 'IFRS_PD_TERM_STRUCTURE';
   
    v_STR_SQL :=
   	'DELETE FROM IFRS_PD_TERM_STRUCTURE A
        WHERE A.EFF_DATE  = TO_DATE(''' || TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || ''', ''YYYY-MM-DD'') 
          AND A.PD_METHOD = ''MAA''
          AND A.MODEL_ID  <> 0';
  	EXECUTE IMMEDIATE v_STR_SQL;
    COMMIT;
    ----------------------------------------------------------------
    -- INSERT TERM STRUCTURE MODEL
    ----------------------------------------------------------------
    v_STR_SQL :=
    'INSERT INTO IFRS_PD_TERM_STRUCTURE (EFF_DATE, SCENARIO_NO, PD_RULE_ID, MODEL_ID,
						 BUCKET_GROUP, BUCKET_ID, FL_SEQ, FL_YEAR, FL_MONTH, PD, PD_OVERRIDE, PD_METHOD,
						 CREATEDBY, CREATEDDATE)
        SELECT "effective_date" AS EFF_DATE,0 AS SCENARIO_NO,"pd_rule_id" as PD_RULE_ID, "workflow_header_draft_pkid" as MODEL_ID,
			 B.BUCKET_GROUP , "bucket" BUCKET_ID, "fl_seq" as FL_SEQ,"year" as FL_YEAR,"month" as FL_MONTH,
			 "weighted" AS PD,A."weighted" AS PD_OVERRIDE,B.PD_METHOD , ''SP_IFRS_SYNC_TERM_STRUCTURE_FL'' as CREATEDBY, SYSDATE AS CREATEDDATE
			FROM "NTT_RISK_MODELLING"."PyWeightingScenarioInterpolasi"@DBCONFIGLINK A
			INNER JOIN IFRS_PD_RULES_CONFIG B ON A."pd_rule_id" = B.PKID 
			WHERE A."effective_date" = TO_DATE(''' || TO_CHAR(v_CURRDATE,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') 
			AND A."is_selected" = 0 AND A."flag" = ''MPD'' AND NVL(A."workflow_header_draft_pkid",0) <> 0 ';

    EXECUTE IMMEDIATE v_STR_SQL;
    COMMIT;
	
   
EXCEPTION
    WHEN OTHERS THEN
        -- ROLLBACK TO SAFE STATE AND RE-RAISE
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR IN SP_IFRS_PD_SYNC_TERM_STRUCTURE_FL: ' || SQLERRM);
        RAISE;
END;
/
