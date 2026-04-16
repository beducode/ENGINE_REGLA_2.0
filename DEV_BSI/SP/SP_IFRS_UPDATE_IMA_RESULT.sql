CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_UPDATE_IMA_RESULT (
    p_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
	p_DOWNLOAD_DATE IN DATE,
    p_PRC           IN CHAR DEFAULT 'M'
)
AS
    v_RULE_ID        VARCHAR2(250);
    v_TABLE_NAME     VARCHAR2(100);
    v_STR_SQL        CLOB;
    v_GROUP_SEGMENT  VARCHAR2(250);
    v_SEGMENT        VARCHAR2(250);
    v_SUB_SEGMENT    VARCHAR2(250);
    v_CONDITION      VARCHAR2(4000);
    v_CURRDATE       DATE;
    v_EXCEPT_ID      VARCHAR2(50);
    v_ERROR_FLAG     NUMBER(1) := 0;
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


    v_TABLENAME :=
        CASE
            WHEN p_PRC = 'M' THEN 'IFRS_MASTER_ACCOUNT_MONTHLY'
            ELSE 'IFRS_MASTER_ACCOUNT_MONTHLY_' || p_RUNID ||''
        END;

    ----------------------------------------------------------------
    -- RESET SEGMENT DATA (DYNAMIC UPDATE)
    ----------------------------------------------------------------
    v_STR_SQL :=
        'UPDATE ' || v_TABLENAME || ' A                
			 SET A.EIL_AMOUNT = 0
			  ,A.BUCKET_ID = 0                
			  ,A.LIFETIME = 0
			  ,A.LT_RULE_ID = 0               
			  ,A.LGD_RULE_ID = 0          
			  ,A.PD_RULE_ID = 0          
			  ,A.PD_ME_MODEL_ID = 0              
			  ,A.EAD_RULE_ID = 0           
			  ,A.EIL_MODEL_ID = 0
			  ,A.EIL_MARGIN = 0
			  ,A.EIL_POKOK = 0
			  ,A.EAD_AMOUNT = 0
			  ,A.MARGIN_ACCRUED = 0
			  ,A.PD_RATE = 0
			  ,A.LGD_RATE = 0               
			 WHERE A.ACCOUNT_STATUS = ''A''                  
			  AND A.IS_IMPAIRED = 1
			  AND A.IMPAIRED_FLAG = ''C''                  
			  AND A.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(v_currdate,'YYYY-MM-DD') || ''',''YYYY-MM-DD'') '; 
			 
	EXECUTE IMMEDIATE v_STR_SQL;
    COMMIT;

    v_STR_SQL := 'MERGE INTO ' || v_TABLENAME || ' t
				USING (
				  SELECT DOWNLOAD_DATE, MASTERID, EIL_AMOUNT, BUCKET_ID, LT_RULE_ID, LIFETIME,
				         LGD_RULE_ID, LGD_SEGMENT, PD_RULE_ID, PD_ME_MODEL_ID, PD_SEGMENT,
				         EAD_RULE_ID, EAD_SEGMENT, EIL_MODEL_ID, EIL_MARGIN, EIL_PRINCIPAL,
				         PD, LGD, EAD_AMOUNT, MARGIN_ACCRUED
				  FROM IFRS_EIL_RESULT_HEADER s
				  WHERE s.DOWNLOAD_DATE = TO_DATE(''' || TO_CHAR(v_currdate,'YYYY-MM-DD') || ''',''YYYY-MM-DD'')
				) s
				ON (s.MASTERID = t.MASTERID AND s.DOWNLOAD_DATE = t.DOWNLOAD_DATE)
				WHEN MATCHED THEN UPDATE SET
				   t.EIL_AMOUNT     = s.EIL_AMOUNT,
				   t.BUCKET_ID      = s.BUCKET_ID,
				   t.LT_RULE_ID     = s.LT_RULE_ID,
				   t.LIFETIME       = s.LIFETIME,
				   t.LGD_RULE_ID    = s.LGD_RULE_ID,
				   t.PD_RULE_ID     = s.PD_RULE_ID,
				   t.PD_ME_MODEL_ID = s.PD_ME_MODEL_ID,
				   t.EAD_RULE_ID    = s.EAD_RULE_ID,
				   t.EIL_MODEL_ID   = s.EIL_MODEL_ID,
				   t.EIL_MARGIN     = s.EIL_MARGIN,
				   t.EIL_POKOK      = s.EIL_PRINCIPAL,
				   t.PD_RATE        = s.PD,
				   t.LGD_RATE       = s.LGD,
				   t.EAD_AMOUNT     = s.EAD_AMOUNT,
				   t.MARGIN_ACCRUED = s.MARGIN_ACCRUED';
   
	DBMS_OUTPUT.PUT_LINE(v_STR_SQL);
	EXECUTE IMMEDIATE v_STR_SQL;
	COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        -- ROLLBACK TO SAFE STATE AND RE-RAISE
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR IN SP_IFRS_UPDATE_IMA_RESULT: ' || SQLERRM);
        RAISE;
END;
/
