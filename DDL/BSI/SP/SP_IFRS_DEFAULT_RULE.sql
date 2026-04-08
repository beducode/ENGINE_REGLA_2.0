CREATE OR REPLACE PROCEDURE PSAK413."SP_IFRS_DEFAULT_RULE" (
	p_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    p_DOWNLOAD_DATE IN DATE,
    p_MODEL_TYPE    IN VARCHAR2 DEFAULT '',
    p_MODEL_ID      IN NUMBER   DEFAULT 0,
    p_PRC           IN CHAR     DEFAULT 'M'
)
AS
    v_CURRDATE      DATE;
    v_TABLENAME     VARCHAR2(50);
    v_STR_SQL       CLOB;
    v_SCRIPT1       CLOB;
	v_SQL_CONDITIONS CLOB;
    v_RULE_ID       NUMBER;
BEGIN
    ------------------------------------------------------------------
    -- INIT
    ------------------------------------------------------------------
    IF p_DOWNLOAD_DATE IS NULL THEN
        SELECT CURRDATE INTO v_CURRDATE FROM IFRS_PRC_DATE;
    ELSE
        v_CURRDATE := p_DOWNLOAD_DATE;
    END IF;

    v_TABLENAME :=
        CASE
            WHEN p_PRC = 'M' THEN 'IFRS_MASTER_ACCOUNT'
            WHEN p_PRC = 'S' THEN 'IFRS_MASTER_ACCOUNT_MONTHLY'
            ELSE 'IFRS_MASTER_ACCOUNT_' || p_RUNID
        END;

   	EXECUTE IMMEDIATE 'TRUNCATE TABLE TMPRULE';

    ------------------------------------------------------------------
    -- LOAD DEFAULT RULES
    ------------------------------------------------------------------
    IF p_MODEL_TYPE IS NULL OR p_MODEL_TYPE = '' THEN

        INSERT INTO TMPRULE
        SELECT DISTINCT DEFAULT_RULE_ID
        FROM IFRS_PD_RULES_CONFIG
        WHERE IS_ACTIVE = 1
          AND IS_DELETED = 0
          AND PD_METHOD <> 'VAS'
          AND (PKID = p_MODEL_ID OR p_MODEL_ID = 0);

        INSERT INTO TMPRULE
        SELECT DISTINCT DEFAULT_RULE_ID
        FROM IFRS_LGD_RULES_CONFIG
        WHERE IS_DELETED = 0
          AND LGD_METHOD <> 'VAS'
          AND (PKID = p_MODEL_ID OR p_MODEL_ID = 0);

    ELSIF p_MODEL_TYPE = 'PD' THEN

        INSERT INTO TMPRULE
        SELECT DISTINCT DEFAULT_RULE_ID
        FROM IFRS_PD_RULES_CONFIG
        WHERE IS_ACTIVE = 1
          AND IS_DELETED = 0
          AND PD_METHOD <> 'VAS'
          AND (PKID = p_MODEL_ID OR p_MODEL_ID = 0);
	
     ELSIF p_MODEL_TYPE = 'LGD' THEN

        INSERT INTO TMPRULE
        SELECT DISTINCT DEFAULT_RULE_ID
        FROM IFRS_LGD_RULES_CONFIG
        WHERE IS_DELETED = 0
          AND LGD_METHOD <> 'VAS'
          AND (PKID = p_MODEL_ID OR p_MODEL_ID = 0);
    END IF;

    ------------------------------------------------------------------
    -- DELETE EXISTING DEFAULT DATA
    ------------------------------------------------------------------
    DELETE FROM IFRS_DEFAULT
    WHERE RULE_ID IN (SELECT DEFAULT_RULE_ID FROM TMPRULE)
      AND DOWNLOAD_DATE = LAST_DAY(v_CURRDATE);

    ------------------------------------------------------------------
    -- LOOP RULE
    ------------------------------------------------------------------
    FOR r_rule IN (
        SELECT
		    A.PKID,
		    A.SQL_CONDITIONS
		FROM IFRS_DEFAULT_CRITERIA A
		JOIN TMPRULE B
		  ON A.PKID = B.DEFAULT_RULE_ID
		ORDER BY A.PKID     
    )
    LOOP
        v_RULE_ID := r_rule.PKID;
    	v_SQL_CONDITIONS := r_rule.SQL_CONDITIONS;
       
        v_SCRIPT1 :=' ' || v_SQL_CONDITIONS || ' ';
        ------------------------------------------------------------------
        -- BUILD FINAL INSERT SQL
        ------------------------------------------------------------------
        v_STR_SQL :=
            'INSERT INTO IFRS_DEFAULT (
                DOWNLOAD_DATE,
                RULE_ID,
                MASTERID,
                FACILITY_NUMBER,
                CUSTOMER_NUMBER,
                OS_AT_DEFAULT,
                EQV_AT_DEFAULT,
                PLAFOND_AT_DEFAULT,
                EQV_PLAFOND_AT_DEFAULT,
                EIR_AT_DEFAULT,
                CCY_AT_DEFAULT,
                CREATED_DATE
            )
            SELECT
                LAST_DAY(ifrs_master_account.DOWNLOAD_DATE),
                ' || v_RULE_ID || ',
                ifrs_master_account.MASTERID,
                ifrs_master_account.FACILITY_NUMBER,
                ifrs_master_account.CUSTOMER_NUMBER,
                NVL(B.OUTSTANDING, ifrs_master_account.OUTSTANDING),
                NVL(B.OUTSTANDING, ifrs_master_account.OUTSTANDING) * ifrs_master_account.EXCHANGE_RATE,
                ifrs_master_account.PLAFOND,
                ifrs_master_account.PLAFOND * ifrs_master_account.EXCHANGE_RATE,
                NVL(ifrs_master_account.EIR, ifrs_master_account.MARGIN_RATE),
                ifrs_master_account.CURRENCY,
                SYSDATE
            FROM ' || v_TABLENAME || ' ifrs_master_account
            LEFT JOIN IFRS_MASTER_ACCOUNT_WO B
                   ON ifrs_master_account.MASTERID = B.MASTERID
                  AND ifrs_master_account.DOWNLOAD_DATE >= B.DOWNLOAD_DATE
            WHERE ifrs_master_account.DOWNLOAD_DATE = DATE ''' ||
            TO_CHAR(v_CURRDATE, 'YYYY-MM-DD') || '''
              AND ' || v_SCRIPT1 ||'';

        EXECUTE IMMEDIATE v_STR_SQL;
    END LOOP;

    COMMIT;
    
    MERGE INTO IFRS_DEFAULT A
	USING (
	    SELECT
	        A.RULE_ID,
	        A.DOWNLOAD_DATE,
	        A.MASTERID,
	        B.OUTSTANDING AS OS_12M,
	        B.OUTSTANDING * B.EXCHANGE_RATE EQV_OS_12M,
	        ROW_NUMBER() OVER (
	            PARTITION BY A.RULE_ID, A.MASTERID
	            ORDER BY B.DOWNLOAD_DATE
	        ) RN
	    FROM IFRS_DEFAULT A
	    JOIN IFRS_MASTER_ACCOUNT_MONTHLY B
	      ON A.MASTERID = B.MASTERID
	    JOIN TMPRULE C
	      ON A.RULE_ID = C.DEFAULT_RULE_ID
	    WHERE B.DOWNLOAD_DATE BETWEEN ADD_MONTHS(A.DOWNLOAD_DATE, -12)
	                              AND A.DOWNLOAD_DATE
	      AND A.DOWNLOAD_DATE = v_CURRDATE
	      AND B.OUTSTANDING > 0
	) B
	ON (
	    A.RULE_ID = B.RULE_ID
	    AND A.MASTERID = B.MASTERID
	    AND A.DOWNLOAD_DATE = B.DOWNLOAD_DATE
	)
	WHEN MATCHED THEN
	UPDATE SET
	    A.OS_12M_BEFORE_DEFAULT      = B.OS_12M,
	    A.EQV_OS_12M_BEFORE_DEFAULT  = B.EQV_OS_12M
	WHERE B.RN = 1;

END;
/
