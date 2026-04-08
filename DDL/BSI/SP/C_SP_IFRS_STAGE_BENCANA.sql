CREATE OR REPLACE PROCEDURE PSAK413.C_SP_IFRS_STAGE_BENCANA (
	p_DOWNLOAD_DATE IN DATE
)
AS
    v_STAGE_DETAIL     VARCHAR2(100);
    v_STR_SQL        VARCHAR2(4000);
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

    v_TABLENAME := 'IFRS_MASTER_ACCOUNT_MONTHLY';
  
    ----------------------------------------------------------------
    -- RESET STAGE (DYNAMIC UPDATE)
    ----------------------------------------------------------------
    v_STR_SQL :=
        'MERGE INTO ' || v_TABLENAME || ' A
			USING (
				SELECT I.MASTERID ,I.ACCOUNT_NUMBER, I.STAGE  FROM ' || v_TABLENAME || ' I 
				INNER JOIN (SELECT X.NOLOAN, MAX(NO_BUCKET) AS NO_BUCKET 
									FROM LIST_NASABAH_RESTRU_BENCANA X GROUP BY NOLOAN) J 
				ON I.ACCOUNT_NUMBER = J.NOLOAN 
				WHERE I.DOWNLOAD_DATE = DATE ''' || TO_CHAR(v_CURRDATE, 'YYYY-MM-DD') || ''' AND NVL(I.STAGE,0) < 2
			)B
			ON(A.DOWNLOAD_DATE = DATE ''' || TO_CHAR(v_CURRDATE, 'YYYY-MM-DD') || '''
				AND A.MASTERID = B.MASTERID)
			WHEN MATCHED THEN 
			UPDATE SET A.STAGE = 2,
					A.SICR_FLAG = 1';

    EXECUTE IMMEDIATE v_STR_SQL;
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        -- ROLLBACK TO SAFE STATE AND RE-RAISE
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR IN C_SP_IFRS_STAGE_BENCANA: ' || SQLERRM);
        RAISE;
END;
/
