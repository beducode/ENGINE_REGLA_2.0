CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_INITIAL_UPDATE_IMP
(
    P_RUNID         VARCHAR2,
    P_DOWNLOAD_DATE DATE,
    P_SYSCODE       VARCHAR2 DEFAULT '0',
    P_PRC           VARCHAR2 DEFAULT 'S'
)
AS
    -- DATE
    V_CURRDATE   DATE;
    V_PREVDATE   DATE;
    V_MODEL_ID   VARCHAR2(22);
    V_COUNT      NUMBER;

    -- QUERY
    V_STR_QUERY  CLOB;

    -- TABLE LIST
    V_TAB_OWNER     CONSTANT VARCHAR2(30) := 'PSAK413';
    V_TABLEINSERT1   VARCHAR2(100);
    V_TABLEINSERT2   VARCHAR2(100);
    V_TABLESELECT1   VARCHAR2(100);
    V_TABLEPDCONFIG  VARCHAR2(100);

    -- CONDITION / LOG
    V_RETURNROWS  NUMBER;
    V_RETURNROWS2 NUMBER;
    V_TABLEDEST   VARCHAR2(100);
    V_COLUMNDEST  VARCHAR2(100);
    V_SPNAME      VARCHAR2(100);
    V_OPERATION   VARCHAR2(100);

    V_QUERYS      CLOB;

BEGIN

	-- set procedure name
    V_SPNAME := 'SP_IFRS_INITIAL_UPDATE_IMP';
    
   ------------------------------------------------------------------------
    -- SET CURRDATE
    ------------------------------------------------------------------------
    IF P_DOWNLOAD_DATE IS NULL THEN
        SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE_AMORT ipda ;
    ELSE
        V_CURRDATE := P_DOWNLOAD_DATE;
    END IF;
	
    V_PREVDATE := V_CURRDATE - 1;
    V_MODEL_ID := NVL(P_SYSCODE, '0');
   
    ------------------------------------------------------------------------
    -- SET TABLE NAMES
    ------------------------------------------------------------------------
    IF P_PRC = 'S' THEN
        V_TABLEINSERT1  := 'IFRS_MASTER_ACCOUNT_' || P_RUNID;
    ELSE
        V_TABLEINSERT1  := 'IFRS_MASTER_ACCOUNT';
    END IF;
   

    ------------------------------------------------------------------------
    -- UPDATE GLOBAL_CUSTOMER_NUMBER
    ------------------------------------------------------------------------
    V_STR_QUERY := 
    '
			MERGE INTO '|| V_TAB_OWNER || '.' || V_TABLEINSERT1 ||' a USING
			(
				SELECT 
					 TO_DATE(SUBSTR(DOWNLOAD_DATE,1,10),''YYYY-MM-DD'') AS DDATE
					,CUSTOMER_NUMBER
					,GLOBAL_CUSTIMER_NUMBER
				FROM BSI_SUPER_CIF_FINAL
				WHERE TO_DATE(SUBSTR(DOWNLOAD_DATE,1,10),''YYYY-MM-DD'') = '|| TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || '
			) cif 
			ON 
			(
				a.DOWNLOAD_DATE = cif.DDATE 
				AND 
				a.CUSTOMER_NUMBER = cif.CUSTOMER_NUMBER
			)
			WHEN MATCHED THEN UPDATE 
			SET 
				GLOBAL_CUSTOMER_NUMBER = dat.GLOBAL_CUSTOMER_NUMBER        
   ';

	EXECUTE IMMEDIATE V_STR_QUERY;
    --DBMS_OUTPUT.PUT_LINE(V_STR_QUERY);
    COMMIT;   

   
    ------------------------------------------------------------------------
    -- UPDATE DIVISION_CODE
    ------------------------------------------------------------------------   
    V_STR_QUERY := 
    '
			MERGE INTO '|| V_TAB_OWNER || '.' || V_TABLEINSERT1 ||' a USING
			(
				SELECT 
					 TO_DATE(SUBSTR(FICMISDATE,1,10),''YYYY-MM-DD'') AS DDATE
					,NOLOAN
					,DIVISI_PISAH
				FROM LOANPERDIVISI_ALL
				WHERE TO_DATE(SUBSTR(FICMISDATE,1,10),''YYYY-MM-DD'') = '|| TO_CHAR(V_CURRDATE,'YYYY-MM-DD') || '
			) cif 
			ON 
			(
				a.DOWNLOAD_DATE = cif.DDATE 
				AND 
				a.ACCOUNT_NUMBER = cif.NOLOAN
				AND
				a.DATA_SOURCE IN (''iBSM'',''JFAST'')
			)
			WHEN MATCHED THEN UPDATE 
			SET 
				DIVISION_CODE = dat.DIVISI_PISAH        
   ';

	EXECUTE IMMEDIATE V_STR_QUERY;
    --DBMS_OUTPUT.PUT_LINE(V_STR_QUERY);
    COMMIT;      
   
   
   
END;
/
