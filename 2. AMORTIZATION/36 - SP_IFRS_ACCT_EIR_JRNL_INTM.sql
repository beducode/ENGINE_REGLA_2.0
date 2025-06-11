---- DROP PROCEDURE SP_IFRS_RESET_AMT_PRC;

CREATE OR REPLACE PROCEDURE SP_IFRS_RESET_AMT_PRC(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000',
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S')
LANGUAGE PLPGSQL AS $$
DECLARE
    ---- DATE
    V_PREVDATE DATE;
    V_CURRDATE DATE;

    ---- QUERY   
    V_STR_QUERY TEXT;

    ---- TABLE LIST       
    V_TABLEUPDATE1 VARCHAR(100);
    V_TABLEUPDATE2 VARCHAR(100);
    V_TABLEUPDATE3 VARCHAR(100);
    V_TABLEUPDATE4 VARCHAR(100);
    V_TABLEUPDATE5 VARCHAR(100);
    V_TABLEUPDATE6 VARCHAR(100);
    V_TABLEUPDATE7 VARCHAR(100);
    V_TABLEUPDATE8 VARCHAR(100);
    V_TABLEUPDATE9 VARCHAR(100);
    V_TABLEUPDATE10 VARCHAR(100);
    V_TABLEUPDATE11 VARCHAR(100);
    V_TABLEUPDATE12 VARCHAR(100);

    --- VARIABLE
    V_SP_NAME VARCHAR(100);
    STACK TEXT; 
    FCESIG TEXT;
BEGIN 
    -------- ====== VARIABLE ======
    GET DIAGNOSTICS STACK = PG_CONTEXT;
	FCESIG := substring(STACK from 'function (.*?) line');
	V_SP_NAME := UPPER(LEFT(fcesig::regprocedure::text, POSITION('(' in fcesig::regprocedure::text)-1));

    IF COALESCE(P_PRC, NULL) IS NULL THEN
        P_PRC := 'S';
    END IF;

    IF COALESCE(P_RUNID, NULL) IS NULL THEN
        P_RUNID := 'S_00000_0000';
    END IF;

    IF P_PRC = 'S' THEN 
        V_TABLEUPDATE1 := 'IFRS_ACCT_SL_ECF_' || P_RUNID || '';
        V_TABLEUPDATE2 := 'IFRS_ACCT_EIR_ECF_' || P_RUNID || '';
        V_TABLEUPDATE3 := 'IFRS_ACCT_SL_ACCRU_PREV_' || P_RUNID || '';
        V_TABLEUPDATE4 := 'IFRS_ACCT_EIR_ACCRU_PREV_' || P_RUNID || '';
        V_TABLEUPDATE5 := 'IFRS_ACCT_SL_ACF_' || P_RUNID || '';
        V_TABLEUPDATE6 := 'IFRS_ACCT_EIR_ACF_' || P_RUNID || '';
        V_TABLEUPDATE7 := 'IFRS_ACCT_SL_COST_FEE_ECF_' || P_RUNID || '';
        V_TABLEUPDATE8 := 'IFRS_ACCT_EIR_COST_FEE_ECF_' || P_RUNID || '';
        V_TABLEUPDATE9 := 'IFRS_ACCT_SL_COST_FEE_PREV_' || P_RUNID || '';
        V_TABLEUPDATE10 := 'IFRS_ACCT_EIR_COST_FEE_PREV_' || P_RUNID || '';
        V_TABLEUPDATE11 := 'IFRS_ACCT_COST_FEE_' || P_RUNID || '';
        V_TABLEUPDATE12 := 'IFRS_ACCT_JOURNAL_INTM_' || P_RUNID || '';
    ELSE 
        V_TABLEUPDATE1 := 'IFRS_ACCT_SL_ECF';
        V_TABLEUPDATE2 := 'IFRS_ACCT_EIR_ECF';
        V_TABLEUPDATE3 := 'IFRS_ACCT_SL_ACCRU_PREV';
        V_TABLEUPDATE4 := 'IFRS_ACCT_EIR_ACCRU_PREV';
        V_TABLEUPDATE5 := 'IFRS_ACCT_SL_ACF';
        V_TABLEUPDATE6 := 'IFRS_ACCT_EIR_ACF';
        V_TABLEUPDATE7 := 'IFRS_ACCT_SL_COST_FEE_ECF';
        V_TABLEUPDATE8 := 'IFRS_ACCT_EIR_COST_FEE_ECF';
        V_TABLEUPDATE9 := 'IFRS_ACCT_SL_COST_FEE_PREV';
        V_TABLEUPDATE10 := 'IFRS_ACCT_EIR_COST_FEE_PREV';
        V_TABLEUPDATE11 := 'IFRS_ACCT_COST_FEE';
        V_TABLEUPDATE12 := 'IFRS_ACCT_JOURNAL_INTM';
    END IF;
    
    IF P_DOWNLOAD_DATE IS NULL 
    THEN
        SELECT
            CURRDATE, PREVDATE INTO V_CURRDATE, V_PREVDATE
        FROM
            IFRS_PRC_DATE;
    ELSE        
        V_CURRDATE := P_DOWNLOAD_DATE;
        V_PREVDATE := V_CURRDATE - INTERVAL '1 DAY';
    END IF;
    -------- ====== VARIABLE ======

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'DROP TABLE IF EXISTS ' || V_TABLEUPDATE1 || ', ' || V_TABLEUPDATE2 || ', ' || V_TABLEUPDATE3 || ', ' || V_TABLEUPDATE4 || ', ' || V_TABLEUPDATE5 || ', ' || V_TABLEUPDATE6 || ', ' || V_TABLEUPDATE7 || ', ' || V_TABLEUPDATE8 || ', ' || V_TABLEUPDATE9 || ', ' || V_TABLEUPDATE10 || ', ' || V_TABLEUPDATE11 || ', ' || V_TABLEUPDATE12 || ' ';
        EXECUTE (V_STR_QUERY);

        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEUPDATE1 || ' AS SELECT * FROM IFRS_ACCT_SL_ECF WHERE 1=0; ';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEUPDATE2 || ' AS SELECT * FROM IFRS_ACCT_EIR_ECF WHERE 1=0; ';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEUPDATE3 || ' AS SELECT * FROM IFRS_ACCT_SL_ACCRU_PREV WHERE 1=0; ';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEUPDATE4 || ' AS SELECT * FROM IFRS_ACCT_EIR_ACCRU_PREV WHERE 1=0; ';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEUPDATE5 || ' AS SELECT * FROM IFRS_ACCT_SL_ACF WHERE 1=0; ';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEUPDATE6 || ' AS SELECT * FROM IFRS_ACCT_EIR_ACF WHERE 1=0; ';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEUPDATE7 || ' AS SELECT * FROM IFRS_ACCT_SL_COST_FEE_ECF WHERE 1=0; ';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEUPDATE8 || ' AS SELECT * FROM IFRS_ACCT_EIR_COST_FEE_ECF WHERE 1=0; ';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEUPDATE9 || ' AS SELECT * FROM IFRS_ACCT_SL_COST_FEE_PREV WHERE 1=0; ';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEUPDATE10 || ' AS SELECT * FROM IFRS_ACCT_EIR_COST_FEE_PREV WHERE 1=0; ';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEUPDATE11 || ' AS SELECT * FROM IFRS_ACCT_COST_FEE WHERE 1=0; ';
        V_STR_QUERY := V_STR_QUERY || 'CREATE TABLE ' || V_TABLEUPDATE12 || ' AS SELECT * FROM IFRS_ACCT_JOURNAL_INTM WHERE 1=0; ';
        EXECUTE (V_STR_QUERY);
    END IF;
    -------- ====== PRE SIMULATION TABLE ======
    -------- ====== BODY ======

USE [IFRS9]
GO
/****** Object:  StoredProcedure [dbo].[SP_IFRS_ACCT_EIR_JRNL_INTM]    Script Date: 14/06/2024 06:32:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE PROCEDURE [dbo].[SP_IFRS_ACCT_EIR_JRNL_INTM]                
AS                
DECLARE @V_CURRDATE DATE                
 ,@V_PREVDATE DATE                
 ,@PARAM_DISABLE_ACCRU_PREV BIGINT                
 ,@V_ROUND INT = 6                
 ,@V_FUNCROUND INT = 1                
                
BEGIN                
 --DISABLE ACCRU PREV CREATE ON NEW ECF AND RETURN ACCRUAL TO UNAMORT                
 --ADD YAHYA                
 SELECT @PARAM_DISABLE_ACCRU_PREV = CASE                 
   WHEN COMMONUSAGE = 'Y'                
    THEN 1                
   ELSE 0                
   END                
 FROM TBLM_COMMONCODEHEADER                
 WHERE COMMONCODE = 'SCM005'                
                
 --SET @PARAM_DISABLE_ACCRU_PREV = 1                  
 SELECT @V_CURRDATE = MAX(CURRDATE)                
  ,@V_PREVDATE = MAX(PREVDATE)                
 FROM IFRS_PRC_DATE_AMORT                
                
 SELECT @V_ROUND = CAST(VALUE1 AS INT)                
  ,@V_FUNCROUND = CAST(VALUE2 AS INT)                
 FROM TBLM_COMMONCODEDETAIL                
 WHERE COMMONCODE = 'SCM003'                
                
 --20171016 SET DEFAULT VALUE                  
 IF @V_ROUND IS NULL                
  SET @V_ROUND = 6                
                
 IF @V_FUNCROUND IS NULL                
  SET @V_FUNCROUND = 1                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'START'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,''                
  )                
                
 --DELETE FIRST                   
 DELETE                
 FROM IFRS_ACCT_JOURNAL_INTM                
 WHERE DOWNLOAD_DATE >= @V_CURRDATE                
  AND SUBSTRING(SOURCEPROCESS, 1, 3) = 'EIR'                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'1'                
  )                
                
 -- PNL = DEFA0 + AMORT OF NEW COST FEE TODAY                  
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,IS_PNL                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  )                
 SELECT FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRD_CODE                
  ,TRX_CODE                
  ,CCY                
  ,'DEFA0'                
  ,'ACT'                
  ,'N'                
  ,CASE                 
   WHEN FLAG_REVERSE = 'Y'                
    THEN - 1 * AMOUNT                
   ELSE AMOUNT                
   END                
  ,CURRENT_TIMESTAMP                
  ,'EIR PNL 1'                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRCODE                
  ,'Y' IS_PNL                
  ,PRD_TYPE                
  ,'ITRCG'                
  ,CF_ID                
 FROM IFRS_ACCT_COST_FEE                
 WHERE DOWNLOAD_DATE = @V_CURRDATE                
  AND STATUS = 'PNL'                
  AND METHOD = 'EIR'                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'       
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'2'                
  )                
                
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE           
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,IS_PNL                
  ,PRDTYPE                
  ,JOURNALCODE2          
  ,CF_ID                
  )                
 SELECT FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRD_CODE                
  ,TRX_CODE                
  ,CCY                
  ,'AMORT'                
  ,'ACT'                
  ,'N'                
  ,- 1 * (                
   CASE                 
    WHEN FLAG_REVERSE = 'Y'                
     THEN - 1 * AMOUNT                
    ELSE AMOUNT                
    END                
   )                
  ,CURRENT_TIMESTAMP                
  ,'EIR PNL 2'                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRCODE                
  ,'Y' IS_PNL                
  ,PRD_TYPE                
  ,'ACCRU'                
  ,CF_ID                
 FROM IFRS_ACCT_COST_FEE                
 WHERE DOWNLOAD_DATE = @V_CURRDATE                
  AND STATUS = 'PNL'                
  AND METHOD = 'EIR'                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'3'                
  )                
                
 -- PNL = AMORT OF UNAMORT BY CURRDATE                  
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  )                
 SELECT FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,'AMORT'                
  ,'ACT'                
  ,'N'                
  ,- 1 * (                
   CASE                 
    WHEN FLAG_REVERSE = 'Y'                
     THEN - 1 * AMOUNT                
    ELSE AMOUNT                
    END                
   )                
  ,CURRENT_TIMESTAMP                
  ,'EIR PNL 3'                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRCODE                
  ,PRDTYPE                
  ,'ACCRU'                
  ,CF_ID                
 FROM IFRS_ACCT_EIR_COST_FEE_PREV                
 WHERE DOWNLOAD_DATE = @V_CURRDATE                
  AND STATUS = 'PNL'                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME     
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'4'                
  )                
                
 -- PNL2 = AMORT OF UNAMORT BY PREVDATE                  
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  ,METHOD                
  )                
 SELECT FACNO                
  ,CIFNO                
  ,@V_CURRDATE AS DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,'AMORT'                
  ,'ACT'                
  ,'N'                
,- 1 * (                
   CASE                 
    WHEN FLAG_REVERSE = 'Y'                
     THEN - 1 * AMOUNT                
    ELSE AMOUNT                
    END                
   )                
  ,CURRENT_TIMESTAMP                
  ,'EIR PNL 4'                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRCODE                
  ,PRDTYPE                
  ,'ACCRU'                
  ,CF_ID                
  ,METHOD                
 FROM (                
  SELECT ACCTNO                
   ,SUM(AMOUNT) AS AMOUNT                
   ,PRDTYPE                
   ,BRCODE                
   ,CCY                
   ,CF_ID                
   ,CIFNO                
   ,DATASOURCE                
   ,DOWNLOAD_DATE                
   ,FACNO                
   ,FLAG_CF                
   ,FLAG_REVERSE                
   ,PRDCODE                
   ,TRXCODE                
   ,MASTERID                
   ,METHOD                
   ,STATUS                
  FROM IFRS_ACCT_EIR_COST_FEE_PREV                
  WHERE DOWNLOAD_DATE = @V_PREVDATE                
   AND STATUS = 'PNL2'                
  GROUP BY ACCTNO                
   ,PRDTYPE                
   ,BRCODE                
   ,CCY                
   ,CF_ID                
   ,CIFNO                
   ,DATASOURCE                
   ,DOWNLOAD_DATE                
   ,FACNO                
   ,FLAG_CF                
   ,FLAG_REVERSE                
   ,PRDCODE                
   ,TRXCODE                
   ,MASTERID                
   ,METHOD                
   ,STATUS                
  ) A                
 WHERE A.DOWNLOAD_DATE = @V_PREVDATE                
  AND A.STATUS = 'PNL2'                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'5'                
  )                
                
 --DEFA0 NORMAL AMORTIZED COST/FEE                  
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  )                
 SELECT FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
,PRD_CODE                
  ,TRX_CODE                
  ,CCY                
  ,'DEFA0'                
  ,'ACT'                
  ,'N'                
  ,CASE                 
   WHEN FLAG_REVERSE = 'Y'                
    THEN - 1 * AMOUNT                
   ELSE AMOUNT                
   END                
  ,CURRENT_TIMESTAMP                
  ,'EIR ACT 1'                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRCODE                
  ,PRD_TYPE                
  ,'ITRCG'                
  ,CF_ID                
 FROM IFRS_ACCT_COST_FEE                
 WHERE DOWNLOAD_DATE = @V_CURRDATE                
  AND STATUS = 'ACT'                
  AND METHOD = 'EIR'                
                
 INSERT INTO IFRS_AMORT_LOG (          
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'6'                
  )                
                
  /*                
 --HRD DEFA0 COME FROM DIFFERENT TABLE                  
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE      ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  )                
 SELECT FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,'DEFA0'                
  ,'ACT'                
  ,'N'                
  ,CASE                 
   WHEN FLAG_REVERSE = 'Y'                
    THEN - 1 * AMOUNT                
   ELSE AMOUNT                
   END                
  ,CURRENT_TIMESTAMP                
  ,'EIR HRD 1'                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRCODE                
  ,PRDTYPE                
  ,'ITRCG'                
  ,CF_ID                
 FROM IFRS_ACCT_EIR_COST_FEE_ECF                
 WHERE DOWNLOAD_DATE = @V_CURRDATE                
  AND STATUS = 'ACT'                
  AND SRCPROCESS = 'STAFFLOAN'                
                
  */                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'7'                
  )                
                
 --REVERSE ACCRUAL                  
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  )                
 SELECT FACNO                
  ,CIFNO                
  ,@V_CURRDATE                
  ,DATASOURCE                
  ,PRDCODE                
 ,TRXCODE                
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,'Y'                
  ,N_AMOUNT                
  ,CURRENT_TIMESTAMP                
  ,'EIR REV ACCRU'                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE                
  ,CF_ID                
 FROM IFRS_ACCT_JOURNAL_INTM                
 WHERE DOWNLOAD_DATE = @V_PREVDATE                
  AND STATUS = 'ACT'                
  AND TRXCODE <> 'BENEFIT'                
  AND JOURNALCODE IN (                
   'ACCRU'                
   ,'ACRU4'                
   ) -- INCLUDE ALSO NO COST FEE ECF                  
  AND REVERSE = 'N'                
  AND SUBSTRING(SOURCEPROCESS, 1, 3) = 'EIR'                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'8'                
  )                
                
 --ACCRU FEE                  
 TRUNCATE TABLE TMP_T5                
                
 INSERT INTO TMP_T5 (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,N_AMOUNT                
  ,ACCTNO                
  ,MASTERID                
  ,BRCODE                
  ,PRDTYPE                
  ,CF_ID                
  )                
 SELECT FACNO                
  ,CIFNO                
  ,ECFDATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,SUM(CASE                 
    WHEN FLAG_REVERSE = 'Y'                
     THEN - 1 * AMOUNT                
    ELSE AMOUNT                
    END) AS N_AMOUNT                
  ,ACCTNO                
  ,MASTERID                
  ,BRCODE                
  ,PRDTYPE                
  ,CF_ID                
 FROM IFRS_ACCT_EIR_COST_FEE_ECF                
 WHERE FLAG_CF = 'F' AND TRXCODE <> 'BENEFIT'  AND STATUS='ACT'               
 GROUP BY FACNO                
  ,CIFNO                
  ,ECFDATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
 ,FLAG_REVERSE                
  ,ACCTNO                
  ,MASTERID                
  ,BRCODE                
  ,PRDTYPE                
  ,CF_ID                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'9'                
  )                
                
 TRUNCATE TABLE TMP_T6                
                
 INSERT INTO TMP_T6 (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,SUM_AMT                
  ,ACCTNO                
  ,MASTERID                
  ,BRCODE                
  )                
 SELECT FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,SUM(N_AMOUNT) AS SUM_AMT                
  ,ACCTNO                
  ,MASTERID                
  ,BRCODE                
 FROM TMP_T5 D                
 GROUP BY FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,ACCTNO                
  ,MASTERID                
  ,BRCODE                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE         
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'10'                
  )                
                
 --EXECUTE IMMEDIATE 'DROP INDEX TMP_T5_IDX1';                  
--EXECUTE IMMEDIATE 'DROP INDEX TMP_T6_IDX1';                  
 --EXECUTE IMMEDIATE 'CREATE INDEX TMP_T5_IDX1 ON TMP_T5(DOWNLOAD_DATE,MASTERID)';                  
 --EXECUTE IMMEDIATE 'CREATE INDEX TMP_T6_IDX1 ON TMP_T6(DOWNLOAD_DATE,MASTERID)';                  
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  )                
 SELECT A.FACNO                
  ,A.CIFNO                
  ,A.DOWNLOAD_DATE                
  ,A.DATASOURCE                
  ,B.PRDCODE                
  ,B.TRXCODE                
  ,B.CCY                
  ,'ACCRU'                
  ,'ACT'                
  ,'N'                
  ,ROUND(A.N_ACCRU_FEE * CAST(CAST(B.N_AMOUNT AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS DECIMAL(32, 20)), @V_ROUND, @V_FUNCROUND)                
  ,CURRENT_TIMESTAMP                
  ,'EIR ACCRU FEE 1'                
  ,A.ACCTNO                
  ,A.MASTERID                
  ,'F'                
  ,B.BRCODE                
  ,B.PRDTYPE                
  ,'ACCRU'                
  ,B.CF_ID                
 FROM IFRS_ACCT_EIR_ACF A                
 JOIN TMP_T5 B ON B.DOWNLOAD_DATE = A.ECFDATE                
  AND B.MASTERID = A.MASTERID                
 JOIN TMP_T6 C ON C.MASTERID = A.MASTERID                
  AND A.ECFDATE = C.DOWNLOAD_DATE                
 WHERE A.DOWNLOAD_DATE = @V_CURRDATE                
  AND A.DO_AMORT = 'N'                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'11'                
  )                
                
 --AMORT FEE                  
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS            
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  )                
 SELECT A.FACNO                
  ,A.CIFNO                
  ,A.DOWNLOAD_DATE                
  ,A.DATASOURCE                
  ,B.PRDCODE                
  ,B.TRXCODE                
  ,B.CCY                
  ,'AMORT'                
  ,'ACT'                
  ,'N'                
  ,ROUND(A.N_ACCRU_FEE * CAST(CAST(B.N_AMOUNT AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS DECIMAL(32, 20)), @V_ROUND, @V_FUNCROUND)                
  ,CURRENT_TIMESTAMP                
  ,'EIR AMORT FEE 1'                
  ,A.ACCTNO                
  ,A.MASTERID                
  ,'F'                
  ,B.BRCODE                
  ,B.PRDTYPE                
  ,'ACCRU'                
  ,B.CF_ID                
 FROM IFRS_ACCT_EIR_ACF A                
 JOIN TMP_T5 B ON B.DOWNLOAD_DATE = A.ECFDATE                
  AND B.MASTERID = A.MASTERID                
 JOIN TMP_T6 C ON C.MASTERID = A.MASTERID                
  AND A.ECFDATE = C.DOWNLOAD_DATE                
 WHERE A.DOWNLOAD_DATE = @V_CURRDATE                
  AND A.DO_AMORT = 'Y'                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'12'                
  )                
                
 --STOP REV DEFA0 FEE 20160619                  
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE               
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  )                
 SELECT A.FACNO                
  ,A.CIFNO                
  ,A.DOWNLOAD_DATE                
  ,A.DATASOURCE                
  ,B.PRDCODE                
  ,B.TRXCODE                
  ,B.CCY                
  ,'DEFA0'                
  ,'ACT'                
  ,'N'                
  ,ROUND(- 1 * A.N_ACCRU_FEE * CAST(CAST(B.N_AMOUNT AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS DECIMAL(32, 20)), @V_ROUND, @V_FUNCROUND)                
  ,CURRENT_TIMESTAMP                
  ,'EIR DEFA0 FEE 1'                
  ,A.ACCTNO                
  ,A.MASTERID                
  ,'F'                
  ,B.BRCODE                
  ,B.PRDTYPE                
  ,'ITRCG'                
  ,B.CF_ID                
 FROM IFRS_ACCT_EIR_ACF A                
 JOIN TMP_T5 B ON B.DOWNLOAD_DATE = A.ECFDATE                
  AND B.MASTERID = A.MASTERID                
 JOIN TMP_T6 C ON C.MASTERID = A.MASTERID                
  AND A.ECFDATE = C.DOWNLOAD_DATE                
 --ONLY FOR STOP REV                  
 JOIN (                
  SELECT DISTINCT MASTERID                
  FROM IFRS_ACCT_EIR_STOP_REV                
  WHERE DOWNLOAD_DATE = @V_CURRDATE                
  ) D ON A.MASTERID = D.MASTERID                
 WHERE A.DOWNLOAD_DATE = @V_CURRDATE                
  AND A.DO_AMORT = 'Y'                
                
 --ONLY FOR STOP REV                  
 --AND A.MASTERID IN (SELECT MASTERID FROM IFRS_ACCT_EIR_STOP_REV WHERE DOWNLOAD_DATE=@V_CURRDATE)                  
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'13'                
  )                
                
 --ACCRU COST                  
 TRUNCATE TABLE TMP_T5                
                
 INSERT INTO TMP_T5 (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,N_AMOUNT                
  ,ACCTNO                
  ,MASTERID                
  ,BRCODE                
  ,PRDTYPE                
  ,CF_ID                
  )                
 SELECT FACNO                
  ,CIFNO                
  ,ECFDATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,SUM(CASE                 
    WHEN FLAG_REVERSE = 'Y'            
     THEN - 1 * AMOUNT                
    ELSE AMOUNT                
    END) AS N_AMOUNT                
  ,ACCTNO                
  ,MASTERID                
  ,BRCODE                
  ,PRDTYPE                
  ,CF_ID                
 FROM IFRS_ACCT_EIR_COST_FEE_ECF                
 WHERE FLAG_CF = 'C' AND TRXCODE <> 'BENEFIT' AND STATUS='ACT'                 
 GROUP BY FACNO                
  ,CIFNO                
  ,ECFDATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,ACCTNO                
  ,MASTERID                
  ,BRCODE                
  ,PRDTYPE                
  ,CF_ID                
  ,FLAG_REVERSE                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'14'                
  )                
                
 TRUNCATE TABLE TMP_T6                
                
 INSERT INTO TMP_T6 (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,SUM_AMT                
  ,ACCTNO                
  ,MASTERID                
  ,BRCODE                
  )                
 SELECT FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,SUM(N_AMOUNT) AS SUM_AMT                
  ,ACCTNO                
  ,MASTERID                
  ,BRCODE                
 FROM TMP_T5 D                
 GROUP BY FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,ACCTNO                
  ,MASTERID                
  ,BRCODE                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'15'                
  )                
                
 --EXECUTE IMMEDIATE 'DROP INDEX TMP_T5_IDX1';                  
 --EXECUTE IMMEDIATE 'DROP INDEX TMP_T6_IDX1';                  
 --EXECUTE IMMEDIATE 'CREATE INDEX TMP_T5_IDX1 ON TMP_T5(DOWNLOAD_DATE,MASTERID)';                  
 --EXECUTE IMMEDIATE 'CREATE INDEX TMP_T6_IDX1 ON TMP_T6(DOWNLOAD_DATE,MASTERID)';                  
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  )                
 SELECT A.FACNO                
  ,A.CIFNO                
  ,A.DOWNLOAD_DATE                
  ,A.DATASOURCE                
  ,B.PRDCODE                
  ,B.TRXCODE                
  ,B.CCY                
  ,'ACCRU'                
  ,'ACT'                
  ,'N'                
  ,ROUND(A.N_ACCRU_COST * CAST(CAST(B.N_AMOUNT AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS DECIMAL(32, 20)), @V_ROUND, @V_FUNCROUND)                
  ,CURRENT_TIMESTAMP                
  ,'EIR ACCRU COST 1'                
  ,A.ACCTNO                
  ,A.MASTERID                
  ,'C'                
  ,B.BRCODE                
  ,B.PRDTYPE                
  ,'ACCRU'                
  ,B.CF_ID                
 FROM IFRS_ACCT_EIR_ACF A    
 JOIN TMP_T5 B ON B.DOWNLOAD_DATE = A.ECFDATE                
  AND B.MASTERID = A.MASTERID                
 JOIN TMP_T6 C ON C.MASTERID = A.MASTERID                
  AND A.ECFDATE = C.DOWNLOAD_DATE                
 WHERE A.DOWNLOAD_DATE = @V_CURRDATE           
  AND A.DO_AMORT = 'N'                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'16'                
  )                
                
 --AMORT COST                  
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  )                
 SELECT A.FACNO                
  ,A.CIFNO                
  ,A.DOWNLOAD_DATE                
  ,A.DATASOURCE                
  ,B.PRDCODE                
  ,B.TRXCODE                
  ,B.CCY                
  ,'AMORT'                
  ,'ACT'                
  ,'N'                
  ,ROUND(A.N_ACCRU_COST * CAST(CAST(B.N_AMOUNT AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS DECIMAL(32, 20)), @V_ROUND, @V_FUNCROUND)                
  ,CURRENT_TIMESTAMP                
  ,'EIR AMORT COST 1'                
  ,A.ACCTNO                
  ,A.MASTERID                
  ,'C'                
  ,B.BRCODE                
  ,B.PRDTYPE                
  ,'ACCRU'                
  ,B.CF_ID                
 FROM IFRS_ACCT_EIR_ACF A                
 JOIN TMP_T5 B ON B.DOWNLOAD_DATE = A.ECFDATE                
  AND B.MASTERID = A.MASTERID                
 JOIN TMP_T6 C ON C.MASTERID = A.MASTERID                
  AND A.ECFDATE = C.DOWNLOAD_DATE                
 WHERE A.DOWNLOAD_DATE = @V_CURRDATE                
  AND A.DO_AMORT = 'Y'                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'17'                
  )                
                
 --STOP REV DEFA0 COST 20160619                  
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  )                
 SELECT A.FACNO                
  ,A.CIFNO                
  ,A.DOWNLOAD_DATE                
  ,A.DATASOURCE                
  ,B.PRDCODE                
  ,B.TRXCODE                
  ,B.CCY                
  ,'DEFA0'                
  ,'ACT'                
  ,'N'                
  ,ROUND(- 1 * A.N_ACCRU_COST * CAST(CAST(B.N_AMOUNT AS FLOAT) / CAST(C.SUM_AMT AS FLOAT) AS DECIMAL(32, 20)), @V_ROUND, @V_FUNCROUND)                
  ,CURRENT_TIMESTAMP                
  ,'EIR AMORT COST 1'                
  ,A.ACCTNO                
 ,A.MASTERID                
  ,'C'                
  ,B.BRCODE                
  ,B.PRDTYPE                
  ,'ITRCG'                
  ,B.CF_ID                
 FROM IFRS_ACCT_EIR_ACF A                
 JOIN TMP_T5 B ON B.DOWNLOAD_DATE = A.ECFDATE                
  AND B.MASTERID = A.MASTERID                
 JOIN TMP_T6 C ON C.MASTERID = A.MASTERID                
  AND A.ECFDATE = C.DOWNLOAD_DATE                
 --STOPREV                  
 JOIN (                
  SELECT DISTINCT MASTERID                
  FROM IFRS_ACCT_EIR_STOP_REV                
  WHERE DOWNLOAD_DATE = @V_CURRDATE                
  ) D ON A.MASTERID = D.MASTERID                
 WHERE A.DOWNLOAD_DATE = @V_CURRDATE                
  AND A.DO_AMORT = 'Y'                
                
 --STOPREV                  
 --AND A.MASTERID IN (SELECT MASTERID FROM IFRS_ACCT_EIR_STOP_REV WHERE DOWNLOAD_DATE=@V_CURRDATE)                  
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'18'                
  )                
                
 -- 20160407 DANIEL S : SET BLK BEFORE ACCRU PREV CODE                  
 -- UPDATE STATUS ACCRU PREV FOR EIR STOP REV                  
 UPDATE IFRS_ACCT_EIR_ACCRU_PREV                
 SET IFRS_ACCT_EIR_ACCRU_PREV.STATUS = CONVERT(VARCHAR, @V_CURRDATE, 112) + 'BLK'                
 FROM IFRS_ACCT_EIR_ACF A                
 JOIN IFRS_ACCT_EIR_STOP_REV E ON E.DOWNLOAD_DATE = @V_CURRDATE                
  AND E.MASTERID = A.MASTERID                
 JOIN IFRS_ACCT_EIR_ACCRU_PREV C ON C.MASTERID = A.MASTERID                
  AND C.STATUS = 'ACT'                
  AND C.DOWNLOAD_DATE <= @V_CURRDATE                
 WHERE A.DOWNLOAD_DATE = @V_PREVDATE                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'19'                
  )                
     
 --EIR ACCRU PREV                  
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  ,METHOD                
  )                
 SELECT A.FACNO                
  ,A.CIFNO                
  ,A.DOWNLOAD_DATE                
  ,A.DATASOURCE                
  ,C.PRDCODE                
  ,C.TRXCODE                
  ,C.CCY                
  ,'ACCRU'                
  ,'ACT'                
  ,'N'                
  ,CASE                 
   WHEN C.FLAG_REVERSE = 'Y'                
    THEN - 1 * C.AMOUNT                
   ELSE C.AMOUNT                
   END                
  ,CURRENT_TIMESTAMP                
  ,'EIR ACCRU PREV'                
  ,A.ACCTNO                
  ,A.MASTERID                
  ,C.FLAG_CF                
  ,A.BRANCH                
  ,C.PRDTYPE                
  ,'ACCRU'                
  ,C.CF_ID                
  ,C.METHOD                
 FROM IFRS_ACCT_EIR_ACF A                
 JOIN IFRS_ACCT_EIR_ACCRU_PREV C ON C.MASTERID = A.MASTERID                
  AND C.STATUS = 'ACT'                
  AND C.DOWNLOAD_DATE <= @V_CURRDATE                
 WHERE A.DOWNLOAD_DATE = @V_CURRDATE                
  AND A.DO_AMORT = 'N'   
  AND  A.MASTERID NOT IN (SELECT DISTINCT MASTERID FROM IFRS_ACCT_SWITCH WHERE DOWNLOAD_DATE = C.DOWNLOAD_DATE  ) -- ADD TO EXCLUDE SWITCH 2 SEP 2019  
  
    
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'20'                
  )                
                
 --EIR AMORT PREV                  
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  ,METHOD                
  )                
 SELECT A.FACNO                
  ,A.CIFNO                
  ,A.DOWNLOAD_DATE                
  ,A.DATASOURCE                
  ,C.PRDCODE                
  ,C.TRXCODE                
  ,C.CCY                
  ,'AMORT'                
  ,'ACT'                
  ,'N'                
  ,CASE                 
   WHEN C.FLAG_REVERSE = 'Y'                
    THEN - 1 * C.AMOUNT                
   ELSE C.AMOUNT                
   END                
  ,CURRENT_TIMESTAMP                
  ,'EIR AMORT PREV'                
  ,A.ACCTNO                
  ,A.MASTERID                
  ,C.FLAG_CF                
  ,A.BRANCH                
  ,C.PRDTYPE                
  ,'ACCRU'                
  ,C.CF_ID                
  ,C.METHOD                
 FROM IFRS_ACCT_EIR_ACF A                
 JOIN IFRS_ACCT_EIR_ACCRU_PREV C ON C.MASTERID = A.MASTERID                
  AND C.STATUS = CONVERT(VARCHAR(8), @V_CURRDATE, 112)                
  AND C.DOWNLOAD_DATE <= @V_CURRDATE                
 WHERE A.DOWNLOAD_DATE = @V_CURRDATE                
  AND A.DO_AMORT = 'Y'                
  AND  A.MASTERID NOT IN (SELECT DISTINCT MASTERID FROM IFRS_ACCT_SWITCH WHERE DOWNLOAD_DATE = C.DOWNLOAD_DATE OR (DOWNLOAD_DATE=A.DOWNLOAD_DATE AND PREV_BRCODE <> BRCODE)  ) -- ADD TO EXCLUDE SWITCH 2 SEP 2019  
    
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'21'                
  )               
                
 --ACCRU PREV WITH NO ACF FOR PNL ED ACCTNO AND DISABLE ACCRU PREV PARAM @ ECF MAIN                  
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT            
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  ,METHOD                
  )                
 SELECT C.FACNO                
  ,C.CIFNO                
  ,@V_CURRDATE AS DOWNLOAD_DATE                
  ,C.DATASOURCE                
  ,C.PRDCODE                
  ,C.TRXCODE                
  ,C.CCY                
  ,'AMORT'                
  ,'ACT'                
  ,'N'                
  ,CASE    
   WHEN C.FLAG_REVERSE = 'Y'                
    THEN - 1 * C.AMOUNT                
   ELSE C.AMOUNT                
   END                
  ,CURRENT_TIMESTAMP                
  ,'EIR AMORT PREV2'                
  ,C.ACCTNO                
  ,C.MASTERID                
  ,C.FLAG_CF                
  ,P.BRANCH_CODE                
  ,C.PRDTYPE                
  ,'ACCRU'                
  ,C.CF_ID                
  ,C.METHOD                
 FROM (                
  SELECT ACCTNO                
   ,AMORTDATE                
   ,SUM(AMOUNT) AS AMOUNT                
   ,PRDTYPE                
   ,CCY                
   ,CF_ID                
   ,CIFNO                
   ,DATASOURCE                
   ,DOWNLOAD_DATE                
   ,FACNO                
   ,FLAG_CF                
   ,FLAG_REVERSE                
   ,PRDCODE                
   ,TRXCODE                
   ,MASTERID                
,METHOD                
   ,STATUS                
  FROM IFRS_ACCT_EIR_ACCRU_PREV                
  WHERE DOWNLOAD_DATE <= @V_CURRDATE                
  GROUP BY ACCTNO                
   ,AMORTDATE                
   ,PRDTYPE                
   ,CCY                
   ,CF_ID                
   ,CIFNO                
   ,DATASOURCE                
   ,DOWNLOAD_DATE                
   ,FACNO                
   ,FLAG_CF                
   ,FLAG_REVERSE                
   ,PRDCODE                
   ,TRXCODE                
   ,MASTERID                
   ,METHOD                
   ,STATUS                
  ) C                
 JOIN IFRS_IMA_AMORT_CURR P ON P.MASTERID = C.MASTERID                
 --20180310 CHANGE FROM ECF TO ACF                
 LEFT JOIN IFRS_ACCT_EIR_ACF A ON A.MASTERID = C.MASTERID                
  AND A.DOWNLOAD_DATE = @V_CURRDATE                
 WHERE C.STATUS = CONVERT(VARCHAR(8), @V_CURRDATE, 112)                
  AND C.DOWNLOAD_DATE <= @V_CURRDATE                
  AND A.MASTERID IS NULL                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'22'                
  )                
                
 --EIR SWITCH AMORT OF ACCRU PREV                  
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  )                
 SELECT A.PREV_FACNO                
  ,A.PREV_CIFNO                
  ,A.DOWNLOAD_DATE                
  ,A.PREV_DATASOURCE                
  ,C.PRDCODE                
  ,C.TRXCODE                
  ,C.CCY                
  ,'AMORT'                
  ,'ACT'                
  ,'N'                
  ,CASE                 
   WHEN C.FLAG_REVERSE = 'Y'                
    THEN - 1 * C.AMOUNT                
   ELSE C.AMOUNT                
   END                
  ,CURRENT_TIMESTAMP                
  ,'EIR ACRU SW'                
  ,A.PREV_ACCTNO                
  ,A.PREV_MASTERID                
  ,C.FLAG_CF              
  ,A.PREV_BRCODE                
  ,C.PRDTYPE                
  ,'ACCRU'                
  ,C.CF_ID                
 FROM IFRS_ACCT_SWITCH A                
 JOIN IFRS_ACCT_EIR_ACCRU_PREV C ON C.MASTERID = A.PREV_MASTERID                
  AND C.STATUS = CONVERT(VARCHAR(8), @V_CURRDATE, 112)                
  ----AND C.DOWNLOAD_DATE = @V_CURRDATE
  AND C.DOWNLOAD_DATE <= @V_CURRDATE ---- PERBAIKAN FLOW SWITCH BRANCH                
 WHERE A.DOWNLOAD_DATE = @V_CURRDATE             
  AND A.PREV_EIR_ECF = 'Y'                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'23'                
  )                
                
              
 -- REV = REV OF UNAMORT BY CURRDATE                  
          
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  )                
 SELECT FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,'DEFA0'                
  ,'ACT'                
  ,'Y'                
  ,1 * (                
   CASE                 
    WHEN FLAG_REVERSE = 'Y'                
     THEN - 1 * AMOUNT                
    ELSE AMOUNT                
    END                
   )                
  ,CURRENT_TIMESTAMP                
  ,'EIR_REV_SWITCH'                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRCODE                
  ,PRDTYPE                
  ,'ITRCG'                
  ,CF_ID                
 FROM IFRS_ACCT_EIR_COST_FEE_PREV                
 WHERE DOWNLOAD_DATE = @V_CURRDATE                
  AND STATUS = 'REV' AND CREATEDBY = 'EIR_SWITCH'                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'24'                
  )                
                
 -- REV2 = REV OF UNAMORT BY PREVDATE                  
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE           
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  )                
 SELECT FACNO                
  ,CIFNO                
  ,@V_CURRDATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,'DEFA0'                
  ,'ACT'                
  ,'Y'                
  ,1 * (                
   CASE                 
    WHEN FLAG_REVERSE = 'Y'                
     THEN - 1 * AMOUNT                
    ELSE AMOUNT                
    END                
   )                
  ,CURRENT_TIMESTAMP                
  ,'EIR_REV_SWITCH'                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRCODE                
  ,PRDTYPE                
  ,'ITRCG'                
  ,CF_ID                
 FROM IFRS_ACCT_EIR_COST_FEE_PREV                
 WHERE DOWNLOAD_DATE = @V_PREVDATE                
  AND STATUS = 'REV2' AND CREATEDBY = 'EIR_SWITCH'           
          
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS    ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'25'                
  )                
                
 -- DEFA0 FOR NEW ACCT OF EIR SWITCH                  
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  )                
 SELECT FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,'DEFA0'                
  ,'ACT'                
  ,'N'                
  ,1 * (                
   CASE                 
    WHEN FLAG_REVERSE = 'Y'                
     THEN - 1 * AMOUNT                
    ELSE AMOUNT                
    END                
   )                
  ,CURRENT_TIMESTAMP                
  ,'EIR_SWITCH'                
  ,ACCTNO                
  ,MASTERID              
  ,FLAG_CF                
  ,BRCODE                
  ,PRDTYPE                
  ,'ITRCG'                
  ,CF_ID                
 FROM IFRS_ACCT_EIR_COST_FEE_PREV                
 WHERE DOWNLOAD_DATE = @V_CURRDATE                
  AND STATUS = 'ACT'                
  AND SEQ = '0'                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'           
  ,'26'                
  )                
            
              
 -- NO COST FEE ECF ACCRUAL JOURNAL INTM                  
 -- NO COST FEE ECF ACCRU                  
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  )                
 SELECT A.FACNO                
  ,A.CIFNO                
  ,A.DOWNLOAD_DATE                
  ,A.DATASOURCE                
  ,B.PRODUCT_CODE                
  ,'EIR_NOCF' TRXCODE                
  ,B.CURRENCY                
  ,'ACRU4'                
  ,'ACT'                
  ,'N'                
  ,                
  --A.N_ACCRU_NOCF ,                  
  A.N_UNAMORT_PREV_NOCF + A.N_ACCRU_NOCF                
  ,--20171016 NOCF IS POST REVERSE SO POST THE WHOLE AMOUNT                  
  CURRENT_TIMESTAMP                
  ,'EIR ACCRU NOCF'                
  ,A.ACCTNO                
  ,A.MASTERID                
  ,'S'                
  ,-- DIUBAH MENGIKUTI WEB 'N' ,                                         -- NOCF                  
  B.BRANCH_CODE                
  ,B.PRODUCT_TYPE                
  ,'ACRU4'                
  ,NULL --CFID                  
 FROM IFRS_ACCT_EIR_ACF A          
 JOIN IFRS_IMA_AMORT_CURR B ON B.MASTERID = A.MASTERID                
 WHERE A.DOWNLOAD_DATE = @V_CURRDATE                
  AND A.DO_AMORT = 'N'                
  AND A.N_ACCRU_NOCF IS NOT NULL                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'27'                
  )                
                
 -- NO COST FEE ECF AMORT                  
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  )                
 SELECT A.FACNO                
  ,A.CIFNO                
  ,A.DOWNLOAD_DATE                
  ,A.DATASOURCE                
  ,B.PRODUCT_CODE                
  ,'EIR_NOCF' TRXCODE                
  ,B.CURRENCY                
  ,'AMRT4'                
  ,'ACT'                
  ,'N'                
  ,A.N_ACCRU_NOCF                
  ,CURRENT_TIMESTAMP                
  ,'EIR AMORT NOCF'                
  ,A.ACCTNO                
  ,A.MASTERID                
  ,'N'                
  ,-- NOCF                  
  B.BRANCH_CODE                
  ,B.PRODUCT_TYPE                
  ,'AMRT4'                
  ,NULL --CFID                  
 FROM IFRS_ACCT_EIR_ACF A                
 JOIN IFRS_IMA_AMORT_CURR B ON B.MASTERID = A.MASTERID                
  AND B.DOWNLOAD_DATE = @V_CURRDATE                
 WHERE A.DOWNLOAD_DATE = @V_CURRDATE                
  AND A.DO_AMORT = 'Y'                
  AND A.N_ACCRU_NOCF IS NOT NULL                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'28'                
  )                
                
 -- PNL FOR NO COST FEE ECF FOR CLOSED ACCOUNT AND EVENT CHANGE                  
 TRUNCATE TABLE TMP_NOCF                
                
 INSERT INTO TMP_NOCF (MASTERID)                
 SELECT DISTINCT MASTERID                
 FROM IFRS_ACCT_CLOSED                
 WHERE DOWNLOAD_DATE = @V_CURRDATE                
                
 INSERT INTO TMP_NOCF (MASTERID)                
 SELECT DISTINCT MASTERID                
 FROM IFRS_ACCT_EIR_ECF (nolock)        
 WHERE DOWNLOAD_DATE = @V_CURRDATE                
  AND MASTERID NOT IN (                
   SELECT DISTINCT MASTERID                
   FROM IFRS_ACCT_CLOSED                
   WHERE DOWNLOAD_DATE = @V_CURRDATE                
   )                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM               
  ,OPS               
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'29'                
  )                
                
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,JOURNALCODE      
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  )                
 SELECT A.FACNO                
  ,A.CIFNO                
  ,@V_CURRDATE AS DOWNLOAD_DATE                
  ,A.DATASOURCE                
  ,ISNULL(B.PRODUCT_CODE, C.PRODUCT_CODE)                
  ,'EIR_NOCF' TRXCODE                
  ,ISNULL(B.CURRENCY, C.CURRENCY)                
  ,'AMRT4'                
  ,'ACT'                
  ,'Y'                
  ,CASE                 
   WHEN A.DO_AMORT = 'Y'                
    THEN A.N_UNAMORT_NOCF                
   ELSE A.N_UNAMORT_PREV_NOCF                
   END                
  ,CURRENT_TIMESTAMP                
  ,'EIR AMORT NOCF'                
  ,A.ACCTNO                
  ,A.MASTERID                
  ,'N'                
  ,-- NOCF                  
  ISNULL(B.BRANCH_CODE, C.BRANCH_CODE)                
  ,ISNULL(B.PRODUCT_TYPE, C.PRODUCT_TYPE)                
  ,'AMRT4'                
  ,NULL --CFID                  
 FROM IFRS_ACCT_EIR_ACF A                
 LEFT JOIN IFRS_IMA_AMORT_CURR B ON B.MASTERID = A.MASTERID                
  AND B.DOWNLOAD_DATE = @V_CURRDATE                
 LEFT JOIN IFRS_IMA_AMORT_PREV C ON C.MASTERID = A.MASTERID                
  AND C.DOWNLOAD_DATE = @V_PREVDATE --@V_CURRDATE                  
 WHERE A.ID IN (                
   SELECT MAX(ID)                
   FROM IFRS_ACCT_EIR_ACF                
   WHERE DOWNLOAD_DATE >= @V_PREVDATE                
    AND DOWNLOAD_DATE <= @V_CURRDATE                
    AND MASTERID IN (                
     SELECT MASTERID                
     FROM TMP_NOCF                
     )                
   GROUP BY MASTERID                
   )                
  AND CASE                 
   WHEN A.DO_AMORT = 'Y'                
    THEN A.N_UNAMORT_NOCF                
   ELSE A.N_UNAMORT_PREV_NOCF                
   END <> 0                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'30'                
  )                
                
 -- 20160407 EIR STOP REVERSE                  
 -- BEFORE EIR ACF RUN                  
 -- REVERSE UNAMORTIZED AND AMORT ACCRU IF EXIST                  
 -- UNAMORTIZED MAY BE USED BY OTHER PROCESS                  
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,JOURNALCODE                
  ,STATUS                
  ,REVERSE                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  )                
 SELECT A.FACNO                
  ,A.CIFNO                
  ,@V_CURRDATE AS DOWNLOAD_DATE                
  ,A.DATASOURCE                
  ,A.PRDCODE                
  ,A.TRXCODE                
  ,A.CCY                
  ,'DEFA0'                
  ,'ACT'                
  ,'Y'                
  ,CASE                 
   WHEN FLAG_REVERSE = 'Y'                
    THEN - 1 * AMOUNT                
   ELSE AMOUNT                
   END                
  ,CURRENT_TIMESTAMP                
  ,'EIR STOP REV 1'                
  ,A.ACCTNO                
  ,A.MASTERID                
  ,A.FLAG_CF                
  ,A.BRCODE                
  ,A.PRDTYPE                
  ,'ITRCG'                
  ,A.CF_ID                
 FROM IFRS_ACCT_EIR_COST_FEE_PREV A -- 20130722 ADD JOIN COND TO PICK LATEST CF PREV                 
 JOIN VW_LAST_EIR_CF_PREV_YEST C ON C.MASTERID = A.MASTERID                 
  AND C.DOWNLOAD_DATE = A.DOWNLOAD_DATE                
  AND ISNULL(C.SEQ, '') = ISNULL(A.SEQ, '')                
 JOIN IFRS_ACCT_EIR_STOP_REV B ON B.DOWNLOAD_DATE = @V_CURRDATE           
  AND B.MASTERID = A.MASTERID                
 WHERE A.DOWNLOAD_DATE = @V_PREVDATE                
  AND A.STATUS = 'ACT'                
                
 INSERT INTO IFRS_AMORT_LOG (           
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'DEBUG'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,'31'                
  )                
                
 -- 20160407 AMORT YESTERDAY ACCRU                  
 -- BLOCK ACCRU PREV GENERATION ON SL_ECF                  
 IF @PARAM_DISABLE_ACCRU_PREV = 0                
 BEGIN                
  INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
   FACNO                
   ,CIFNO                
   ,DOWNLOAD_DATE                
   ,DATASOURCE                
   ,PRDCODE                
   ,TRXCODE                
   ,CCY                
   ,JOURNALCODE                
   ,STATUS                
   ,REVERSE                
   ,N_AMOUNT                
   ,CREATEDDATE                
   ,SOURCEPROCESS                
   ,ACCTNO                
   ,MASTERID                
   ,FLAG_CF                
   ,BRANCH                
   ,PRDTYPE                
   ,JOURNALCODE2                
   ,CF_ID                
   )                
  SELECT FACNO                
   ,CIFNO                
   ,@V_CURRDATE                
   ,DATASOURCE                
   ,PRDCODE                
   ,TRXCODE                
   ,CCY                
   ,'AMORT'                
   ,STATUS                
   ,'N'                
   ,N_AMOUNT                
   ,CURRENT_TIMESTAMP                
   ,'EIR STOP REV 2'                
   ,ACCTNO                
   ,X.MASTERID                
   ,FLAG_CF                
   ,BRANCH                
   ,PRDTYPE                
   ,'ACCRU'                
   ,CF_ID                
  FROM IFRS_ACCT_JOURNAL_INTM X                
  INNER JOIN (                
   SELECT DISTINCT MASTERID                
   FROM IFRS_ACCT_EIR_STOP_REV                
   WHERE DOWNLOAD_DATE = @V_CURRDATE                
   ) Y ON X.MASTERID = Y.MASTERID                
  WHERE DOWNLOAD_DATE = @V_PREVDATE                
   AND STATUS = 'ACT'                
   AND TRXCODE <> 'BENEFIT'                
   AND JOURNALCODE = 'ACCRU'                
   AND REVERSE = 'N'                
   AND SUBSTRING(SOURCEPROCESS, 1, 3) = 'EIR'                
   /*   AND MASTERID IN (                  
                                SELECT  MASTERID                  
                                FROM    IFRS_ACCT_EIR_STOP_REV                  
                                WHERE   DOWNLOAD_DATE = @V_CURRDATE ) */                
 END                
 ELSE                
 BEGIN                
  -- REVERSE ACCRU                  
  INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
   FACNO                
   ,CIFNO                
   ,DOWNLOAD_DATE                
   ,DATASOURCE                
   ,PRDCODE                
   ,TRXCODE                
   ,CCY                
   ,JOURNALCODE                
   ,STATUS                
   ,REVERSE                
   ,N_AMOUNT                
   ,CREATEDDATE                
   ,SOURCEPROCESS                
   ,ACCTNO                
   ,MASTERID                
   ,FLAG_CF                
   ,BRANCH                
   ,PRDTYPE                
   ,JOURNALCODE2                
   ,CF_ID                
   )                
  SELECT FACNO                
   ,CIFNO                
   ,@V_CURRDATE                
   ,DATASOURCE                
   ,PRDCODE                
   ,TRXCODE                
   ,CCY                
   ,'DEFA0'                
   ,STATUS                
   ,'Y'                
   ,- 1 * N_AMOUNT                
   ,CURRENT_TIMESTAMP                
   ,'EIR STOP REV 2'                
   ,ACCTNO                
   ,MASTERID                
 ,FLAG_CF                
   ,BRANCH                
   ,PRDTYPE                
   ,'ITRCG'                
   ,CF_ID                
  FROM IFRS_ACCT_JOURNAL_INTM                
  WHERE DOWNLOAD_DATE = @V_PREVDATE                
   AND STATUS = 'ACT'                
   AND JOURNALCODE = 'ACCRU'                
   AND TRXCODE <> 'BENEFIT'                
   AND REVERSE = 'N'                
   AND SUBSTRING(SOURCEPROCESS, 1, 3) = 'EIR'                
   AND MASTERID IN (                
    SELECT MASTERID                
    FROM IFRS_ACCT_EIR_STOP_REV                
    WHERE DOWNLOAD_DATE = @V_CURRDATE                
    )                
 END                
                
 /*                  
-- INTM REVERSE DATA                  
UPDATE IFRS_ACCT_JOURNAL_INTM                  
SET N_AMOUNT_IDR = IFRS_ACCT_JOURNAL_INTM.N_AMOUNT * ISNULL (RATE_AMOUNT, 1)                  
FROM PSAK_MASTER_EXCHANGE_RATE_CURR B                  
WHERE  IFRS_ACCT_JOURNAL_INTM.CCY = B.CURRENCY                  
 AND IFRS_ACCT_JOURNAL_INTM.DOWNLOAD_DATE = @V_CURRDATE                   
 AND IFRS_ACCT_JOURNAL_INTM.REVERSE = 'Y'                  
*/                
 --20180226 GAIN LOSS PARTIAL PAYMENT                  
 INSERT INTO IFRS_ACCT_JOURNAL_INTM (                
  FACNO                
  ,CIFNO                
  ,DOWNLOAD_DATE                
  ,DATASOURCE                
  ,PRDCODE                
  ,TRXCODE                
  ,CCY                
  ,JOURNALCODE                
  ,[STATUS]                
  ,[REVERSE]                
  ,N_AMOUNT                
  ,CREATEDDATE                
  ,SOURCEPROCESS                
  ,ACCTNO                
  ,MASTERID                
  ,FLAG_CF                
  ,BRANCH                
  ,PRDTYPE                
  ,JOURNALCODE2                
  ,CF_ID                
  ,METHOD                
  )                
 SELECT A.FACILITY_NUMBER                
  ,A.CUSTOMER_NUMBER                
  ,A.DOWNLOAD_DATE                
  ,A.DATA_SOURCE                
  ,C.PRDCODE                
  ,C.TRXCODE                
  ,C.CCY                
  ,'AMORT'                
  ,'ACT'                
  ,'N'                
  ,CASE                 
   WHEN C.FLAG_REVERSE = 'Y'                
    THEN - 1 * C.AMOUNT                
   ELSE C.AMOUNT                
   END                
  ,CURRENT_TIMESTAMP                
  ,'EIR GAIN LOSS'                
  ,A.ACCOUNT_NUMBER                
  ,A.MASTERID                
  ,C.FLAG_CF                
  ,A.BRANCH_CODE                
  ,C.PRDTYPE                
  ,'ACCRU'                
  ,C.CF_ID                
  ,C.METHOD                
 FROM IFRS_IMA_AMORT_CURR A                
 JOIN IFRS_ACCT_EIR_GAIN_LOSS C ON C.MASTERID = A.MASTERID                
  AND C.DOWNLOAD_DATE = @V_CURRDATE                
 WHERE A.DOWNLOAD_DATE = @V_CURRDATE                
                
 INSERT INTO IFRS_AMORT_LOG (                
  DOWNLOAD_DATE                
  ,DTM                
  ,OPS                
  ,PROCNAME                
  ,REMARK                
  )                
 VALUES (                
  @V_CURRDATE                
  ,CURRENT_TIMESTAMP                
  ,'END'                
  ,'SP_IFRS_ACCT_EIR_JOURNAL_INTM'                
  ,''                
  )                
END   
  
GO
