
  
CREATE PROCEDURE [dbo].[SP_IFRS_IMP_SEQUENCE_SML]     
@DOWNLOAD_DATE DATE = NULL      
AS        
 DECLARE @V_CURRDATE DATE;        
 DECLARE @V_CURRMONTH DATE;        
 DECLARE @V_PREVMONTH DATE;      
 DECLARE @PARAM_WITHPRC VARCHAR(MAX);        
 DECLARE @PARAM_WITHPRCFLAG VARCHAR(MAX);       
BEGIN      
       
    SET NOCOUNT ON;        
             
    IF @DOWNLOAD_DATE IS NULL       
    BEGIN       
        SELECT        
     @V_CURRDATE = CURRDATE,        
     @V_CURRMONTH = EOMONTH(CURRDATE),        
     @V_PREVMONTH = EOMONTH(DATEADD(MM, -1, CURRDATE))        
        FROM IFRS_PRC_DATE_SML         
    END      
    ELSE      
    BEGIN       
        SET @V_CURRDATE = @DOWNLOAD_DATE        
        SET @V_CURRMONTH = EOMONTH(@DOWNLOAD_DATE)        
        SET @V_PREVMONTH = EOMONTH(DATEADD(MM, -1, @DOWNLOAD_DATE))        
    END      
       
    SET @PARAM_WITHPRC = '@DOWNLOAD_DATE = ''' + ISNULL(CONVERT(VARCHAR(50), @V_CURRDATE), 'NULL') + '''';      
    SET @PARAM_WITHPRCFLAG = '@DOWNLOAD_DATE = ''' + ISNULL(CONVERT(VARCHAR(50), @V_CURRDATE), 'NULL') + ''', @PRC = ''S''';      
         
 UPDATE A SET SESSIONID = NEWID()  
 FROM IFRS_PRC_DATE_SML A   
  
    BEGIN TRY      
        --DELETE IFRS_STATISTIC_SML WHERE DOWNLOAD_DATE = @V_CURRDATE AND PRC_NAME IN ('IMP_SML','PD_SEQ_SML')       
        
        DECLARE @TABLE_NAME VARCHAR(50)              
        DECLARE @V_STR_SQL VARCHAR(MAX)              
        DECLARE SEG1             
        CURSOR FOR    
   SELECT TABLE_NAME            
   FROM INFORMATION_SCHEMA.COLUMNS             
   WHERE COLUMN_NAME = 'IS_DELETE' AND TABLE_CATALOG = 'IFRS9'             
      
  OPEN SEG1;       
  FETCH SEG1 INTO @TABLE_NAME    
      
  WHILE @@FETCH_STATUS = 0    
  BEGIN      
      SET @V_STR_SQL = ''      
      SET @V_STR_SQL = 'DELETE ' + @TABLE_NAME + ' WHERE IS_DELETE = 1'              
          
      EXEC (@V_STR_SQL);            
            
      FETCH NEXT FROM SEG1 INTO @TABLE_NAME    
  END       
  CLOSE SEG1;    
  DEALLOCATE SEG1;              
                    
        -- INSERT IMA IMP CURR      
        EXEC SP_IFRS_EXEC_AND_LOG_SML      
        @P_SP_NAME = 'SP_IFRS_IMP_FILL_IMA_PREV_CURR',      
        @DOWNLOAD_DATE = @V_CURRDATE,      
        @P_PRC_NAME = 'IMP_SML',        
        @EXECUTE_FLAG = 'Y',      
        @PARAMETER = @PARAM_WITHPRCFLAG;       
        
        -- PD FL Yearly to Monthly      
        EXEC SP_IFRS_EXEC_AND_LOG_SML      
        @P_SP_NAME = 'SP_IFRS_IMP_PD_FL_TERM_YEARLY',      
        @DOWNLOAD_DATE = @V_CURRDATE,      
        @P_PRC_NAME = 'IMP_SML',        
        @EXECUTE_FLAG = 'Y',      
        @PARAMETER = @PARAM_WITHPRCFLAG;              
          
        EXEC SP_IFRS_EXEC_AND_LOG_SML       
        @P_SP_NAME = 'SP_IFRS_IMP_PD_FL_YEAR_TO_MONTH',      
        @DOWNLOAD_DATE = @V_CURRDATE,      
        @P_PRC_NAME = 'IMP_SML',             
        @EXECUTE_FLAG = 'Y',      
        @PARAMETER = @PARAM_WITHPRCFLAG;           
           
        -- ECL COLLECTIVE      
        EXEC SP_IFRS_EXEC_AND_LOG_SML      
        @P_SP_NAME = 'SP_IFRS_IMP_ECL_GENERATE_IMA',      
        @DOWNLOAD_DATE = @V_CURRDATE,      
        @P_PRC_NAME = 'IMP_SML',        
        @EXECUTE_FLAG = 'Y',      
        @PARAMETER = @PARAM_WITHPRCFLAG;       
          
        EXEC SP_IFRS_EXEC_AND_LOG_SML      
        @P_SP_NAME = 'SP_IFRS_IMP_DEFAULT_RULE_ECL',      
        @DOWNLOAD_DATE = @V_CURRDATE,      
        @P_PRC_NAME = 'IMP_SML',        
        @EXECUTE_FLAG = 'Y',      
        @PARAMETER = @PARAM_WITHPRCFLAG;       
            
        EXEC SP_IFRS_EXEC_AND_LOG_SML      
        @P_SP_NAME = 'SP_IFRS_IMP_EXEC_RULE_STAGE',      
        @DOWNLOAD_DATE = @V_CURRDATE,       
        @P_PRC_NAME = 'IMP_SML',        
        @EXECUTE_FLAG = 'Y',      
        @PARAMETER = @PARAM_WITHPRCFLAG;       
          
        EXEC SP_IFRS_EXEC_AND_LOG_SML      
        @P_SP_NAME = 'SP_IFRS_IMP_ECL_EAD_RESULT_NONPRK',      
        @DOWNLOAD_DATE = @V_CURRDATE,      
        @P_PRC_NAME = 'IMP_SML',       
        @EXECUTE_FLAG = 'Y',      
        @PARAMETER = @PARAM_WITHPRCFLAG;       
          
        EXEC SP_IFRS_EXEC_AND_LOG_SML      
        @P_SP_NAME = 'SP_IFRS_IMP_ECL_EAD_RESULT_PRK',      
        @DOWNLOAD_DATE = @V_CURRDATE,      
        @P_PRC_NAME = 'IMP_SML',        
        @EXECUTE_FLAG = 'Y',      
        @PARAMETER = @PARAM_WITHPRCFLAG;       
          
        EXEC SP_IFRS_EXEC_AND_LOG_SML      
        @P_SP_NAME = 'SP_IFRS_ECL_RESULT_DETAIL',      
        @DOWNLOAD_DATE = @V_CURRDATE,    
        @P_PRC_NAME = 'IMP_SML',        
        @EXECUTE_FLAG = 'Y',      
        @PARAMETER = @PARAM_WITHPRCFLAG;       
          
        -- SYNC IMA        
        EXEC SP_IFRS_EXEC_AND_LOG_SML      
        @P_SP_NAME = 'SP_IFRS_IMP_SYNC_IMA_MONTHLY',      
        @DOWNLOAD_DATE = @V_CURRDATE,      
        @P_PRC_NAME = 'IMP_SML',        
        @EXECUTE_FLAG = 'Y',        
        @PARAMETER = @PARAM_WITHPRCFLAG;        
           
        EXEC SP_IFRS_EXEC_AND_LOG_SML     
        @P_SP_NAME = 'SP_IFRS_IMP_NOMINATIVE_OUTPUT',      
        @DOWNLOAD_DATE = @V_CURRDATE,      
        @P_PRC_NAME = 'IMP_SML',        
        @EXECUTE_FLAG = 'Y',      
        @PARAMETER = @PARAM_WITHPRCFLAG;        
       
    END TRY      
      
    BEGIN CATCH      
      
        DECLARE @ErrorSeverity INT,      
        @ErrorState INT,      
        @ErrorMessageDescription NVARCHAR(4000),      
        @DateProcess VARCHAR(100),      
        @V_ERRM VARCHAR(255),      
        @V_SPNAME VARCHAR(100);     
      
        SELECT  @DateProcess = CONVERT(VARCHAR(20), GETDATE(), 107)        
      
        SELECT  @V_ERRM = ERROR_MESSAGE(),      
        @ErrorSeverity = ERROR_SEVERITY(),      
        @ErrorState = ERROR_STATE();        
      
        UPDATE  A      
        SET     A.END_DATE = GETDATE(),      
        ISCOMPLETE = 'N',      
        REMARK = @V_ERRM      
        FROM    IFRS_STATISTIC_SML A      
        WHERE   A.DOWNLOAD_DATE = @v_currdate      
        AND A.SP_NAME = @V_SPNAME;     
      
        UPDATE  IFRS_PRC_DATE_SML      
        SET     BATCH_STATUS = 'ERROR!!..',      
        Remark = @V_ERRM,      
        Last_Process_Date = GETDATE();         
      
        SET @ErrorMessageDescription = @V_SPNAME + ' ( ' + @DateProcess + ' ) --> ' + @V_ERRM              
        RAISERROR (@ErrorMessageDescription, 11, 1)       
        RETURN          
    END CATCH;            
    
END     
