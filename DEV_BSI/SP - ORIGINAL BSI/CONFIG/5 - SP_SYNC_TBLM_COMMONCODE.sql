CREATE OR REPLACE PROCEDURE PSAK413.SP_SYNC_TBLM_COMMONCODE
AS 
 
BEGIN
	  --- TBLM_COMMONCODEHEADER ---
    MERGE INTO TBLM_COMMONCODEHEADER a
    USING (
    	SELECT "common_code" AS COMMONCODE,"common_code_name" AS COMMONNAME,"code_app" AS MODULEID, "description" AS COMMONDESC,"common_group" AS CODETYPE,"created_by" AS CREATEDBY, "created_date" AS CREATEDDATE, "updated_by" AS UPDATEDBY, "updated_date" AS UPDATEDDATE
			, "is_deleted" AS IS_DELETE
			FROM "CommonCodeSetting"@DBCONFIGLINK
    ) b
    ON (a.COMMONCODE = b.COMMONCODE)  
    WHEN MATCHED THEN
        UPDATE SET   
            a.UPDATEDBY              = 'SP_SYNC_TBLM_COMMONCODE',
            a.UPDATEDDATE            = sysdate,
            a.COMMONNAME			 = b.COMMONNAME,
            a.COMMONDESC			 = b.COMMONDESC,
            a.CODETYPE	 			 = b.CODETYPE,
            a.MODULEID 				 = b.MODULEID
    
    WHEN NOT MATCHED THEN
        INSERT (
            COMMONCODE,
            COMMONNAME,
            COMMONDESC,
            CODETYPE,
            MODULEID,
            CREATEDBY,
            CREATEDDATE,
            UPDATEDBY,
            UPDATEDDATE
		    )
        VALUES (
           b.COMMONCODE,
            b.COMMONNAME,
            b.COMMONDESC,
            b.CODETYPE,
            b.MODULEID,
            'SP_SYNC_TBLM_COMMONCODE',
            sysdate,
            b.UPDATEDBY,
            b.UPDATEDDATE
        );
        commit;
       
   --- TBLM_COMMONCODEDETAIL ---
    EXECUTE IMMEDIATE 'TRUNCATE TABLE TEMP_COMMONCODE';
       
	INSERT INTO TEMP_COMMONCODE
	SELECT 
	    "common_code",
	    "row_seq",
	    TO_CHAR("columns_1"),
	    TO_CHAR("columns_2"),
	    TO_CHAR("columns_3"),
	    "created_by",
	    "created_date",
	    "updated_by",
	    "updated_date",
	    "is_deleted"
	FROM "CommonCodeDetailDynamicData"@DBCONFIGLINK;
	COMMIT;

       MERGE INTO TBLM_COMMONCODEDETAIL a
	    USING (
				SELECT COMMONCODE,
				        SEQ,
				        VALUE1,
				        VALUE2,
				        VALUE3,
				        CREATEDBY,
				        CREATEDDATE,
				        UPDATEDBY,
				        UPDATEDDATE,
				        IS_DELETE
				 FROM TEMP_COMMONCODE
	    ) b
	    ON (a.COMMONCODE = b.COMMONCODE
	    	AND a."SEQUENCE" = b.SEQ)  
	    WHEN MATCHED THEN
	        UPDATE SET   
	            a.UPDATEDBY              = 'SP_SYNC_TBLM_COMMONCODE',
	            a.UPDATEDDATE            = sysdate,
	            a.VALUE1			 	 = b.VALUE1,
	            a.VALUE2			 	 = b.VALUE2,
	            a.VALUE3	 			 = b.VALUE3
	    
	    WHEN NOT MATCHED THEN
        INSERT (
            COMMONCODE, VALUE1, VALUE2, VALUE3, "SEQUENCE", CREATEDBY,CREATEDDATE,UPDATEDBY,UPDATEDDATE, IS_DELETE
		    )
        VALUES (
           b.COMMONCODE, b.VALUE1, b.VALUE2, b.VALUE3, b.SEQ ,
            'SP_SYNC_TBLM_COMMONCODE',
            sysdate,
            b.UPDATEDBY,
            b.UPDATEDDATE,
            b.IS_DELETE
        );
	    commit;
     
END;