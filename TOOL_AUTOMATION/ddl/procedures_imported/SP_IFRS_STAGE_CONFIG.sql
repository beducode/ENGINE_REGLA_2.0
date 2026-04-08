CREATE OR REPLACE EDITIONABLE PROCEDURE "PSAK413"."SP_IFRS_STAGE_CONFIG" 
AS 
    
BEGIN
	
	
     MERGE INTO IFRS_STAGE_CONFIG a
	    USING (
	    
		    SELECT b."pkid",x."syscode_staging" AS syscode_header, b."syscode_staging" AS syscode_detail,x."staging_name" AS group_name,
		    		b."staging_name" AS detail_name,TO_CHAR(b."sql_conditions") AS sql_conditions,
		    		b."is_deleted", b."is_sicr", b."is_publish"
		    FROM (
			    selecT A."pkid", A."syscode_staging",
			       A."staging_name" 
			    from "Staging"@DBCONFIGLINK A
			    WHERE "level" = 1 AND "is_publish"= 1 ) x 
			    LEFT JOIN "Staging"@DBCONFIGLINK b ON x."syscode_staging" = b."parent_code_level_0" AND b."level" = 2
		    
	    ) b
	    ON ( a.SYSCODE_STAGE_HEADER = b.syscode_header 
	    	AND a.SYSCODE_STAGE_DETAIL = b.syscode_detail ) 
	    WHEN MATCHED THEN
	        UPDATE SET  
	            a.UPDATEDBY              = 'SP_IFRS_STAGE_CONFIG',
	            a.UPDATEDDATE            = sysdate,
	            a.IS_DELETE              = b."is_deleted",
	            a.IS_SICR 				 = b."is_sicr",
	            a.SQL_CONDITIONS		 = b.sql_conditions
	    WHEN NOT MATCHED THEN
	        INSERT (
	            SYSCODE_STAGE_HEADER, SYSCODE_STAGE_DETAIL, STAGE_HEADER, STAGE_DETAIL,
	            SQL_CONDITIONS, IS_DELETE,IS_SICR , UPDATEDBY, UPDATEDDATE
	        )
	        VALUES (
	            b.syscode_header, b.syscode_detail,
	            b.group_name,b.detail_name,b.sql_conditions, b."is_deleted",b."is_sicr" , 'SP_IFRS_STAGE_CONFIG', sysdate 
	        );
	        
	        commit;
END