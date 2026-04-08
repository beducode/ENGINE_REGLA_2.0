CREATE OR REPLACE EDITIONABLE PROCEDURE "PSAK413"."SP_IFRS_EIL_MODEL_HEADER" 
AS 
    
BEGIN
    
	--EilConfiguration	-- HEADER
    MERGE INTO IFRS_EIL_MODEL_HEADER a
    USING (
    
    SELECT * FROM (
	    selecT "pkid",
	        "syscode_eil_configuration", "eil_configuration_name",
	        "effective_start_date","description", "is_active",
	        "is_eom", "is_deleted",ROW_NUMBER() OVER (PARTITION BY "syscode_eil_configuration" ORDER BY "pkid" DESC ) rn 
	    from "EilConfiguration"@DBCONFIGLINK ) x 
    where x.rn = 1
    ) b
    ON ( a.SYSCODE_EIL = b."syscode_eil_configuration" )  
     
    WHEN MATCHED THEN
        UPDATE SET  
            a.DATA_DATE    			  = b."effective_start_date",
            a.ACTIVE_STATUS			  = b."is_active",
            a.RUN_STATUS			  = b."is_eom",
            a.UPDATEDBY              = 'SP_IFRS_EIL_CONFIG_MAPPING',
            a.UPDATEDDATE            = sysdate,
            a.IS_DELETE              = b."is_deleted",
            A.IS_EOM = B."is_eom"
 
    WHEN NOT MATCHED THEN
        INSERT (
            SYSCODE_EIL, EIL_MODEL_NAME,
            DATA_DATE, ACTIVE_STATUS, is_eom,
            IS_DELETE, CREATEDBY,  
            UPDATEDBY, UPDATEDDATE
        )
        VALUES (
            b."syscode_eil_configuration", b."eil_configuration_name",
            b."effective_start_date",b."is_active",nvl(b."is_eom",0) ,nvl(b."is_deleted",0), 'SP_IFRS_EIL_CONFIG_MAPPING', sysdate , sysdate
        );
        
        commit;
       
END