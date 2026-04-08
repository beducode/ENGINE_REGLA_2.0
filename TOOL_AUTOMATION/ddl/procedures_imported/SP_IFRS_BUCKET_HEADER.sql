CREATE OR REPLACE EDITIONABLE PROCEDURE "PSAK413"."SP_IFRS_BUCKET_HEADER" SP_IFRS_BUCKET_HEADER
AS 
BEGIN
     
    
    MERGE INTO IFRS_BUCKET_HEADER a
    USING (
     
    SELECT * 
    from(
     SELECT "unique_code",
		"bucket_group_name",
		"bucket_type",
		"is_deleted",
		"syscode_group_bucket",
		ROW_NUMBER() OVER (PARTITION BY "syscode_group_bucket" ORDER BY "pkid" desc) rn 
	FROM "GroupBucket"@DBCONFIGLINK) WHERE rn = 1 
    ) b 
    ON (a.syscode_group_bucket = b."syscode_group_bucket" )  
     
    WHEN MATCHED THEN
        UPDATE SET   
            a.UPDATED_BY              = 'SP_IFRS_BUCKET_HEADER',
            a.UPDATED_DATE            = sysdate,
            a.IS_DELETED              = nvl(b."is_deleted",0)
 
    WHEN NOT MATCHED THEN
        INSERT (
            BUCKET_GROUP, BUCKET_DESCRIPTION, OPTION_GROUPING, IS_DELETED,
            CREATED_BY, CREATED_DATE,
            SYSCODE_GROUP_BUCKET)
        VALUES (
            b."unique_code", b."bucket_group_name",
            b."bucket_type", NVL(b."is_deleted",0),
            'SP_IFRS_BUCKET_HEADER', sysdate ,
            b."syscode_group_bucket"
        );
        
    commit;
 
     
END;