CREATE OR REPLACE EDITIONABLE PROCEDURE "PSAK413"."SP_IFRS_BUCKET_DETAIL" SP_IFRS_BUCKET_DETAIL
AS 
BEGIN
     
    
    MERGE INTO IFRS_BUCKET_DETAIL a
    USING (
    
    	 SELECT * 
	 FROM (
	 SELECT b."unique_code", a."bucket_id",  a."bucket_name",
		a."range_from_days", a."range_to_days",
		a."rating_value", a."bi_collectability_value",
		nvl(a."is_deleted",0) IS_DELETED,
		ROW_NUMBER() OVER (PARTITION BY b."unique_code", a."bucket_id" ORDER BY a."pkid" DESC ) RN 
	 FROM "GroupBucketDetail"@DBCONFIGLINK a 
	JOIN "GroupBucket"@DBCONFIGLINK b ON a."syscode_group_bucket" = b."syscode_group_bucket"
	WHERE a."bucket_id" IS NOT NULL ) x 
	WHERE x.rn = 1 
    ) b
    ON (a.bucket_group = b."unique_code" AND a.bucket_id = b."bucket_id" )  
    
     WHEN MATCHED THEN
        UPDATE SET   
            a.UPDATED_BY              = 'SP_IFRS_BUCKET_DETAIL',
            a.UPDATED_DATE            = sysdate,
            a.IS_DELETED              = B.IS_DELETED
    
    WHEN NOT MATCHED THEN
        INSERT (
            BUCKET_GROUP, bucket_id, BUCKET_NAME, RANGE_START, RANGE_END,
            RATING_VALUE, BI_COLLECTABILITY_VALUE,
            CREATED_BY, CREATED_DATE , IS_DELETED)
        VALUES (
            b."unique_code", b."bucket_id",
            b."bucket_name", b."range_from_days", b."range_to_days",
		b."rating_value", b."bi_collectability_value",
		'SP_IFRS_BUCKET_DETAIL',sysdate, B.IS_DELETED
         );
        
    commit;
     
END;