CREATE OR REPLACE PROCEDURE PSAK413."SP_IFRS_DEFAULT_CRITERIA" 
AS 
 
BEGIN
     
    
    MERGE INTO IFRS_DEFAULT_CRITERIA a
    USING (
    
     SELECT * 
     FROM (
     selecT "pkid", "syscode_default_criteria", "default_criteria_name",  
        "sql_conditions", "source_table_conditions", "is_deleted",
        ROW_NUMBER() OVER (PARTITION BY "syscode_default_criteria" ORDER BY "pkid" DESC ) rn 
     from "DefaultCriteria"@DBCONFIGLINK) A 
     WHERE RN = 1 
    ) b
    ON (a.SYSCODE_DEFAULT_CRITERIA = b."syscode_default_criteria"
        )  
     
    WHEN MATCHED THEN
        UPDATE SET  
            a.sql_conditions          = b."sql_conditions",
            a.UPDATED_BY              = 'SP_IFRS_DEFAULT_CRITERIA',
            a.UPDATED_DATE            = sysdate,
            a.IS_DELETED              = b."is_deleted"
 
    WHEN NOT MATCHED THEN
        INSERT (
            SYSCODE_DEFAULT_CRITERIA, DEFAULT_CRITERIA_NAME,
            SQL_CONDITIONS, SOURCE_TABLE_CONDITIONS,
            IS_DELETED, 
            CREATED_BY, CREATED_DATE 
            )
        VALUES (
            b."syscode_default_criteria", b."default_criteria_name",
            b."sql_conditions", b."source_table_conditions",
            nvl(b."is_deleted", 0),
            'SP_IFRS_DEFAULT_CRITERIA', sysdate
        );
        
        commit;
     
END;
/
