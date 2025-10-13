DROP VIEW IF EXISTS VW_IFRS_FIRST_DEFAULT;

CREATE VIEW VW_IFRS_FIRST_DEFAULT AS
WITH CTE_VIEW            
(              
    RULE_ID,    
    MASTERID,      
    CUSTOMER_NUMBER,    
    DEFAULT_DATE,    
    OS_AT_DEFAULT,    
    EQV_AT_DEFAULT,            
    PLAFOND_AT_DEFAULT,              
    EQV_PLAFOND_AT_DEFAULT,    
    EIR_AT_DEFAULT,                 
    PLAFOND_12M_BEFORE_DEFAULT ,                
    EQV_PLAFOND_12M_BEFORE_DEFAULT,                 
    OS_12M_BEFORE_DEFAULT,                
    EQV_OS_12M_BEFORE_DEFAULT,    
    FACILITY_NUMBER,              
    RN          
)           
AS              
(              
    SELECT        
        RULE_ID,     
        MASTERID,     
        CUSTOMER_NUMBER,    
        DOWNLOAD_DATE AS DEFAULT_DATE,    
        OS_AT_DEFAULT,    
        EQV_AT_DEFAULT,             
        PLAFOND_AT_DEFAULT,              
        EQV_PLAFOND_AT_DEFAULT,                 
        EIR_AT_DEFAULT,    
        PLAFOND_12M_BEFORE_DEFAULT ,                
        EQV_PLAFOND_12M_BEFORE_DEFAULT,                 
        OS_12M_BEFORE_DEFAULT ,                
        EQV_OS_12M_BEFORE_DEFAULT,    
        FACILITY_NUMBER,    
        ROW_NUMBER() OVER (PARTITION BY RULE_ID, MASTERID ORDER BY DOWNLOAD_DATE ASC) RN    
    FROM IFRS_DEFAULT    
)            
SELECT           
    RULE_ID,     
    MASTERID,     
    CUSTOMER_NUMBER,    
    DEFAULT_DATE,    
    OS_AT_DEFAULT,    
    EQV_AT_DEFAULT,                
    PLAFOND_AT_DEFAULT,              
    EQV_PLAFOND_AT_DEFAULT,              
    EIR_AT_DEFAULT,                
    PLAFOND_12M_BEFORE_DEFAULT ,                
    EQV_PLAFOND_12M_BEFORE_DEFAULT,                 
    OS_12M_BEFORE_DEFAULT,                
    EQV_OS_12M_BEFORE_DEFAULT,    
    FACILITY_NUMBER    
FROM CTE_VIEW              
WHERE RN = 1;