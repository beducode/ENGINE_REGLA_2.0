CREATE OR REPLACE FUNCTION F_GET_RULES_SICR(
	P_RULE_ID BIGINT,
	P_STAGE_FROM CHARACTER VARYING)
    RETURNS TEXT
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$

DECLARE
V_STR_SQL TEXT;            
V_SCRIPT1 TEXT;            
V_SCRIPT2 TEXT;            
RULE_CODE1 BIGINT;          
VALUE  VARCHAR(250);            
RULE_TYPE  VARCHAR(50);             
V_PKID  INT;           
AOC  VARCHAR(3);            
MAX_PKID  INT;            
MIN_PKID  INT;            
QG   INT;            
PREV_QG  INT;            
NEXT_QG INT;            
V_JML INT;            
V_RN INT;            
V_COLUMN_NAME  VARCHAR(250);        
V_STAGE_TO VARCHAR(250);        
V_DATA_TYPE  VARCHAR(250);            
V_OPERATOR  VARCHAR(50);            
V_VALUE1 VARCHAR(250);            
V_VALUE2 VARCHAR(250); 
BEGIN
	V_STR_SQL := ' ';      
	V_SCRIPT2 := ' ';

	FOR RULE_CODE1, V_STAGE_TO IN
		SELECT DISTINCT RULE_ID, STAGE_TO    
		FROM IFRS_SCENARIO_RULES_DETAIL    
		WHERE RULE_ID = P_RULE_ID    
		AND DETAIL_TYPE = 'SICR'    
		AND STAGE_FROM = P_STAGE_FROM    
		ORDER BY STAGE_TO DESC
	LOOP
		V_SCRIPT1 := ' ';            
		V_STR_SQL := ' ';

		FOR V_COLUMN_NAME,V_DATA_TYPE,V_OPERATOR,V_VALUE1, V_VALUE2, QG,AOC, PREV_QG,NEXT_QG,V_JML ,V_RN,V_PKID IN
		SELECT 'A.' || COLUMN_NAME AS COLUMN_NAME,             
             DATA_TYPE,            
             OPERATOR,            
             VALUE1,            
             VALUE2,            
             QUERY_GROUPING,            
             AND_OR_CONDITION,            
             LAG (QUERY_GROUPING, 1, MIN_QG) OVER (PARTITION BY RULE_ID ORDER BY QUERY_GROUPING,PKID) PREV_QG,            
             LEAD (QUERY_GROUPING, 1, MAX_QG) OVER (PARTITION BY RULE_ID ORDER BY QUERY_GROUPING, PKID) NEXT_QG,             
             JML,            
             RN,            
            PKID            
           FROM (SELECT MIN (QUERY_GROUPING) OVER (PARTITION BY RULE_ID) MIN_QG,            
                MAX (QUERY_GROUPING) OVER (PARTITION BY RULE_ID) MAX_QG,            
                ROW_NUMBER() OVER (PARTITION BY RULE_ID ORDER BY QUERY_GROUPING, SEQUENCE ) RN,            
                COUNT (0) OVER (PARTITION BY RULE_ID) JML,            
                 COLUMN_NAME,            
                DATA_TYPE,            
                OPERATOR,            
                VALUE1,            
                VALUE2,            
                QUERY_GROUPING,            
                RULE_ID,            
                AND_OR_CONDITION,              
                PKID            
              FROM IFRS_SCENARIO_RULES_DETAIL            
             WHERE RULE_ID = 22 AND DETAIL_TYPE = 'SICR' AND STAGE_FROM = '1' AND STAGE_TO = '2'    
             ) A
			 ORDER BY RN ASC
		LOOP
			V_SCRIPT1 :=  COALESCE(V_SCRIPT1, ' ') || ' ' || AOC || ' ' || CASE WHEN  QG <> PREV_QG   THEN '(' ELSE ' ' END             
			|| 
			COALESCE(
			CASE WHEN RTRIM(LTRIM (V_DATA_TYPE)) IN ('NUMBER', 'DECIMAL', 'NUMERIC', 'FLOAT', 'INT') THEN 
			CASE WHEN V_OPERATOR IN ('=','<>','>','<','>=','<=') THEN COALESCE(V_COLUMN_NAME, '')
			|| ' '            
			|| COALESCE(V_OPERATOR, '')            
			|| ' '            
			|| COALESCE(V_VALUE1, '')            
			WHEN UPPER (V_OPERATOR) = 'BETWEEN'            
			THEN            
			COALESCE(V_COLUMN_NAME, '')            
			|| ' '            
			|| COALESCE(V_OPERATOR, '')            
			|| ' '            
			|| COALESCE(V_VALUE1, '')            
			|| ' AND '            
			|| COALESCE(V_VALUE2, '')            
			WHEN UPPER (V_OPERATOR) IN ('IN','NOT IN')            
			THEN            
			COALESCE(V_COLUMN_NAME, '')            
			|| ' '            
			|| COALESCE(V_OPERATOR, '')            
			|| ' '            
			|| '('            
			|| COALESCE(V_VALUE1, '')            
			|| ')'            
			ELSE            
			'XXX'            
			END            
			WHEN RTRIM(LTRIM (V_DATA_TYPE)) = 'DATE'            
			THEN            
			CASE            
			WHEN V_OPERATOR IN ('=','<>','>','<','>=','<=')            
			THEN            
			COALESCE(V_COLUMN_NAME, '')            
			|| ' '            
			|| COALESCE(V_OPERATOR, '')            
			|| '  TO_DATE('''            
			|| COALESCE(V_VALUE1, '')            
			|| ''',''MM/DD/YYYY'')'            
			WHEN UPPER (V_OPERATOR) = 'BETWEEN'            
			THEN            
			COALESCE(V_COLUMN_NAME, '')            
			|| ' '            
			|| COALESCE(V_OPERATOR, '')            
			|| ' '            
			|| '   CONVERT(DATE,'''            
			|| COALESCE(V_VALUE1, '')            
			|| ''',110)'            
			|| ' AND '            
			|| '  CONVERT(DATE,'''            
			|| COALESCE(V_VALUE2, '')            
			|| ''',110)'            
			WHEN UPPER (V_OPERATOR) IN ('=','<>','>','<','>=','<=')            
			THEN            
			COALESCE(V_COLUMN_NAME, '')            
			|| ' '            
			|| COALESCE(V_OPERATOR, '')            
			|| ' '            
			|| '('            
			|| '  TO_DATE('''            
			|| COALESCE(V_VALUE1, '')            
			|| ''',''MM/DD/YYYY'')'            
			|| ')'            
			ELSE            
			'XXX'            
			END            
			WHEN UPPER(RTRIM(LTRIM (V_DATA_TYPE))) IN ('CHAR','CHARACTER', 'VARCHAR', 'VARCHAR2','BIT')            
			THEN            
			CASE            
			WHEN RTRIM(LTRIM (V_OPERATOR)) = '='            
			THEN            
			COALESCE(V_COLUMN_NAME, ' ')            
			|| ' '            
			|| COALESCE(V_OPERATOR, ' ')            
			|| ''''            
			|| COALESCE(V_VALUE1, ' ')            
			|| ''''            
			WHEN RTRIM(LTRIM (UPPER (V_OPERATOR))) = 'BETWEEN'            
			THEN            
			COALESCE(V_COLUMN_NAME, '')         
			|| ' '            
			|| COALESCE(V_OPERATOR, '')            
			|| '  '            
			|| COALESCE(V_VALUE1, '')            
			|| ' AND '            
			|| COALESCE(V_VALUE2, '')            
			WHEN RTRIM(LTRIM (UPPER (V_OPERATOR))) IN ('IN','NOT IN')        
			THEN            
			COALESCE(V_COLUMN_NAME, '')            
			|| ' '            
			|| COALESCE(V_OPERATOR, '')            
			|| '  '            
			|| '('''            
			|| COALESCE(REPLACE (V_VALUE1, ',', ''','''), '')            
			|| ''')'            
			ELSE            
			'XXX'            
			END            
			ELSE            
			'XXX'            
			END , ' ')  
			|| CASE WHEN   QG <> NEXT_QG   OR V_RN = V_JML THEN ')' ELSE ' ' END;
			
		END LOOP;
		V_SCRIPT1 := '(' || LTRIM(SUBSTRING (V_SCRIPT1, 6, LENGTH(V_SCRIPT1) ));
		V_STR_SQL := V_STR_SQL || 'WHEN (' || V_SCRIPT1 || ') THEN ''' || LTRIM(RTRIM(CAST(V_STAGE_TO AS VARCHAR))) || '''';    
		V_SCRIPT2 := COALESCE(V_SCRIPT2,'') || V_STR_SQL;

	END LOOP;
		
	
	RETURN (V_SCRIPT2);
END;
$BODY$;