---- DROP PROCEDURE SP_IFRS_IMP_INITIAL_CONFIG;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_INITIAL_CONFIG(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000', 
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S',
    IN P_STATUS VARCHAR(1) DEFAULT 'N')
LANGUAGE PLPGSQL 
AS $$
DECLARE
    ---- DATE
    V_PREVDATE DATE;
    V_PREVMONTH DATE;
    V_CURRDATE DATE;
    V_LASTYEAR DATE;
    V_LASTYEARNEXTMONTH DATE;

    ---- QUERY   
    V_STR_QUERY TEXT;

    ---- TABLE LIST       
    V_TABLENAME VARCHAR(100); 
    V_TABLENAME_MON VARCHAR(100);
    V_TABLEINSERT1 VARCHAR(100);
    V_TABLEINSERT2 VARCHAR(100);
    V_TABLEINSERT3 VARCHAR(100);
    V_TABLEINSERT4 VARCHAR(100);
    V_TABLEINSERT5 VARCHAR(100);
    V_TABLEINSERT6 VARCHAR(100);
    V_TABLEINSERT7 VARCHAR(100);

    ---- CONDITION
    V_RETURNROWS INT;
    V_RETURNROWS2 INT;
    V_TABLEDEST VARCHAR(100);
    V_COLUMNDEST VARCHAR(100);
    V_SPNAME VARCHAR(100);
    V_OPERATION VARCHAR(100);

    ---- RESULT
    V_QUERYS TEXT;

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

    IF COALESCE(P_STATUS, NULL) IS NULL THEN
        P_STATUS := 'N';
    END IF;

    IF COALESCE(P_RUNID, NULL) IS NULL THEN
        P_RUNID := 'S_00000_0000';
    END IF;

    IF P_PRC = 'S' THEN 
        V_TABLEINSERT1 := 'IFRS_PD_RULES_CONFIG_' || P_RUNID || '';
        V_TABLEINSERT2 := 'IFRS_CCF_RULES_CONFIG_' || P_RUNID || '';
        V_TABLEINSERT3 := 'IFRS_EAD_RULES_CONFIG_' || P_RUNID || '';
        V_TABLEINSERT4 := 'IFRS_LGD_RULES_CONFIG_' || P_RUNID || '';
        V_TABLEINSERT5 := 'IFRS_MASTER_PRODUCT_PARAM_' || P_RUNID || '';
    ELSE 
        V_TABLEINSERT1 := 'IFRS_PD_RULES_CONFIG';
        V_TABLEINSERT2 := 'IFRS_CCF_RULES_CONFIG';
        V_TABLEINSERT3 := 'IFRS_EAD_RULES_CONFIG';
        V_TABLEINSERT4 := 'IFRS_LGD_RULES_CONFIG';
        V_TABLEINSERT5 := 'IFRS_MASTER_PRODUCT_PARAM';
    END IF;

    IF P_DOWNLOAD_DATE IS NULL 
    THEN
        SELECT
            CURRDATE INTO V_CURRDATE
        FROM
            IFRS_PRC_DATE;
    ELSE        
        V_CURRDATE := P_DOWNLOAD_DATE;
    END IF;
    
    V_PREVMONTH := F_EOMONTH(V_CURRDATE, 1, 'M', 'PREV');
    V_LASTYEAR := F_EOMONTH(V_CURRDATE, 1, 'Y', 'PREV');
    V_LASTYEARNEXTMONTH := F_EOMONTH(V_LASTYEAR, 1, 'M', 'NEXT');
    
    V_RETURNROWS2 := 0;
    -------- ====== VARIABLE ======

    -------- RECORD RUN_ID --------
    CALL SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, P_RUNID, PG_BACKEND_PID(), CURRENT_DATE);
    -------- RECORD RUN_ID --------

    -------- ====== PRE SIMULATION TABLE ======
    IF P_PRC = 'S' THEN
        IF P_STATUS = 'N' THEN
            V_STR_QUERY := 'DROP TABLE IF EXISTS ' || V_TABLEINSERT1 || '';
            V_STR_QUERY := V_STR_QUERY || '';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := 'CREATE TABLE ' || V_TABLEINSERT1 || ' AS SELECT * FROM IFRS_PD_RULES_CONFIG';
            V_STR_QUERY := V_STR_QUERY || '';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := 'DROP TABLE IF EXISTS ' || V_TABLEINSERT2 || '';
            V_STR_QUERY := V_STR_QUERY || '';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := 'CREATE TABLE ' || V_TABLEINSERT2 || ' AS SELECT * FROM IFRS_CCF_RULES_CONFIG';
            V_STR_QUERY := V_STR_QUERY || '';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := 'DROP TABLE IF EXISTS ' || V_TABLEINSERT3 || '';
            V_STR_QUERY := V_STR_QUERY || '';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := 'CREATE TABLE ' || V_TABLEINSERT3 || ' AS SELECT * FROM IFRS_EAD_RULES_CONFIG';
            V_STR_QUERY := V_STR_QUERY || '';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := 'DROP TABLE IF EXISTS ' || V_TABLEINSERT4 || '';
            V_STR_QUERY := V_STR_QUERY || '';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := 'CREATE TABLE ' || V_TABLEINSERT4 || ' AS SELECT * FROM IFRS_LGD_RULES_CONFIG';
            V_STR_QUERY := V_STR_QUERY || '';
            EXECUTE (V_STR_QUERY);


            V_STR_QUERY := 'DROP TABLE IF EXISTS ' || V_TABLEINSERT5 || '';
            V_STR_QUERY := V_STR_QUERY || '';
            EXECUTE (V_STR_QUERY);

            V_STR_QUERY := 'CREATE TABLE ' || V_TABLEINSERT5 || ' AS SELECT * FROM IFRS_MASTER_PRODUCT_PARAM';
            V_STR_QUERY := V_STR_QUERY || '';
            EXECUTE (V_STR_QUERY);
        ELSE
            -------- ====== PD ======
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT1 || ' (
            PKID, TM_RULE_NAME, SEGMENTATION_ID, PD_METHOD, CALC_METHOD, BUCKET_GROUP, EXPECTED_LIFE, INCREMENT_PERIOD,
            HISTORICAL_DATA, CUT_OFF_DATE, ACTIVE_FLAG, IS_DELETE, CREATEDBY, CREATEDDATE,
            CREATEDHOST, DEFAULT_RATIO_BY, LAG_1MONTH_FLAG, RUNNING_STATUS
            )
            SELECT * FROM dblink(''workflow_db_access'', ''SELECT pkid, pd_name, RIGHT(segment_code, 2) AS segment_code, pd_method, calculation_method_id, bucket_code, expected_lifetime, windows_moving,
            historical_data, effective_end_date, CASE WHEN is_active = TRUE THEN 1 ELSE 0 END AS is_active, 0 AS is_delete,
            created_by, created_date, created_host, default_ratio_by, CASE pd_method WHEN ''''MAA_CORP'''' THEN 0 WHEN ''''EXT'''' THEN 0 ELSE 1 END AS lag_1month_flag,
            ''''PENDING'''' AS running_status
            FROM "PdConfiguration_Dev" ORDER BY pkid ASC'') 
            AS IFRS_PD_RULES_CONFIG_DATA(
            PKID BIGINT, TM_RULE_NAME VARCHAR(250), SEGMENTATION_ID BIGINT, PD_METHOD VARCHAR(50), CALC_METHOD VARCHAR(20), BUCKET_GROUP VARCHAR(30), EXPECTED_LIFE BIGINT, INCREMENT_PERIOD BIGINT,
            HISTORICAL_DATA BIGINT, CUT_OFF_DATE DATE, ACTIVE_FLAG BIGINT, IS_DELETE BIGINT, CREATEDBY VARCHAR(36), CREATEDDATE DATE,
            CREATEDHOST VARCHAR(30), DEFAULT_RATIO_BY VARCHAR(20), LAG_1MONTH_FLAG BIGINT, RUNNING_STATUS VARCHAR(20)
            )';
            EXECUTE (V_STR_QUERY);

            GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
            V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
            V_RETURNROWS := 0;
            -------- ====== PD ======

            -------- ====== CCF ======
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT2 || ' (
            PKID, CCF_RULE_NAME, SEGMENTATION_ID, CALC_METHOD, AVERAGE_METHOD, DEFAULT_RULE_ID, CUT_OFF_DATE,
            CCF_OVERRIDE, ACTIVE_FLAG, IS_DELETE, CREATEDBY, CREATEDDATE,
            CREATEDHOST, LAG_1MONTH_FLAG, RUNNING_STATUS
            )
            SELECT * FROM dblink(''workflow_db_access'', ''SELECT pkid, ccf_name, CASE WHEN RIGHT(segment_code, 3)::INT > 100 THEN RIGHT(segment_code, 3) ELSE RIGHT(segment_code, 2) END AS segment_code, ccf_method, calculation_method, calculation_method_id, effective_end_date, expected_ccf,
            CASE WHEN is_active = TRUE THEN 1 ELSE 0 END AS is_active, 0 AS is_delete, created_by, created_date, created_host,
            CASE calculation_method WHEN ''''Simple'''' THEN 0 ELSE 1 END AS lag_1month_flag, ''''PENDING'''' AS running_status
            FROM "CcfConfiguration_Dev" ORDER BY pkid ASC'') 
            AS IFRS_CCF_RULES_CONFIG_DATA(
            PKID BIGINT, CCF_RULE_NAME VARCHAR(250), SEGMENTATION_ID BIGINT, CALC_METHOD VARCHAR(20), AVERAGE_METHOD VARCHAR(20), DEFAULT_RULE_ID BIGINT, CUT_OFF_DATE DATE,
            CCF_OVERRIDE BIGINT, ACTIVE_FLAG BIGINT, IS_DELETE BIGINT, CREATEDBY VARCHAR(36), CREATEDDATE DATE,
            CREATEDHOST VARCHAR(30), LAG_1MONTH_FLAG BIGINT, RUNNING_STATUS VARCHAR(20)
            )';
            EXECUTE (V_STR_QUERY);

            GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
            V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
            V_RETURNROWS := 0;
            -------- ====== CCF ======

            -------- ====== EAD ======
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT3 || ' (
            PKID, EAD_RULE_NAME, SEGMENTATION_ID, EAD_BALANCE, ACTIVE_FLAG, IS_DELETE
            )
            SELECT * FROM dblink(''workflow_db_access'', ''SELECT pkid, syscode_ead_config
            ,RIGHT(segment_code, 2) AS segment_code, balance_source
            ,CASE WHEN is_active = TRUE THEN 1 ELSE 0 END AS is_active, 0 AS is_delete 
            FROM "EadConfiguration_Dev" ORDER BY pkid ASC'') 
            AS IFRS_EAD_RULES_CONFIG_DATA (
            PKID BIGINT, EAD_RULE_NAME VARCHAR(100)
            ,SEGMENTATION_ID BIGINT, EAD_BALANCE VARCHAR(250)
            ,ACTIVE_FLAG BIGINT, IS_DELETE BIGINT)';
            EXECUTE (V_STR_QUERY);

            GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
            V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
            V_RETURNROWS := 0;
            -------- ====== EAD ======

            -------- ====== LGD ======
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT4 || ' (
            PKID, DEFAULT_RULE_ID, LGD_RULE_NAME, SEGMENTATION_ID, LGD_METHOD, LGW_HISTORICAL_DATA, WORKOUT_PERIOD
            ,MIN_VALUE, MAX_VALUE,CUT_OFF_DATE, CALC_METHOD, ACTIVE_FLAG, IS_DELETE, CREATEDBY, CREATEDDATE,
            CREATEDHOST, LAG_1MONTH_FLAG, RUNNING_STATUS
            )
            SELECT * FROM dblink(''workflow_db_access'', ''SELECT pkid, RIGHT(default_criteria, 2) AS default_criteria, lgd_name, RIGHT(segment_code, 2) AS segment_code,lgd_method
            ,historical_data, workout_period, lgd_min, lgd_max, effective_end_date, calculation_method, CASE WHEN is_active = TRUE THEN 1 ELSE 0 END AS is_active, 0 AS is_delete,
            created_by, created_date, created_host, CASE direct_cost WHEN ''''0'''' THEN 0 ELSE 1 END AS lag_1month_flag, ''''PENDING'''' AS running_status
            FROM "LgdConfiguration_Dev" ORDER BY pkid ASC'') 
            AS IFRS_LGD_RULES_CONFIG_DATA(
            PKID BIGINT, DEFAULT_RULE_ID BIGINT, LGD_RULE_NAME VARCHAR(250), SEGMENTATION_ID BIGINT, LGD_METHOD VARCHAR(50), LGW_HISTORICAL_DATA BIGINT, WORKOUT_PERIOD BIGINT
            ,MIN_VALUE BIGINT, MAX_VALUE BIGINT,CUT_OFF_DATE DATE, CALC_METHOD VARCHAR(250), ACTIVE_FLAG BIGINT, IS_DELETE BIGINT, CREATEDBY VARCHAR(36), CREATEDDATE DATE,
            CREATEDHOST VARCHAR(30), LAG_1MONTH_FLAG BIGINT, RUNNING_STATUS VARCHAR(20)
            )';
            EXECUTE (V_STR_QUERY);

            GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
            V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
            V_RETURNROWS := 0;
            -------- ====== LGD ======


            -------- ====== MASTER PRODUCT PARAM ======
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ' || V_TABLEINSERT5 || ' (
            PKID, DATA_SOURCE, PRD_TYPE, PRD_CODE, PRD_GROUP , IS_DELETE, 
            CREATEDBY, CREATEDDATE, CREATEDHOST, UPDATEDBY, UPDATEDDATE, UPDATEDHOST
            )
            SELECT * FROM dblink(''workflow_ifrs_db_access'', ''SELECT pkid, data_source, product_type, product_code, product_group, CASE WHEN is_active = TRUE THEN 1 ELSE 0 END AS is_active, 
            created_by, created_date, created_host, updated_by, updated_date, updated_host 
            FROM "MstProduct" ORDER BY pkid ASC'') 
            AS IFRS_MASTER_PRODUCT_PARAM_DATA(
            PKID BIGINT, DATA_SOURCE VARCHAR(20), PRD_TYPE VARCHAR(50), PRD_CODE VARCHAR(20), PRD_GROUP VARCHAR(20), IS_DELETE INT, 
            CREATEDBY VARCHAR(36), CREATEDDATE DATE, CREATEDHOST VARCHAR(30), UPDATEDBY VARCHAR(36), UPDATEDDATE DATE, UPDATEDHOST VARCHAR(30)
            )';
            EXECUTE (V_STR_QUERY);

            GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
            V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
            V_RETURNROWS := 0;
            -------- ====== MASTER PRODUCT PARAM ======

            RAISE NOTICE 'SP_IFRS_IMP_INITIAL_CONFIG | AFFECTED RECORD : %', V_RETURNROWS2;
        END IF;
    END IF;
    
END;

$$;