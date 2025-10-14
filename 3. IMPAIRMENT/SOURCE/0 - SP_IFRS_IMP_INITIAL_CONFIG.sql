---- DROP PROCEDURE SP_IFRS_IMP_INITIAL_CONFIG;

CREATE OR REPLACE PROCEDURE SP_IFRS_IMP_INITIAL_CONFIG(
    IN P_RUNID VARCHAR(20) DEFAULT 'S_00000_0000', 
    IN P_DOWNLOAD_DATE DATE DEFAULT NULL,
    IN P_PRC VARCHAR(1) DEFAULT 'S')
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

    ---- CONDITION
    V_RETURNROWS INT;
    V_RETURNROWS2 INT;
    V_TABLEDEST VARCHAR(100);
    V_COLUMNDEST VARCHAR(100);
    V_SPNAME VARCHAR(100);
    V_OPERATION VARCHAR(100);

    ---- RESULT
    V_QUERYS TEXT;
    
    ---- VARIABLE CONDITION
    V_CHECKDIFF BIGINT;
    V_ACTIVEFLAG BOOLEAN := FALSE;

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

    IF COALESCE(P_RUNID, NULL) IS NULL THEN
        P_RUNID := 'S_00000_0000';
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
    V_CHECKDIFF := 0;
    -------- ====== VARIABLE ======

    -------- ====== BODY ======

    IF V_ACTIVEFLAG = TRUE THEN

        -------- START ifrs_mstr_segment_rules_header
        -------- CHECK DATA EXIST IN WORKFLOW
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT COUNT(*) FROM vw_source_mstr_segment_rules_header';
        EXECUTE (V_STR_QUERY) INTO V_CHECKDIFF;

        ----- IF DATA NOT EXIST, INSERT DATA
        IF V_CHECKDIFF > 0 THEN
			---- DELETE DATA MODIFY EXISTING
			V_STR_QUERY := '';
			V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ifrs_mstr_segment_rules_header WHERE PKID IN(SELECT PKID FROM vw_source_mstr_segment_rules_header)';
        	EXECUTE (V_STR_QUERY);
			
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ifrs_mstr_segment_rules_header(pkid
			,group_segment
			,segment
			,sub_segment
			,active_flag
			,segment_type
			,sequence
			,is_new
			,is_delete
			,createdby
			,createddate
			,createdhost
			,updatedby
			,updateddate
			,updatedhost)
            SELECT pkid
			,group_segment
			,segment
			,sub_segment
			,active_flag
			,segment_type
			,sequence
			,is_new
			,is_delete
			,createdby
			,createddate
			,createdhost
			,updatedby
			,updateddate
			,updatedhost FROM vw_source_mstr_segment_rules_header';
            EXECUTE (V_STR_QUERY);

            V_CHECKDIFF := 0;
            
            GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
            V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
            V_RETURNROWS := 0;

            RAISE NOTICE 'INSERT TABLE ifrs_mstr_segment_rules_header | SUCCESS, TOTAL INSERTED %', V_RETURNROWS2;
        END IF;
        -------- END ifrs_mstr_segment_rules_header
		
		-------- START ifrs_ecl_model_detail_ead
        -------- CHECK DATA EXIST IN WORKFLOW
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT COUNT(*) FROM vw_source_ecl_model_detail_ead';
        EXECUTE (V_STR_QUERY) INTO V_CHECKDIFF;

        ----- IF DATA NOT EXIST, INSERT DATA
        IF V_CHECKDIFF > 0 THEN
			---- DELETE DATA MODIFY EXISTING
			V_STR_QUERY := '';
			V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ifrs_ecl_model_detail_ead WHERE PKID IN(SELECT PKID FROM vw_source_ecl_model_detail_ead)';
        	EXECUTE (V_STR_QUERY);
			
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ifrs_ecl_model_detail_ead(pkid
			,ecl_model_id
			,segmentation_id
			,ead_model_id
			,is_delete
			,createdby
			,createddate
			,createdhost
			,updatedby
			,updateddate
			,updatedhost
			,ccf_model_id
			,ccf_eff_date_option
			,ccf_eff_date)
            SELECT pkid
			,ecl_model_id
			,segmentation_id
			,ead_model_id
			,is_delete
			,createdby
			,createddate
			,createdhost
			,updatedby
			,updateddate
			,updatedhost
			,ccf_model_id
			,ccf_eff_date_option
			,ccf_eff_date FROM vw_source_ecl_model_detail_ead';
            EXECUTE (V_STR_QUERY);

            V_CHECKDIFF := 0;
            
            GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
            V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
            V_RETURNROWS := 0;

            RAISE NOTICE 'INSERT TABLE ifrs_ecl_model_detail_ead | SUCCESS, TOTAL INSERTED %', V_RETURNROWS2;
        END IF;
        -------- END ifrs_ecl_model_detail_ead

		-------- START ifrs_ecl_model_detail_lgd
        -------- CHECK DATA EXIST IN WORKFLOW
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT COUNT(*) FROM vw_source_ecl_model_detail_lgd';
        EXECUTE (V_STR_QUERY) INTO V_CHECKDIFF;

        ----- IF DATA NOT EXIST, INSERT DATA
        IF V_CHECKDIFF > 0 THEN
			---- DELETE DATA MODIFY EXISTING
			V_STR_QUERY := '';
			V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ifrs_ecl_model_detail_lgd WHERE PKID IN(SELECT PKID FROM vw_source_ecl_model_detail_lgd)';
        	EXECUTE (V_STR_QUERY);
			
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ifrs_ecl_model_detail_lgd(pkid
			,ecl_model_id
			,segmentation_id
			,lgd_model_id
			,me_model_id
			,eff_date_option
			,eff_date
			,is_delete
			,createdby
			,createddate
			,createdhost
			,updatedby
			,updateddate
			,updatedhost)
            SELECT pkid
			,ecl_model_id
			,segmentation_id
			,lgd_model_id
			,me_model_id
			,eff_date_option
			,eff_date
			,is_delete
			,createdby
			,createddate
			,createdhost
			,updatedby
			,updateddate
			,updatedhost FROM vw_source_ecl_model_detail_lgd';
            EXECUTE (V_STR_QUERY);

            V_CHECKDIFF := 0;
            
            GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
            V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
            V_RETURNROWS := 0;

            RAISE NOTICE 'INSERT TABLE ifrs_ecl_model_detail_lgd | SUCCESS, TOTAL INSERTED %', V_RETURNROWS2;
        END IF;
        -------- END ifrs_ecl_model_detail_lgd

		-------- START ifrs_ecl_model_detail_pd
        -------- CHECK DATA EXIST IN WORKFLOW
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT COUNT(*) FROM vw_source_ecl_model_detail_pd';
        EXECUTE (V_STR_QUERY) INTO V_CHECKDIFF;

        ----- IF DATA NOT EXIST, INSERT DATA
        IF V_CHECKDIFF > 0 THEN
			---- DELETE DATA MODIFY EXISTING
			V_STR_QUERY := '';
			V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ifrs_ecl_model_detail_pd WHERE PKID IN(SELECT PKID FROM vw_source_ecl_model_detail_pd)';
        	EXECUTE (V_STR_QUERY);
			
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ifrs_ecl_model_detail_pd(pkid
			,ecl_model_id
			,segmentation_id
			,pd_model_id
			,me_model_id
			,eff_date_option
			,eff_date
			,is_delete
			,createdby
			,createddate
			,createdhost
			,updatedby
			,updateddate
			,updatedhost
			,scalar_eff_date_option
			,scalar_eff_date)
            SELECT pkid
			,ecl_model_id
			,segmentation_id
			,pd_model_id
			,me_model_id
			,eff_date_option
			,eff_date
			,is_delete
			,createdby
			,createddate
			,createdhost
			,updatedby
			,updateddate
			,updatedhost
			,scalar_eff_date_option
			,scalar_eff_date FROM vw_source_ecl_model_detail_pd';
            EXECUTE (V_STR_QUERY);

            V_CHECKDIFF := 0;
            
            GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
            V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
            V_RETURNROWS := 0;

            RAISE NOTICE 'INSERT TABLE ifrs_ecl_model_detail_pd | SUCCESS, TOTAL INSERTED %', V_RETURNROWS2;
        END IF;
        -------- END ifrs_ecl_model_detail_pd

		-------- START ifrs_ecl_model_detail_pf
        -------- CHECK DATA EXIST IN WORKFLOW
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT COUNT(*) FROM vw_source_ecl_model_detail_pf';
        EXECUTE (V_STR_QUERY) INTO V_CHECKDIFF;

        ----- IF DATA NOT EXIST, INSERT DATA
        IF V_CHECKDIFF > 0 THEN
			---- DELETE DATA MODIFY EXISTING
			V_STR_QUERY := '';
			V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ifrs_ecl_model_detail_pf WHERE PKID IN(SELECT PKID FROM vw_source_ecl_model_detail_pf)';
        	EXECUTE (V_STR_QUERY);
			
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ifrs_ecl_model_detail_pf(pkid
			,ecl_model_id
			,segmentation_id
			,lt_rule_id
			,sicr_rule_id
			,upside_value
			,base_value
			,downside_value
			,upside_prob_outcome
			,base_prob_outcome
			,downside_prob_outcome
			,overlay_amount
			,is_delete
			,createdby
			,createddate
			,createdhost
			,updatedby
			,updateddate
			,updatedhost
			,bucket_group
			,default_rule_id)
            SELECT pkid
			,ecl_model_id
			,segmentation_id
			,lt_rule_id
			,sicr_rule_id
			,upside_value
			,base_value
			,downside_value
			,upside_prob_outcome
			,base_prob_outcome
			,downside_prob_outcome
			,overlay_amount
			,is_delete
			,createdby
			,createddate
			,createdhost
			,updatedby
			,updateddate
			,updatedhost
			,bucket_group
			,default_rule_id FROM vw_source_ecl_model_detail_pf';
            EXECUTE (V_STR_QUERY);

            V_CHECKDIFF := 0;
            
            GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
            V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
            V_RETURNROWS := 0;

            RAISE NOTICE 'INSERT TABLE ifrs_ecl_model_detail_pf | SUCCESS, TOTAL INSERTED %', V_RETURNROWS2;
        END IF;
        -------- END ifrs_ecl_model_detail_pf

		-------- START IFRS_ecl_model_header
        -------- CHECK DATA EXIST IN WORKFLOW
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT COUNT(*) FROM vw_source_ecl_model_header';
        EXECUTE (V_STR_QUERY) INTO V_CHECKDIFF;

        ----- IF DATA NOT EXIST, INSERT DATA
        IF V_CHECKDIFF > 0 THEN
			---- DELETE DATA MODIFY EXISTING
			V_STR_QUERY := '';
			V_STR_QUERY := V_STR_QUERY || 'DELETE FROM ifrs_ecl_model_header WHERE PKID IN(SELECT PKID FROM vw_source_ecl_model_header)';
        	EXECUTE (V_STR_QUERY);
			
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO ifrs_ecl_model_header(pkid
			,ecl_model_name
			,data_date
			,active_status
			,run_status
			,is_delete
			,createdby
			,createddate
			,createdhost
			,updatedby
			,updateddate
			,updatedhost)
            SELECT pkid
			,ecl_model_name
			,data_date
			,active_status
			,run_status
			,is_delete
			,createdby
			,createddate
			,createdhost
			,updatedby
			,updateddate
			,updatedhost FROM vw_source_ecl_model_header';
            EXECUTE (V_STR_QUERY);

            V_CHECKDIFF := 0;
            
            GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
            V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
            V_RETURNROWS := 0;

            RAISE NOTICE 'INSERT TABLE ifrs_ecl_model_header | SUCCESS, TOTAL INSERTED %', V_RETURNROWS2;
        END IF;
        -------- END ifrs_ecl_model_header

        -------- START IFRS_PD_RULES_CONFIG
        -------- CHECK DATA EXIST IN WORKFLOW
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT COUNT(*) FROM VW_SOURCE_PD_RULES_CONFIG';
        EXECUTE (V_STR_QUERY) INTO V_CHECKDIFF;

        ----- IF DATA NOT EXIST, INSERT DATA
        IF V_CHECKDIFF > 0 THEN
			---- DELETE DATA MODIFY EXISTING
			V_STR_QUERY := '';
			V_STR_QUERY := V_STR_QUERY || 'DELETE FROM IFRS_PD_RULES_CONFIG WHERE PKID IN(SELECT PKID FROM VW_SOURCE_PD_RULES_CONFIG)';
        	EXECUTE (V_STR_QUERY);
			
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO IFRS_PD_RULES_CONFIG(PKID
            ,TM_RULE_NAME
            ,SEGMENTATION_ID
            ,PD_METHOD
            ,CALC_METHOD
            ,BUCKET_GROUP
            ,EXPECTED_LIFE
            ,INCREMENT_PERIOD
            ,HISTORICAL_DATA
            ,CUT_OFF_DATE
            ,ACTIVE_FLAG
            ,IS_DELETE
            ,CREATEDBY
            ,CREATEDDATE
            ,CREATEDHOST
            ,LAG_1MONTH_FLAG
            ,RUNNING_STATUS)
            SELECT PKID
            ,TM_RULE_NAME
            ,SEGMENTATION_ID
            ,PD_METHOD
            ,CALC_METHOD
            ,BUCKET_GROUP
            ,EXPECTED_LIFE
            ,INCREMENT_PERIOD
            ,HISTORICAL_DATA
            ,CUT_OFF_DATE
            ,ACTIVE_FLAG
            ,IS_DELETE
            ,CREATEDBY
            ,CREATEDDATE
            ,CREATEDHOST
            ,LAG_1MONTH_FLAG
            ,RUNNING_STATUS FROM VW_SOURCE_PD_RULES_CONFIG';
            EXECUTE (V_STR_QUERY);

            V_CHECKDIFF := 0;
            
            GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
            V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
            V_RETURNROWS := 0;

            RAISE NOTICE 'INSERT TABLE IFRS_PD_RULES_CONFIG | SUCCESS, TOTAL INSERTED %', V_RETURNROWS2;
        END IF;
        -------- END IFRS_PD_RULES_CONFIG

        -------- START IFRS_LGD_RULES_CONFIG
        -------- CHECK DATA EXIST IN WORKFLOW
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT COUNT(*) FROM VW_SOURCE_LGD_RULES_CONFIG';
        EXECUTE (V_STR_QUERY) INTO V_CHECKDIFF;

        ----- IF DATA NOT EXIST, INSERT DATA
        IF V_CHECKDIFF > 0 THEN
			---- DELETE DATA MODIFY EXISTING
			V_STR_QUERY := '';
			V_STR_QUERY := V_STR_QUERY || 'DELETE FROM IFRS_LGD_RULES_CONFIG WHERE PKID IN(SELECT PKID FROM VW_SOURCE_LGD_RULES_CONFIG)';
        	EXECUTE (V_STR_QUERY);
			
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO IFRS_LGD_RULES_CONFIG(PKID
            ,DEFAULT_RULE_ID
            ,LGD_RULE_NAME
            ,SEGMENTATION_ID
            ,LGD_METHOD
            ,LGW_HISTORICAL_DATA
            ,WORKOUT_PERIOD
            ,MIN_VALUE
            ,MAX_VALUE
            ,CUT_OFF_DATE
            ,ACTIVE_FLAG
            ,IS_DELETE
            ,CREATEDBY
            ,CREATEDDATE
            ,CREATEDHOST
            ,LAG_1MONTH_FLAG
            ,RUNNING_STATUS)
            SELECT PKID
            ,DEFAULT_RULE_ID
            ,LGD_RULE_NAME
            ,SEGMENTATION_ID
            ,LGD_METHOD
            ,LGW_HISTORICAL_DATA
            ,WORKOUT_PERIOD
            ,MIN_VALUE
            ,MAX_VALUE
            ,CUT_OFF_DATE
            ,ACTIVE_FLAG
            ,IS_DELETE
            ,CREATEDBY
            ,CREATEDDATE
            ,CREATEDHOST
            ,LAG_1MONTH_FLAG
            ,RUNNING_STATUS FROM VW_SOURCE_LGD_RULES_CONFIG';
            EXECUTE (V_STR_QUERY);

            V_CHECKDIFF := 0;
            
            GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
            V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
            V_RETURNROWS := 0;

            RAISE NOTICE 'INSERT TABLE IFRS_LGD_RULES_CONFIG | SUCCESS, TOTAL INSERTED %', V_RETURNROWS2;
        END IF;
        -------- END IFRS_LGD_RULES_CONFIG

        -------- START IFRS_EAD_RULES_CONFIG
        -------- CHECK DATA EXIST IN WORKFLOW
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT COUNT(*) FROM VW_SOURCE_EAD_RULES_CONFIG';
        EXECUTE (V_STR_QUERY) INTO V_CHECKDIFF;

        ----- IF DATA NOT EXIST, INSERT DATA
        IF V_CHECKDIFF > 0 THEN
			---- DELETE DATA MODIFY EXISTING
			V_STR_QUERY := '';
			V_STR_QUERY := V_STR_QUERY || 'DELETE FROM IFRS_EAD_RULES_CONFIG WHERE PKID IN(SELECT PKID FROM VW_SOURCE_EAD_RULES_CONFIG)';
        	EXECUTE (V_STR_QUERY);
			
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO IFRS_EAD_RULES_CONFIG(PKID
            ,EAD_RULE_NAME
            ,SEGMENTATION_ID
            ,EAD_BALANCE
            ,CCF_FLAG
            ,CCF_RULES_ID
            ,PREPAYMENT_FLAG
            ,PREPAYMENT_RULES_ID
            ,ACTIVE_FLAG
            ,IS_DELETE
            ,CREATEDBY
            ,CREATEDDATE
            ,CREATEDHOST)
            SELECT PKID
            ,EAD_RULE_NAME
            ,SEGMENTATION_ID
            ,EAD_BALANCE
            ,CCF_FLAG
            ,CCF_RULES_ID
            ,PREPAYMENT_FLAG
            ,PREPAYMENT_RULES_ID
            ,ACTIVE_FLAG
            ,IS_DELETE
            ,CREATEDBY
            ,CREATEDDATE
            ,CREATEDHOST FROM VW_SOURCE_EAD_RULES_CONFIG';
            EXECUTE (V_STR_QUERY);

            V_CHECKDIFF := 0;
            
            GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
            V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
            V_RETURNROWS := 0;

            RAISE NOTICE 'INSERT TABLE IFRS_EAD_RULES_CONFIG | SUCCESS, TOTAL INSERTED %', V_RETURNROWS2;
        END IF;
        -------- END IFRS_EAD_RULES_CONFIG

        -------- START IFRS_CCF_RULES_CONFIG
        -------- CHECK DATA EXIST IN WORKFLOW
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT COUNT(*) FROM VW_SOURCE_CCF_RULES_CONFIG';
        EXECUTE (V_STR_QUERY) INTO V_CHECKDIFF;

        ----- IF DATA NOT EXIST, INSERT DATA
        IF V_CHECKDIFF > 0 THEN
			---- DELETE DATA MODIFY EXISTING
			V_STR_QUERY := '';
			V_STR_QUERY := V_STR_QUERY || 'DELETE FROM IFRS_CCF_RULES_CONFIG WHERE PKID IN(SELECT PKID FROM VW_SOURCE_CCF_RULES_CONFIG)';
        	EXECUTE (V_STR_QUERY);
			
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO IFRS_CCF_RULES_CONFIG(PKID
            ,CCF_RULE_NAME
            ,SEGMENTATION_ID
            ,CALC_METHOD
            ,AVERAGE_METHOD
            ,DEFAULT_RULE_ID
            ,CUT_OFF_DATE
            ,CCF_OVERRIDE
            ,ACTIVE_FLAG
            ,IS_DELETE
            ,CREATEDBY
            ,CREATEDDATE
            ,CREATEDHOST
            ,LAG_1MONTH_FLAG
            ,RUNNING_STATUS)
            SELECT PKID
            ,CCF_RULE_NAME
            ,SEGMENTATION_ID
            ,CALC_METHOD
            ,AVERAGE_METHOD
            ,DEFAULT_RULE_ID
            ,CUT_OFF_DATE
            ,CCF_OVERRIDE
            ,ACTIVE_FLAG
            ,IS_DELETE
            ,CREATEDBY
            ,CREATEDDATE
            ,CREATEDHOST
            ,LAG_1MONTH_FLAG
            ,RUNNING_STATUS FROM VW_SOURCE_CCF_RULES_CONFIG';
            EXECUTE (V_STR_QUERY);

            V_CHECKDIFF := 0;
            
            GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
            V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
            V_RETURNROWS := 0;

            RAISE NOTICE 'INSERT TABLE IFRS_CCF_RULES_CONFIG | SUCCESS, TOTAL INSERTED %', V_RETURNROWS2;
        END IF;
        -------- END IFRS_CCF_RULES_CONFIG

        -------- START IFRS_MASTER_PRODUCT_PARAM
        -------- CHECK DATA EXIST IN WORKFLOW
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT COUNT(*) FROM VW_SOURCE_MASTER_PRODUCT_PARAM';
        EXECUTE (V_STR_QUERY) INTO V_CHECKDIFF;

        ----- IF DATA NOT EXIST, INSERT DATA
        IF V_CHECKDIFF > 0 THEN
			---- DELETE DATA MODIFY EXISTING
			V_STR_QUERY := '';
			V_STR_QUERY := V_STR_QUERY || 'DELETE FROM IFRS_MASTER_PRODUCT_PARAM WHERE PKID IN(SELECT PKID FROM VW_SOURCE_MASTER_PRODUCT_PARAM)';
        	EXECUTE (V_STR_QUERY);
			
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO IFRS_MASTER_PRODUCT_PARAM(PKID
            ,DATA_SOURCE
            ,PRD_TYPE
            ,PRD_CODE
            ,PRD_GROUP
            ,PRD_DESC
            ,INST_CLS_VALUE
            ,IS_DELETE
            ,CREATEDBY
            ,CREATEDDATE
            ,CREATEDHOST)
            SELECT PKID
            ,DATA_SOURCE
            ,PRD_TYPE
            ,PRD_CODE
            ,PRD_GROUP
            ,PRD_DESC
            ,INST_CLS_VALUE
            ,IS_DELETE
            ,CREATEDBY
            ,CREATEDDATE
            ,CREATEDHOST FROM VW_SOURCE_MASTER_PRODUCT_PARAM';
            EXECUTE (V_STR_QUERY);

            V_CHECKDIFF := 0;
            
            GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
            V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
            V_RETURNROWS := 0;

            RAISE NOTICE 'INSERT TABLE IFRS_MASTER_PRODUCT_PARAM | SUCCESS, TOTAL INSERTED %', V_RETURNROWS2;
        END IF;
        -------- END IFRS_MASTER_PRODUCT_PARAM

        -------- START IFRS_MASTER_EXCHANGE_RATE
        -------- CHECK DATA EXIST IN WORKFLOW
        V_STR_QUERY := '';
        V_STR_QUERY := V_STR_QUERY || 'SELECT COUNT(*) FROM VW_SOURCE_MASTER_EXCHANGE_RATE';
        EXECUTE (V_STR_QUERY) INTO V_CHECKDIFF;

        ----- IF DATA NOT EXIST, INSERT DATA
        IF V_CHECKDIFF > 0 THEN
			---- DELETE DATA MODIFY EXISTING
			V_STR_QUERY := '';
			V_STR_QUERY := V_STR_QUERY || 'DELETE FROM IFRS_MASTER_EXCHANGE_RATE WHERE PKID IN(SELECT PKID FROM VW_SOURCE_MASTER_EXCHANGE_RATE)';
        	EXECUTE (V_STR_QUERY);
			
            V_STR_QUERY := '';
            V_STR_QUERY := V_STR_QUERY || 'INSERT INTO IFRS_MASTER_EXCHANGE_RATE(PKID
            ,DOWNLOAD_DATE
            ,CURRENCY
            ,CURRENCY_DESC
            ,RATE_AMOUNT
            ,MAINTAIN_DATE)
            SELECT PKID
            ,DOWNLOAD_DATE
            ,CURRENCY
            ,CURRENCY_DESC
            ,RATE_AMOUNT
            ,MAINTAIN_DATE FROM VW_SOURCE_MASTER_EXCHANGE_RATE';
            EXECUTE (V_STR_QUERY);

            V_CHECKDIFF := 0;
            
            GET DIAGNOSTICS V_RETURNROWS = ROW_COUNT;
            V_RETURNROWS2 := V_RETURNROWS2 + V_RETURNROWS;
            V_RETURNROWS := 0;

            RAISE NOTICE 'INSERT TABLE IFRS_MASTER_EXCHANGE_RATE | SUCCESS, TOTAL INSERTED %', V_RETURNROWS2;
        END IF;
        -------- END IFRS_MASTER_EXCHANGE_RATE

        -------- ====== BODY ======
        
        RAISE NOTICE 'SP_IFRS_IMP_INITIAL_CONFIG | SUCCESSFULY';
    ELSE
        RAISE NOTICE 'SP_IFRS_IMP_INITIAL_CONFIG | SKIPPED, ACTIVEFLAG IS FALSE';
    END IF;
END;

$$;