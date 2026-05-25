CREATE OR REPLACE PROCEDURE PSAK413.SP_TBLT_PAYMENTEXPECTED
AS 
 
BEGIN

	MERGE INTO TBLT_PAYMENTEXPECTED a
    USING (
    		SELECT X."syscode_product_configuration",
    		X."syscode_product",
    		J."product_code",
    		J."product_type", 
    		J."product_name",
    		J."product_category",
    		J."description",
    		J."product_group",
    		J."data_source",
    		J."market_rate",
    		I."instrument_classification",
    		I."cde_curr" AS CCY,
    		I."is_impaired",
    		J."origination_fee_amount",
    		J."origination_fee_type",
    		J."transaction_cost_type",
    		J."transaction_cost_amount",
    		X."is_deleted"
    		FROM "ProductConfigurationDetail"@DBCONFIGLINK X
		 		LEFT JOIN "ProductConfiguration"@DBCONFIGLINK I ON X."syscode_product_configuration" = I."syscode_product_configuration"
		 		LEFT JOIN "MstProduct"@DBCONFIGLINK J ON X."syscode_product" = J."syscode_product"
    ) b
    ON (a.SYSCODE_PRODUCT_PARAM = b."syscode_product_configuration" )  
    WHEN MATCHED THEN
        UPDATE SET   
            a.UPDATEDBY              	= 'SP_IFRS_MASTER_PRODUCT_PARAM',
            a.UPDATEDDATE            	= sysdate,
            a.IS_DELETE              	= B."is_deleted",
            a.PRD_TYPE  			 	= B."product_type",
            A.PRD_DESC 					= B."description",
            A.PRD_GROUP 				= B."product_group",
            A.MKT_INT_RATE			 	= NVL(B."market_rate",0),
            A.IS_IMPAIRED 				= NVL(B."is_impaired",0)
    
    WHEN NOT MATCHED THEN
        INSERT (
        	SYSCODE_PRODUCT_PARAM,
        	DATA_SOURCE,
        	PRD_TYPE,
        	PRD_CODE,
        	PRD_GROUP,
        	PRD_DESC,
        	CCY,
        	ORG_FEE_MAT_TYPE,
        	ORG_FEE_MAT_AMT,
        	MKT_INT_RATE,
        	IS_IMPAIRED,
        	--PRD_CATEGORY,
		    IS_DELETE,
		    CREATEDBY,
		    CREATEDDATE
		    )
        VALUES (
            B."syscode_product_configuration",b."data_source",b."product_type",b."product_code",b."product_group",b."description",b.CCY, 
            b."origination_fee_type",NVL(b."origination_fee_amount",0),NVL(b."market_rate",0),NVL(B."is_impaired",0) ,B."is_deleted",'SP_IFRS_MASTER_PRODUCT_PARAM',sysdate
        );
        
        commit;
     
END;