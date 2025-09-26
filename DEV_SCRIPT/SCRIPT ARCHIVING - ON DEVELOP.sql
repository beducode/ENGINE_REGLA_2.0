SELECT COUNT(*) FROM ifrs_master_account WHERE (download_date = '2024-12-30') --- 15782

SELECT COUNT(*) FROM ifrs_master_account WHERE (download_date <> '2024-12-30') --- 31403

SELECT 15782+31403 --- 47185

SELECT COUNT(*) FROM ifrs_master_account --- 15782

SELECT COUNT(*) FROM ifrs_master_account_dev --- 47185

-- SELECT * INTO ifrs_master_account_dev FROM ifrs_master_account

SELECT download_date, count(*) from ifrs_master_account_dev GROUP BY download_date


SELECT * FROM information_schema.tables WHERE table_name LIKE '%ifrs_master_account%'


SELECT 
    indexdef || ';' AS create_index_script
FROM 
    pg_indexes
WHERE 
    schemaname = 'public'
    AND tablename = 'ifrs_master_account';
	
-- ALTER TABLE ifrs_master_account_dev RENAME TO ifrs_master_account;

-- DROP TABLE ifrs_master_account

/*

CREATE INDEX "pk_ifrs_master_accounterEBUAvIMADEV" 
ON public.ifrs_master_account 
USING btree (download_date, masterid, data_source, customer_number, product_code, group_segment, segment, sub_segment);

CREATE INDEX "nonclusteredindexima-20181022-141440IMADEV" 
ON public.ifrs_master_account 
USING btree (download_date, masterid, master_account_code, product_code, account_number, data_source, group_segment, segment, sub_segment);

*/