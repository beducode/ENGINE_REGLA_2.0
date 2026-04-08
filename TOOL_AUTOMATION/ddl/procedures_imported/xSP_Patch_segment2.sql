CREATE OR REPLACE EDITIONABLE PROCEDURE "PSAK413"."xSP_Patch_segment2" 
AS 
 
    
BEGIN
    
DECLARE
  v_start_date DATE := DATE '2020-01-31';  
  v_end_date   DATE := DATE '2025-12-31';  
 
BEGIN 

  WHILE v_start_date <= v_end_date LOOP
	  
	
UPDATE IFRS_MASTER_ACCOUNT_MONTHLY  a
SET (a.sub_segment, a.segment, a.group_segment) =
    (SELECT x.sub_segment, x.segment, x.group_segment
     FROM TMP_IMA_SEGMENT x
     WHERE a.masterid = x.masterid)
WHERE a.download_date = v_start_date
AND A.PRODUCT_TYPE  IN ('QARDH','RAHN','MURABAHAH')
AND A.ACCOUNT_STATUS  = 'A'
AND EXISTS (
SELECT 1 
FROM TMP_IMA_SEGMENT Y 
WHERE A.MASTERID  = Y.MASTERID
AND Y.SUB_SEGMENT IS NOT NULL 
AND (A.GROUP_SEGMENT <> Y.GROUP_SEGMENT OR A.SEGMENT <> Y.SEGMENT OR A.SUB_SEGMENT <> Y.SUB_SEGMENT)
);

COMMIT ; 
    -- tambah 1 bulan
DBMS_OUTPUT.PUT_LINE('Tanggal: ' || TO_CHAR(v_start_date, 'YYYY-MM-DD'));
    v_start_date := LAST_DAY(ADD_MONTHS(v_start_date, 1));
  END LOOP;
     
END