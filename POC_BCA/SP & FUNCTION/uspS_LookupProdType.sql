CREATE OR REPLACE PROCEDURE  uspS_LookupProdType (
   v_blank      IN     NUMBER DEFAULT NULL,
   Cur_out      OUT SYS_REFCURSOR)
AS
   v_query   VARCHAR2 (2000);
BEGIN
   IF (v_blank = 1)
   THEN
      BEGIN
         OPEN Cur_out FOR
            SELECT '  ALL' AS "PRD_TYPE", 'ALL product Type' AS "Product Type" FROM DUAL
            UNION
            SELECT DISTINCT PRD_TYPE, (PRD_TYPE || ' - ' || PRD_DESC) AS "Product Type" FROM IFRS_MASTER_PRODUCT_PARAM ORDER BY PRD_TYPE ASC;
      END;
   ELSE
      BEGIN
         OPEN Cur_out FOR
            SELECT DISTINCT PRD_TYPE as "PRD_TYPE", (PRD_TYPE || ' - ' || PRD_DESC) AS "Product Type" FROM IFRS_MASTER_PRODUCT_PARAM ORDER BY PRD_TYPE ASC;
      END;
   END IF;
END;