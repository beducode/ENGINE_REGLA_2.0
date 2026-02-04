CREATE OR REPLACE PROCEDURE  USPS_LOOKUPPRODCODE
(
   v_blank      IN     NUMBER DEFAULT NULL,
   Cur_out      OUT SYS_REFCURSOR
)
AS
   v_query   VARCHAR2 (2000);
BEGIN
   IF (v_blank = 1)
   THEN
      BEGIN
         OPEN Cur_out FOR
            SELECT ' ALL' AS "PRD_CODE", 'ALL product code' AS "Product Code" FROM DUAL
            UNION
            SELECT DISTINCT PRD_CODE, (PRD_CODE || ' - ' || PRD_DESC) AS "Product Code" FROM IFRS_MASTER_PRODUCT_PARAM;
      END;
   ELSE
      BEGIN
         OPEN Cur_out FOR
            SELECT DISTINCT PRD_CODE as "PRD_CODE", (PRD_CODE || ' - ' || PRD_DESC) AS "Product Code" FROM IFRS_MASTER_PRODUCT_PARAM ORDER BY PRD_CODE ASC;
      END;
   END IF;
END;