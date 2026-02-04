CREATE OR REPLACE PROCEDURE  uspS_LookupCurrency (
   v_blank   IN     NUMBER DEFAULT NULL,
   Cur_out      OUT SYS_REFCURSOR)
AS
   v_query   VARCHAR2 (2000);
BEGIN
   IF (v_blank = 1)
   THEN
      BEGIN
         OPEN Cur_out FOR
            SELECT 'ALL' AS "CCY", 'All Currency' AS "Currency" FROM DUAL
            UNION
            SELECT CCY AS "CCY", (CCY || ' - ' || CCY_DESC) AS "Currency"
              FROM tblM_Currency;
      END;
   ELSE
      BEGIN
         OPEN Cur_out FOR
            SELECT CCY AS "CCY", (CCY || ' - ' || CCY_DESC) AS "Currency"
              FROM tblM_Currency;
      END;
   END IF;
END;