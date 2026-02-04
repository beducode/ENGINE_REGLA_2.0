CREATE OR REPLACE PROCEDURE uspS_LookupRetentionField (
   v_source    IN     VARCHAR2 DEFAULT ' ',
   v_catalog   IN     VARCHAR2 DEFAULT ' ',
   v_schema    IN     VARCHAR2 DEFAULT ' ',
   Cur_out        OUT SYS_REFCURSOR)
AS
   --v_owner   VARCHAR2 (50);
BEGIN
  --v_owner := v_schema || '_' || v_catalog;

   OPEN Cur_out FOR
      SELECT COLUMN_NAME AS "Field", COLUMN_NAME AS "SPECIFIC_FIELD"
        FROM all_tab_cols
       WHERE                                   --TABLE_CATALOG = v_catalog AND
            --OWNER = v_schema AND-- v_owner AND
            TABLE_NAME = v_source;

END;