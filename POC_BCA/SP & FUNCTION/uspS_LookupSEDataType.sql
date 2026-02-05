CREATE OR REPLACE PROCEDURE  uspS_LookupSEDataType
(
  v_TABLE_NAME IN VARCHAR2 DEFAULT NULL ,
  v_COLUMN_NAME IN VARCHAR2 DEFAULT NULL,
  Cur_out OUT SYS_REFCURSOR
)
AS
BEGIN


  -- BEGIN
      OPEN  Cur_out FOR
         SELECT UPPER(DATA_TYPE) DATA_TYPE
           FROM user_tab_cols
           WHERE TABLE_NAME = v_TABLE_NAME
                   AND COLUMN_NAME = v_COLUMN_NAME ;

   --END;
   /*OPEN  Cur_out FOR
      SELECT pd_segment
        FROM IFRS_MASTER_ACCOUNT  ;
        */

END;