CREATE OR REPLACE PROCEDURE USPS_GETDROPDOWNLIST
(
  V_TABLE_NAME varchar2 default '',
  V_COLUMN_NAME varchar2 default '',
   Cur_out OUT SYS_REFCURSOR
)
AS
BEGIN

    if V_COLUMN_NAME = 'ASSET_TYPE_PRODUCT1' THEN
             OPEN Cur_out FOR
            select VALUE1 CODE from TBLM_COMMONCODEDETAIL where COMMONCODE = 'B77' and VALUE1 is not null order by VALUE1 asc;

    elsif V_COLUMN_NAME = 'ASSET_TYPE_PRODUCT2' THEN

             OPEN Cur_out FOR
            select VALUE1 CODE from TBLM_COMMONCODEDETAIL where PARENTCOMMONCODE = 'B77' and VALUE1 is not null order by VALUE1 asc;

    else

        --set @TABLE_NAME = 'IFRS_MASTER_PRODUCT_PARAMETER'
             OPEN Cur_out FOR
                SELECT
                    VALUE CODE
                FROM TBLM_DROPDOWNLIST
                WHERE TABLE_NAME = V_TABLE_NAME
                AND COLUMN_NAME = V_COLUMN_NAME
                AND VALUE IS NOT NULL;
      END IF;

END;