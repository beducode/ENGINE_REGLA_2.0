CREATE OR REPLACE PROCEDURE USPS_DATASOURCE
(
    V_dataSourceid VARCHAR2 DEFAULT ' ',
    Cur_out OUT SYS_REFCURSOR
)
AS
BEGIN
OPEN Cur_out FOR
SELECT * FROM tblS_DataSource nolock where DataSourceID = V_dataSourceid ;

END;