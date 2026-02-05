CREATE OR REPLACE PROCEDURE uspS_QueryDataByKey
(
  v_CountryID IN VARCHAR2 DEFAULT NULL ,
  v_ModuleID IN VARCHAR2 DEFAULT NULL ,
  v_QueryID IN VARCHAR2 DEFAULT NULL,
  Cur_out OUT SYS_REFCURSOR
)
AS
BEGIN

   OPEN  Cur_out FOR
      SELECT CountryID ,
             ModuleID ,
             QueryID ,
             PKID ,
             ObjectName ,
             SelectQuery ,
             CreatedBy ,
             CreatedDate ,
             CreatedHost ,
             UpdatedBy ,
             UpdatedDate ,
             UpdatedHost
        FROM tblM_QueryData
        WHERE ( CountryID = v_CountryID
                OR v_CountryID IS NULL )
                AND ( ModuleID = v_ModuleID
                OR v_ModuleID IS NULL )
                AND ( QueryID = v_QueryID
                OR v_QueryID IS NULL ) ;

END;