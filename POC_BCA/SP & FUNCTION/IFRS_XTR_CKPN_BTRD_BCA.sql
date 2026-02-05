CREATE OR REPLACE PROCEDURE IFRS_XTR_CKPN_BTRD_BCA
AS

reporting_date date;
DBS VARCHAR2;

Begin
  reporting_date := '31-JAN-2020';
  SELECT to_date('1900-01-01','yyyy-MM-dd') - to_date(reporting_date,'yyyy-MM-dd') as POSTING_DATE, Extract(Year From reporting_date) into DBS
  FROM dual;
  dbms_output.put_line(DBS);
End;