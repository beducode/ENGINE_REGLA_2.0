CREATE OR REPLACE Function FN_MAX_HARI_KERJA(p_TRXDATE IN date) return DATE AS
  DtTanggal VARCHAR(12);
  --DtTanggal DATE;
  DtLast    Date;
BEGIN
  DtTanggal := '01'||TO_CHAR(TO_DATE(p_TRXDATE),'-MON-YYYY');


select MAX(tmp_tanggal) INTO DtLast from (
select to_date(DtTanggal,'dd-mm-YYYY')+level-1 tmp_tanggal
 from dual
 connect by level <= TO_CHAR(LAST_DAY(to_date(DtTanggal,'dd-mm-YYYY')),'DD')) BULAN
 where to_char(TO_DATE(tmp_tanggal), 'd') not in ('1','7')
 AND NOT EXISTS (SELECT 1 FROM IFRS_HOLIDAY HOLI
 WHERE BULAN.tmp_tanggal = HOLI.HOLIDAY_DATE);


  RETURN DtLast;
END;