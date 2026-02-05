CREATE OR REPLACE PROCEDURE IFRS_RUN_ENGINE2
AS
   V_CURRDATE   DATE;
   V_PREVDATE   DATE;
BEGIN

    EXECUTE IMMEDIATE 'alter session set temp_undo_enabled=true';
    EXECUTE IMMEDIATE 'alter session enable parallel dml';

    V_CURRDATE := '31-jan-2012';
    V_PREVDATE := '31-JAN-2011';

    WHILE V_CURRDATE <= '30-jun-2024' loop

             UPDATE IFRS_PRC_DATE_TEST
             SET CURRDATE = V_CURRDATE, PREVDATE = V_PREVDATE;COMMIT;

    SP_IFRS_CCF_OROS(V_CURRDATE);
    SP_IFRS_CCF_6(V_CURRDATE);

             V_CURRDATE := ADD_MONTHS(V_CURRDATE,1);
             SELECT ADD_MONTHS (V_CURRDATE, -12) INTO V_PREVDATE FROM DUAL;

             end loop;

             END;