CREATE OR REPLACE EDITIONABLE PROCEDURE "PSAK413"."SP_LIFETIME_SEQ" 
AS
--declare 
dtcurr DATE;
dtprev DATE;
BEGIN
	
	dtcurr := '31-JAN-2021'; 
  
    COMMIT;
	 
	WHILE  dtcurr <=   '31-DEC-2025'
	LOOP 
		
		SP_IFRS_LIFETIME_RULE_DATA('S_00000_0000', dtcurr, '0', 'M');
		
		SP_IFRS_LIFETIME_DATA('S_00000_0000', dtcurr, '0', 'M');
		
	--	SP_IFRS_LIFETIME_DETAIL('S_00000_0000', dtcurr, '0', 'M');
		--SP_IFRS_LIFETIME_HEADER('S_00000_0000', dtcurr, '0', 'M');
		
		 
	dbms_output.put_line(dtcurr);
	UPDATE PSAK413.X_IFRS_PRCDATE  SET CURRDATE  = dtcurr ; COMMIT ;
	dtcurr := LAST_DAY(ADD_MONTHS(dtcurr, 1));

END LOOP;

END