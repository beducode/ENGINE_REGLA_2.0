CREATE OR REPLACE EDITIONABLE PROCEDURE "PSAK413"."SP_IFRS_LOOPING_ADAM" 
IS
    v_loop INTEGER := 0;
BEGIN
    WHILE v_loop < 71 LOOP
--	WHILE v_loop < 1 LOOP
        -- Panggil prosedur lain
        SP_IFRS_MASTERID;

        -- Update tanggal
        UPDATE IFRS_PRC_DATE
        SET CURRDATE = LAST_DAY(ADD_MONTHS(CURRDATE, 1)),
            PREVDATE = LAST_DAY(ADD_MONTHS(PREVDATE, 1));

        -- Increment counter
        v_loop := v_loop + 1;
    END LOOP;
END