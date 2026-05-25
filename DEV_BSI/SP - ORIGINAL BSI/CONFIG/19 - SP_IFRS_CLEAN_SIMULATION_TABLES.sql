CREATE OR REPLACE PROCEDURE PSAK413.SP_IFRS_CLEAN_SIMULATION_TABLES (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_SYSCODE		IN VARCHAR2 DEFAULT '0',
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AS

    ALTER SESSION SET recyclebin = OFF;
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_BUCKET_DETAIL';
    TYPE T_TABLES IS TABLE OF VARCHAR2(300);
    TYPE T_RUNIDS IS TABLE OF VARCHAR2(300);

    V_TABLES T_TABLES;
    V_RUNIDS T_RUNIDS;
BEGIN

    /*
        AMBIL RUN_ID YANG AKAN DIHAPUS (SELAIN 5 RUN_ID TERBARU)
    */
    SELECT DISTINCT run_id
    BULK COLLECT INTO v_runids
    FROM (
        SELECT run_id,
               DENSE_RANK() OVER (
                   ORDER BY process_date DESC
               ) rn
        FROM ifrs_logs_process
    )
    WHERE rn > 5;

    DBMS_OUTPUT.PUT_LINE('TOTAL RUN_ID TO CLEAN : ' || v_runids.COUNT);

    /*
        AMBIL TABLE YANG MATCH RUN_ID
    */
    SELECT ut.table_name
    BULK COLLECT INTO v_tables
    FROM user_tables ut
    WHERE EXISTS (
        SELECT 1
        FROM (
            SELECT DISTINCT REPLACE(run_id, '/', '_') run_id
            FROM (
                SELECT run_id,
                       DENSE_RANK() OVER (
                           ORDER BY process_date DESC
                       ) rn
                FROM ifrs_logs_process
            )
            WHERE rn > 5
        ) r
        WHERE ut.table_name LIKE '%' || r.run_id
    );

    DBMS_OUTPUT.PUT_LINE('TOTAL TABLE DROP : ' || v_tables.COUNT);

    /*
        DROP TABLE
    */
    FOR i IN 1 .. v_tables.COUNT
    LOOP
        BEGIN
            EXECUTE IMMEDIATE 'DROP TABLE ' || v_tables(i) || ' PURGE';

            DBMS_OUTPUT.PUT_LINE('DROP SUCCESS : ' || v_tables(i));

        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('DROP FAILED : ' || v_tables(i) || ' -> ' || SQLERRM);
        END;
    END LOOP;

    /*
        DELETE IFRS_LOGS_PROCESS
    */
    FOR i IN 1 .. v_runids.COUNT
    LOOP
        BEGIN
            DELETE FROM ifrs_logs_process
            WHERE run_id = v_runids(i);

            DBMS_OUTPUT.PUT_LINE('DELETE LOG SUCCESS : ' || v_runids(i));

        EXCEPTION
            WHEN OTHERS THEN
                DBMS_OUTPUT.PUT_LINE('DELETE LOG FAILED : ' || v_runids(i) || ' -> ' || SQLERRM);
        END;
    END LOOP;

    COMMIT;

    DBMS_OUTPUT.PUT_LINE('PROCEDURE ' || V_SP_NAME || ' EXECUTED SUCCESSFULLY.');


EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;

        RAISE_APPLICATION_ERROR(-20001,'ERROR IN ' || V_SP_NAME || ' : ' || SQLERRM);
END;
