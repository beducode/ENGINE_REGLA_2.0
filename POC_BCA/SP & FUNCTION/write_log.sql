CREATE OR REPLACE PROCEDURE write_log (
    p_level       IN VARCHAR2,
    p_process     IN VARCHAR2,
    p_message     IN VARCHAR2,
    p_error_code  IN NUMBER DEFAULT NULL
) AS
    PRAGMA AUTONOMOUS_TRANSACTION; -- Sangat Penting!
BEGIN
    INSERT INTO IFRS.app_log (
        log_level,
        process_name,
        log_message,
        error_code,
        error_backtrace -- Hanya untuk error
    ) VALUES (
        p_level,
        p_process,
        p_message,
        p_error_code,
        CASE WHEN p_level = 'ERROR' THEN DBMS_UTILITY.FORMAT_ERROR_BACKTRACE ELSE NULL END
    );
    COMMIT; -- Karena ini transaksi otonom
END;