CREATE OR REPLACE PROCEDURE PSAK413.sp_testing_oracle (
    p_runid         IN VARCHAR2 DEFAULT 'S_00000_0000',
    p_download_date IN DATE     DEFAULT NULL,
    p_prc           IN VARCHAR2 DEFAULT 'S',
    p_model_id      IN NUMBER   DEFAULT 0
)
IS
BEGIN
    DBMS_OUTPUT.PUT_LINE('Stored Procedure testing berhasil dijalankan!');
    DBMS_OUTPUT.PUT_LINE('p_runid         = ' || p_runid);
    DBMS_OUTPUT.PUT_LINE('p_download_date = ' || p_download_date);
    DBMS_OUTPUT.PUT_LINE('p_prc           = ' || p_prc);
    DBMS_OUTPUT.PUT_LINE('p_model_id      = ' || p_model_id);
END;
/
