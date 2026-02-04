CREATE OR REPLACE FUNCTION FN_ISDATE(v_date IN VARCHAR2,v_format IN VARCHAR2) RETURN NUMBER IS
    v_date1 DATE;
BEGIN
    select to_date(v_date,v_format) into v_date1 from dual;
        RETURN 1;
    Exception WHEN Others THEN
        RETURN 0;
END;