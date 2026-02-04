CREATE OR REPLACE FUNCTION FN_NEXT_WORKING_DAY (p_PRCDATE IN date)
return date
AS
    v_date date;
BEGIN
    v_date := p_PRCDATE + 1;

    while (FN_HOLIDAY(v_date) = 1) loop
        v_date := v_date + 1;
    end loop;


    RETURN v_date;


END;