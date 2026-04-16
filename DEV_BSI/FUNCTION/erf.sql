CREATE OR REPLACE FUNCTION PSAK413.erf(x NUMBER) RETURN NUMBER IS
    -- konstanta aproksimasi
    t NUMBER; tau NUMBER;
BEGIN
    t := 1.0 / (1.0 + 0.5 * ABS(x));
    tau := t * EXP(-x*x - 1.26551223 +
                   t*(1.00002368 +
                   t*(0.37409196 +
                   t*(0.09678418 +
                   t*(-0.18628806 +
                   t*(0.27886807 +
                   t*(-1.13520398 +
                   t*(1.48851587 +
                   t*(-0.82215223 +
                   t*0.17087277)))))))));
    IF x >= 0 THEN
        RETURN 1 - tau;
    ELSE
        RETURN tau - 1;
    END IF;
END;