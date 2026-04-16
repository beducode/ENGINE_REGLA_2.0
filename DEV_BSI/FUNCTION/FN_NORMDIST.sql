CREATE OR REPLACE FUNCTION PSAK413.FN_NORMDIST(
    x          IN NUMBER,
    mean       IN NUMBER DEFAULT 0 ,
    stdev      IN NUMBER DEFAULT 1,
    cumulative IN NUMBER DEFAULT 1  
) RETURN NUMBER IS
    p CONSTANT NUMBER := 0.3275911;
    a1 CONSTANT NUMBER := 0.254829592;
    a2 CONSTANT NUMBER := -0.284496736;
    a3 CONSTANT NUMBER := 1.421413741;
    a4 CONSTANT NUMBER := -1.453152027;
    a5 CONSTANT NUMBER := 1.061405429;

    z NUMBER;  
    t NUMBER;  
    erf_approx NUMBER;
BEGIN
    IF cumulative = 1 THEN
        -- CDF calculation
        z := (x - mean) / (stdev * SQRT(2));
        t := 1 / (1 + p * ABS(z));
        erf_approx := 1 - (((((a5*t + a4)*t + a3)*t + a2)*t + a1)*t) * EXP(-z*z);

        IF z < 0 THEN
            erf_approx := -erf_approx;
        END IF;

        RETURN 0.5 * (1 + erf_approx);
    ELSE
        -- PDF calculation
        RETURN (1 / (stdev * SQRT(2 * ACOS(-1)))) *
               EXP(-POWER(x - mean, 2) / (2 * POWER(stdev, 2)));
    END IF;
END;