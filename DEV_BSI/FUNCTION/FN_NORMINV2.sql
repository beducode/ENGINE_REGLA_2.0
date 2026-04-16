CREATE OR REPLACE FUNCTION PSAK413.FN_NORMINV2(p IN NUMBER) RETURN NUMBER IS
    -- Koefisien aproksimasi Beasley-Springer-Moro
    a1 CONSTANT NUMBER := -39.6968302866538;
    a2 CONSTANT NUMBER := 220.946098424521;
    a3 CONSTANT NUMBER := -275.928510446969;
    a4 CONSTANT NUMBER := 138.357751867269;
    a5 CONSTANT NUMBER := -30.6647980661472;
    a6 CONSTANT NUMBER := 2.50662827745924;

    b1 CONSTANT NUMBER := -54.4760987982241;
    b2 CONSTANT NUMBER := 161.585836858041;
    b3 CONSTANT NUMBER := -155.698979859887;
    b4 CONSTANT NUMBER := 66.8013118877197;
    b5 CONSTANT NUMBER := -13.2806815528857;

    c1 CONSTANT NUMBER := -0.00778489400243029;
    c2 CONSTANT NUMBER := -0.322396458041136;
    c3 CONSTANT NUMBER := -2.40075827716184;
    c4 CONSTANT NUMBER := -2.54973253934373;
    c5 CONSTANT NUMBER := 4.37466414146497;
    c6 CONSTANT NUMBER := 2.93816398269878;

    d1 CONSTANT NUMBER := 0.00778469570904146;
    d2 CONSTANT NUMBER := 0.32246712907004;
    d3 CONSTANT NUMBER := 2.445134137143;
    d4 CONSTANT NUMBER := 3.75440866190742;

    q NUMBER; r NUMBER; result NUMBER;
BEGIN
    IF p <= 0 OR p >= 1 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Input must be between 0 and 1');
    END IF;

    -- Aproksimasi awal
    IF p < 0.02425 THEN
        q := SQRT(-2 * LN(p));
        result := (((((c1*q+c2)*q+c3)*q+c4)*q+c5)*q+c6) /
                  (((d1*q+d2)*q+d3)*q+d4);
    ELSIF p > 1 - 0.02425 THEN
        q := SQRT(-2 * LN(1-p));
        result := -(((((c1*q+c2)*q+c3)*q+c4)*q+c5)*q+c6) /
                   (((d1*q+d2)*q+d3)*q+d4);
    ELSE
        q := p - 0.5;
        r := q*q;
        result := (((((a1*r+a2)*r+a3)*r+a4)*r+a5)*r+a6)*q /
                  (((((b1*r+b2)*r+b3)*r+b4)*r+b5)*r+1);
    END IF;

    RETURN result;
END;