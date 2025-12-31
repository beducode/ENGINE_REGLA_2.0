CREATE OR REPLACE FUNCTION F_EOMONTH (
    P_DOWNLOAD_DATE IN DATE,
    P_COUNT         IN NUMBER,
    P_OPR           IN VARCHAR2, -- 'M', 'D', selain itu = YEAR
    P_LEVEL         IN VARCHAR2  -- 'PREV' atau 'NEXT'
)
RETURN DATE
IS
    V_DATERETURN DATE;
    V_SIGN       NUMBER := 1;
BEGIN
    -- Tentukan arah interval
    IF UPPER(P_LEVEL) = 'PREV' THEN
        V_SIGN := -1;
    ELSE
        V_SIGN := 1;
    END IF;

    -- =========================
    -- LOGIC UTAMA
    -- =========================
    IF P_OPR = 'M' THEN
        -- Bulanan: EOMONTH +/- X bulan
        V_DATERETURN :=
            LAST_DAY(
                ADD_MONTHS(
                    TRUNC(P_DOWNLOAD_DATE, 'MM'),
                    V_SIGN * P_COUNT
                )
            );

    ELSIF P_OPR = 'D' THEN
        -- Harian: langsung tambah/kurang bulan (TIDAK EOMONTH)
        V_DATERETURN :=
            ADD_MONTHS(
                P_DOWNLOAD_DATE,
                V_SIGN * P_COUNT
            );

    ELSE
        -- Tahunan: EOMONTH +/- X tahun
        V_DATERETURN :=
            LAST_DAY(
                ADD_MONTHS(
                    TRUNC(P_DOWNLOAD_DATE, 'MM'),
                    V_SIGN * P_COUNT * 12
                )
            );
    END IF;

    RETURN V_DATERETURN;
END F_EOMONTH;
/
