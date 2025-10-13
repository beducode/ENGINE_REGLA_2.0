CREATE OR REPLACE FUNCTION F_EOMONTH(
    P_DOWNLOAD_DATE DATE, 
    P_COUNT INT,
    P_OPR VARCHAR(1),
    P_LEVEL VARCHAR(4)
)
RETURNS DATE
LANGUAGE plpgsql
AS $$
DECLARE
    V_DATERETURN DATE;
    v_interval TEXT;
BEGIN
    -- Tentukan arah interval (PREV = minus, NEXT = plus)
    IF P_LEVEL = 'PREV' THEN
        v_interval := '-' || P_COUNT;
    ELSE
        v_interval := '+' || P_COUNT;
    END IF;

    -- Logika utama
    IF P_OPR = 'M' THEN
        -- Bulanan: EOMONTH setelah/before X bulan
        V_DATERETURN := (
            DATE_TRUNC('MONTH', (DATE_TRUNC('MONTH', P_DOWNLOAD_DATE) + (v_interval || ' MONTH')::INTERVAL))
            + INTERVAL '1 MONTH - 1 day'
        )::DATE;

    ELSIF P_OPR = 'D' THEN
        -- Harian: tambah/kurang bulan langsung
        V_DATERETURN := (P_DOWNLOAD_DATE + (v_interval || ' MONTH')::INTERVAL)::DATE;

    ELSE
        -- Tahunan: EOMONTH setelah/before X tahun
        V_DATERETURN := (
            DATE_TRUNC('MONTH', (DATE_TRUNC('MONTH', P_DOWNLOAD_DATE) + (v_interval || ' YEAR')::INTERVAL))
            + INTERVAL '1 MONTH - 1 day'
        )::DATE;
    END IF;

    RETURN V_DATERETURN;
END;
$$;
