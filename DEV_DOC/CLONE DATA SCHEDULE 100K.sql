DO $$
DECLARE
    duplicate_count INT := 20000;
BEGIN
    WITH numbers AS (
        SELECT ROW_NUMBER() OVER () AS n
        FROM generate_series(1, 10) a1,
             generate_series(1, 10) a2,
             generate_series(1, 10) a3,
             generate_series(1, 10) a4,
             generate_series(1, 2)  a5   -- 10⁴ * 2 = 20,000
    )
    INSERT INTO ifrs_paym_schd_all_100k (
        DOWNLOAD_DATE,
        MASTERID,
        PMTDATE,
        INTEREST_RATE,
        OSPRN,
        PRINCIPAL,
        INTEREST,
        DISB_PERCENTAGE,
        DISB_AMOUNT,
        PLAFOND,
        I_DAYS,
        COUNTER,
        ICC,
        OUTSTANDING,
        SOURCE_PROCESS,
        SCH_FLAG,
        GRACE_DATE,
        STATUS,
        END_DATE,
        CREATED_DATE
    )
    SELECT
        src.DOWNLOAD_DATE,
        src.MASTERID || '_' || n.n AS MASTERID,
        src.PMTDATE,
        src.INTEREST_RATE,
        src.OSPRN,
        src.PRINCIPAL,
        src.INTEREST,
        src.DISB_PERCENTAGE,
        src.DISB_AMOUNT,
        src.PLAFOND,
        src.I_DAYS,
        src.COUNTER,  -- tetap dari source
        src.ICC,
        src.OUTSTANDING,
        src.SOURCE_PROCESS,
        src.SCH_FLAG,
        src.GRACE_DATE,
        src.STATUS,
        src.END_DATE,
        src.CREATED_DATE
    FROM IFRS_PAYM_SCHD_ALL src
    CROSS JOIN numbers n
    WHERE n.n <= duplicate_count
    ORDER BY src.PMTDATE, n.n;
END $$;
