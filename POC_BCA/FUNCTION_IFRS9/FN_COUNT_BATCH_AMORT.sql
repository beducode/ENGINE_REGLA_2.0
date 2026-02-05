CREATE OR REPLACE FUNCTION FN_COUNT_BATCH_AMORT (p_PROCDATE IN date)
return NUMBER
AS
 v_CURRDATE date;
 v_COUNT number(3);
BEGIN

    IF(p_PROCDATE IS NULL)
    THEN
        SELECT CURRDATE INTO v_CURRDATE FROM ifrs_prc_date;
    ELSE
        v_CURRDATE := p_PROCDATE;
    END IF;

    v_COUNT := 0;

    LOOP
      v_COUNT := v_COUNT + 1;
      v_CURRDATE := v_CURRDATE + 1;

      IF((FN_HOLIDAY (v_CURRDATE) = 0 AND FN_HOLIDAY (v_CURRDATE + 1) = 0) AND v_COUNT <> 1)
      THEN
        v_COUNT := v_COUNT - 1;
      END IF;

      --jika tanggal 1 awal bulan libur
      IF(EXTRACT(day FROM v_CURRDATE) = 1 AND FN_HOLIDAY (v_CURRDATE) = 1)
      THEN
        v_COUNT := v_COUNT + 1;
      END IF;

      --hari kejepit tengah minggu
      IF(FN_HOLIDAY (v_CURRDATE - 1) = 1 AND FN_HOLIDAY (v_CURRDATE) = 0 AND FN_HOLIDAY (v_CURRDATE + 1) = 1 AND v_COUNT <> 1)
      THEN
        v_COUNT := v_COUNT - 1;
      END IF;

      EXIT WHEN (FN_HOLIDAY (v_CURRDATE) = 0 AND FN_HOLIDAY (v_CURRDATE + 1) = 0)
                    OR (EXTRACT(month FROM v_CURRDATE + 1) <> EXTRACT(month FROM v_CURRDATE) --kondisi akhir bulan libur
                    OR (FN_HOLIDAY (v_CURRDATE - 1) = 1 AND FN_HOLIDAY (v_CURRDATE) = 0 AND FN_HOLIDAY (v_CURRDATE + 1) = 1 AND v_COUNT <> 1));
   END LOOP;

RETURN v_COUNT;

END;