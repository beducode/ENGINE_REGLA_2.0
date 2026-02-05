CREATE OR REPLACE PROCEDURE RECREATE_VW_IFRS_REPORT_BIAYA_CKPN_SUMM
AS
    v_pivot_clause          CLOB;
    v_select_clause         CLOB;
    v_sql_statement         CLOB;
    v_difference_count      number;
    v_prevdate              date;
    v_process_name CONSTANT VARCHAR2(100) := 'RECREATE_VW_IFRS_REPORT_BIAYA_CKPN_SUMM'; -- Nama proses untuk logging

BEGIN

    SELECT add_months(currdate, -1) INTO v_prevdate FROM IFRS_PRC_DATE;

    -- Hitung total perbedaan menggunakan FULL OUTER JOIN
    WITH
    MasterSegments AS (
        SELECT DISTINCT SEGMENT AS segment_name FROM IFRS_MSTR_SEGMENT_RULES_HEADER WHERE SEGMENT_TYPE = 'REP_SEG'
    ),
    LastMonthDataSegments AS (
        SELECT DISTINCT REPORT_SEGMENT AS segment_name FROM IFRS_REPORT_BIAYA_CKPN_SUMM WHERE REPORT_DATE = v_prevdate
    )
    SELECT COUNT(*)
    INTO v_difference_count
    FROM MasterSegments m
    FULL OUTER JOIN LastMonthDataSegments d ON m.segment_name = d.segment_name
    WHERE m.segment_name IS NULL OR d.segment_name IS NULL; -- Filter ini adalah kuncinya


    if v_difference_count != 0
    then
        -- Log #1: Menandakan proses dimulai
        write_log('INFO', v_process_name, 'Proses pembuatan view dinamis dimulai.');

        -- Langkah 1: Membuat klausa PIVOT dan SELECT secara dinamis
        SELECT LISTAGG(
                       '''' || SEGMENT || ''' AS "' ||
                       REPLACE(REPLACE(REPLACE(SEGMENT, ' - ', '_'), ' & ', '_DAN_'), ' ', '_') || '"',
                       ','
               ) WITHIN GROUP (ORDER BY PKID),

               LISTAGG(
                       'nvl("' || REPLACE(REPLACE(REPLACE(SEGMENT, ' - ', '_'), ' & ', '_DAN_'), ' ', '_') ||
                       '", 0) AS "' || REPLACE(REPLACE(REPLACE(SEGMENT, ' - ', '_'), ' & ', '_DAN_'), ' ', '_') || '"',
                       ','
               ) WITHIN GROUP (ORDER BY PKID)
        INTO v_pivot_clause, v_select_clause
        FROM IFRS_MSTR_SEGMENT_RULES_HEADER
        where SEGMENT_TYPE = 'REP_SEG';

        -- Validasi jika tidak ada segmen ditemukan
        IF v_pivot_clause IS NULL THEN
            RAISE_APPLICATION_ERROR(-20001, 'Tidak ada data segmen yang ditemukan di tabel IFRS_MASTER_SEGMENT.');
        END IF;

        write_log('DEBUG', v_process_name, 'Klausa PIVOT dan SELECT berhasil dibuat dari IFRS_MASTER_SEGMENT.');

        -- Langkah 2: Membangun seluruh statement CREATE VIEW
        v_sql_statement := '
  CREATE OR REPLACE VIEW VW_IFRS_REPORT_BIAYA_CKPN_SUMM AS
  select
    b.VALUE AS REASON,
    ' || v_select_clause || '
  from (
    select *
    from (
      select REPORT_SEGMENT, REASON, BIAYA_CADANGAN_BULAN_INI
      from IFRS_REPORT_BIAYA_CKPN_SUMM
      where REPORT_DATE = (select currdate from IFRS_PRC_DATE)
    )
    pivot (
      max(BIAYA_CADANGAN_BULAN_INI)
      for REPORT_SEGMENT IN (' || v_pivot_clause || ')
    )
  ) a
  right join IFRS_PRIORITY b on a.REASON = b.VALUE
  where b.TYPE = ''REASON''
  order by b.PRIORITY asc';

        -- Log #2 (Opsional, untuk debug): Menampilkan query yang akan dieksekusi
        -- Karena v_sql_statement adalah CLOB, kita mungkin hanya bisa log sebagian kecil saja
        write_log('DEBUG', v_process_name, 'Generated SQL: ' || SUBSTR(v_sql_statement, 1, 3500));

        -- Langkah 3: Menjalankan statement DDL
        EXECUTE IMMEDIATE v_sql_statement;

        -- Log #3: Menandakan proses berhasil
        write_log('INFO', v_process_name, 'View VW_IFRS_REPORT_BIAYA_CKPN_SUMM berhasil dibuat ulang.');
else
        -- Jika tidak ada segmen baru
        write_log('INFO', v_process_name, 'Tidak ada segmen baru. Pembuatan ulang view dilewati.');

    end if;

EXCEPTION
    WHEN OTHERS THEN
        -- Log #4: Menangkap SEMUA jenis error yang mungkin terjadi
        write_log(
                p_level => 'ERROR',
                p_process => v_process_name,
                p_message => 'Proses gagal. Error: ' || SQLERRM, -- SQLERRM berisi pesan error
                p_error_code => SQLCODE -- SQLCODE berisi kode error
        );
        -- Melemparkan kembali error agar aplikasi/user yang memanggil tahu bahwa prosesnya gagal.
        RAISE;
END ;