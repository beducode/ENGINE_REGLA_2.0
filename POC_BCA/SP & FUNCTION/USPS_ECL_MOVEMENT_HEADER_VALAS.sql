CREATE OR REPLACE PROCEDURE USPS_ECL_MOVEMENT_HEADER_VALAS(V_PERIOD_FROM     DATE,
                                                                V_PERIOD_TO       DATE,
                                                                V_SEGMENTS        VARCHAR2,
                                                                V_TYPE            VARCHAR2,
                                                                V_ASETS           VARCHAR2,
                                                                V_TRAS            VARCHAR2,
                                                                CUR_OUT       OUT SYS_REFCURSOR)
  AS
    V_SUM                     VARCHAR2(4000);
    V_TYPE_DESC               VARCHAR2(4000);
    V_TYPE_COLOUMN            VARCHAR2(4000);
    V_QUERY                   VARCHAR2(32000);
    V_IS_PENEMPATAN_BI        NUMBER(1, 0) := 0;
    V_IS_TAGIHAN_REVERSE_REPO NUMBER(1, 0) := 0;
  BEGIN

    IF INSTR(',''' || V_ASETS || ''',', ',' || '''PENEMPATAN PADA BANK INDONESIA''') > 0
    THEN
      V_IS_PENEMPATAN_BI := 1;
    END IF;

    IF INSTR(',''' || V_ASETS || ''',', ',' || '''TAGIHAN REVERSE REPO PADA BANK INDONESIA''') > 0
    THEN
      V_IS_TAGIHAN_REVERSE_REPO := 1;
    END IF;

    V_TYPE_DESC := V_TYPE;

    IF (V_TYPE_DESC = 'ECL_ON_BS')
    THEN
      V_SUM := ', SUM(OUTSTANDING_ON_BS) AS OS_ON ';
      V_TYPE_COLOUMN := 'OUTSTANDING_ON_BS, ';

    ELSIF (V_TYPE_DESC = 'ECL_OFF_BS')
    THEN
      V_SUM := ', SUM(OUTSTANDING_OFF_BS) AS OS_OFF ';
      V_TYPE_COLOUMN := 'OUTSTANDING_OFF_BS, ';

    ELSIF (V_TYPE_DESC = 'ECL_TOTAL')
    THEN
      V_SUM := ', SUM(OUTSTANDING_ON_BS) AS OS_ON,  SUM(OUTSTANDING_OFF_BS) AS OS_OFF ';
      V_TYPE_COLOUMN := 'OUTSTANDING_ON_BS, OUTSTANDING_OFF_BS, ';

    ELSIF (V_TYPE_DESC = 'NILAI_TERCATAT_ON_BS')
    THEN
      V_SUM := ', SUM(ECL_ON_BS) AS ECL_ON_BS ';
      V_TYPE_COLOUMN := 'ECL_ON_BS, ';

    ELSE
      V_SUM := '';
    END IF;

    -- ada TRA aja
    IF (V_TRAS != '''NONE'''
      AND V_ASETS = '''NONE''')
    THEN
      V_QUERY := 'SELECT  *
                FROM(
                SELECT /*+ PARALLEL(auto) */ SEQ_NO, IMP_CHANGE_REASON, ' || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
            V_TRAS = '''KELONGGARAN TARIK''') THEN 'ECL_OFF_BS' ELSE V_TYPE_DESC END || ', '
      || V_TYPE_COLOUMN || ' STAGE
                FROM IFRS_REPORT_ECL_MOVE_VALAS_DTL
                WHERE SEQ_NO NOT IN (0, 99) '
      || CASE WHEN (V_TYPE_DESC = 'ECL_ON_BS' OR
            V_TYPE_DESC = 'NILAI_TERCATAT_ON_BS') THEN 'AND SEQ_NO = 999999' ELSE '' END || --supaya gak muncul data
      ' AND REPORT_DATE BETWEEN ''' || TO_CHAR(V_PERIOD_FROM) || ''' AND ''' || TO_CHAR(V_PERIOD_TO) || ''' ' || CASE WHEN V_TRAS = 'ALL' THEN '' ELSE 'AND TRA IN (' || V_TRAS || ') ' END || CASE WHEN V_SEGMENTS = 'ALL' THEN '' ELSE ' AND SUB_SEGMENT IN (' || V_SEGMENTS || ')' END || 'UNION ALL
                SELECT /*+ PARALLEL(auto) */ SEQ_NO, IMP_CHANGE_REASON, ' || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
            V_TRAS = '''KELONGGARAN TARIK''') THEN 'ECL_OFF_BS' ELSE V_TYPE_DESC END || ', '
      || V_TYPE_COLOUMN || ' STAGE
                FROM IFRS_RPT_MOVE_ANALY_AW_AK
                WHERE SEQ_NO = 0 '
      || CASE WHEN (V_TYPE_DESC = 'ECL_ON_BS' OR
            V_TYPE_DESC = 'NILAI_TERCATAT_ON_BS') THEN 'AND SEQ_NO = 999999' ELSE '' END || --supaya gak muncul data
      ' AND REPORT_DATE = ''' || TO_CHAR(V_PERIOD_FROM) || ''' ' || CASE WHEN V_TRAS = 'ALL' THEN '' ELSE 'AND  TRA IN (' || V_TRAS || ') ' END || CASE WHEN V_SEGMENTS = 'ALL' THEN '' ELSE 'AND SUB_SEGMENT IN (' || V_SEGMENTS || ')' END || 'UNION ALL
                SELECT /*+ PARALLEL(auto) */ SEQ_NO, IMP_CHANGE_REASON, ' || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
            V_TRAS = '''KELONGGARAN TARIK''') THEN 'ECL_OFF_BS' ELSE V_TYPE_DESC END || ', '
      || V_TYPE_COLOUMN || ' STAGE
                FROM IFRS_RPT_MOVE_ANALY_AW_AK
                WHERE SEQ_NO = 99 '
      || CASE WHEN (V_TYPE_DESC = 'ECL_ON_BS' OR
            V_TYPE_DESC = 'NILAI_TERCATAT_ON_BS') THEN 'AND SEQ_NO = 999999' ELSE '' END || --supaya gak muncul data
      'AND REPORT_DATE = ''' || TO_CHAR(V_PERIOD_TO) || ''' ' || CASE WHEN V_TRAS = 'ALL' THEN '' ELSE 'AND TRA IN (' || V_TRAS || ') ' END || CASE WHEN V_SEGMENTS = 'ALL' THEN '' ELSE 'AND SUB_SEGMENT IN (' || V_SEGMENTS || ')' END || '
                    )
                PIVOT(
                    SUM(' || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
            V_TRAS = '''KELONGGARAN TARIK''') THEN 'ECL_OFF_BS' ELSE V_TYPE_DESC END || ')' || V_SUM || '
                    FOR (STAGE)
                    IN (''1'' AS "STAGE_1",''2'' AS "STAGE_2",''3'' AS "STAGE_3", ''POCI'' AS "POCI")
                )
                ORDER BY SEQ_NO, IMP_CHANGE_REASON';

    END IF;

    -- ada aset aja
    IF (V_TRAS = '''NONE'''
      AND V_ASETS != '''NONE''')
    THEN
      V_QUERY := 'SELECT  *
                FROM (SELECT /*+ PARALLEL(auto) */ SEQ_NO,
                             IMP_CHANGE_REASON, '
      || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
            V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ', '
      || V_TYPE_COLOUMN || '
                             STAGE
                    FROM IFRS_REPORT_ECL_MOVE_VALAS_DTL
                    WHERE SEQ_NO NOT IN (0, 99)
                      AND REPORT_DATE BETWEEN ''' || TO_CHAR(V_PERIOD_FROM) || ''' AND ''' || TO_CHAR(V_PERIOD_TO) || ''' '
      || CASE WHEN V_TYPE_DESC = 'ECL_OFF_BS' THEN ' AND SEQ_NO = 999999 ' ELSE '' END                        --supaya gak muncul data
      || CASE WHEN V_ASETS = 'ALL' THEN '' ELSE ' AND ASET_KEUANGAN IN (' || V_ASETS || ') ' END
      || CASE WHEN V_SEGMENTS = 'ALL' THEN '' ELSE ' AND SUB_SEGMENT IN (' || V_SEGMENTS || ')' END
      || CASE WHEN (V_IS_PENEMPATAN_BI = 0 OR
            V_SEGMENTS = 'ALL') THEN '' ELSE ' UNION ALL
                  SELECT /*+ PARALLEL(auto) */ SEQ_NO,
                         IMP_CHANGE_REASON, '
            || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
                  V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ', '
            || V_TYPE_COLOUMN || '
                         STAGE
                    FROM IFRS_REPORT_ECL_MOVE_VALAS_DTL
                    WHERE SEQ_NO NOT IN (0, 99)
                      AND REPORT_DATE BETWEEN ''' || TO_CHAR(V_PERIOD_FROM) || ''' AND ''' || TO_CHAR(V_PERIOD_TO) || ''' '
            || CASE WHEN V_TYPE_DESC = 'ECL_OFF_BS' THEN ' AND SEQ_NO = 999999 ' ELSE '' END                       --supaya gak muncul data
            || ' AND ASET_KEUANGAN IN (''PENEMPATAN PADA BANK INDONESIA'') ' END

      || CASE WHEN (V_IS_TAGIHAN_REVERSE_REPO = 0 OR
            V_SEGMENTS = 'ALL') THEN '' ELSE ' UNION ALL
                  SELECT /*+ PARALLEL(auto) */ SEQ_NO,
                         IMP_CHANGE_REASON, '
            || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
                  V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ', '
            || V_TYPE_COLOUMN || '
                         STAGE
                    FROM IFRS_REPORT_ECL_MOVE_VALAS_DTL
                    WHERE SEQ_NO NOT IN (0, 99)
                      AND REPORT_DATE BETWEEN ''' || TO_CHAR(V_PERIOD_FROM) || ''' AND ''' || TO_CHAR(V_PERIOD_TO) || ''' '
            || CASE WHEN V_TYPE_DESC = 'ECL_OFF_BS' THEN ' AND SEQ_NO = 999999 ' ELSE '' END                       --supaya gak muncul data
            || ' AND ASET_KEUANGAN IN (''TAGIHAN REVERSE REPO'') ' END ||
      ' UNION ALL
                  SELECT /*+ PARALLEL(auto) */ SEQ_NO,
                         IMP_CHANGE_REASON, '
      || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
            V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ', '
      || V_TYPE_COLOUMN || '
                         STAGE
                    FROM IFRS_RPT_MOVE_ANALY_AW_AK
                    WHERE SEQ_NO = 0
                      AND REPORT_DATE = ''' || TO_CHAR(V_PERIOD_FROM) || ''' '
      || CASE WHEN V_TYPE_DESC = 'ECL_OFF_BS' THEN ' AND SEQ_NO = 999999 ' ELSE '' END                        --supaya gak muncul data
      || CASE WHEN V_ASETS = 'ALL' THEN '' ELSE ' AND ASET_KEUANGAN IN (' || V_ASETS || ') ' END
      || CASE WHEN V_SEGMENTS = 'ALL' THEN '' ELSE ' AND SUB_SEGMENT IN (' || V_SEGMENTS || ')' END

      || CASE WHEN (V_IS_PENEMPATAN_BI = 0 OR
            V_SEGMENTS = 'ALL') THEN '' ELSE ' UNION ALL
                  SELECT /*+ PARALLEL(auto) */ SEQ_NO,
                         IMP_CHANGE_REASON, '
            || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
                  V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ', '
            || V_TYPE_COLOUMN || '
                         STAGE
                    FROM IFRS_RPT_MOVE_ANALY_AW_AK
                    WHERE SEQ_NO = 0
                      AND REPORT_DATE = ''' || TO_CHAR(V_PERIOD_FROM) || ''' '
            || CASE WHEN V_TYPE_DESC = 'ECL_OFF_BS' THEN ' AND SEQ_NO = 999999 ' ELSE '' END                       --supaya gak muncul data
            || ' AND ASET_KEUANGAN IN (''PENEMPATAN PADA BANK INDONESIA'') ' END

      || CASE WHEN (V_IS_TAGIHAN_REVERSE_REPO = 0 OR
            V_SEGMENTS = 'ALL') THEN '' ELSE ' UNION ALL
                  SELECT /*+ PARALLEL(auto) */ SEQ_NO,
                         IMP_CHANGE_REASON, '
            || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
                  V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ', '
            || V_TYPE_COLOUMN || '
                         STAGE
                    FROM IFRS_RPT_MOVE_ANALY_AW_AK
                    WHERE SEQ_NO = 0
                      AND REPORT_DATE = ''' || TO_CHAR(V_PERIOD_FROM) || ''' '
            || CASE WHEN V_TYPE_DESC = 'ECL_OFF_BS' THEN ' AND SEQ_NO = 999999 ' ELSE '' END                       --supaya gak muncul data
            || ' AND ASET_KEUANGAN IN (''TAGIHAN REVERSE REPO'') ' END ||

      ' UNION ALL
                  SELECT /*+ PARALLEL(auto) */ SEQ_NO,
                         IMP_CHANGE_REASON, '
      || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
            V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ', '
      || V_TYPE_COLOUMN || '
                         STAGE
                    FROM IFRS_RPT_MOVE_ANALY_AW_AK
                    WHERE SEQ_NO = 99
                      AND REPORT_DATE = ''' || TO_CHAR(V_PERIOD_TO) || ''' '
      || CASE WHEN V_TYPE_DESC = 'ECL_OFF_BS' THEN ' AND SEQ_NO = 999999 ' ELSE '' END                       --supaya gak muncul data
      || CASE WHEN V_ASETS = 'ALL' THEN '' ELSE ' AND ASET_KEUANGAN IN (' || V_ASETS || ') ' END
      || CASE WHEN V_SEGMENTS = 'ALL' THEN '' ELSE ' AND SUB_SEGMENT IN (' || V_SEGMENTS || ')' END
      || CASE WHEN (V_IS_PENEMPATAN_BI = 0 OR
            V_SEGMENTS = 'ALL') THEN '' ELSE ' UNION ALL
                  SELECT /*+ PARALLEL(auto) */ SEQ_NO,
                         IMP_CHANGE_REASON, '
            || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
                  V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ', '
            || V_TYPE_COLOUMN || '
                         STAGE
                    FROM IFRS_RPT_MOVE_ANALY_AW_AK
                    WHERE SEQ_NO = 99
                      AND REPORT_DATE = ''' || TO_CHAR(V_PERIOD_TO) || ''' '
            || CASE WHEN V_TYPE_DESC = 'ECL_OFF_BS' THEN ' AND SEQ_NO = 999999 ' ELSE '' END                       --supaya gak muncul data
            || ' AND ASET_KEUANGAN IN (''PENEMPATAN PADA BANK INDONESIA'') ' END
      || CASE WHEN (V_IS_TAGIHAN_REVERSE_REPO = 0 OR
            V_SEGMENTS = 'ALL') THEN '' ELSE ' UNION ALL
                  SELECT /*+ PARALLEL(auto) */ SEQ_NO,
                         IMP_CHANGE_REASON, '
            || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
                  V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ', '
            || V_TYPE_COLOUMN || '
                         STAGE
                    FROM IFRS_RPT_MOVE_ANALY_AW_AK
                    WHERE SEQ_NO = 99
                      AND REPORT_DATE = ''' || TO_CHAR(V_PERIOD_TO) || ''' '
            || CASE WHEN V_TYPE_DESC = 'ECL_OFF_BS' THEN ' AND SEQ_NO = 999999 ' ELSE '' END                       --supaya gak muncul data
            || ' AND ASET_KEUANGAN IN (''TAGIHAN REVERSE REPO'') ' END ||
      ') PIVOT (
                SUM(' || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
            V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ')'
      || V_SUM ||
      'FOR (STAGE)
                IN (''1'' AS "STAGE_1", ''2'' AS "STAGE_2", ''3'' AS "STAGE_3", ''POCI'' AS "POCI")
                )
                ORDER BY SEQ_NO,
                         IMP_CHANGE_REASON';

    END IF;

    -- ada aset dan TRA
    IF (V_TRAS != '''NONE'''
      AND V_ASETS != '''NONE''')
    THEN

      V_QUERY := 'SELECT *
                FROM (SELECT /*+ PARALLEL(auto) */ SEQ_NO,
                             IMP_CHANGE_REASON, '
      || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
            V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ', '
      || V_TYPE_COLOUMN || '
                             STAGE
                    FROM IFRS_REPORT_ECL_MOVE_VALAS_DTL
                    WHERE SEQ_NO NOT IN (0, 99)
                      AND REPORT_DATE BETWEEN ''' || TO_CHAR(V_PERIOD_FROM) || ''' AND ''' || TO_CHAR(V_PERIOD_TO) || ''' '
      || CASE WHEN V_ASETS = 'ALL' THEN '' ELSE ' AND (ASET_KEUANGAN IN (' || V_ASETS || ') OR TRA IN (' || V_TRAS || ')) ' END
      || CASE WHEN V_SEGMENTS = 'ALL' THEN '' ELSE ' AND SUB_SEGMENT IN (' || V_SEGMENTS || ')' END
      || CASE WHEN (V_IS_PENEMPATAN_BI = 0 OR
            V_SEGMENTS = 'ALL') THEN '' ELSE ' UNION ALL
                  SELECT /*+ PARALLEL(auto) */ SEQ_NO,
                         IMP_CHANGE_REASON, '
            || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
                  V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ', '
            || V_TYPE_COLOUMN || '
                         STAGE
                    FROM IFRS_REPORT_ECL_MOVE_VALAS_DTL
                    WHERE SEQ_NO NOT IN (0, 99)
                      AND REPORT_DATE BETWEEN ''' || TO_CHAR(V_PERIOD_FROM) || ''' AND ''' || TO_CHAR(V_PERIOD_TO) || ''' '
            || ' AND ASET_KEUANGAN IN (''PENEMPATAN PADA BANK INDONESIA'') ' END

      || CASE WHEN (V_IS_TAGIHAN_REVERSE_REPO = 0 OR
            V_SEGMENTS = 'ALL') THEN '' ELSE ' UNION ALL
                  SELECT /*+ PARALLEL(auto) */ SEQ_NO,
                         IMP_CHANGE_REASON, '
            || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
                  V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ', '
            || V_TYPE_COLOUMN || '
                         STAGE
                    FROM IFRS_REPORT_ECL_MOVE_VALAS_DTL
                    WHERE SEQ_NO NOT IN (0, 99)
                      AND REPORT_DATE BETWEEN ''' || TO_CHAR(V_PERIOD_FROM) || ''' AND ''' || TO_CHAR(V_PERIOD_TO) || ''' '
            || ' AND ASET_KEUANGAN IN (''TAGIHAN REVERSE REPO'') ' END ||
      ' UNION ALL
                  SELECT /*+ PARALLEL(auto) */ SEQ_NO,
                         IMP_CHANGE_REASON, '
      || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
            V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ', '
      || V_TYPE_COLOUMN || '
                         STAGE
                    FROM IFRS_RPT_MOVE_ANALY_AW_AK
                    WHERE SEQ_NO = 0
                      AND REPORT_DATE = ''' || TO_CHAR(V_PERIOD_FROM) || ''' '
      || CASE WHEN V_ASETS = 'ALL' THEN '' ELSE ' AND (ASET_KEUANGAN IN (' || V_ASETS || ') OR TRA IN (' || V_TRAS || ')) ' END
      || CASE WHEN V_SEGMENTS = 'ALL' THEN '' ELSE ' AND SUB_SEGMENT IN (' || V_SEGMENTS || ')' END

      || CASE WHEN (V_IS_PENEMPATAN_BI = 0 OR
            V_SEGMENTS = 'ALL') THEN '' ELSE ' UNION ALL
                  SELECT /*+ PARALLEL(auto) */ SEQ_NO,
                         IMP_CHANGE_REASON, '
            || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
                  V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ', '
            || V_TYPE_COLOUMN || '
                         STAGE
                    FROM IFRS_RPT_MOVE_ANALY_AW_AK
                    WHERE SEQ_NO = 0
                      AND REPORT_DATE = ''' || TO_CHAR(V_PERIOD_FROM) || ''' '
            || ' AND ASET_KEUANGAN IN (''PENEMPATAN PADA BANK INDONESIA'') ' END

      || CASE WHEN (V_IS_TAGIHAN_REVERSE_REPO = 0 OR
            V_SEGMENTS = 'ALL') THEN '' ELSE ' UNION ALL
                  SELECT /*+ PARALLEL(auto) */ SEQ_NO,
                         IMP_CHANGE_REASON, '
            || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
                  V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ', '
            || V_TYPE_COLOUMN || '
                         STAGE
                    FROM IFRS_RPT_MOVE_ANALY_AW_AK
                    WHERE SEQ_NO = 0
                      AND REPORT_DATE = ''' || TO_CHAR(V_PERIOD_FROM) || ''' '
            || ' AND ASET_KEUANGAN IN (''TAGIHAN REVERSE REPO'') ' END ||

      ' UNION ALL
                  SELECT /*+ PARALLEL(auto) */ SEQ_NO,
                         IMP_CHANGE_REASON, '
      || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
            V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ', '
      || V_TYPE_COLOUMN || '
                         STAGE
                    FROM IFRS_RPT_MOVE_ANALY_AW_AK
                    WHERE SEQ_NO = 99
                      AND REPORT_DATE = ''' || TO_CHAR(V_PERIOD_TO) || ''' '
      || CASE WHEN V_ASETS = 'ALL' THEN '' ELSE ' AND (ASET_KEUANGAN IN (' || V_ASETS || ') OR TRA IN (' || V_TRAS || ')) ' END
      || CASE WHEN V_SEGMENTS = 'ALL' THEN '' ELSE ' AND SUB_SEGMENT IN (' || V_SEGMENTS || ')' END
      || CASE WHEN (V_IS_PENEMPATAN_BI = 0 OR
            V_SEGMENTS = 'ALL') THEN '' ELSE ' UNION ALL
                  SELECT /*+ PARALLEL(auto) */ SEQ_NO,
                         IMP_CHANGE_REASON, '
            || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
                  V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ', '
            || V_TYPE_COLOUMN || '
                         STAGE
                    FROM IFRS_RPT_MOVE_ANALY_AW_AK
                    WHERE SEQ_NO = 99
                      AND REPORT_DATE = ''' || TO_CHAR(V_PERIOD_TO) || ''' '
            || ' AND ASET_KEUANGAN IN (''PENEMPATAN PADA BANK INDONESIA'') ' END
      || CASE WHEN (V_IS_TAGIHAN_REVERSE_REPO = 0 OR
            V_SEGMENTS = 'ALL') THEN '' ELSE ' UNION ALL
                  SELECT /*+ PARALLEL(auto) */ SEQ_NO,
                         IMP_CHANGE_REASON, '
            || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
                  V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ', '
            || V_TYPE_COLOUMN || '
                         STAGE
                    FROM IFRS_RPT_MOVE_ANALY_AW_AK
                    WHERE SEQ_NO = 99
                      AND REPORT_DATE = ''' || TO_CHAR(V_PERIOD_TO) || ''' '
            || ' AND ASET_KEUANGAN IN (''TAGIHAN REVERSE REPO'') ' END ||
      ') PIVOT (
                SUM(' || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
            V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ')'
      || V_SUM ||
      'FOR (STAGE)
                IN (''1'' AS "STAGE_1", ''2'' AS "STAGE_2", ''3'' AS "STAGE_3", ''POCI'' AS "POCI")
                )
                ORDER BY SEQ_NO,
                         IMP_CHANGE_REASON';


    END IF;


    OPEN CUR_OUT FOR V_QUERY;
  END;