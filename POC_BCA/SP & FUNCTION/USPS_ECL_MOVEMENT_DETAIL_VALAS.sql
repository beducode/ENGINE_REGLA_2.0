CREATE OR REPLACE PROCEDURE USPS_ECL_MOVEMENT_DETAIL_VALAS(V_PERIOD_FROM     DATE,
                                                                V_PERIOD_TO       DATE,
                                                                V_SEGMENTS        VARCHAR2,
                                                                V_CHANGE_REASON   VARCHAR2,
                                                                V_PAGENUMBER      NUMBER,
                                                                V_PAGESIZE        NUMBER,
                                                                V_SORTPARAMETER   VARCHAR2,
                                                                V_COUNT           VARCHAR2,
                                                                V_TYPE            VARCHAR2,
                                                                V_ASETS           VARCHAR2,
                                                                V_TRAS            VARCHAR2,
                                                                CUR_OUT       OUT SYS_REFCURSOR)
  AS
    V_QUERY                   VARCHAR2(32000);
    V_SELECT                  VARCHAR2(4000);
    V_SORTPARAMETER2          VARCHAR2(32000);
    V_PAGESIZE2               NUMBER;
    V_SUM                     VARCHAR2(4000) := '';
    V_TYPE_COLOUMN            VARCHAR2(4000) := '';
    V_TYPE_DESC               VARCHAR2(4000) := '';
    V_IS_PENEMPATAN_BI        NUMBER(1, 0)   := 0;
    V_IS_TAGIHAN_REVERSE_REPO NUMBER(1, 0)   := 0;

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

    IF V_SORTPARAMETER IS NULL
    THEN
      V_SORTPARAMETER2 := '"DATA_SOURCE"';
    ELSE
      V_SORTPARAMETER2 := V_SORTPARAMETER;
    END IF;

    IF (V_PAGESIZE = 0)
    THEN
      V_PAGESIZE2 := 10;
    ELSE
      V_PAGESIZE2 := V_PAGESIZE;
    END IF;

    IF (V_COUNT = 'YES')
    THEN
      V_SELECT := 'SELECT /*+ PARALLEL(auto) */ COUNT(*)';
    ELSE
      V_SELECT := 'SELECT /*+ PARALLEL(auto) */ * ';
    END IF;

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

    -- TRA Only
    IF (V_TRAS != '''NONE'''
      AND V_ASETS = '''NONE''')
    THEN
      V_QUERY := V_SELECT || 'FROM(
                            SELECT /*+ PARALLEL(auto) */
                            DATA_SOURCE,
                            SUB_SEGMENT,
                            ACCOUNT_NUMBER,
                            CUSTOMER_NAME,
                            CUSTOMER_NUMBER,'
      || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
            V_TRAS = '''KELONGGARAN TARIK''') THEN 'ECL_OFF_BS' ELSE V_TYPE_DESC END || ',
                            ASET_KEUANGAN,
                            TRA, '
      || V_TYPE_COLOUMN || '
                            STAGE
                            FROM IFRS_REPORT_ECL_MOVE_VALAS_DTL
                            WHERE IMP_CHANGE_REASON = ''' || V_CHANGE_REASON || '''
                            AND REPORT_DATE BETWEEN ''' || TO_CHAR(V_PERIOD_FROM) || ''' AND ''' || TO_CHAR(V_PERIOD_TO) || '''' || CASE WHEN V_TRAS = 'ALL' THEN '' ELSE 'AND TRA IN (' || V_TRAS || ') ' END || CASE WHEN V_SEGMENTS = 'ALL' THEN '' ELSE 'AND SUB_SEGMENT IN (' || V_SEGMENTS || ')' END || '
       UNION ALL            SELECT /*+ PARALLEL(auto) */
                            DATA_SOURCE,
                            SUB_SEGMENT,
                            ACCOUNT_NUMBER,
                            CUSTOMER_NAME,
                            CUSTOMER_NUMBER,'
      || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
            V_TRAS = '''KELONGGARAN TARIK''') THEN 'ECL_OFF_BS' ELSE V_TYPE_DESC END || ',
                            ASET_KEUANGAN,
                            TRA, '
      || V_TYPE_COLOUMN || '
                            STAGE
                            FROM IFRS_RPT_MOVE_ANALY_AW_AK
                            WHERE IMP_CHANGE_REASON = ''' || V_CHANGE_REASON || '''
                            AND ECL_OFF_BS != 0
                            AND REPORT_DATE BETWEEN ''' || TO_CHAR(V_PERIOD_FROM) || ''' AND ''' || TO_CHAR(V_PERIOD_TO) || '''' || CASE WHEN V_TRAS = 'ALL' THEN '' ELSE 'AND TRA IN (' || V_TRAS || ') ' END || CASE WHEN V_SEGMENTS = 'ALL' THEN '' ELSE 'AND SUB_SEGMENT IN (' || V_SEGMENTS || ')' END || '
                        )
                    PIVOT(
                        SUM(' || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
            V_TRAS = '''KELONGGARAN TARIK''') THEN 'ECL_OFF_BS' ELSE V_TYPE_DESC END || ')' || V_SUM || '
                        FOR (STAGE)
                        IN (''1'' AS "STAGE_1",''2'' AS "STAGE_2",''3'' AS "STAGE_3", ''POCI'' AS "POCI")
                     )' || CASE WHEN V_COUNT = 'YES' THEN '' ELSE '
                          ORDER BY ' || V_SORTPARAMETER2 || '' || '
                          OFFSET ' || TO_CHAR(V_PAGENUMBER) || ' ROWS
                          FETCH NEXT ' || TO_CHAR(V_PAGESIZE2) || ' ROWS ONLY' END;

    END IF;

    -- Aset Only
    IF (V_TRAS = '''NONE'''
      AND V_ASETS != '''NONE''')
    THEN
      V_QUERY := V_SELECT || 'FROM(
                            SELECT /*+ PARALLEL(auto) */
                            DATA_SOURCE,
                            SUB_SEGMENT,
                            ACCOUNT_NUMBER,
                            CUSTOMER_NAME,
                            CUSTOMER_NUMBER,'
      || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
            V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ',
                            ASET_KEUANGAN,
                            TRA, '
      || V_TYPE_COLOUMN || '
                            STAGE
                            FROM IFRS_REPORT_ECL_MOVE_VALAS_DTL
                            WHERE IMP_CHANGE_REASON = ''' || V_CHANGE_REASON || '''
                            AND REPORT_DATE BETWEEN ''' || TO_CHAR(V_PERIOD_FROM) || ''' AND ''' || TO_CHAR(V_PERIOD_TO) || '''' || CASE WHEN V_ASETS = 'ALL' THEN '' ELSE 'AND ASET_KEUANGAN IN (' || V_ASETS || ')  ' END || CASE WHEN V_SEGMENTS = 'ALL' THEN '' ELSE 'AND SUB_SEGMENT IN (' || V_SEGMENTS || ')' END

      || CASE WHEN (V_IS_PENEMPATAN_BI = 0 OR
            V_SEGMENTS = 'ALL') THEN '' ELSE ' UNION ALL
                 SELECT /*+ PARALLEL(auto) */
                            DATA_SOURCE,
                            SUB_SEGMENT,
                            ACCOUNT_NUMBER,
                            CUSTOMER_NAME,
                            CUSTOMER_NUMBER,'
            || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
                  V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ',
                            ASET_KEUANGAN,
                            TRA, '
            || V_TYPE_COLOUMN || '
                            STAGE
                   FROM IFRS_REPORT_ECL_MOVE_VALAS_DTL
                            WHERE IMP_CHANGE_REASON = ''' || V_CHANGE_REASON || '''
                            AND REPORT_DATE BETWEEN ''' || TO_CHAR(V_PERIOD_FROM) || ''' AND ''' || TO_CHAR(V_PERIOD_TO) || ''''
            || ' AND ASET_KEUANGAN IN (''PENEMPATAN PADA BANK INDONESIA'') ' END

      || CASE WHEN (V_IS_TAGIHAN_REVERSE_REPO = 0 OR
            V_SEGMENTS = 'ALL') THEN '' ELSE ' UNION ALL
                 SELECT /*+ PARALLEL(auto) */
                            DATA_SOURCE,
                            SUB_SEGMENT,
                            ACCOUNT_NUMBER,
                            CUSTOMER_NAME,
                            CUSTOMER_NUMBER,'
            || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
                  V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ',
                            ASET_KEUANGAN,
                            TRA, '
            || V_TYPE_COLOUMN || '
                            STAGE
                   FROM IFRS_REPORT_ECL_MOVE_VALAS_DTL
                            WHERE IMP_CHANGE_REASON = ''' || V_CHANGE_REASON || '''
                            AND REPORT_DATE BETWEEN ''' || TO_CHAR(V_PERIOD_FROM) || ''' AND ''' || TO_CHAR(V_PERIOD_TO) || ''''
            || ' AND ASET_KEUANGAN IN (''TAGIHAN REVERSE REPO'') ' END ||

      ' UNION ALL
                            SELECT /*+ PARALLEL(auto) */
                            DATA_SOURCE,
                            SUB_SEGMENT,
                            ACCOUNT_NUMBER,
                            CUSTOMER_NAME,
                            CUSTOMER_NUMBER,'
      || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
            V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ',
                            ASET_KEUANGAN,
                            TRA, '
      || V_TYPE_COLOUMN || '
                            STAGE
                            FROM IFRS_RPT_MOVE_ANALY_AW_AK
                            WHERE IMP_CHANGE_REASON = ''' || V_CHANGE_REASON || '''
                            AND REPORT_DATE BETWEEN ''' || TO_CHAR(V_PERIOD_FROM) || ''' AND ''' || TO_CHAR(V_PERIOD_TO) || '''' || CASE WHEN V_ASETS = 'ALL' THEN '' ELSE 'AND ASET_KEUANGAN IN (' || V_ASETS || ') ' END || CASE WHEN V_SEGMENTS = 'ALL' THEN '' ELSE 'AND SUB_SEGMENT IN (' || V_SEGMENTS || ')' END
      || CASE WHEN (V_IS_PENEMPATAN_BI = 0 OR
            V_SEGMENTS = 'ALL') THEN '' ELSE ' UNION ALL
                 SELECT /*+ PARALLEL(auto) */
                            DATA_SOURCE,
                            SUB_SEGMENT,
                            ACCOUNT_NUMBER,
                            CUSTOMER_NAME,
                            CUSTOMER_NUMBER,'
            || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
                  V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ',
                            ASET_KEUANGAN,
                            TRA, '
            || V_TYPE_COLOUMN || '
                            STAGE
                   FROM IFRS_RPT_MOVE_ANALY_AW_AK
                            WHERE IMP_CHANGE_REASON = ''' || V_CHANGE_REASON || '''
                            AND REPORT_DATE BETWEEN ''' || TO_CHAR(V_PERIOD_FROM) || ''' AND ''' || TO_CHAR(V_PERIOD_TO) || ''''
            || ' AND ASET_KEUANGAN IN (''PENEMPATAN PADA BANK INDONESIA'') ' END

      || CASE WHEN (V_IS_TAGIHAN_REVERSE_REPO = 0 OR
            V_SEGMENTS = 'ALL') THEN '' ELSE ' UNION ALL
                 SELECT /*+ PARALLEL(auto) */
                            DATA_SOURCE,
                            SUB_SEGMENT,
                            ACCOUNT_NUMBER,
                            CUSTOMER_NAME,
                            CUSTOMER_NUMBER,'
            || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
                  V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ',
                            ASET_KEUANGAN,
                            TRA, '
            || V_TYPE_COLOUMN || '
                            STAGE
                   FROM IFRS_RPT_MOVE_ANALY_AW_AK
                            WHERE IMP_CHANGE_REASON = ''' || V_CHANGE_REASON || '''
                            AND REPORT_DATE BETWEEN ''' || TO_CHAR(V_PERIOD_FROM) || ''' AND ''' || TO_CHAR(V_PERIOD_TO) || ''''
            || ' AND ASET_KEUANGAN IN (''TAGIHAN REVERSE REPO'') ' END ||

      ' )
                    PIVOT(
                        SUM(' || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
            V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ')' || V_SUM || '
                        FOR (STAGE)
                        IN (''1'' AS "STAGE_1",''2'' AS "STAGE_2",''3'' AS "STAGE_3", ''POCI'' AS "POCI")
                     )' || CASE WHEN V_COUNT = 'YES' THEN '' ELSE '
                          ORDER BY ' || V_SORTPARAMETER2 || '' || '
                          OFFSET ' || TO_CHAR(V_PAGENUMBER) || ' ROWS
                          FETCH NEXT ' || TO_CHAR(V_PAGESIZE2) || ' ROWS ONLY' END;
    END IF;


    IF (V_TRAS != '''NONE'''
      AND V_ASETS != '''NONE''')
    THEN

      V_QUERY := V_SELECT || 'FROM(
                            SELECT /*+ PARALLEL(auto) */
                            DATA_SOURCE,
                            SUB_SEGMENT,
                            ACCOUNT_NUMBER,
                            CUSTOMER_NAME,
                            CUSTOMER_NUMBER,'
      || V_TYPE_DESC || ',
                            ASET_KEUANGAN,
                            TRA, '
      || V_TYPE_COLOUMN || '
                            STAGE
                            FROM IFRS_REPORT_ECL_MOVE_VALAS_DTL
                            WHERE IMP_CHANGE_REASON = ''' || V_CHANGE_REASON || '''
                            AND REPORT_DATE BETWEEN ''' || TO_CHAR(V_PERIOD_FROM) || ''' AND ''' || TO_CHAR(V_PERIOD_TO) || '''' || CASE WHEN V_ASETS = 'ALL' THEN '' ELSE 'AND (ASET_KEUANGAN IN (' || V_ASETS || ') OR TRA IN (' || V_TRAS || ') )  ' END || CASE WHEN V_SEGMENTS = 'ALL' THEN '' ELSE 'AND SUB_SEGMENT IN (' || V_SEGMENTS || ')' END
      || CASE WHEN (V_IS_PENEMPATAN_BI = 0 OR
            V_SEGMENTS = 'ALL') THEN '' ELSE ' UNION ALL
                 SELECT /*+ PARALLEL(auto) */
                            DATA_SOURCE,
                            SUB_SEGMENT,
                            ACCOUNT_NUMBER,
                            CUSTOMER_NAME,
                            CUSTOMER_NUMBER,'
            || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
                  V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ',
                            ASET_KEUANGAN,
                            TRA, '
            || V_TYPE_COLOUMN || '
                            STAGE
                   FROM IFRS_REPORT_ECL_MOVE_VALAS_DTL
                            WHERE IMP_CHANGE_REASON = ''' || V_CHANGE_REASON || '''
                            AND REPORT_DATE BETWEEN ''' || TO_CHAR(V_PERIOD_FROM) || ''' AND ''' || TO_CHAR(V_PERIOD_TO) || ''''
            || ' AND ASET_KEUANGAN IN (''PENEMPATAN PADA BANK INDONESIA'') ' END

      || CASE WHEN (V_IS_TAGIHAN_REVERSE_REPO = 0 OR
            V_SEGMENTS = 'ALL') THEN '' ELSE ' UNION ALL
                 SELECT /*+ PARALLEL(auto) */
                            DATA_SOURCE,
                            SUB_SEGMENT,
                            ACCOUNT_NUMBER,
                            CUSTOMER_NAME,
                            CUSTOMER_NUMBER,'
            || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
                  V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ',
                            ASET_KEUANGAN,
                            TRA, '
            || V_TYPE_COLOUMN || '
                            STAGE
                   FROM IFRS_REPORT_ECL_MOVE_VALAS_DTL
                            WHERE IMP_CHANGE_REASON = ''' || V_CHANGE_REASON || '''
                            AND REPORT_DATE BETWEEN ''' || TO_CHAR(V_PERIOD_FROM) || ''' AND ''' || TO_CHAR(V_PERIOD_TO) || ''''
            || ' AND ASET_KEUANGAN IN (''TAGIHAN REVERSE REPO'') ' END ||


      '  UNION ALL
                            SELECT /*+ PARALLEL(auto) */
                            DATA_SOURCE,
                            SUB_SEGMENT,
                            ACCOUNT_NUMBER,
                            CUSTOMER_NAME,
                            CUSTOMER_NUMBER,'
      || V_TYPE_DESC || ',
                            ASET_KEUANGAN,
                            TRA, '
      || V_TYPE_COLOUMN || '
                            STAGE
                            FROM IFRS_RPT_MOVE_ANALY_AW_AK
                            WHERE IMP_CHANGE_REASON = ''' || V_CHANGE_REASON || '''
                            AND REPORT_DATE BETWEEN ''' || TO_CHAR(V_PERIOD_FROM) || ''' AND ''' || TO_CHAR(V_PERIOD_TO) || '''' || CASE WHEN V_ASETS = 'ALL' THEN '' ELSE 'AND (ASET_KEUANGAN IN (' || V_ASETS || ') OR TRA IN (' || V_TRAS || ') ) ' END || CASE WHEN V_SEGMENTS = 'ALL' THEN '' ELSE 'AND SUB_SEGMENT IN (' || V_SEGMENTS || ')' END
      || CASE WHEN (V_IS_PENEMPATAN_BI = 0 OR
            V_SEGMENTS = 'ALL') THEN '' ELSE ' UNION ALL
                 SELECT /*+ PARALLEL(auto) */
                            DATA_SOURCE,
                            SUB_SEGMENT,
                            ACCOUNT_NUMBER,
                            CUSTOMER_NAME,
                            CUSTOMER_NUMBER,'
            || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
                  V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ',
                            ASET_KEUANGAN,
                            TRA, '
            || V_TYPE_COLOUMN || '
                            STAGE
                   FROM IFRS_RPT_MOVE_ANALY_AW_AK
                            WHERE IMP_CHANGE_REASON = ''' || V_CHANGE_REASON || '''
                            AND REPORT_DATE BETWEEN ''' || TO_CHAR(V_PERIOD_FROM) || ''' AND ''' || TO_CHAR(V_PERIOD_TO) || ''''
            || ' AND ASET_KEUANGAN IN (''PENEMPATAN PADA BANK INDONESIA'') ' END

      || CASE WHEN (V_IS_TAGIHAN_REVERSE_REPO = 0 OR
            V_SEGMENTS = 'ALL') THEN '' ELSE ' UNION ALL
                 SELECT /*+ PARALLEL(auto) */
                            DATA_SOURCE,
                            SUB_SEGMENT,
                            ACCOUNT_NUMBER,
                            CUSTOMER_NAME,
                            CUSTOMER_NUMBER,'
            || CASE WHEN (V_TYPE = 'ECL_TOTAL' AND
                  V_ASETS = '''CREDIT YANG DIBERIKAN''') THEN 'ECL_ON_BS' ELSE V_TYPE_DESC END || ',
                            ASET_KEUANGAN,
                            TRA, '
            || V_TYPE_COLOUMN || '
                            STAGE
                   FROM IFRS_RPT_MOVE_ANALY_AW_AK
                            WHERE IMP_CHANGE_REASON = ''' || V_CHANGE_REASON || '''
                            AND REPORT_DATE BETWEEN ''' || TO_CHAR(V_PERIOD_FROM) || ''' AND ''' || TO_CHAR(V_PERIOD_TO) || ''''
            || ' AND ASET_KEUANGAN IN (''TAGIHAN REVERSE REPO'') ' END ||

      ' )
                    PIVOT(
                        SUM(' || V_TYPE_DESC || ')' || V_SUM || '
                        FOR (STAGE)
                        IN (''1'' AS "STAGE_1",''2'' AS "STAGE_2",''3'' AS "STAGE_3", ''POCI'' AS "POCI")
                     )' || CASE WHEN V_COUNT = 'YES' THEN '' ELSE '
                          ORDER BY ' || V_SORTPARAMETER2 || '' || '
                          OFFSET ' || TO_CHAR(V_PAGENUMBER) || ' ROWS
                          FETCH NEXT ' || TO_CHAR(V_PAGESIZE2) || ' ROWS ONLY' END;





    END IF;

    OPEN CUR_OUT FOR V_QUERY;
  END;