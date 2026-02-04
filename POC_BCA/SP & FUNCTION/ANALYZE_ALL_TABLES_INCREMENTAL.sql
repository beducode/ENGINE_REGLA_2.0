CREATE OR REPLACE PROCEDURE ANALYZE_ALL_TABLES_INCREMENTAL
IS
    --------------------------------------------------------------------------------------------------------------
    --Notice : Under intellectual property rights - Licensable by Initial Developer,
    --         to use, reproduce, modify, display, perform and distribute
    --         the Original Software, with or without Modifications
    --Prog. Name        : ANALYZE_ALL_TABLES
    --Prog. Desc        : Procedure for batch analyze all of tables
    --Prog. Version     : 4.0
    --Initial Developer : Agus Wibawa
    --Create Date       : Jul 29, 2010
    --Modified by       : Agus Wibawa
    --Modified date     : Dec 05,2024
    --Modified desc     : Add incremental feature to partition table and check table prefs for incremental
    --------------------------------------------------------------------------------------------------------------

    CURSOR C1 IS
          SELECT table_name
            FROM user_tables
           WHERE     partitioned = 'NO'
        ORDER BY table_name;

    CURSOR C2 IS
          SELECT table_name
            FROM user_tables
           WHERE     partitioned = 'YES'
        ORDER BY table_name;

    vbufferdate         DATE;
    vc1                 c1%ROWTYPE;
    vc2                 c2%ROWTYPE;
    vc_current_date_1   VARCHAR2 (25);
    vc_current_date_2   VARCHAR2 (25);
    vtype               VARCHAR2 (30);
    vdatesrc            DATE;
    vdatedst            DATE;
    vpartcheck          VARCHAR2 (30);
    vcuser              all_users.username%TYPE;
    vexist              VARCHAR (30);
    srcname             VARCHAR (30);
    dstname             VARCHAR (30);
    vcount              NUMBER;
BEGIN
    SELECT TO_CHAR (SYSDATE, 'yyyymmdd hh24:mi:ss')
      INTO vc_current_date_1
      FROM DUAL;

    DBMS_OUTPUT.PUT_LINE ('Analyze start date :' || vc_current_date_1);

    BEGIN
        FOR vc1 IN c1
        LOOP
            SELECT TO_CHAR (SYSDATE, 'yyyymmdd hh24:mi:ss')
              INTO vc_current_date_1
              FROM DUAL;

            SELECT COUNT (*)
              INTO vexist
              FROM user_tab_stat_prefs
             WHERE table_name = vc1.table_name;

            IF vexist = 0
            THEN
                DBMS_STATS.set_table_prefs (vcuser,
                                            vc1.table_name,
                                            'INCREMENTAL',
                                            'TRUE');
                DBMS_STATS.set_table_prefs (vcuser,
                                            vc1.table_name,
                                            'STALE_PERCENT',
                                            '10');
            END IF;

            DBMS_STATS.gather_table_stats (ownname   => vcuser,
                                           tabname   => vc1.table_name,
                                           cascade   => TRUE,
                                           degree    => 8);

            SELECT TO_CHAR (SYSDATE, 'yyyymmdd hh24:mi:ss')
              INTO vc_current_date_2
              FROM DUAL;

            DBMS_OUTPUT.PUT_LINE (
                   'Analyze table '
                || vc1.table_name
                || ' Start :'
                || vc_current_date_1
                || ' End :'
                || vc_current_date_2);
        END LOOP;
    END;

    BEGIN
        FOR vc2 IN c2
        LOOP
            SELECT TO_CHAR (SYSDATE, 'yyyymmdd hh24:mi:ss')
              INTO vc_current_date_1
              FROM DUAL;

            SELECT COUNT (*)
              INTO vexist
              FROM user_tab_stat_prefs
             WHERE table_name = vc2.table_name;

            IF vexist = 0
            THEN
                DBMS_STATS.set_table_prefs (vcuser,
                                            vc2.table_name,
                                            'INCREMENTAL',
                                            'TRUE');
                DBMS_STATS.set_table_prefs (vcuser,
                                            vc2.table_name,
                                            'STALE_PERCENT',
                                            '10');
            END IF;

            DBMS_STATS.gather_table_stats (
                ownname       => vcuser,
                tabname       => vc2.table_name,
                cascade       => TRUE,
                degree        => 8,
                granularity   => 'GLOBAL AND PARTITION');

            SELECT partitioning_type
              INTO vtype
              FROM user_part_tables
             WHERE table_name = vc2.table_name;



            SELECT TO_CHAR (SYSDATE, 'yyyymmdd hh24:mi:ss')
              INTO vc_current_date_2
              FROM DUAL;

            DBMS_OUTPUT.PUT_LINE (
                   'Analyze table '
                || vc2.table_name
                || ' Start :'
                || vc_current_date_1
                || ' End :'
                || vc_current_date_2);
        END LOOP;
    END;


    SELECT TO_CHAR (SYSDATE, 'yyyymmdd hh24:mi:ss')
      INTO vc_current_date_2
      FROM DUAL;

    DBMS_OUTPUT.PUT_LINE ('Analyze end date :' || vc_current_date_2);
END;