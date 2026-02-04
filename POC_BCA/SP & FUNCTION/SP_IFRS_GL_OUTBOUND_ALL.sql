CREATE OR REPLACE PROCEDURE SP_IFRS_GL_OUTBOUND_ALL
IS
    V_CURDATE   DATE;
BEGIN
    SELECT CURRDATE INTO V_CURDATE FROM IFRS_PRC_DATE;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE IFRS_GL_OUTBOUND_ALL';

    INSERT /*+ append */
           INTO IFRS_GL_OUTBOUND_ALL
        (SELECT AAK_DBID                                    VLJ,
                AAK_CORP                                    CORP,
                SUBSTR (AAK_JRNLID, 1, 4)                   CBG,
                RPAD (SUBSTR (AAK_JRNLID, 5, 5), 6)         JRNL_ID,
                AAK_EFFDT                                   EFFDT,
                SUBSTR (AAK_VLMKEY, 1, 4)                   JRNL_BRANCH,
                SUBSTR (AAK_VLMKEY, 5, 12)                  COA,
                AAK_VLMKEY_SEQ                              SEQ,
                RPAD (NVL (AAK_VLMKEY_FILLER, ' '), 25)     FILLER,
                AAK_CURRCD                                  CCY,
                RPAD (NVL (AAK_SLID, ' '), 1)               SLID,
                RPAD (NVL (AAK_SLAC, ' '), 16)              SLAC,
                RPAD (NVL (AAK_SOURCE, ' '), 10)            SOURCES,
                RPAD (NVL (AAK_DESC, ' '), 75)              DESCRIPTION,
                AAK_JA                                      CY,
                AAK_JT                                      CP,
                RPAD (AAK_DCCD, 2)                          DC,
                NVL (AAK_RP_SIGN, ' ')                      RP_SIGN,
                LPAD (NVL (amt_1, 0), 15, '0')              AMT_1,
                NVL (AAK_VA_SIGN, ' ')                      VA_SIGN,
                LPAD (NVL (amt_3, 0), 15, '0')              AMT_3
          FROM (SELECT AAK_DBID,
                       AAK_CORP,
                       AAK_JRNLID,
                       AAK_EFFDT,
                       AAK_VLMKEY,
                       AAK_VLMKEY_SEQ,
                       AAK_VLMKEY_FILLER,
                       AAK_CURRCD,
                       AAK_SLID,
                       AAK_SLAC,
                       AAK_SOURCE,
                       AAK_DESC,
                       AAK_JA,
                       AAK_JT,
                       AAK_DCCD,
                       AAK_RP_SIGN,
                       LPAD (TO_CHAR (AAK_AMT_RP * 100), 15, '0')     AMT_1,
                       AAK_VA_SIGN,
                       LPAD (TO_CHAR (AAK_AMT_VA * 100), 15, '0')     AMT_3
                  FROM IFRS_GL_OUTBOUND_IMP
                 WHERE LAST_DAY (DOWNLOAD_DATE) = V_CURDATE
                UNION ALL
                SELECT AAK_DBID,
                       AAK_CORP,
                       AAK_JRNLID,
                       AAK_EFFDT,
                       AAK_VLMKEY,
                       AAK_VLMKEY_SEQ,
                       AAK_VLMKEY_FILLER,
                       AAK_CURRCD,
                       AAK_SLID,
                       AAK_SLAC,
                       AAK_SOURCE,
                       AAK_DESC,
                       AAK_JA,
                       AAK_JT,
                       AAK_DCCD,
                       AAK_RP_SIGN,
                       LPAD (TO_CHAR (AAK_AMT_RP * 100), 15, '0')     AMT_1,
                       AAK_VA_SIGN,
                       LPAD (TO_CHAR (AAK_AMT_VA * 100), 15, '0')     AMT_3
                  FROM IFRS_GL_OUTBOUND_IMP_R
                 WHERE LAST_DAY (DOWNLOAD_DATE) = V_CURDATE
                UNION ALL
                SELECT AAK_DBID,
                       AAK_CORP,
                       AAK_JRNLID,
                       AAK_EFFDT,
                       AAK_VLMKEY,
                       AAK_VLMKEY_SEQ,
                       AAK_VLMKEY_FILLER,
                       AAK_CURRCD,
                       AAK_SLID,
                       AAK_SLAC,
                       AAK_SOURCE,
                       AAK_DESC,
                       AAK_JA,
                       AAK_JT,
                       AAK_DCCD,
                       AAK_RP_SIGN,
                       LPAD (TO_CHAR (AAK_AMT_RP * 100), 15, '0')     AMT_1,
                       AAK_VA_SIGN,
                       LPAD (TO_CHAR (AAK_AMT_VA * 100), 15, '0')     AMT_3
                  FROM IFRS_GL_OUTBOUND_AMT
                 WHERE LAST_DAY (DOWNLOAD_DATE) = V_CURDATE
                UNION ALL
                SELECT AAK_DBID,
                       AAK_CORP,
                       AAK_JRNLID,
                       AAK_EFFDT,
                       AAK_VLMKEY,
                       AAK_VLMKEY_SEQ,
                       AAK_VLMKEY_FILLER,
                       AAK_CURRCD,
                       AAK_SLID,
                       AAK_SLAC,
                       AAK_SOURCE,
                       AAK_DESC,
                       AAK_JA,
                       AAK_JT,
                       AAK_DCCD,
                       AAK_RP_SIGN,
                       LPAD (TO_CHAR (AAK_AMT_RP * 100), 15, '0')     AMT_1,
                       AAK_VA_SIGN,
                       LPAD (TO_CHAR (AAK_AMT_VA * 100), 15, '0')     AMT_3
                  FROM IFRS_GL_OUTBOUND_AMT_R
                 WHERE LAST_DAY (DOWNLOAD_DATE) = V_CURDATE
                UNION ALL
                SELECT AAK_DBID,
                       AAK_CORP,
                       AAK_JRNLID,
                       AAK_EFFDT,
                       AAK_VLMKEY,
                       AAK_VLMKEY_SEQ,
                       AAK_VLMKEY_FILLER,
                       AAK_CURRCD,
                       AAK_SLID,
                       AAK_SLAC,
                       AAK_SOURCE,
                       AAK_DESC,
                       AAK_JA,
                       AAK_JT,
                       AAK_DCCD,
                       AAK_RP_SIGN,
                       LPAD (TO_CHAR (AAK_AMT_RP * 100), 15, '0')     AMT_1,
                       AAK_VA_SIGN,
                       LPAD (TO_CHAR (AAK_AMT_VA * 100), 15, '0')     AMT_3
                  FROM IFRS_GL_OUTBOUND_FS_AMT
                 WHERE LAST_DAY (DOWNLOAD_DATE) = V_CURDATE
                UNION ALL
                SELECT AAK_DBID,
                       AAK_CORP,
                       AAK_JRNLID,
                       AAK_EFFDT,
                       AAK_VLMKEY,
                       AAK_VLMKEY_SEQ,
                       AAK_VLMKEY_FILLER,
                       AAK_CURRCD,
                       AAK_SLID,
                       AAK_SLAC,
                       AAK_SOURCE,
                       AAK_DESC,
                       AAK_JA,
                       AAK_JT,
                       AAK_DCCD,
                       AAK_RP_SIGN,
                       LPAD (TO_CHAR (AAK_AMT_RP * 100), 15, '0')     AMT_1,
                       AAK_VA_SIGN,
                       LPAD (TO_CHAR (AAK_AMT_VA * 100), 15, '0')     AMT_3
                  FROM IFRS_GL_OUTBOUND_FS_AMT_R
                 WHERE LAST_DAY (DOWNLOAD_DATE) = V_CURDATE
                UNION ALL
                SELECT VLJ,
                       CORP,
                       CBG || JRNL_ID,
                       EFFDT,
                       JRNL_BRANCH || COA,
                       SEQ,
                       FILLER,
                       CCY,
                       SLID,
                       SLAC,
                       SOURCES,
                       DESCRIPTION,
                       CY,
                       CP,
                       DC,
                       RP_SIGN,
                       AMT_1,
                       VA_SIGN,
                       AMT_3
                  FROM IFRS_PENY_KOREKSI_YADIM_VLJ_7))
        ORDER BY AAK_JRNLID, AAK_VLMKEY, AAK_VLMKEY_SEQ, AAK_CURRCD,AAK_DCCD;

    COMMIT;
END;