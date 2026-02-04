CREATE OR REPLACE procedure SP_IFRS_JOURNAL_BRANCH as
    V_CURRDATE DATE;
    V_COUNT    NUMBER;
begin

    SELECT CURRDATE INTO V_CURRDATE FROM IFRS_PRC_DATE;

    select count(1) into V_COUNT from TBLU_JOURNAL_BRANCH where DOWNLOAD_DATE = V_CURRDATE;

    IF V_COUNT != 0
    THEN
        merge into IFRS_GL_OUTBOUND_IMP_R A
        using TBLU_JOURNAL_BRANCH B
        on (A.DOWNLOAD_DATE = B.DOWNLOAD_DATE)
        when matched then
            UPDATE
            SET A.AAK_JRNLID = B.BRANCH_DESTINATION || substr(A.AAK_JRNLID, 5),
                A.AAK_VLMKEY = B.BRANCH_DESTINATION || substr(A.AAK_VLMKEY, 5)
            where A.DOWNLOAD_DATE = V_CURRDATE
              and substr(A.AAK_JRNLID, 0, 4) = B.BRANCH_SOURCE
              and substr(A.AAK_VLMKEY, 0, 4) = B.BRANCH_SOURCE;

        merge into IFRS_GL_OUTBOUND_AMT_R A
        using TBLU_JOURNAL_BRANCH B
        on (A.DOWNLOAD_DATE = B.DOWNLOAD_DATE)
        when matched then
            UPDATE
            SET A.AAK_JRNLID = B.BRANCH_DESTINATION || substr(A.AAK_JRNLID, 5),
                A.AAK_VLMKEY = B.BRANCH_DESTINATION || substr(A.AAK_VLMKEY, 5)
            where A.DOWNLOAD_DATE = V_CURRDATE
              and substr(A.AAK_JRNLID, 0, 4) = B.BRANCH_SOURCE
              and substr(A.AAK_VLMKEY, 0, 4) = B.BRANCH_SOURCE;

        merge into IFRS_GL_OUTBOUND_FS_AMT_R A
        using TBLU_JOURNAL_BRANCH B
        on (A.DOWNLOAD_DATE = B.DOWNLOAD_DATE)
        when matched then
            UPDATE
            SET A.AAK_JRNLID = B.BRANCH_DESTINATION || substr(A.AAK_JRNLID, 5),
                A.AAK_VLMKEY = B.BRANCH_DESTINATION || substr(A.AAK_VLMKEY, 5)
            where A.DOWNLOAD_DATE = V_CURRDATE
              and substr(A.AAK_JRNLID, 0, 4) = B.BRANCH_SOURCE
              and substr(A.AAK_VLMKEY, 0, 4) = B.BRANCH_SOURCE;

        commit;
    end if;

end;