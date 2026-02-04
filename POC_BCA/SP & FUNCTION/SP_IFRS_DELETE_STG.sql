CREATE OR REPLACE procedure SP_IFRS_DELETE_STG(MOMENT IN VARCHAR2) as
begin
    if MOMENT = 'EOD'
    then
        delete from IFRS_STG_ILS_IMA where DOWNLOAD_DATE in (select currdate from IFRS_PRC_DATE_AMORT);
        commit;
        delete from IFRS_STG_ILS_VALU where DOWNLOAD_DATE in (select currdate from IFRS_PRC_DATE_AMORT);
        commit;
        delete from IFRS_STG_ILS_TRX where DOWNLOAD_DATE in (select currdate from IFRS_PRC_DATE_AMORT);
        commit;
        delete from IFRS_STG_ILS_LIMIT where DOWNLOAD_DATE in (select currdate from IFRS_PRC_DATE_AMORT);
        commit;
        delete from IFRS_STG_ILS_PAYSET where DOWNLOAD_DATE in (select currdate from IFRS_PRC_DATE_AMORT);
        commit;
        delete from IFRS_STG_ILS_BRANCH where DOWNLOAD_DATE in (select currdate from IFRS_PRC_DATE_AMORT);
        commit;
        delete from IFRS_STG_ILS_COLTR where DOWNLOAD_DATE in (select currdate from IFRS_PRC_DATE_AMORT);
        commit;
        delete from IFRS_STG_ILS_TRADE where DOWNLOAD_DATE in (select currdate from IFRS_PRC_DATE_AMORT);
        commit;
        delete from IFRS_STG_GL_EXCH_RT where DOWNLOAD_DATE in (select currdate from IFRS_PRC_DATE_AMORT);
        commit;
        delete from IFRS_STG_INDUSTRY_DIMENSION where DOWNLOAD_DATE in (select currdate from IFRS_PRC_DATE_AMORT);
        commit;
    elsif MOMENT = 'EOM'
    then
        delete from IFRS_STG_BTRD_IMA where DOWNLOAD_DATE in (select currdate from IFRS_PRC_DATE);
        commit;
        delete from IFRS_STG_BTRD_TRX where DOWNLOAD_DATE in (select currdate from IFRS_PRC_DATE);
        commit;
        delete from IFRS_STG_CRD_IMA where DOWNLOAD_DATE in (select currdate from IFRS_PRC_DATE);
        commit;
        delete from IFRS_STG_CRD_CHARGE where DOWNLOAD_DATE in (select currdate from IFRS_PRC_DATE);
        commit;
        delete from IFRS_STG_TRS_IMAMM where DOWNLOAD_DATE in (select currdate from IFRS_PRC_DATE);
        commit;
        delete from IFRS_STG_TRS_IMASEC where DOWNLOAD_DATE in (select currdate from IFRS_PRC_DATE);
        commit;
        delete from IFRS_MDL_NONGOV where DOWNLOAD_DATE in (select currdate from IFRS_PRC_DATE);
        commit;
        delete from IFRS_STG_ILS_PBMM where DOWNLOAD_DATE in (select currdate from IFRS_PRC_DATE);
        commit;
    elsif MOMENT = 'RKN'
    then
        delete from IFRS_STG_NOSTRO_RAKNOS where DOWNLOAD_DATE in (select currdate from IFRS_PRC_DATE);
        commit;
        delete from IFRS_STG_NOSTRO_MANUAL where DOWNLOAD_DATE in (select currdate from IFRS_PRC_DATE);
        commit;
    elsif MOMENT = 'RATING'
    then
        delete from IFRS_STG_DWH_RATING where DOWNLOAD_DATE in (select currdate from IFRS_PRC_DATE);
        commit;
    end if;
end;