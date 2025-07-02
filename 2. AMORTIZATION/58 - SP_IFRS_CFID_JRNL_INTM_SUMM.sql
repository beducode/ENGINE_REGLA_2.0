USE [IFRS9]
GO
/****** Object:  StoredProcedure [dbo].[SP_IFRS_CFID_JRNL_INTM_SUMM]    Script Date: 14/06/2024 06:32:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[SP_IFRS_CFID_JRNL_INTM_SUMM]
AS
declare @v_currdate    date
    ,@v_prevdate    date

begin
 
select @v_currdate=max(currdate),@v_prevdate=max(prevdate) from IFRS_PRC_DATE_AMORT


insert into IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
VALUES(@v_currdate,current_timestamp,'START','SP_IFRS_CFID_JOURNAL_INTM_SUMM','')

--delete first
truncate table TMP_CFID1
truncate table TMP_CFID2

delete from IFRS_CFID_JOURNAL_INTM_SUMM where DOWNLOAD_DATE=@v_currdate

insert into TMP_CFID1(masterid,cf_id,ITRCG_AMT,AMORT_AMT)
select masterid,cf_id,ITRCG_AMT,AMORT_AMT
from IFRS_CFID_JOURNAL_INTM_SUMM
where DOWNLOAD_DATE=@v_prevdate

insert into TMP_CFID1(masterid,cf_id,ITRCG_AMT,AMORT_AMT)
select masterid,cf_id
,sum(case when journalcode2 IN('ITRCG','ITRCG_SL') then case when reverse='N' then n_amount else -1 * n_amount end else 0 end)
,sum(case when journalcode2 IN ('ACCRU','ACCRU_SL') then case when reverse='N' then n_amount else -1 * n_amount end else 0 end)
from IFRS_ACCT_JOURNAL_INTM
where DOWNLOAD_DATE=@v_currdate
group by masterid,cf_id

insert into TMP_CFID2(masterid,cf_id,ITRCG_AMT,AMORT_AMT)
select masterid,cf_id,sum(ITRCG_AMT),sum(AMORT_AMT)
from TMP_CFID1
group by masterid,cf_id

insert into IFRS_CFID_JOURNAL_INTM_SUMM(DOWNLOAD_DATE,masterid,cf_id,itrcg_amt,amort_amt,unamort_amt,createddate)
select @v_currdate,masterid,cf_id,itrcg_amt,amort_amt,(itrcg_amt+amort_amt),current_timestamp
from TMP_CFID2

insert into IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
VALUES(@v_currdate,current_timestamp,'END','SP_IFRS_CFID_JOURNAL_INTM_SUMM','')

end





GO
