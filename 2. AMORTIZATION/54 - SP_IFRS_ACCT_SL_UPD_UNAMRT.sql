USE [IFRS9]
GO
/****** Object:  StoredProcedure [dbo].[SP_IFRS_ACCT_SL_UPD_UNAMRT]    Script Date: 14/06/2024 06:32:28 ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[SP_IFRS_ACCT_SL_UPD_UNAMRT]

AS

declare @v_currdate	date
	,@v_prevdate	date	


begin

select @v_currdate=max(currdate),@v_prevdate=max(prevdate) from IFRS_PRC_DATE_AMORT 


insert into IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
VALUES(@v_currdate,current_timestamp,'START','SP_IFRS_ACCT_SL_UPD_UNAMORT','')


-- clean up
update IFRS_IMA_AMORT_CURR
set UNAMORT_FEE_AMT = 0
    ,UNAMORT_COST_AMT = 0
    ,FAIR_VALUE_AMOUNT = null
    ,LOAN_START_AMORTIZATION = null
    ,LOAN_END_AMORTIZATION = null
where DOWNLOAD_DATE=@v_currdate


update IFRS_MASTER_ACCOUNT 
set --UNAMORT_AMT_TOTAL = 0
    --,unamortizedamount_sl = 0
    UNAMORT_FEE_AMT = 0
	--,unamortized_fee_amount_sl = 0
    ,UNAMORT_COST_AMT = 0
	--,unamortized_cost_amount_sl = 0
    ,FAIR_VALUE_AMOUNT = null
    ,LOAN_START_AMORTIZATION = null
    ,LOAN_END_AMORTIZATION = null
where DOWNLOAD_DATE=@v_currdate
 
insert into IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
VALUES(@v_currdate,current_timestamp,'DEBUG','SP_IFRS_ACCT_SL_UPD_UNAMORT','')

--get active ecf
truncate table TMP_B1
insert into TMP_B1(MASTERID)
select distinct masterid 
from IFRS_ACCT_SL_ECF
where AMORTSTOPDATE is null
 
--get last acf id
truncate table TMP_P1
insert into TMP_P1(id)
select max(id) id
from IFRS_ACCT_SL_ACF
where DOWNLOAD_DATE=@v_currdate and masterid in (select masterid from TMP_B1)
group by masterid

insert into IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
VALUES(@v_currdate,current_timestamp,'DEBUG1','SP_IFRS_ACCT_SL_UPD_UNAMORT','')

-- update to master acct
update dbo.IFRS_IMA_AMORT_CURR
set UNAMORT_FEE_AMT = x.n_unamort_fee
    ,UNAMORT_COST_AMT = x.n_unamort_cost
    ,FAIR_VALUE_AMOUNT = dbo.IFRS_IMA_AMORT_CURR.outstanding + x.n_unamort_fee + x.n_unamort_cost
    --,FAIR_VALUE_AMOUNT = dbo.IFRS_IMA_AMORT_CURR.OUTSTANDING_JF + x.n_unamort_fee + x.n_unamort_cost 
	,LOAN_START_AMORTIZATION=x.ecfdate
    ,LOAN_END_AMORTIZATION=x.amortenddate
    ,amort_type='SL'
from 
(	select b.DOWNLOAD_DATE,b.masterid,b.n_unamort_fee,b.n_unamort_cost,b.ecfdate,e.amortenddate
	from IFRS_ACCT_SL_ACF b
	join TMP_P1 c on c.id=b.id
	left join IFRS_ACCT_SL_ECF e on e.masterid=b.masterid and e.prevdate=e.pmtdate and e.DOWNLOAD_DATE=b.ecfdate
) x 
where x.masterid=dbo.IFRS_IMA_AMORT_CURR.masterid
and dbo.IFRS_IMA_AMORT_CURR.DOWNLOAD_DATE = @v_currdate

insert into IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
VALUES(@v_currdate,current_timestamp,'DEBUG2','SP_IFRS_ACCT_SL_UPD_UNAMORT','')

update IFRS_MASTER_ACCOUNT
set UNAMORT_FEE_AMT = b.UNAMORT_FEE_AMT
    ,UNAMORT_COST_AMT = b.UNAMORT_COST_AMT
    ,FAIR_VALUE_AMOUNT = b.FAIR_VALUE_AMOUNT
    ,LOAN_START_AMORTIZATION = b.LOAN_START_AMORTIZATION
    ,LOAN_END_AMORTIZATION = b.LOAN_END_AMORTIZATION
    ,amort_type = b.amort_type
	--20160407 update to sl unamort fields
	--,unamortizedamount_sl = b.unamortizedamount
	--,unamortized_fee_amount_sl = b.unamortized_fee_amount
    --,unamortized_cost_amount_sl = b.unamortized_cost_amount
from IFRS_IMA_AMORT_CURR b
where dbo.IFRS_MASTER_ACCOUNT.MASTERID=b.masterid and dbo.IFRS_MASTER_ACCOUNT.DOWNLOAD_DATE=b.DOWNLOAD_DATE and b.amort_type='SL'
--and b.DOWNLOAD_DATE = @v_currdate


--20160407 update to sl unamort fields
/* pindah ke atas dijadikan single update
update dbo.PMA
set  unamortizedamount_sl = b.unamortizedamount
    ,unamortized_fee_amount_sl = b.unamortized_fee_amount
    ,unamortized_cost_amount_sl = b.unamortized_cost_amount
from IFRS_IMA_AMORT_CURR b
where dbo.PMA.masterid=b.masterid and dbo.PMA.DOWNLOAD_DATE=b.DOWNLOAD_DATE and b.amort_type='SL'
*/

insert into IFRS_AMORT_LOG(DOWNLOAD_DATE,DTM,OPS,PROCNAME,REMARK)
VALUES(@v_currdate,current_timestamp,'END','SP_IFRS_ACCT_SL_UPD_UNAMORT','')

end





GO
