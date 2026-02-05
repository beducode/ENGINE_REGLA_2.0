CREATE OR REPLACE procedure SP_IFRS_MOVEMENT_CRD_WO(V_CurrDate in date) is
begin

  delete IFRS_MASTER_WO_CRD_CHARGE where download_date = V_CurrDate;

  COMMIT;

  insert into IFRS_MASTER_WO_CRD_CHARGE
    (download_date,
     customer_number,
     chargeoff_date,
     chargeoff_amount,
     chargeoff_status,
     corp_id,
     writeoff_date,
     writeoff_amount)
    select distinct download_date,
           customer_number,
           chargeoff_date,
           chargeoff_amount,
           chargeoff_status,
           corp_id,
           nvl(writeoff_date,download_date) writeoff_date,
           nvl(writeoff_amount,0) writeoff_amount
      from IFRS_STG_CRD_CHARGE
     where download_date = V_CurrDate;

     COMMIT;
end SP_IFRS_MOVEMENT_CRD_WO;