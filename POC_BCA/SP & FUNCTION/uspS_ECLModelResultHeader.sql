CREATE OR REPLACE Procedure       uspS_ECLModelResultHeader
(
      Cur_out OUT SYS_REFCURSOR
)
as
begin

    OPEN Cur_out FOR

    select A.PKID, A.ECL_MODEL_NAME
    from ifrs_ecl_model_header A
    order by A.ECL_MODEL_NAME;

end;