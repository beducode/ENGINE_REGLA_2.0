CREATE OR REPLACE PROCEDURE USPU_CANCELTHREAD
(
    V_Status VARCHAR2 DEFAULT ' '
)
AS
BEGIN

update TBLT_APPLICATIONLOG
 set Finish_Time = SYSDATE,
 Status = V_Status,
 Message = 'Thread Canceled when agent start at ' ||  to_char(sysdate, 'dd-MON-yyyy hh:mm')
 where FINISH_TIME is null ;

END;