CREATE OR REPLACE PROCEDURE  uspS_RevflagByLTMethod
(
    v_blank      IN     NUMBER DEFAULT NULL,
    v_RevolvingFlag IN Varchar2 default ' ',
    Cur_out      OUT SYS_REFCURSOR
)
AS
    v_query   VARCHAR2 (2000);
BEGIN

    IF (v_RevolvingFlag = 'True') THEN
        BEGIN
            OPEN Cur_out FOR

                 SELECT DISTINCT VALUE1 as "LIFETIME_METHOD" , (VALUE1 || ' - ' || DESCRIPTION) AS "Method"
                FROM tblm_commoncodedetail
                WHERE COMMONCODE = 'B115' and VALUE1='Revolving'
                ORDER BY VALUE1 ASC;

        END;
       END IF;
     IF (v_RevolvingFlag = 'False') THEN
        BEGIN
            OPEN Cur_out FOR

                SELECT DISTINCT VALUE1 as "LIFETIME_METHOD", (VALUE1 || ' - ' || DESCRIPTION) AS "Method" FROM tblm_commoncodedetail
                 WHERE COMMONCODE = 'B115' and VALUE1<>'Revolving'
                ORDER BY VALUE1 ASC;
        END;
    END IF;

END;