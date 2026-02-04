CREATE OR REPLACE FUNCTION FN_LGD_DAYS_30_360 (fromDate IN DATE, toDate IN DATE)
RETURN NUMBER
AS
    y1 NUMBER:= EXTRACT(YEAR FROM fromDate);
    m1 NUMBER:= EXTRACT(MONTH FROM fromDate);
    d1 NUMBER:= EXTRACT(DAY FROM fromDate);
    y2 NUMBER:= EXTRACT(YEAR FROM toDate);
    m2 NUMBER:= EXTRACT(MONTH FROM toDate);
    d2 NUMBER:= EXTRACT(DAY FROM toDate);
BEGIN

	--- If from date is last of february, set @d1=30.
    IF (m1=2 AND EXTRACT(DAY FROM fromDate+1)=1) THEN
        d1:=30;
    END IF;

	--- If to date is last of february, set @d1=30.
	IF (m2=2 AND EXTRACT(DAY FROM toDate+1)=1) THEN
        d2:=30;
    END IF;

	--- Starting and ending dates on the 31st become the 30th.
    IF (d1=31) THEN d1:=30; END IF;
    IF (d2=31) THEN d2:=30; END IF;

    --- Add it all together and return
    RETURN 360*(y2-y1) + 30*(m2-m1) + (d2-d1);

END;