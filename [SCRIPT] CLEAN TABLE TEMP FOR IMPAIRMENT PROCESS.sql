DO
$$
DECLARE
	V_TABLENAME VARCHAR(100);
	V_STR_QUERY TEXT;
BEGIN
	FOR V_TABLENAME IN
	SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND (
	table_name LIKE '%dpd_1%'
	OR table_name LIKE '%dpd_2%'
	OR table_name LIKE '%dpd_3%'
	OR table_name LIKE '%dpd_4%'
	OR table_name LIKE '%dpd_5%')
	LOOP
		V_STR_QUERY := '';
		V_STR_QUERY := V_STR_QUERY || ' DROP TABLE IF EXISTS ' || V_TABLENAME || '';
		EXECUTE (V_STR_QUERY);
		RAISE NOTICE 'DROP TABLE ---> %', V_STR_QUERY;
	END LOOP;
END;
$$;
