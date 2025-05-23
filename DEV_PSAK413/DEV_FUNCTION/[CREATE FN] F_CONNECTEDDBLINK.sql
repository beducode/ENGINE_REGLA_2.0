-- DROP FUNCTION IF EXISTS F_CONNECTEDDBLINK

CREATE OR REPLACE FUNCTION F_CONNECTEDDBLINK() 
RETURNS text
LANGUAGE plpgsql
AS $$
BEGIN
    RETURN(
	SELECT dblink_disconnect('conn_db_link')
	);

	EXCEPTION 
	WHEN OTHERS THEN
	RETURN (
		SELECT dblink_connect('conn_db_link', 'workflow_db_access')
	);
END;
$$ 