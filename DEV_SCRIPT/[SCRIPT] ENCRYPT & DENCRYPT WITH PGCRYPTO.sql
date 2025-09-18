CREATE EXTENSION pgcrypto;
		
DROP TABLE IF EXISTS data_testing;
CREATE TABLE data_testing AS
SELECT pgp_sym_encrypt('muhammad abduh', 'Man0f$$$') AS kolomrahasia;

SELECT * FROM data_testing

SELECT pgp_sym_decrypt(kolomrahasia, 'Man0f$$$') FROM data_testing;