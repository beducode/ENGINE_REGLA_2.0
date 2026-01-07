
DECLARE
  -- Daftar schema yang ingin dibuat
  TYPE t_schema_list IS TABLE OF VARCHAR2(100);
    v_schemas t_schema_list := t_schema_list(
    'NTT_APPROVAL',
    'NTT_AUDIT',
    'NTT_CUSTOM_REPORT',
    'NTT_DATA_MANAGENENT',
    'NTT_EMAIL_NOTIFICATION',
    'NTT_FILE_MANAGER',
    'NTT_HANGFIRE',
    'NTT_JOURNAL',
    'NTT_PARAMETER',
    'NTT_PLATFORM_SETTING',
    'NTT_PSAK413_IMPAIRMENT',
    'NTT_RISK_MODELLING',
    'NTT_USER',
    'NTT_WORKFLOW'
    );

  -- Nama user admin yang akan akses semua schema
  v_admin_user VARCHAR2(50) := 'REGLAAPPS';
  v_admin_pass VARCHAR2(50) := 'Gp8USXo48nTCIb7U1kSgsB2';
  v_sql VARCHAR2(4000);
BEGIN
  -- 1️⃣ Buat user admin jika belum ada
  BEGIN
    v_sql := 'CREATE USER ' || v_admin_user || ' IDENTIFIED BY ' || v_admin_pass ||
             ' DEFAULT TABLESPACE users TEMPORARY TABLESPACE temp QUOTA UNLIMITED ON users';
    EXECUTE IMMEDIATE v_sql;
  EXCEPTION
    WHEN OTHERS THEN
      IF SQLCODE = -01920 THEN
        DBMS_OUTPUT.PUT_LINE('User ' || v_admin_user || ' sudah ada.');
      ELSE
        DBMS_OUTPUT.PUT_LINE('Info: ' || SQLERRM);
      END IF;
  END;

  -- Berikan hak akses dasar ke admin
  BEGIN
    EXECUTE IMMEDIATE 'GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE, CREATE PROCEDURE TO ' || v_admin_user;
  EXCEPTION WHEN OTHERS THEN NULL;
  END;

  -- 2️⃣ Loop: buat semua schema dari daftar
  FOR i IN 1 .. v_schemas.COUNT LOOP
    BEGIN
      v_sql := 'CREATE USER ' || v_schemas(i) || ' IDENTIFIED BY ' || LOWER(v_schemas(i)) ||
               ' DEFAULT TABLESPACE users TEMPORARY TABLESPACE temp QUOTA UNLIMITED ON users';
      EXECUTE IMMEDIATE v_sql;
      DBMS_OUTPUT.PUT_LINE('User/schema ' || v_schemas(i) || ' berhasil dibuat.');
    EXCEPTION
      WHEN OTHERS THEN
        IF SQLCODE = -01920 THEN
          DBMS_OUTPUT.PUT_LINE('User/schema ' || v_schemas(i) || ' sudah ada.');
        ELSE
          DBMS_OUTPUT.PUT_LINE('Error buat ' || v_schemas(i) || ': ' || SQLERRM);
        END IF;
    END;

    -- Berikan hak akses dasar untuk tiap schema
    BEGIN
      EXECUTE IMMEDIATE 'GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE SEQUENCE TO ' || v_schemas(i);
      EXECUTE IMMEDIATE 'GRANT UNLIMITED TABLESPACE TO ' || v_schemas(i);
    EXCEPTION WHEN OTHERS THEN NULL;
    END;

    -- 3️⃣ Berikan akses admin ke semua schema
    BEGIN
      EXECUTE IMMEDIATE 'GRANT SELECT ANY TABLE TO ' || v_admin_user;
      EXECUTE IMMEDIATE 'GRANT INSERT ANY TABLE TO ' || v_admin_user;
      EXECUTE IMMEDIATE 'GRANT UPDATE ANY TABLE TO ' || v_admin_user;
      EXECUTE IMMEDIATE 'GRANT DELETE ANY TABLE TO ' || v_admin_user;
    EXCEPTION WHEN OTHERS THEN NULL;
    END;
  END LOOP;

  DBMS_OUTPUT.PUT_LINE('Selesai membuat schema dan memberikan akses.');
END;