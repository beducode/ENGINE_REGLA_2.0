CREATE OR REPLACE EDITIONABLE PROCEDURE PSAK413.SP_IFRS_EIL_MODEL_SCENARIO
	p_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    p_download_date IN DATE,
    p_model_id      IN NUMBER DEFAULT 0,
    p_prc           IN CHAR   DEFAULT 'M'
)
IS
    v_currdate    	DATE;
    v_ecl_model_id 	NUMBER := 0;
    v_script      	CLOB;
    v_tmp1        	VARCHAR2(100);
BEGIN
    ------------------------------------------------------------------
    -- Set tanggal
    ------------------------------------------------------------------
    IF p_DOWNLOAD_DATE IS NULL THEN
        SELECT CURRDATE INTO v_CURRDATE FROM IFRS_PRC_DATE;
    ELSE
        v_currdate := p_DOWNLOAD_DATE;
    END IF;
    ------------------------------------------------------------------
    -- Ambil ECL Model ID
    ------------------------------------------------------------------
    SELECT pkid
    INTO   v_ecl_model_id
    FROM   IFRS_EIL_MODEL_HEADER
    WHERE  (pkid = p_model_id OR p_model_id = 0) AND (active_status = 1)
    FETCH FIRST 1 ROW ONLY;
    ------------------------------------------------------------------
    -- Nama tabel target
    ------------------------------------------------------------------
    v_tmp1 := 'IFRS_EIL_MODEL_SCENARIO';
    ------------------------------------------------------------------
    -- Build dynamic SQL
    ------------------------------------------------------------------
    v_script := 'TRUNCATE TABLE ' || v_tmp1;

    EXECUTE IMMEDIATE v_script;

    v_script := 'INSERT INTO ' || v_tmp1 || '
        (DOWNLOAD_DATE, EIL_MODEL_ID, SEGMENTATION_ID, SCENARIO_NO,
         PROB, PRC, CREATEDBY, CREATEDDATE, CREATEDHOST)
    SELECT DISTINCT
        DATE ''' || TO_CHAR(v_currdate, 'YYYY-MM-DD') || ''' AS DOWNLOAD_DATE,
        A.PKID AS ECL_MODEL_ID,
        B.SEGMENTATION_ID,
        G.SCENARIO_NO,
        NVL(G.PROB, 0),
        ''' || p_prc || ''' AS PRC,
        ''SP_IFRS_ECL_MODEL_SCENARIO'' AS CREATEDBY,
        SYSDATE AS CREATEDDATE,
        '''' AS CREATEDHOST
    FROM IFRS_EIL_MODEL_HEADER A
    JOIN IFRS_EIL_MODEL_DETAIL_EAD B
        ON A.PKID = B.ECL_MODEL_ID
    JOIN IFRS_EIL_MODEL_DETAIL_PF F
        ON A.PKID = F.ECL_MODEL_ID
       AND B.SEGMENTATION_ID = F.SEGMENTATION_ID
    CROSS JOIN (
        SELECT 0 AS SCENARIO_NO, CAST(1 AS NUMBER) AS PROB FROM DUAL
        UNION ALL
        SELECT 1, CAST(F.SCENARIO_1_PROB_OUTCOME / 100 AS NUMBER) FROM DUAL
        UNION ALL
        SELECT 2, CAST(F.SCENARIO_2_PROB_OUTCOME / 100 AS NUMBER) FROM DUAL
        UNION ALL
        SELECT 3, CAST(F.SCENARIO_3_PROB_OUTCOME / 100 AS NUMBER) FROM DUAL
        UNION ALL
        SELECT 4, CAST(F.SCENARIO_4_PROB_OUTCOME / 100 AS NUMBER) FROM DUAL
        UNION ALL
        SELECT 5, CAST(F.SCENARIO_5_PROB_OUTCOME / 100 AS NUMBER) FROM DUAL
    ) G
    WHERE ((' || p_model_id || ' = 0 AND A.ACTIVE_STATUS = 1)
           OR A.PKID = ' || p_model_id || ')
    ';

    EXECUTE IMMEDIATE v_script;
	COMMIT;
END