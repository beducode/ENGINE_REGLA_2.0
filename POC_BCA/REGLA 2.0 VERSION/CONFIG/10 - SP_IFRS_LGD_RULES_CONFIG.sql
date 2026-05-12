CREATE OR REPLACE PROCEDURE IFRS9_BCA.SP_IFRS_LGD_RULES_CONFIG (
    P_RUNID         IN VARCHAR2 DEFAULT 'S_00000_0000',
    P_DOWNLOAD_DATE IN DATE     DEFAULT NULL,
    P_PRC           IN VARCHAR2 DEFAULT 'S'
)
AUTHID CURRENT_USER
AS
    ----------------------------------------------------------------
    -- VARIABLES
    ----------------------------------------------------------------
    V_SP_NAME     VARCHAR2(100) := 'SP_IFRS_LGD_RULES_CONFIG';
    V_OWNER       VARCHAR2(30);
    V_CURRDATE      DATE;

    -- TABLE NAMES (UNQUALIFIED PARTS)
    V_TABLEINSERT1  VARCHAR2(100);


    -- MISC
    V_RETURNROWS2   NUMBER := 0;
    V_TABLEDEST     VARCHAR2(100);
    V_COLUMNDEST    VARCHAR2(100);
    V_OPERATION     VARCHAR2(100);
    V_RUNID        VARCHAR2(30);

    -- RESULT QUERY
    V_QUERYS        CLOB;
BEGIN

    ----------------------------------------------------------------
    -- GET OWNER
    ----------------------------------------------------------------
    SELECT USERNAME INTO V_OWNER FROM USER_USERS;

    ----------------------------------------------------------------
    -- INSERT VCURRDATE DETERMINATION IF NULL
    ----------------------------------------------------------------
    IF P_DOWNLOAD_DATE IS NULL THEN
        BEGIN
            SELECT CURRDATE INTO V_CURRDATE FROM IFRS9_BCA.IFRS_PRC_DATE;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE_APPLICATION_ERROR(-20010, 'IFRS_PRC_DATE has no CURRDATE row');
        END;
    ELSE
        V_CURRDATE := P_DOWNLOAD_DATE;
    END IF;

    V_RUNID := NVL(P_RUNID, 'P_00000_0000');

    ----------------------------------------------------------------
    -- TABLE DETERMINATION
    ----------------------------------------------------------------
    V_TABLEINSERT1 := 'GTMP_IFRS_LGD_RULES_CONFIG';


    IFRS9_BCA.SP_IFRS_RUNNING_LOG(V_CURRDATE, V_SP_NAME, V_RUNID, TO_NUMBER(SYS_CONTEXT('USERENV','SESSIONID')), SYSDATE);
    COMMIT;
     
    
    ---- PUT YOUR MAIN CODE HERE (E.G. MERGE STATEMENT)
    -----------------------------------------------------------------
    
	MERGE INTO IFRS9_BCA.GTMP_IFRS_LGD_RULES_CONFIG a
    USING (
        SELECT * 
        FROM (
            SELECT
                A."syscode_lgd_config"      AS SYSCODE_LGD,
                A."lgd_name"                AS LGD_RULE_NAME,
                B.PKID                      AS SEGMENTATION_ID,
                A."workout_period"          AS WORKOUT_PERIOD,
                A."start_historical_date"   AS CUT_OFF_DATE,
                A."historical_data"         AS HISTORICAL_MONTH,
                A."calculation_method"      AS CALC_METHOD,
                A."lgd_method"              AS METHOD,
                1                           AS IS_NEW,
                NVL(A."is_deleted",0)       AS IS_DELETED,
                ROW_NUMBER() OVER (
                    PARTITION BY A."syscode_lgd_config" 
                    ORDER BY A."pkid" DESC
                ) RN
            FROM "LgdConfiguration"@DBCONFIGLINK A 
            JOIN IFRS_SEGMENTATION_MAPPING B 
                ON A."segment_code" = B.SYSCODE_SEGMENTATION
        ) x
        WHERE x.RN = 1
    ) b
    ON (
        a.SYSCODE_LGD = b.SYSCODE_LGD
        AND a.SEGMENTATION_ID = b.SEGMENTATION_ID
        AND a.LGD_RULE_NAME = b.LGD_RULE_NAME
    )

    WHEN MATCHED THEN
        UPDATE SET   
            a.WORKOUT_PERIOD   = b.WORKOUT_PERIOD,
            a.CUT_OFF_DATE     = b.CUT_OFF_DATE,
            a.HISTORICAL_MONTH = b.HISTORICAL_MONTH,
            a.CALC_METHOD      = b.CALC_METHOD,
            a.METHOD           = b.METHOD,
            a.IS_NEW           = b.IS_NEW,
            a.IS_DELETED       = b.IS_DELETED,
            a.UPDATEDBY        = 'SP_IFRS_LGD_RULES_CONFIG',
            a.UPDATEDDATE      = SYSTIMESTAMP,
            a.UPDATEDHOST      = 'LOCALHOST'

    WHEN NOT MATCHED THEN
        INSERT (
            LGD_RULE_NAME,
            SEGMENTATION_ID,
            WORKOUT_PERIOD,
            CUT_OFF_DATE,
            HISTORICAL_MONTH,
            CALC_METHOD,
            METHOD,
            IS_NEW,
            IS_DELETED,
            CREATEDBY,
            CREATEDDATE,
            CREATEDHOST
        )
        VALUES (
            b.LGD_RULE_NAME,
            b.SEGMENTATION_ID,
            b.WORKOUT_PERIOD,
            b.CUT_OFF_DATE,
            b.HISTORICAL_MONTH,
            b.CALC_METHOD,
            b.METHOD,
            1,
            b.IS_DELETED,
            'SYSTEM',
            SYSTIMESTAMP,
            'LOCALHOST'
        );
        
	COMMIT;

    ------------------------------------------------------------------

    DBMS_OUTPUT.PUT_LINE('PROCEDURE ' || V_SP_NAME || ' EXECUTED SUCCESSFULLY.');

    ----------------------------------------------------------------
    -- LOG: CALL EXEC_AND_LOG (ASSUMED SIGNATURE)
    ----------------------------------------------------------------
    V_TABLEDEST := V_OWNER || '.' || V_TABLEINSERT1;
    V_COLUMNDEST := '-';
    V_OPERATION := 'INSERT';

    IFRS9_BCA.SP_IFRS_EXEC_AND_LOG(V_CURRDATE, V_TABLEDEST, V_COLUMNDEST, V_SP_NAME, V_OPERATION, NVL(V_RETURNROWS2,0), V_RUNID);
    COMMIT;

    ----------------------------------------------------------------
    -- RESULT PREVIEW
    ----------------------------------------------------------------
    V_QUERYS := 'SELECT * FROM ' || V_OWNER || '.' || V_TABLEINSERT1;

    IFRS9_BCA.SP_IFRS_RESULT_PREV(V_CURRDATE, V_QUERYS, V_SP_NAME, NVL(V_RETURNROWS2,0), V_RUNID);
    COMMIT;

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;

        RAISE_APPLICATION_ERROR(-20001,'ERROR IN ' || V_SP_NAME || ' : ' || SQLERRM);
END;