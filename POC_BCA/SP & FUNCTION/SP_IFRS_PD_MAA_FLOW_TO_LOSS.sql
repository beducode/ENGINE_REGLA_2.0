CREATE OR REPLACE PROCEDURE SP_IFRS_PD_MAA_FLOW_TO_LOSS
AS
  v_CURRDATE DATE;
  v_COUNT   NUMBER;
  v_CURSOR NUMBER;
  v_MAX_ID  NUMBER;
  v_PD_RULE_ID NUMBER;
  v_TEMP NUMBER;
  v_MIG_LOSS_BUCKET NUMBER;
BEGIN
    SELECT CURRDATE
	INTO v_CURRDATE
	FROM IFRS_PRC_DATE;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_IFRS_PD_RUNNING_DATE';

    INSERT INTO TMP_IFRS_PD_RUNNING_DATE
    (
        EFF_DATE,
        BASE_DATE,
        PD_RULE_ID,
        BUCKET_GROUP,
        MIG_LOSS_BUCKET
    )
    SELECT
        v_CURRDATE AS EFF_DATE,
		LAST_DAY(ADD_MONTHS(v_CURRDATE, A.INCREMENT_PERIOD * -1)) AS BASE_DATE,
	    A.PKID AS PD_RULE_ID,
		A.BUCKET_GROUP,
		A.MIG_LOSS_BUCKET
    FROM IFRS_PD_RULES_CONFIG A
    WHERE NVL(A.ACTIVE_FLAG,0) = 1
	AND IS_DELETED = 0
	AND PD_METHOD ='MAA'
	AND A.DERIVED_PD_MODEL IS NULL;

    COMMIT;

    DELETE IFRS_PD_MAA_FLOW_TO_LOSS
    WHERE EFF_DATE = v_CURRDATE
        AND PD_RULE_ID IN (SELECT PD_RULE_ID FROM TMP_IFRS_PD_RUNNING_DATE);

    COMMIT;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE TMP_IFRS_PD_MAX_BUCKET';

    INSERT INTO TMP_IFRS_PD_MAX_BUCKET
    (
        PKID,
        PD_RULE_ID,
        MAX_BUCKET_ID
    )
    SELECT ROWNUM,
        PD_RULE_ID,
        MAX_BUCKET_ID
    FROM
    (
        SELECT A.PD_RULE_ID,
            MAX(A.BUCKET_FROM) AS MAX_BUCKET_ID
        FROM IFRS_PD_MAA_FLOWRATE A
        INNER JOIN TMP_IFRS_PD_RUNNING_DATE B
        ON A.PD_RULE_ID = B.PD_RULE_ID
            AND A.BASE_DATE = B.BASE_DATE
            AND A.EFF_DATE = B.EFF_DATE
        GROUP BY A.PD_RULE_ID
    ) A;

    COMMIT;


    v_CURSOR := 1;
    SELECT COUNT(1) INTO v_COUNT FROM TMP_IFRS_PD_MAX_BUCKET;

    WHILE v_CURSOR <= v_COUNT LOOP

        SELECT PD_RULE_ID, MAX_BUCKET_ID
        INTO v_PD_RULE_ID, v_MAX_ID
        FROM TMP_IFRS_PD_MAX_BUCKET
        WHERE PKID = v_CURSOR;

        SELECT MIG_LOSS_BUCKET
        INTO v_MIG_LOSS_BUCKET
        FROM TMP_IFRS_PD_RUNNING_DATE
        WHERE PD_RULE_ID = v_PD_RULE_ID;

        EXECUTE IMMEDIATE ' TRUNCATE TABLE TMP_IFRS_PD_MAA_FLOW_TO_LOSS ';

        v_TEMP := v_MAX_ID;
        WHILE v_TEMP > 0 LOOP

            INSERT INTO TMP_IFRS_PD_MAA_FLOW_TO_LOSS
            (
                BASE_DATE,
                EFF_DATE,
                PD_RULE_ID,
                --PROJECTION_DATE,
                BUCKET_GROUP,
                BUCKET_ID,
                FLOW_TO_LOSS
            )
              SELECT C.BASE_DATE,
                    C.EFF_DATE,
                    A.PD_RULE_ID,
                    --A.EFF_DATE PROJECTION_DATE,
                    A.BUCKET_GROUP,
                    A.BUCKET_FROM,
                     CASE
                       WHEN A.BUCKET_FROM = v_MAX_ID THEN
                        1
                       ELSE
                        CASE
                          WHEN NVL(B.FLAG_DEFAULT, 0) = 1 THEN
                           NVL(B.PD_DEFAULT, 0)
                          ELSE
                           SUM(NVL(A.FLOWRATE, 0) * NVL(D.FLOW_TO_LOSS, 0))
                        END
                     END FLOW_TO_LOSS
                FROM IFRS_PD_MAA_FLOWRATE A
                JOIN IFRS_BUCKET_DETAIL B
                ON A.BUCKET_GROUP = B.BUCKET_GROUP
                AND A.EFF_DATE = v_CURRDATE
                AND A.PD_RULE_ID = v_PD_RULE_ID
                AND A.BUCKET_FROM = B.BUCKET_ID
                AND A.BUCKET_FROM = v_TEMP
                AND A.BUCKET_TO >= v_TEMP
                --AND A.BUCKET_TO > v_MIG_LOSS_BUCKET
                JOIN TMP_IFRS_PD_RUNNING_DATE C
                ON A.PD_RULE_ID = C.PD_RULE_ID
                LEFT JOIN TMP_IFRS_PD_MAA_FLOW_TO_LOSS D
                  ON A.PD_RULE_ID = D.PD_RULE_ID
                 AND A.BUCKET_TO = D.BUCKET_ID
               GROUP BY C.BASE_DATE,
                    C.EFF_DATE,
                    A.PD_RULE_ID,
                    A.EFF_DATE,
                    A.BUCKET_GROUP,
                    A.BUCKET_FROM,
                    B.FLAG_DEFAULT,
                    B.PD_DEFAULT;

            COMMIT;

            v_TEMP := v_TEMP - 1;

        END LOOP;

        INSERT INTO IFRS_PD_MAA_FLOW_TO_LOSS
        (
            EFF_DATE,
            BASE_DATE,
            PD_RULE_ID,
            BUCKET_GROUP,
            BUCKET_ID,
            FLOW_TO_LOSS
        )
        SELECT
            A.EFF_DATE,
            A.BASE_DATE,
            A.PD_RULE_ID,
            A.BUCKET_GROUP,
            A.BUCKET_ID,
            A.FLOW_TO_LOSS
        FROM TMP_IFRS_PD_MAA_FLOW_TO_LOSS A
        JOIN TMP_IFRS_PD_RUNNING_DATE B
        ON A.PD_RULE_ID = B.PD_RULE_ID
        ORDER BY A.BUCKET_ID;

        COMMIT;

        v_CURSOR := v_CURSOR + 1;

    END LOOP;

END;