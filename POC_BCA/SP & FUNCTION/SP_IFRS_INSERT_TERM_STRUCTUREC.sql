CREATE OR REPLACE PROCEDURE SP_IFRS_INSERT_TERM_STRUCTUREC (v_MODEL_ID NUMBER default null)
AS
   vPkIdFrom      NUMBER;
   vPkIdTo        NUMBER;
   vPDRuleNameTo    VARCHAR (150);
   vBucketGruopTo   VARCHAR (30);
   vEffDate date;
BEGIN

   SELECT config.pkid,
               config.PD_RULE_NAME,
               config.BUCKET_GROUP
     INTO vPkIdTo,
              vPDRuleNameTo,
              vBucketGruopTo
     FROM TBLM_COMMONCODEDETAIL common, IFRS_PD_RULES_CONFIG config
    WHERE common.COMMONCODE = 'B1004' AND PD_RULE_NAME = common.value1;

   SELECT config.pkid
     INTO vPkIdFrom
     FROM TBLM_COMMONCODEDETAIL common, IFRS_PD_RULES_CONFIG config
    WHERE common.COMMONCODE = 'B1004' AND PD_RULE_NAME = common.value2;*
    IF v_MODEL_ID IS NULL THEN
        BEGIN
           SELECT MAX (eff_date)
             INTO vEffDate
             FROM IFRS_PD_TERM_STRUCTURE
            WHERE PD_RULE_ID = vPkIdFrom
                AND MODEL_ID <> 0;
        END;
        ELSE
            BEGIN
               SELECT MAX (eff_date)
                 INTO vEffDate
                 FROM IFRS_PD_TERM_STRUCTURE
                WHERE PD_RULE_ID = vPkIdFrom
                    AND MODEL_ID <> 0
                    AND MODEL_ID = v_MODEL_ID;
            END;
    END IF;
*/

   SELECT MAX (eff_date)
     INTO vEffDate
     FROM IFRS_PD_TERM_STRUCTURE
    WHERE PD_RULE_ID = vPkIdFrom
        AND MODEL_ID <> 0
        AND MODEL_ID = nvl(v_MODEL_ID, MODEL_ID);

DELETE IFRS_PD_TERM_STRUCTURE
 WHERE PD_RULE_ID = vPkIdTo
 and eff_date = vEffDate ;

 COMMIT;

INSERT INTO IFRS_PD_TERM_STRUCTURE (EFF_DATE,
                                    BASE_DATE,
                                    PD_RULE_ID,
                                    PD_RULE_NAME,
                                    MODEL_ID,
                                    BUCKET_GROUP,
                                    BUCKET_ID,
                                    FL_SEQ,
                                    FL_YEAR,
                                    FL_MONTH,
                                    FL_DATE,
                                    PD,
                                    PRODUCTION_PD,
                                    OVERRIDE_PD,
                                    PRC_FLAG,
                                    TM_TYPE)
SELECT ts.EFF_DATE,
          ts.BASE_DATE,
          vPkIdTo PD_RULE_ID,
          vPDRuleNameTo PD_RULE_NAME,
          0 MODEL_ID,
          vBucketGruopTo BUCKET_GROUP,
          ts.BUCKET_ID,
          ts.FL_SEQ,
          ts.FL_YEAR,
          ts.FL_MONTH,
          ts.FL_DATE,
          ts.PD,
          ts.PRODUCTION_PD,
          ts.OVERRIDE_PD,
          ts.PRC_FLAG,
          ts.TM_TYPE
     FROM IFRS_PD_TERM_STRUCTURE ts
    WHERE     ts.PD_RULE_ID = vPkIdFrom
          AND TS.MODEL_ID <> 0
          and TS.EFF_DATE = vEffDate;

COMMIT;

INSERT INTO IFRS_PD_TERM_STRUCTURE (EFF_DATE,
                                    BASE_DATE,
                                    PD_RULE_ID,
                                    PD_RULE_NAME,
                                    MODEL_ID,
                                    BUCKET_GROUP,
                                    BUCKET_ID,
                                    FL_SEQ,
                                    FL_YEAR,
                                    FL_MONTH,
                                    FL_DATE,
                                    PD,
                                    PRODUCTION_PD,
                                    OVERRIDE_PD,
                                    PRC_FLAG,
                                    TM_TYPE)
     SELECT EFF_DATE,
            BASE_DATE,
            PD_RULE_ID,
            PD_RULE_NAME,
            MODEL_ID,
            BUCKET_GROUP,
            BUCKET_ID,
            FL_SEQ,
            FL_YEAR,
            FL_MONTH,
            FL_DATE,
            AVG (PD),
            AVG (PRODUCTION_PD) PRODUCTION_PD,
            AVG (OVERRIDE_PD) OVERRIDE_PD,
            PRC_FLAG,
            TM_TYPE
       FROM (SELECT ts.EFF_DATE,
                    ts.BASE_DATE,
                    vPkIdTo PD_RULE_ID,
                    vPDRuleNameTo PD_RULE_NAME,
                    0 MODEL_ID,
                    vBucketGruopTo BUCKET_GROUP,
                    12 BUCKET_ID,
                    ts.FL_SEQ,
                    ts.FL_YEAR,
                    ts.FL_MONTH,
                    ts.FL_DATE,
                    ts.PD,
                    ts.PRODUCTION_PD,
                    ts.OVERRIDE_PD,
                    ts.PRC_FLAG,
                    ts.TM_TYPE
               FROM IFRS_PD_TERM_STRUCTURE ts
              WHERE     ts.PD_RULE_ID = vPkIdFrom
                    AND TS.MODEL_ID <> 0
                    AND BUCKET_ID < 8
                    and TS.EFF_DATE = vEffDate) a
   GROUP BY EFF_DATE,
            BASE_DATE,
            PD_RULE_ID,
            PD_RULE_NAME,
            MODEL_ID,
            BUCKET_GROUP,
            BUCKET_ID,
            FL_SEQ,
            FL_YEAR,
            FL_MONTH,
            FL_DATE,
            PRC_FLAG,
            TM_TYPE;



COMMIT;

END;