CREATE OR REPLACE PROCEDURE  USPS_LOOKUPPRODCODEALT
(
    v_blank      IN     NUMBER DEFAULT NULL,
    v_DataSource IN Varchar2 default ' ',
    v_ProdType IN Varchar2 default ' ',
    Cur_out      OUT SYS_REFCURSOR
)
AS
    v_query   VARCHAR2 (2000);
BEGIN

    IF (v_blank = 1) THEN
        BEGIN
            IF (v_ProdType <> ' ') THEN
                BEGIN
                    OPEN Cur_out FOR
                        SELECT 'ALL' AS "PRD_CODE", 'ALL product code' AS "Product Code"
                        FROM DUAL
                            UNION ALL

                            SELECT PRD_CODE, "Product Code"
                            FROM
                            (
                                SELECT DISTINCT PRD_CODE, (PRD_CODE || ' - ' || PRD_DESC) AS "Product Code"
                                FROM IFRS_MASTER_PRODUCT_PARAM
                                WHERE PRD_TYPE = v_ProdType
                                ORDER BY PRD_CODE ASC
                            )


                        ;
                END;
            ELSE
                BEGIN
                OPEN Cur_out FOR
                    SELECT 'ALL' AS "PRD_CODE", 'ALL product code' AS "Product Code" FROM DUAL
                        UNION ALL
                    SELECT  PRD_CODE, "Product Code"
                    FROM
                    (
                         SELECT DISTINCT PRD_CODE, (PRD_CODE || ' - ' || PRD_DESC) AS "Product Code"
                    FROM IFRS_MASTER_PRODUCT_PARAM
                    WHERE DATA_SOURCE = v_DataSource
                    ORDER BY PRD_CODE ASC
                    );

                END;
            END IF;
        END;
        ELSE
        BEGIN
            IF (v_ProdType <> ' ') THEN
                BEGIN
                    OPEN Cur_out FOR
                        SELECT DISTINCT PRD_CODE as "PRD_CODE", (PRD_CODE || ' - ' || PRD_DESC) AS "Product Code"
                        FROM IFRS_MASTER_PRODUCT_PARAM
                        WHERE PRD_TYPE = v_ProdType
                        ORDER BY PRD_CODE ASC;
                END;
            ELSE
                BEGIN
                    OPEN Cur_out FOR
                        SELECT DISTINCT PRD_CODE as "PRD_CODE", (PRD_CODE || ' - ' || PRD_DESC) AS "Product Code" FROM IFRS_MASTER_PRODUCT_PARAM
                        WHERE DATA_SOURCE = v_DataSource
                        ORDER BY PRD_CODE ASC;
                END;
            END IF;
        END;
    END IF;

END;