CREATE OR REPLACE PROCEDURE  USPS_APPROVALDBYSCREENID (
   v_ScreenID    IN     VARCHAR2 DEFAULT NULL,
   v_FieldName   IN     VARCHAR2 DEFAULT NULL,
   Cur_out          OUT SYS_REFCURSOR)
AS
BEGIN

   OPEN Cur_out FOR
      SELECT
            DISTINCT b.ScreenID,
                     a.FieldName,
                     TO_CHAR (a.OldValue)
        FROM    tblT_ApprovalDetail a
             LEFT JOIN
                tblT_ApprovalHeader b
             ON a.HeaderID = b.PKID AND b.ACTION IN ('UPDATE', 'DELETE')
       WHERE     b.ScreenID = v_ScreenID
             AND b.STATUS = 'P'
             AND FieldName = v_FieldName
      UNION
      (SELECT DISTINCT screenid,
                       fieldname,
                       TO_CHAR (oldvalue)
         FROM    tblT_ApprovalHeader H
              RIGHT JOIN
                 tblT_ApprovalDetail D
              ON H.PKID = D.HeaderID
        WHERE     H.STATUS = 'P'
              AND screenid = 'MENU_MANAGEMENT'
              AND ACTION IN ('INSERT', 'UPDATE', 'DELETE')
              AND fieldname = 'HeaderID')
      UNION
      (SELECT DISTINCT                                          --a. HeaderID,
                      'GROUPIMPAIRBUCKET_H',
                       'BUCKET_GROUP',
                       TO_CHAR (a.OldValue)
         FROM    tblT_ApprovalDetail a
              LEFT JOIN
                 tblT_ApprovalHeader b
              ON a.HeaderID = b.PKID AND b.ACTION IN ('UPDATE', 'DELETE')
        WHERE     b.ScreenID = 'GROUPIMPAIRBUCKET_D'
              AND A.FieldName = 'Bucket_Group'
              AND b.STATUS = 'P')
      UNION
      (SELECT DISTINCT                                          --a. HeaderID,
                      'BRANCH_GROUP_H',
                       'PKID',
                       TO_CHAR (a.OldValue)
         FROM    tblT_ApprovalDetail a
              LEFT JOIN
                 tblT_ApprovalHeader b
              ON a.HeaderID = b.PKID AND b.ACTION IN ('UPDATE', 'DELETE')
        WHERE     b.ScreenID = 'BRANCH_GROUP_D'
              AND A.FieldName = 'BranchHeaderID'
              AND b.STATUS = 'P')
      UNION
      (SELECT DISTINCT                                          --a. HeaderID,
                      'MAPPING_RULES',
                       'PKID',
                       TO_CHAR (a.OldValue)
         FROM    tblT_ApprovalDetail a
              LEFT JOIN
                 tblT_ApprovalHeader b
              ON a.HeaderID = b.PKID AND b.ACTION IN ('UPDATE', 'DELETE')
        WHERE     b.ScreenID = 'MAPPING_RULES_D'
              AND A.FieldName = 'PKID'
              AND b.STATUS = 'P')
      UNION
      (SELECT DISTINCT                                          --a. HeaderID,
                      'PDRULESEGMENTATION_D',
                       'PKID',
                       TO_CHAR (a.OldValue)
         FROM    tblT_ApprovalDetail a
              LEFT JOIN
                 tblT_ApprovalHeader b
              ON a.HeaderID = b.PKID AND b.ACTION IN ('UPDATE', 'DELETE')
        WHERE     b.ScreenID = 'PDRULESEGMENTATION_D_EDITOR'
              AND A.FieldName = 'PKID'
              AND b.STATUS = 'P')
      UNION
      (SELECT DISTINCT                                          --a. HeaderID,
                      'PARAMETER_COA',
                       'PKID',
                       TO_CHAR (a.OldValue)
         FROM    tblT_ApprovalDetail a
              LEFT JOIN
                 tblT_ApprovalHeader b
              ON a.HeaderID = b.PKID AND b.ACTION IN ('UPDATE', 'DELETE')
        WHERE     b.ScreenID = 'PARAMETER_COA'
              AND A.FieldName = 'PKID'
              AND b.STATUS = 'P');

END;