CREATE OR REPLACE PROCEDURE SP_UPDATE_EOM_ECL_REPORT_EMAIL(
    P_PKID IN NUMBER,
    P_MAIL_TO IN VARCHAR2 DEFAULT NULL,
    P_MAIL_CC IN VARCHAR2 DEFAULT NULL,
    P_MAIL_BCC IN VARCHAR2 DEFAULT NULL,
    P_MAIL_SUBJECT IN VARCHAR2 DEFAULT NULL,
    P_MAIL_MESSAGE IN VARCHAR2 DEFAULT NULL
)
IS
BEGIN
    UPDATE IFRS.IFRS_EMAIL
    SET
        MAIL_TO = CASE
                     WHEN P_MAIL_TO = 'null' THEN NULL
                     WHEN P_MAIL_TO = '0' THEN MAIL_TO
                     ELSE P_MAIL_TO
                 END,
        MAIL_CC = CASE
                     WHEN P_MAIL_CC = 'null' THEN NULL
                     WHEN P_MAIL_CC = '0' THEN MAIL_CC
                     ELSE P_MAIL_CC
                 END,
        MAIL_BCC = CASE
                     WHEN P_MAIL_BCC = 'null' THEN NULL
                     WHEN P_MAIL_BCC = '0' THEN MAIL_BCC
                     ELSE P_MAIL_BCC
                 END,
        MAIL_SUBJECT = CASE
                         WHEN P_MAIL_SUBJECT = 'null' THEN NULL
                         WHEN P_MAIL_SUBJECT = '0' THEN MAIL_SUBJECT
                         ELSE P_MAIL_SUBJECT
                       END,
        MAIL_MESSAGE = CASE
                         WHEN P_MAIL_MESSAGE = 'null' THEN NULL
                         WHEN P_MAIL_MESSAGE = '0' THEN MAIL_MESSAGE
                         ELSE P_MAIL_MESSAGE
                       END
    WHERE PKID = P_PKID;

    COMMIT;
END;