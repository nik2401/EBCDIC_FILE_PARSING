try:
    import sys
    import Mail
    import datetime
    import SQL_Connections
    from Logger import Logger
    import AMEX_Select_And_Updates
    # package should be install, if not [Command: pip install modulename]
except ModuleNotFoundError as err:
    print(str({err.name.strip()})," module is missing, Command to Install - {"" pip install ", str(err.name.strip()), "}")
    sys.exit(1)

######################################################################################################################################################

logger = Logger()

######################################################################################################################################################

def SP_Call_ChangeFileStatus(Connection_String, FProcess):
    try:
        SP_result = SQL_Connections.udf_SPCall(Connection_String,f"EXEC PR_AMEXChangeFileStatus '{FProcess}'")
    except Exception as e:
        ErrorReason = f"Error Raised PR_AMEXChangeFileStatus : {e} and result of SP = {SP_result[-1][-1]}"
        logger.debug(ErrorReason, True)
        logger.log_exception(*sys.exc_info())
        sys.exit()
        
    return SP_result[-1][-1]

#############################################################################################################################################

def AMEX_SPCall(Connection_String, Inp_Jobid, Parse_rec_count, ValidationEnable, UseCCard, Stage_StartTime, EmailTo, EmailFrom, SMTP_SERVER, SMTP_PORT, FileSource):

    pfurther = False
    SP_result = 0
    SP_result = SQL_Connections.udf_SPCall(Connection_String,f"EXEC PR_AMEXInsertToInterim {Inp_Jobid},{Parse_rec_count}",Inp_Jobid)
    logger.info(f"SP Result = {SP_result[-1][-1]}")

    pfurther = CheckFileStatus(Connection_String, Inp_Jobid)

    if pfurther and SP_result[-1][-1] == 1 :
        pfurther = False
        SP_result = 0

        Stage_EndTime = datetime.datetime.now()
        TimeDiff = str(Stage_EndTime - Stage_StartTime)
        TimeTaken = f"PARSING {TimeDiff[:-3]}"
        AMEX_Select_And_Updates.AMEX_Update(1, Connection_String, Inp_Jobid, 'UPDATEACCOUNT', 'PARSING', TimeTaken, To = 4, From = 2)
        Stage_StartTime = datetime.datetime.now()
        
        logger.info("************************************ PARSING END ************************************",True)
        
        Mail.SendEmail(3, 'Clearing And Settlement', EmailTo, EmailFrom, SMTP_SERVER, SMTP_PORT, Connection_String, Inp_Jobid, FileSource)
        
        SP_result = SQL_Connections.udf_SPCall(Connection_String,f"EXEC PR_AMEXUpdateAccDetails {Inp_Jobid}",Inp_Jobid)
        logger.info(f"SP Result = {SP_result[-1][-1]}")
        
        pfurther = CheckFileStatus(Connection_String, Inp_Jobid)
        
    else:
        ErrorReason = "File Status in ClearingFiles gets ERROR while executing PR_AMEX_InsertToInterim Or SP Fail To Execute Successfully"
        ErrorReason = ErrorReason.replace("'","")
        AMEX_Select_And_Updates.AMEX_Update(5,Connection_String, Inp_Jobid, 'ERROR', ErrorReason)
        logger.error(ErrorReason)
        sys.exit()

    if pfurther and SP_result[-1][-1] == 1 :
        pfurther = False
        SP_result = 0
        
        Stage_EndTime = datetime.datetime.now()
        TimeDiff = str(Stage_EndTime - Stage_StartTime)
        TimeTaken = f"UPDATEACCOUNT {TimeDiff[:-3]}"
        Stage_StartTime = datetime.datetime.now()
        AMEX_Select_And_Updates.AMEX_Update(1, Connection_String, Inp_Jobid, 'AUTHMATCHING','UPDATEACCOUNT', TimeTaken, To = 5, From = 3)
        Mail.SendEmail(3, 'Clearing And Settlement', EmailTo, EmailFrom, SMTP_SERVER, SMTP_PORT, Connection_String, Inp_Jobid, FileSource)
                
        ProcessingDateTime = datetime.datetime.now()
        Ins_ProcessingDateTime = ProcessingDateTime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        SP_result = SQL_Connections.udf_SPCall(Connection_String,f"EXEC PR_AMEXAuthMatchingDynamic '{Ins_ProcessingDateTime}',{Inp_Jobid},{ValidationEnable}",Inp_Jobid)
        logger.info(f"SP Result = {SP_result[-1][-1]}")   
                    
        pfurther = CheckFileStatus(Connection_String, Inp_Jobid)
        
    else:
        ErrorReason = f"File Status in ClearingFiles gets ERROR while executing PR_AMEXUpdateAccDetails Or SP Fail To Execute Successfully"
        logger.debug(ErrorReason)
        AMEX_Select_And_Updates.AMEX_Update(5,Connection_String, Inp_Jobid, 'ERROR', ErrorReason)
        sys.exit()

    if pfurther and SP_result[-1][-1] == 1 :
        pfurther = False
        SP_result = 0
        
        Stage_EndTime = datetime.datetime.now()
        TimeDiff = str(Stage_EndTime - Stage_StartTime)
        TimeTaken = f"AUTHMATCHING {TimeDiff[:-3]}"
        Stage_StartTime = datetime.datetime.now()
        AMEX_Select_And_Updates.AMEX_Update(1, Connection_String, Inp_Jobid, 'TRANCODEMAPPING','AUTHMATCHING', TimeTaken, To = 6, From = 4)
        Mail.SendEmail(3, 'Clearing And Settlement', EmailTo, EmailFrom, SMTP_SERVER, SMTP_PORT, Connection_String, Inp_Jobid, FileSource)

        SP_result = SQL_Connections.udf_SPCall(Connection_String,f"EXEC PR_AMEXTranCodeMapping {Inp_Jobid}",Inp_Jobid)
        logger.info(f"SP Result = {SP_result[-1][-1]}")            

        pfurther = CheckFileStatus(Connection_String, Inp_Jobid)
        
    else:
        ErrorReason = "File Status in ClearingFiles gets ERROR while executing PR_AMEXAuthMatchingDynamic Or SP Fail To Execute Successfully"
        logger.debug(ErrorReason)
        AMEX_Select_And_Updates.AMEX_Update(5,Connection_String, Inp_Jobid, 'ERROR', ErrorReason)
        sys.exit()

    if pfurther and SP_result[-1][-1] == 1 :
        pfurther = False
        SP_result = 0
        
        Stage_EndTime = datetime.datetime.now()
        TimeDiff = str(Stage_EndTime - Stage_StartTime)
        TimeTaken = f"TRANCODEMAPPING {TimeDiff[:-3]}"
        Stage_StartTime = datetime.datetime.now()
        AMEX_Select_And_Updates.AMEX_Update(1, Connection_String, Inp_Jobid, 'INVALIDTXN','TRANCODEMAPPING', TimeTaken, To = 7, From = 5)
        Mail.SendEmail(3, 'Clearing And Settlement', EmailTo, EmailFrom, SMTP_SERVER, SMTP_PORT, Connection_String, Inp_Jobid, FileSource)
        
        SP_result = SQL_Connections.udf_SPCall(Connection_String,f"EXEC PR_AMEXGenerateTxnForInvalidCard {Inp_Jobid},{ValidationEnable}",Inp_Jobid)
        logger.info(f"SP Result = {SP_result[-1][-1]}")            
            
        pfurther = CheckFileStatus(Connection_String, Inp_Jobid)
        
    else:
        ErrorReason = "File Status in ClearingFiles gets ERROR while executing PR_AMEXTranCodeMapping Or SP Fail To Execute Successfully"
        logger.debug(ErrorReason)
        AMEX_Select_And_Updates.AMEX_Update(5,Connection_String, Inp_Jobid, 'ERROR', ErrorReason)
        sys.exit()

    if pfurther and SP_result[-1][-1] == 1 :
        pfurther = False
        SP_result = 0
        
        Stage_EndTime = datetime.datetime.now()
        TimeDiff = str(Stage_EndTime - Stage_StartTime)
        TimeTaken = f"INVALIDTXN {TimeDiff[:-3]}"
        Stage_StartTime = datetime.datetime.now()
        AMEX_Select_And_Updates.AMEX_Update(1, Connection_String, Inp_Jobid, 'SECONDPRESENTMENT','INVALIDTXN', TimeTaken, To = 8, From = 6)
        Mail.SendEmail(3, 'Clearing And Settlement', EmailTo, EmailFrom, SMTP_SERVER, SMTP_PORT, Connection_String, Inp_Jobid, FileSource)

        SP_result = SQL_Connections.udf_SPCall(Connection_String,f"EXEC PR_AMEXSecondPresentment {Inp_Jobid},{ValidationEnable}",Inp_Jobid)
        logger.info(f"SP Result = {SP_result[-1][-1]}")
            
        pfurther = CheckFileStatus(Connection_String, Inp_Jobid)
        
    else:
        ErrorReason = "File Status in ClearingFiles gets ERROR while executing PR_AMEXGenerateTxnForInvalidCard Or SP Fail To Execute Successfully"
        logger.debug(ErrorReason)
        AMEX_Select_And_Updates.AMEX_Update(5,Connection_String, Inp_Jobid, 'ERROR', ErrorReason)
        sys.exit()

    if pfurther and SP_result[-1][-1] == 1 :
        pfurther = False
        SP_result = 0

        Stage_EndTime = datetime.datetime.now()
        TimeDiff = str(Stage_EndTime - Stage_StartTime)
        TimeTaken = f"SECONDPRESENTMENT {TimeDiff[:-3]}"
        Stage_StartTime = datetime.datetime.now()
        AMEX_Select_And_Updates.AMEX_Update(1, Connection_String, Inp_Jobid, 'CLEARING','SECONDPRESENTMENT', TimeTaken, To = 9, From = 7)
        Mail.SendEmail(3, 'Clearing And Settlement', EmailTo, EmailFrom, SMTP_SERVER, SMTP_PORT, Connection_String, Inp_Jobid, FileSource)

        SP_result = SQL_Connections.udf_SPCall(Connection_String,f"EXEC PR_AMEXGenerateTransactions {Inp_Jobid},{UseCCard},{ValidationEnable}",Inp_Jobid)
        logger.info(f"SP Result = {SP_result[-1][-1]}")
            
        pfurther = CheckFileStatus(Connection_String, Inp_Jobid)
        
    else:
        ErrorReason = "File Status in ClearingFiles gets ERROR while executing PR_AMEXSecondPresentment Or SP Fail To Execute Successfully"
        logger.debug(ErrorReason)
        AMEX_Select_And_Updates.AMEX_Update(5,Connection_String, Inp_Jobid, 'ERROR', ErrorReason)
        sys.exit()

    if pfurther and SP_result[-1][-1] == 1 :
        pfurther = True

        Stage_EndTime = datetime.datetime.now()
        TimeDiff = str(Stage_EndTime - Stage_StartTime)
        TimeTaken = f"CLEARING {TimeDiff[:-3]}"
        Stage_StartTime = datetime.datetime.now()
        AMEX_Select_And_Updates.AMEX_Update(1, Connection_String, Inp_Jobid, 'PENDING_ACK','CLEARING', TimeTaken, To = 10, From = 8)
        Mail.SendEmail(3, 'Clearing And Settlement', EmailTo, EmailFrom, SMTP_SERVER, SMTP_PORT, Connection_String, Inp_Jobid, FileSource)

        logger.info(f"SP Calling completed for JobId = {Inp_Jobid}, Acknowledgment Is Pending")
        logger.info("About To Call SP PR_CreateDisputeAlert For SecondPresentment Alerts",True)
        
        SP_result = SQL_Connections.udf_SPCall(Connection_String,f"EXEC PR_CreateDisputeAlert {Inp_Jobid}",Inp_Jobid)
        logger.info(f"Rec_Count = {SP_result[-1][-1]}")
        
        if SP_result[-1][-1] == 1 :
            pass
        else:
            ErrorReason = "Error Raised PR_CreateDisputeAlert : Error Occur during execution of SP PR_CreateDisputeAlert"
            logger.debug(ErrorReason,True)
        
    else:
        ErrorReason = "File Status in ClearingFiles gets ERROR while executing PR_AMEXGenerateTransactions Or SP Fail To Execute Successfully"
        logger.debug(ErrorReason)
        AMEX_Select_And_Updates.AMEX_Update(5,Connection_String, Inp_Jobid, 'ERROR', ErrorReason)
        sys.exit()

    return pfurther
######################################################################################################################################################

def CheckFileStatus(Connection_String, Inp_Jobid):
    res = AMEX_Select_And_Updates.AMEX_Select(8,Connection_String, Inp_Jobid, ArgVar_1 = 'AMEXCLEARING')
    if res[0][0] == 'ERROR':
        return False
    else:
        return True

######################################################################################################################################################