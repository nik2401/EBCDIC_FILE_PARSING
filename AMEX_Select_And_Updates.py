try:
    import os
    import sys
    import datetime
    import SQL_Connections
    from Logger import Logger
except ModuleNotFoundError as err:
    print(str({err.name.strip()})," module is missing, Command to Install - {"" pip install ", str(err.name.strip()), "}")
    sys.exit(1)

######################################################################################################################################################

logger = Logger()

######################################################################################################################################################

def AMEX_Select(Sel_qry, Connection_String, Inp_Jobid = 0, ArgVar_1 = '', ArgVar_2 = ''):
    if Sel_qry == 1:
        qry = f"SELECT COUNT(1) FROM ClearingFiles WITH(NOLOCK) WHERE FileSource = '{ArgVar_1}' AND FileStatus NOT IN ('DONE','MDONE','MERROR','INQUEUE','READY','PENDING_ACK')"
    elif Sel_qry == 2:
        qry = f"SELECT COUNT(1) FROM ClearingFiles WITH(NOLOCK) WHERE FileSource = '{ArgVar_1}' AND FileStatus IN ('VALIDATION')"
    elif Sel_qry == 3:
        qry = f"SELECT JobId, Path_FileName, FileId FROM ClearingFiles WITH(NOLOCK) WHERE FileSource = '{ArgVar_1}' AND FileStatus IN ('VALIDATION')"
    elif Sel_qry == 4:
        qry = f"SELECT CONVERT(DATE,(MAX(TranTime))) FROM CommonTNP WITH(NOLOCK) WHERE ATID = 60"
    elif Sel_qry == 5:
        qry = f"SELECT COUNT(1) FROM ClearingFiles WITH(NOLOCK) WHERE FileSource = '{ArgVar_1}' AND FileStatus = 'READY' AND FileId LIKE '%{ArgVar_2}%'"
    elif Sel_qry == 6:
        qry = f"SELECT JobId FROM ClearingFiles WITH(NOLOCK) WHERE FileSource = '{ArgVar_1}' AND FileStatus = 'READY' AND FileId LIKE '%{ArgVar_2}%'"
    elif Sel_qry == 7:
        qry = f"SELECT ISNULL(MAX(JobId),0) FROM ClearingFiles WITH(NOLOCK)"
    elif Sel_qry == 8:
        qry = f"SELECT FileStatus FROM ClearingFiles WITH(NOLOCK) WHERE FileSource = '{ArgVar_1}' AND Jobid = {Inp_Jobid}"
    elif Sel_qry == 9:
        qry = f"SELECT FileId, ISNULL(ErrorReason,' '), FileStatus, ISNULL(CompletedStatus,' ') FROM ClearingFiles WITH(NOLOCK) WHERE FileSource = '{ArgVar_1}' AND JobId = {Inp_Jobid}"
    elif Sel_qry == 10:
        qry = f"SELECT FileId, Date_Received, FileDate, TotalTxnReceivedInFile, LastParsedMessageNumber, FileStatus FROM ClearingFiles WITH(NOLOCK) WHERE FileSource = '{ArgVar_1}' AND JobId = {Inp_Jobid}"
    elif Sel_qry == 11:
        qry = f"SELECT COUNT(1) FROM ClearingFiles WITH(NOLOCK) WHERE FileSource = '{ArgVar_1}' AND FileStatus IN ('InQueue')"  
          
    result = SQL_Connections.execute_select_query(Connection_String, qry)
    return result

######################################################################################################################################################
   
def AMEX_Update(Upd_qry ,Connection_String, Inp_Jobid, ArgVar_1 = '', ArgVar_2 = '', ArgVar_3 = '', From = 0, To = 0):
    if Upd_qry == 1:
        qry = f"UPDATE ClearingFiles SET FileStatus = '{ArgVar_1}', CompletedStatus = '{ArgVar_2}', Stage{To}ReachTime = GETDATE(), TimeTaken_Stage{From}ToStage{To-1} = '{ArgVar_3}' WHERE Jobid = {Inp_Jobid}"
    if Upd_qry == 2:
        qry = f"UPDATE ClearingFiles SET CompletedStatus = '{ArgVar_1}', TimeTaken_Stage{From}ToStage{To} = '{ArgVar_2}', FileStatus = '{ArgVar_3}' WHERE JobId = {Inp_Jobid}"
    if Upd_qry == 3:
        qry = f"UPDATE ClearingFiles SET LastParsedMessageNumber = '{From}' WHERE JobId = {Inp_Jobid}"
    if Upd_qry == 4:
        qry = f"UPDATE ClearingFiles SET ErrorReason = '{ArgVar_1}' WHERE JobId = {Inp_Jobid}"
    if Upd_qry == 5:
        qry = f"UPDATE ClrFile SET FileStatus = '{ArgVar_1}', ErrorReason = SUBSTRING((ISNULL(ErrorReason,' ') + ' | {ArgVar_2}'),0,200) FROM ClearingFiles ClrFile WHERE Jobid  = {Inp_Jobid}"
    if Upd_qry == 6:
        qry = f"UPDATE ClearingFiles SET FileValidationStartTime = GETDATE(), CompletedStatus = '{ArgVar_1}' WHERE JobId = {Inp_Jobid}"
        
    SQL_Connections.udf_InsSingleRecIntoDB(Connection_String, qry)
  
######################################################################################################################################################
  
def CreateJobIntoClearingFiles(Connection_String, InFileName, OutFilePath, Upd_InFileName, FileHash, FileSource, FTID, FType):
    iJobId = MaxJobIdFromDB = 0
    IsProdFile = False
        
    IncomingFileExtension = os.path.splitext(Upd_InFileName)[1][1:]
    CurrentTime = datetime.datetime.now().strftime("%H:%M:%S.%f")[:12]
    Date_Received = f"{AMEX_Select(4, Connection_String)[0][0]} {CurrentTime}"
    
    FileDateFromFile = ""
    try:
        if len(InFileName) == 32:
            DatePart = InFileName[18:26]
            TimePart = InFileName[26:32]
            
            FileDateFromFile = datetime.datetime.strptime(DatePart+TimePart,'%d%m%Y%H%M%S')
            IsProdFile = True
            logger.debug(f"FileDateFromFile = {FileDateFromFile}")
        else:
            logger.debug("File does not have Standard Name, FileDate would be assigned as NULL into DB")
    except Exception as e:
        logger.debug(f"Invalid DateTime Formart at position 18-26 and/or 26-32 {e}",True)
        logger.log_exception(*sys.exc_info())
        
    ReadyRecCnt = AMEX_Select(5,Connection_String, ArgVar_1 = FileSource, ArgVar_2 = InFileName)[0][0]

    if ReadyRecCnt > 0:
        JobIdReadyRec = AMEX_Select(6, Connection_String, ArgVar_1 = FileSource, ArgVar_2 = InFileName)[0][0]

        if IsProdFile:
            InsQuery = f"UPDATE ClearingFiles SET FileStatus = 'InQueue', CompletedStatus = 'READY', Date_Received = '{Date_Received}', FileHash = '{FileHash}', FileDate = '{FileDateFromFile}', Path_FileName = '{OutFilePath}', FileTypeId = '{FTID}', FileTypeName = '{FType}', SystemLastDateTime = GETDATE() WHERE JobId = {JobIdReadyRec}"
        else:
            InsQuery = f"UPDATE ClearingFiles SET FileStatus = 'InQueue', CompletedStatus = 'READY', Date_Received = '{Date_Received}', FileHash = '{FileHash}', Path_FileName = '{OutFilePath}', FileTypeId = '{FTID}', FileTypeName = '{FType}', SystemLastDateTime = GETDATE() WHERE JobId = {JobIdReadyRec}"
                
        iJobId = JobIdReadyRec
    
    else:        
        MaxJobIdFromDB = AMEX_Select(7, Connection_String)[0][0]
        iJobId = 100 if MaxJobIdFromDB == 0 else int(MaxJobIdFromDB) + 1
        
        if IsProdFile:
            InsQuery = "INSERT INTO ClearingFiles(FileId, Path_FileName, FileStatus, Date_Received, FileHash, FileSource, FileDate, Jobid, FileExtension, FileTypeId, FileTypeName, SystemLastDateTime)"
            InsQuery = f"{InsQuery} VALUES ( '{Upd_InFileName}', '{OutFilePath}', 'InQueue', '{Date_Received}', '{FileHash}', '{FileSource}', '{FileDateFromFile}', {iJobId}, '{IncomingFileExtension}', {FTID}, '{FType}', GETDATE())"	
        else:
            InsQuery = "INSERT INTO ClearingFiles(FileId, Path_FileName, FileStatus, Date_Received, FileHash, FileSource, Jobid, FileExtension, FileTypeId, FileTypeName, SystemLastDateTime)"
            InsQuery = f"{InsQuery} VALUES ( '{Upd_InFileName}', '{OutFilePath}', 'InQueue', '{Date_Received}', '{FileHash}', '{FileSource}', {iJobId}, '{IncomingFileExtension}', {FTID}, '{FType}', GETDATE())"
    
    SQL_Connections.udf_InsSingleRecIntoDB(Connection_String,InsQuery)            
    
    logger.debug("Clearing File Job Inserted/Modified",True)
    
    return OutFilePath, iJobId, IsProdFile, FileDateFromFile