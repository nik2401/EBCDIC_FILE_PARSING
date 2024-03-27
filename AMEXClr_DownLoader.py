#Import libraries
try:
    import os
    import sys
    import datetime
    import Functions
    import AMEX_Select_And_Updates
    import SQL_Connections as SQL_Connect  
    from SetUp import SetUp

    # package should be install, if not [Command: pip install modulename]
except ModuleNotFoundError as err:
    print(str({err.name.strip()})," module is missing, Command to Install - {"" pip install ", str(err.name.strip()), "}")
    sys.exit(1)

######################################################################################################################################################
def CreateJobIntoClearingFiles(Connection_String, InFileName, OutFilePath, Upd_InFileName, FileHash, FileSource, FTID, FType):
    iJobId = MaxJobIdFromDB = 0
    IsProdFile = False
        
    IncomingFileExtension = os.path.splitext(Upd_InFileName)[1][1:]
    CurrentTime = datetime.datetime.now().strftime("%H:%M:%S.%f")[:12]
    Date_Received = f"{AMEX_Select_And_Updates.AMEX_Select(4, Connection_String)[0][0]} {CurrentTime}"
    
    FileDateFromFile = ""

    MaxJobIdFromDB = AMEX_Select_And_Updates.AMEX_Select(7, Connection_String)[0][0]
    iJobId = 100 if MaxJobIdFromDB == 0 else int(MaxJobIdFromDB) + 1

    InsQuery = "INSERT INTO ClearingFiles(FileId, Path_FileName, FileStatus, Date_Received, FileHash, FileSource, Jobid, FileExtension, FileTypeId, FileTypeName, SystemLastDateTime)"
    InsQuery = f"{InsQuery} VALUES ( '{Upd_InFileName}', '{OutFilePath}', 'READY', '{Date_Received}', '{FileHash}', '{FileSource}', {iJobId}, '{IncomingFileExtension}', {FTID}, '{FType}', GETDATE())"
    
    SQL_Connect.udf_InsSingleRecIntoDB(Connection_String,InsQuery)            
    
    print("Clearing File Job Inserted/Modified Successfully",True)
    
    return OutFilePath, iJobId, IsProdFile, FileDateFromFile

if __name__ == "__main__":
    FTP_Path = "F:\\Project\\AMEX_Python\\Dump\\AMEXClr\\INTERIM\\"
    InPath = ""
    global Parse_rec_count, FProcess,FileInError, Inp_Jobid, FTID, FType, EnvVariable_Dict, Rec_Count, GotoReadFile, IsProdFileName, FileDateFromFile, ErrorReason

    # Initialize variables
    ErrorReason = file_list = ""
    GotoReadFile = FileInError = IsProdFileName = False
    Parse_rec_count = Rec_Count = 0
    Sub_File_Success_Error = "SUCCESS"
    FProcess = FTID = 1
    FileSource = "AMEXCLEARING"
    Sub_FileType = FType = 'Clearing And Settlement'
    EnvVar = SetUp()
    try:
        EnvVariable_Dict = {
                                "DB_Server_NAME"          : EnvVar.DB_Server_NAME.strip(),\
                                "DBName_CI"               : EnvVar.DBName_CI.strip(),\
                                "SqlOdbcDriver"           : EnvVar.SqlOdbcDriver.strip(),\
                                "InputDir"                : EnvVar.ClearingAndSettlement().AMEXFileIN.strip()
                            }
    except Exception as e:
        print(e)
        sys.exit()

        

    Connection_String = SQL_Connect.udf_GetConnectionString(EnvVariable_Dict['SqlOdbcDriver'], EnvVariable_Dict['DB_Server_NAME'], EnvVariable_Dict['DBName_CI'])
    
    print("*************************** Clearing And Settlement Processing Starts ***************************")
    
    file_list = [name for name in os.listdir(FTP_Path) if os.path.isfile(os.path.join(FTP_Path, name)) ]
    file_list.sort(key=lambda s: os.path.getmtime(os.path.join(FTP_Path, s)))

    for fname in file_list:                        
        InFileName = fname
        InFilePath = f"{FTP_Path}{InFileName}"

        FileHash = Functions.Gen_FileHash(InFilePath)
        
        print(f"Processing Filename : {InFileName}",True)
        
        #Upd_InFileName = f"{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}_{InFileName}"
        Upd_InFileName = InFileName
        
        InFilePath = f"{FTP_Path}{Upd_InFileName}"
        
        Functions.File_Movement(InFilePath,EnvVariable_Dict.get('InputDir'))
        OutFilePath = EnvVariable_Dict.get('InputDir') + Upd_InFileName
        print(f"Out File Path : {OutFilePath}",True)
        
        # Inserting new record in clearingfiles table
        res = CreateJobIntoClearingFiles(Connection_String, InFileName, OutFilePath, Upd_InFileName, FileHash, FileSource, FTID, FType)