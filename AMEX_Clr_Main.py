#Import libraries
try:
    import os
    import sys
    import time
    import Mail
    import codecs
    import ctypes
    import datetime
    import singleton
    import Functions
    import AMEX_SPCall
    import tracemalloc
    import File_Processer
    import multiprocessing
    import MultiProcess
    import AMEX_Select_And_Updates
    import SQL_Connections as SQL_Connect
    
    from SetUp import SetUp
    from Logger import Logger
    from cryptography.fernet import Fernet

    # package should be install, if not [Command: pip install modulename]
except ModuleNotFoundError as err:
    print(str({err.name.strip()})," module is missing, Command to Install - {"" pip install ", str(err.name.strip()), "}")
    sys.exit(1)

######################################################################################################################################################

if __name__ == "__main__":
    # Main
    multiprocessing.freeze_support()
    ctypes.windll.kernel32.SetConsoleTitleW("AMEX Clearing And Settlement Module")
    
    """
    try:
        print("CHECKING INSTANCE")
        multi_check = singleton.SingleInstance()
    except:
        print("Another Module is Already Running")
        sys.exit(-1)
    """
    ######################################################################################################################################################

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
                                "SMTP_SERVER"             : EnvVar.SMTP_SERVER.strip(),\
                                "SMTP_PORT"               : EnvVar.SMTP_PORT,\
                                "EmailFrom"               : EnvVar.EmailFrom.strip(),\
                                "Temp_EmailTo"            : EnvVar.Temp_EmailTo.strip(),\
                                "InputDir"                : EnvVar.ClearingAndSettlement().AMEXFileIN.strip(),\
                                "OutputDir"               : EnvVar.ClearingAndSettlement().AMEXFileOUT.strip(),\
                                "ErrorDir"                : EnvVar.ClearingAndSettlement().AMEXFileError.strip(),\
                                "LogDir"                  : EnvVar.ClearingAndSettlement().AMEXFileLog.strip(),\
                                "TxnInsertToDB"           : EnvVar.ClearingAndSettlement().TxnInsertToDB,\
                                "AMEX_ValidationEnable"   : EnvVar.ClearingAndSettlement().AMEX_ValidationEnable,\
                                "AMEX_UseCCardOrCCard2"   : EnvVar.ClearingAndSettlement().AMEX_UseCCardOrCCard2,\
                                "FileRecheckTime"         : EnvVar.ClearingAndSettlement().FileRecheckTime,\
                                "FileSizeRecheckTime"     : EnvVar.ClearingAndSettlement().FileSizeRecheckTime
                            }
    except Exception as e:
        print(e)
        sys.exit()
    
    logger = Logger()
        
    for key,value in EnvVariable_Dict.items():
        logger.info(f"{key} : {value}", True)

    EmailTo = []
    EmailTo = EnvVariable_Dict['Temp_EmailTo'].split(",")

    Connection_String = SQL_Connect.udf_GetConnectionString(EnvVariable_Dict['SqlOdbcDriver'], EnvVariable_Dict['DB_Server_NAME'], EnvVariable_Dict['DBName_CI'])
    
    logger.info("*************************** Clearing And Settlement Processing Starts ***************************",True)
    
    while True:
        logger.info("*************************** Going To Check For File ***************************")
        GoToProcessFile = "Any"
        
        res = len([name for name in os.listdir(EnvVariable_Dict.get('InputDir')) if os.path.isfile(os.path.join(EnvVariable_Dict.get('InputDir'), name))])
            
        GoToProcessFile = Functions.process_files_size_check(EnvVariable_Dict.get('InputDir'),EnvVariable_Dict.get('FileSizeRecheckTime'),EnvVariable_Dict.get('ErrorDir')) if res > 0 else False
        
        if GoToProcessFile:
            Rec_Count = AMEX_Select_And_Updates.AMEX_Select(1, Connection_String, ArgVar_1 = FileSource)
            
            if Rec_Count[0][0] > 0: 
                GoToProcessFile = False
                logger.debug("File Status in ClearingFiles is not as per expectation for AMEX Clearing File",True)
            
            if GoToProcessFile:
                time.sleep(EnvVariable_Dict.get('FileSizeRecheckTime'))
                
                logger.info("*************************** FILE FOUND PROCESSING STARTS ***************************",True)
                
                tracemalloc.start()
                file_list = [name for name in os.listdir(EnvVariable_Dict.get('InputDir')) if os.path.isfile(os.path.join(EnvVariable_Dict.get('InputDir'), name)) ]
                file_list.sort(key=lambda s: os.path.getmtime(os.path.join(EnvVariable_Dict.get('InputDir'), s)))

                logger.info(f"TotalFileCount In Folder = {len(file_list)}")
                
                current,peak = tracemalloc.get_traced_memory()
                logger.info("Before Start Memory Trace Current = " + str(current/1024) + " MB And Peak = " + str(peak/1024) + " MB")
                tracemalloc.clear_traces()

                for fname in file_list:                        
                    InFileName = fname
                    InFilePath = f"{EnvVariable_Dict.get('InputDir')}{InFileName}"
                    
                    res = Functions.check_file_out_dir(EnvVariable_Dict.get('OutputDir'),InFileName)
                    if res: Functions.File_Movement(InFilePath,EnvVariable_Dict.get('ErrorDir'),1)
                    
                    FileHash = Functions.Gen_FileHash(InFilePath)
                    
                    logger.info(f"Processing Filename : {InFileName}",True)
                    
                    #Upd_InFileName = f"{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}_{InFileName}"
                    Upd_InFileName = InFileName
                    Functions.change_file_name(InFilePath,Upd_InFileName)
                    
                    InFilePath = f"{EnvVariable_Dict.get('InputDir')}{Upd_InFileName}"
                    
                    Functions.File_Movement(InFilePath,EnvVariable_Dict.get('OutputDir'))
                    OutFilePath = EnvVariable_Dict.get('OutputDir') + Upd_InFileName
                    logger.info(f"Out File Path : {OutFilePath}",True)
                    
                    # Inserting new record in clearingfiles table
                    res = AMEX_Select_And_Updates.CreateJobIntoClearingFiles(Connection_String, InFileName, OutFilePath, Upd_InFileName, FileHash, FileSource, FTID, FType)
                    IsProdFileName = res[2]
                    FileDateFromFile = res[3]
                    
                    res = AMEX_SPCall.SP_Call_ChangeFileStatus(Connection_String, FProcess)
                    GotoReadFile = True if res == 1 else False
                    
                    if not GotoReadFile:
                        logger.debug("SP PR_AMEXChangeFileStatus doesnot return Positive result after executing")

                    current,peak = tracemalloc.get_traced_memory()
                    logger.info("InQueue Memory Trace Current = " + str(current/1024) + " MB And Peak = " + str(peak/1024) + " MB")
                    tracemalloc.clear_traces()
                    
                    while GotoReadFile:
                        res = AMEX_Select_And_Updates.AMEX_Select(2, Connection_String, ArgVar_1 = FileSource)

                        if res[0][0] != 1:
                            logger.error("More than 1 Or No file has filestatus VALIDATION Check Log and SP")
                            sys.exit()
                        else:
                            res             = AMEX_Select_And_Updates.AMEX_Select(3, Connection_String, ArgVar_1 = FileSource)
                            Inp_Jobid       = res[0][0]
                            InputFilePath   = res[0][1]
                            INFileName      = res[0][2]
                            
                            Stage_StartTime = datetime.datetime.now()
                            Ins_StartTime = Stage_StartTime.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
                            AMEX_Select_And_Updates.AMEX_Update(6, Connection_String, Inp_Jobid, ArgVar_1 = 'InQueue')
                            Mail.SendEmail(3, 'Clearing And Settlement', EmailTo, EnvVariable_Dict.get('EmailFrom'),EnvVariable_Dict.get('SMTP_SERVER'),EnvVariable_Dict.get('SMTP_PORT'),Connection_String,Inp_Jobid,FileSource)

                            logger.info("**************************************** VALIDATION STARTS ****************************************")
                            print("**************************************** VALIDATION STARTS ****************************************")
                            
                            res = File_Processer.File_Validate(InputFilePath)
                            IsUnexpectedMTI = res[0]
                            FileInError     = res[1]
                            ErrorReason     = res[2]
                            OtherRecCount   = res[3]
                            Head_Trail_Error = res[4]
                            
                            if FileInError or Head_Trail_Error:
                                AMEX_Select_And_Updates.AMEX_Update(5,Connection_String, Inp_Jobid, 'ERROR', ErrorReason)
                                Functions.File_Movement(OutFilePath,EnvVariable_Dict.get('ErrorDir'))
                                Mail.SendEmail(2, 'Clearing And Settlement', EmailTo, EnvVariable_Dict.get('EmailFrom'),EnvVariable_Dict.get('SMTP_SERVER'),EnvVariable_Dict.get('SMTP_PORT'),Connection_String,Inp_Jobid,FileSource)
                                sys.exit()
                                
                            elif IsUnexpectedMTI or OtherRecCount == 0:
                                AMEX_Select_And_Updates.AMEX_Update(4, Connection_String, Inp_Jobid, ErrorReason)
                                
                            else:
                                pass
                            
                            #Truncating Table AMEXCle_Interim
                            logger.debug('Going To Truncate Table AMEXClr_Interim',True)
                            InsQuery = "TRUNCATE TABLE AMEXClr_Interim"
                            SQL_Connect.udf_InsSingleRecIntoDB(Connection_String, InsQuery)
                            logger.debug('AMEXClr_Interim Truncated',True)
                            
                            logger.info("**************************************** VALIDATION END & PARSING STARTS ****************************************")
                            
                            current,peak = tracemalloc.get_traced_memory()
                            logger.info("Validation End Memory Trace Current = " + str(current/1024) + " MB And Peak = " + str(peak/1024) + " MB")
                            tracemalloc.clear_traces()
                            
                            Stage_EndTime = datetime.datetime.now()
                            TimeDiff = str(Stage_EndTime - Stage_StartTime)
                            TimeTaken = f"VALIDATION {TimeDiff[:-3]}"
                            AMEX_Select_And_Updates.AMEX_Update(1, Connection_String, Inp_Jobid, 'PARSING','VALIDATION', TimeTaken, To = 3, From = 1)
                            Mail.SendEmail(3, 'Clearing And Settlement', EmailTo, EnvVariable_Dict.get('EmailFrom'),EnvVariable_Dict.get('SMTP_SERVER'),EnvVariable_Dict.get('SMTP_PORT'),Connection_String,Inp_Jobid,FileSource)
                            Stage_StartTime = datetime.datetime.now()
                            
                            try:
                                Insert_Limit_Value      = EnvVariable_Dict.get('TxnInsertToDB')
                                Header_POS              = ['JobId', 'MsgTypeIdentifier', 'FunctionCode', 'ProcCode_Org', 'ProcCode', 'ProcCodeFROMAccType', 'ProcCodeToAccType', 'PrimaryAccNoLength', 'TxnDate_Org', 'TxnTime_Org', 'TxnDateTime', 'TxnAmt_Org', 'TxnAmt', 'TxnIdentifier', 'TxnCurrencyCode', 'FormatCode', 'MerchantCategoryCode', 'MsgTxnSequenceNum', 'MsgNumber', 'CardExpirationDate_Org', 'CardExpirationDate', 'CaptureDate', 'CaptureTime', 'IssuerSettleDateTime', 'IssuerSettleDate_Org', 'IssuerSettleTime_Org', 'IssuerGrossSettleAmt', 'IssuerNetSettleAmt', 'IssuerGrossSettleAmt_Org', 'IssuerNetSettleAmt_Org', 'IssuerSettleCurrencyCode', 'IssuerSettleDecimalization', 'IssuerProcessorIdentifier', 'IssuerInstIdentifier', 'FPTxnAmt', 'FPPresentmentAmt', 'FPTxnAmt_Org', 'FPPresentmentAmt_Org', 'FPPresentmentDecimalization', 'FPPresentmentCurrencyCode', 'FPTxntoPresentmentConversionRate', 'FPTxntoPresentmentConversionRate_Org', 'FP_TxnDecimalization', 'FP_PINIndicator', 'FP_ProgramIndicator', 'FP_ReservedField', 'FP_ReservedField9', 'SP_TxnDecimalization', 'SP_FPTxnDateTime', 'SP_FPTxnDate_Org', 'SP_FPTxnTime_Org', 'SP_FPTxnCurrencyCode', 'SP_FPTxnDecimalization', 'SP_FPNetworkProcessDateTime', 'SP_FPNetworkProcessDate_Org', 'SP_FPNetworkProcessTime_Org', 'SP_ItemizedDocCode', 'SP_ItemizedDocRefNum', 'SP_ReservedField1', 'SP_ReservedField2', 'SP_MsgReasonCode', 'NetworkRateAmt', 'NetworkRateAmt_Org', 'NetworkProcessDateTime', 'NetworkProcessDate_Org', 'NetworkProcessTime_Org', 'CATerminalID', 'CAIDCode', 'CAName_Org', 'CAName', 'CAAddressLine1_Org', 'CAAddressLine1', 'CAAddressLine2', 'CACity_Org', 'CACity', 'CAPostalCode', 'CACountryCode', 'CARegionCode', 'CALocationText', 'CAMultinationalIndicator', 'InvoiceNum', 'AcquiringInstIDCode', 'AcquiringInstProcessorId', 'AcquirerRefNum', 'ApprovalCodeLength', 'ApprovalCode', 'MatchingKeyType', 'MatchingKey', 'TaxReasonCode', 'CardCapability', 'POSDataCode', 'ElectronicCommerceIndicator', 'AddAmtAccingEffectTypeCode1', 'AddAmt1', 'AddAmt1_Org', 'AddAmtType1', 'AddAmtAccingEffectTypeCode2', 'AddAmt2', 'AddAmt2_Org', 'AddAmtType2', 'AddAmtAccingEffectTypeCode3', 'AddAmt3', 'AddAmt3_Org', 'AddAmtType3', 'AddAmtAccingEffectTypeCode4', 'AddAmt4', 'AddAmt4_Org', 'AddAmtType4', 'AddAmtAccingEffectTypeCode5', 'AddAmt5', 'AddAmt5_Org', 'AddAmtType5', 'AlternateCAIDCodeLength', 'AlternateCAIDCode', 'MediaCode', 'ExtendedPaymentData', 'RejectReasonCodes', 'ReservedField1', 'ReservedField2', 'ReservedField3', 'ReservedField4', 'ReservedField5', 'ReservedField6', 'ReservedField7', 'ReservedField8', 'ReservedField10', 'ReservedField11', 'ReservedField12', 'ReservedField13', 'ReservedField14', 'ReservedField15', 'ReservedField16', 'ReservedField17', 'ReservedField18', 'ReservedField19', 'ReservedField20', 'ReservedField21', 'ReservedField22', 'ReservedField23', 'ReservedField24', 'ReservedField25', 'ReservedField26', 'ReservedField27', 'ReservedField28', 'ReservedField29', 'ReservedField30', 'ReservedField31', 'ReservedField32', 'ReservedField33', 'ReservedField34', 'CardNumber4Digits', 'PAN_Hash', 'Bin_Number', 'AMEX_AlgorithmID', 'CANameLocation_Org', 'CANameLocation']
                                Header_ATM              = ['JobId', 'MsgTypeIdentifier', 'FunctionCode', 'ProcCode_Org', 'ProcCode', 'ProcCodeFROMAccType', 'ProcCodeToAccType', 'PrimaryAccNoLength', 'TxnDate_Org', 'TxnTime_Org', 'TxnDateTime', 'TxnAmt_Org', 'TxnAmt', 'TxnIdentifier', 'TxnCurrencyCode', 'FormatCode', 'MerchantCategoryCode', 'MsgTxnSequenceNum', 'MsgNumber', 'CardExpirationDate_Org', 'CardExpirationDate', 'CaptureDate', 'CaptureTime', 'IssuerSettleDateTime', 'IssuerSettleDate_Org', 'IssuerSettleTime_Org', 'IssuerGrossSettleAmt', 'IssuerNetSettleAmt', 'IssuerGrossSettleAmt_Org', 'IssuerNetSettleAmt_Org', 'IssuerSettleCurrencyCode', 'IssuerSettleDecimalization', 'IssuerProcessorIdentifier', 'IssuerInstIdentifier', 'FPTxnAmt', 'FPPresentmentAmt', 'FPTxnAmt_Org', 'FPPresentmentAmt_Org', 'FPPresentmentDecimalization', 'FPPresentmentCurrencyCode', 'FPTxntoPresentmentConversionRate', 'FPTxntoPresentmentConversionRate_Org', 'FP_TxnDecimalization', 'SP_TxnDecimalization', 'SP_FPTxnDateTime', 'SP_FPTxnDate_Org', 'SP_FPTxnTime_Org', 'SP_FPTxnCurrencyCode', 'SP_FPTxnDecimalization', 'SP_FPNetworkProcessDateTime', 'SP_FPNetworkProcessDate_Org', 'SP_FPNetworkProcessTime_Org', 'SP_MsgReasonCode', 'NetworkProcessDateTime', 'NetworkProcessDate_Org', 'NetworkProcessTime_Org', 'CATerminalID', 'CAIDCode', 'CAName_Org', 'CAName', 'CAAddressLine1_Org', 'CAAddressLine1', 'CAAddressLine2', 'CACity_Org', 'CACity', 'CAPostalCode', 'CACountryCode', 'CARegionCode', 'AcquiringInstIDCode', 'AcquiringInstProcessorId', 'AcquirerRefNum', 'ApprovalCodeLength', 'ApprovalCode', 'POSDataCode', 'AddAmtAccingEffectTypeCode1', 'AddAmt1', 'AddAmt1_Org', 'AddAmtType1', 'AddAmtAccingEffectTypeCode2', 'AddAmt2', 'AddAmt2_Org', 'AddAmtType2', 'AddAmtAccingEffectTypeCode3', 'AddAmt3', 'AddAmt3_Org', 'AddAmtType3', 'AddAmtAccingEffectTypeCode4', 'AddAmt4', 'AddAmt4_Org', 'AddAmtType4', 'AddAmtAccingEffectTypeCode5', 'AddAmt5', 'AddAmt5_Org', 'AddAmtType5', 'MediaCode', 'RejectReasonCodes', 'ATM_FP_IssuerAuthAmt', 'ATM_FP_IssuerAuthAmt_Org', 'ATM_ReservedField1', 'ATM_ReservedField2', 'ATM_ReservedField3', 'ATM_ReservedField4', 'ATM_FP_ReservedField5', 'ATM_ReservedField6', 'ATM_ReservedField7', 'ATM_ReservedField8', 'ATM_ReservedField9', 'ATM_ReservedField10', 'ATM_ReservedField11', 'ATM_ReservedField12', 'ATM_ReservedField13', 'ATM_ReservedField14', 'ATM_ReservedField15', 'ATM_ReservedField16', 'ATM_FP_ReservedField17', 'ATM_ReservedField18', 'ATM_ReservedField19', 'ATM_ReservedField20', 'ATM_ReservedField21', 'ATM_ReservedField22', 'ATM_ReservedField23', 'ATM_ReservedField24', 'ATM_ReservedField25', 'ATM_ReservedField26', 'ATM_ReservedField27', 'ATM_ReservedField28', 'ATM_ReservedField29', 'ATM_FP_ReservedField30', 'ATM_ReservedField31', 'ATM_ReservedField32', 'ATM_ReservedField33', 'ATM_ReservedField34', 'ATM_ReservedField35', 'ATM_ReservedField36', 'ATM_ReservedField37', 'ATM_ReservedField38', 'ATM_ReservedField39', 'ATM_ReservedField40', 'ATM_ReservedField41', 'ATM_ReservedField42', 'ATM_ReservedField43', 'ATM_ReservedField44', 'ATM_ReservedField45', 'CardNumber4Digits', 'PAN_Hash', 'Bin_Number', 'AMEX_AlgorithmID', 'CANameLocation_Org', 'CANameLocation']
                                Header_FeeCollect       = ['JobId','MsgTypeIdentifier','Filler1','ProcCode','ProcCodeFromAccType','ProcCodeToAccType','TxnAmt','TxnAmt_Org','ReservedField1','Filler2','ReservedField2','Filler3','TxnDate_Org','TxnTime_Org','TxnDateTime','Filler4','FunctionCode','Filler5','ValidBillingUnitCode','SettleDate_Org','ReservedField3','Filler6','AcquiringInstIDCode','ForwardingInstIDCode','Filler7','FeeReasonText','ReservedField4','Filler8','FeeTypeCode','Filler9','ReservedField5','SettleAmt','SettleAmt_Org','Filler10','SettleCurrencyCode','SettleDecimalization','TxnCurrencyCode','TxnDecimalization','PresentmentAmt','PresentmentAmt_Org','TxntoPresenConversionRate','PresenCurrencyCode','PresentmentDecimalization','Filler11','Filler12','ReservedField6','Filler13','NetworkProcessDate','NetworkProcessTime','NetworkProcessDateTime','ReservedField7','Filler14','SettleTime_Org','SettleDateTime','Filler15','IssuerInstIdentifier','Filler16','MsgTxnSequenceNum','Filler17','TxnIdentifier','Filler18','MsgNum','ReceivingInstIdentifier','Filler19','ReservedField8','Filler20','DbCrIndicator','FileReferenceDate']
                                Header_ATMFee           = ['JobId','MsgTypeidentifier','PrimaryAccNumLength','PrimaryAccNum','ProcCode','ProcCodeFromAccType','ProcCodeToAccType','TxnAmt_Org','TxnAmt','ReservedField1','Filler1','ReservedField2','Filler2','TxnDate_Org','TxnTime_Org','TxnDateTime','Filler3','FunctionCode','Filler4','SettleDate_Org','Filler5','SendingInstId','SendingProcessInstId','Filler6','PassThroughFeereasonText','Filler7','FeeTypeCode','Filler8','ReservedField3','SettleAmt','SettleAmt_Org','Filler9','SettleCurrencyCode','SettleDecimalization','Filler10','TxnDecimalization','PresentmentAmt','PresentmentAmt_Org','TxntoPresentConversionrate','PresentmentCurrencyCode','PresentmentDecimalization','Filler11','TxnCurrencyCode','ReservedField4','Filler12','NetworkProcessDate','NetworkProcessTime','NetworkProcessDateTime','Filler13','SettleTime_Org','SettleDateTime','Filler14','ReceivingInstIdentifier','Filler15','MsgTxnSequenceNum','Filler16','TxnIdentifier','Filler17','MsgNum','ReceivingProcessorInstId','Filler18','RejectReasonCodes','Filler19','CardNumber4Digits','Pan_Hash','Bin_Number','AMEX_AlgorithmID']
                                Header_AirIndus         = ['JobId','MsgTypeidentifier','AddendaTypeCode','E_TicketIndicator','DepartureLocationCode1','DepartureDate1','DepartureLocationCode2','DepartureDate2','DepartureLocationCode3','DepartureDate3','DepartureLocationCode4','DepartureDate4','Faresegment1','Faresegment2','Faresegment3','Faresegment4','ConjunctionTicket','SegFareBasis1','SegFareBasis2','SegFareBasis3','SegFareBasis4','Filler1','TravelProcessId','TicketIssuerName','TicketNumber','AirlineInvoiceNum','TravelTxnTypeCode','TicketIssueCity','NumInParty','PassengerName','Filler2','DestinationLocationCode1','Filler3','ArrivalClassofService1','CarrierCodesegment1','FlightNum1','DestinationLocationCode2','Filler4','ArrivalClassofService2','CarrierCodesegment2','FlightNum2','DestinationLocationCode3','Filler5','ArrivalClassofService3','CarrierCodesegment3','FlightNum3','DestinationLocationCode4','Filler6','ArrivalClassofService4','CarrierCodesegment4','FlightNum4','Filler7','TravelDocTypeCode','AirlineDocNum','AirlineDocParts','Filler8','TicketIssueDate','TravelAgencyName','TravelAgencyNum','ExchangedTicketNum','Filler9','StopOverInd1','StopOverInd2','StopOverInd3','StopOverInd4','StopOverInd5','Filler10','FormatCode','Filler11','MessageTxnSequenceNumber','Filler12','TxnIdentifier','Filler13','MessageNumber','Filler14','RejectReasonCodes','Filler15']
                                Header_AutoRental       = ['JobId','MsgTypeidentifier','AddenaTypeCode','Filler1','AutoRentalAgreementNum','AutoRentalAgencyName','AutoRentalCityName','AutoRentalRegionCode','AutoRentalCountryCode','AutoRentalPickupCityName','AutoRentalPickupRegionCode','AutoRentalPickupCountryCode','AutoRentalPickupDate','AutoRentalPickupTime','AutoRentalReturnCityName','AutoRentalReturnRegionCode','AutoRentalReturnCountryCode','AutoRentalReturnDate','AutoRentalReturnTime','AutoRentalAuditAdjustmentInd','AutoRentalAuditAdjustmentAmt','AutoRentalRenterName','ReservedField1','ReservedField2','ReservedField3','ReservedField4','TaxiVehicleIndNum','TaxiDriverIndNum','TaxiDriverTaxIndNum','TaxiDropoffLocation','Filler2','FormatCode','Filler3','MessageTxnSequenceNumber','Filler4','TxnIdentifier','Filler5','MessageNumber','AutoRentalClassCode','PickupLocation','AutoRentalDistance','AutoRentalDistUnitofMeasure','Filler6','RejectReasonCodes','Filler7']
                                Header_CommServ         = ['JobId','MsgTypeidentifier','AddenaTypeCode','Filler1','CallDate','CallTime','CallDurationTime','CallFromCityName','CallFromRegionName','CallFromCountryName','CallFromPhnNum','CallToCityName','CallToRegionCode','CallToCountryCode','CallToPhnNum','CommunicationClassRateCode','CommunicationCallTypeCode','ReservedField1','ReservedField2','Filler2','Filler3','CardPhnNum','ServiceDescription','BillingPeriod','Filler4','FormatCode','Filler5','MessageTxnSequenceNumber','Filler6','TxnIdentifier','Filler7','MessageNumber','Filler8','RejectReasonCodes','Filler9']
                                Header_EntTickInd       = ['JobId','MsgTypeidentifier','AddenaTypeCode','Filler1','EventName','EventDate','EventIndiTickPriceAmt','EventTicketQuantity','EventLocation','EventRegionCode','EventCountryCode','Filler2','FormatCode','Filler3','MessageTxnSequenceNumber','Filler4','TxnIdentifier','Filler5','MessageNumber','Filler6','RejectReasonCodes','Filler7']
                                Header_GenForInd        = ['JobId','MsgTypeidentifier','AddendaTypeCode','Filler1','ReservedField1','ReservedField2','ReservedField3','ReservedField4','ReservedField5','ReservedField6','Filler2','FormatCode','Filler3','MessageTxnSequenceNumber','Filler4','TxnIdentifier','Filler5','MessageNumber','Filler6','RejectReasonCodes','Filler7']
                                Header_InsurInd         = ['JobId','MsgTypeidentifier','AddenaTypeCode','Filler1','InsurancePolicyNum','InsuranceCovDateRange','InsurancePolipremiFrequency','AdditionalReferenceNum','TypeOfPolicy','NameOfInsured','Filler2','FormatCode','Filler3','MessageTxnSequenceNumber','Filler4','TxnIdentifier','Filler5','MessageNumber','Filler6','RejectReasonCodes','Filler7']
                                Header_LodgingInd       = ['JobId','MsgTypeidentifier','AddenaTypeCode','Filler1','LodgingSpecProgCode','LodgingChargeTypeCode','LodgingCheckInDate','LodgingCheckInTime','LodgingCheckOutDate','LodgingCheckOutTime','LodgingStayDuration','LodgingRoomRate1','LodgingRenterName','LodgingFolioNum','ReservedField1','ReservedField2','ReservedField3','LodgingRoomRate2','LodgingRoomRate3','NumOfNightsAtRoomRate1','NumOfNightsAtRoomRate2','NumOfNightsAtRoomRate3','Filler2','FormatCode','Filler3','MessageTxnSequenceNumber','Filler4','TxnIdentifier','Filler5','MessageNumber','Filler6','RejectReasonCodes','Filler7']
                                Header_RailInd          = ['JobId','MsgTypeidentifier','AddenaTypeCode','Filler1','ArrivalRailStationName1','ArrivalRailStationName2','ArrivalRailStationName3','ArrivalRailStationName4','TicketNum','DepartureRailStationName1','DepartureDate1','DepartureRailStationName2','DepartureDate2','DepartureRailStationName3','DepartureDate3','DepartureRailStationName4','DepartureDate4','Filler2','TravelTxnTypeCode','PassengerName','IATACarrierCode','TicketIssuerName','TicketIssueCity','Filler3','FormatCode','Filler4','MessageTxnSequenceNumber','Filler5','TxnIdentifier','Filler6','MessageNumber','Filler7','RejectReasonCodes','Filler8']
                                Header_RestInd          = ['JobId','MsgTypeidentifier','AddenaTypeCode','Filler1','RestaurantItemDescription1','RestaurantItemAmt1','RestaurantItemDescription2','RestaurantItemAmt2','RestaurantItemDescription3','RestaurantItemAmt3','RestaurantItemDescription4','RestaurantItemAmt4','Filler2','FormatCode','Filler3','MessageTxnSequenceNumber','Filler4','TxnIdentifier','Filler5','MessageNumber','Filler6','RejectReasonCodes','Filler7']
                                Header_RetailInd        = ['JobId','MsgTypeidentifier','AddendaTypeCode','Filler1','RetailDepartName','RetailItemDescription1','RetailItemQuantity1','RetailItemAmt1','RetailItemDescription2','RetailItemQuantity2','RetailItemAmt2','RetailItemDescription3','RetailItemQuantity3','RetailItemAmt3','RetailItemDescription4','RetailItemQuantity4','RetailItemAmt4','RetailItemDescription5','RetailItemQuantity5','RetailItemAmt5','RetailItemDescription6','RetailItemQuantity6','RetailItemAmt6','Filler2','FormatCode','Filler3','MessageTxnSequenceNumber','Filler4','TxnIdentifier','Filler5','MessageNumber','Filler6','RejectReasonCodes','Filler7']
                                Header_TravelCruise     = ['JobId','MsgTypeIdentifier','AddendaTypeCode','Filler1','PackageIndicator','TravelAgencyNum','PassengerName','DepartureDate1','DepartureDate2','DepartureDate3','DepartureDate4','DestinationLocationCode1','DestinationLocationCode2','DestinationLocationCode3','DestinationLocationCode4','CarrierCodeSegment1','CarrierCodeSegment2','CarrierCodeSegment3','CarrierCodeSegment4','FlightNumber1','FlightNumber2','FlightNumber3','FlightNumber4','ClassOfServiceCode1','ClassOfServiceCode2','ClassOfServiceCode3','ClassOfServiceCode4','LodgingCheckInDate','LodgingCheckOutDate','LodgingRoomRate','NumofNightsatRoomRate','CardAcceptorName','CardAcceptorRegionCode','CardAcceptorCountryCode','CardAcceptorCity','IATACarrierCode','TicketNumber','Filler2','FormatCode','Filler3','MessageTxnSequenceNumber','Filler4','TxnIdentifier','Filler5','MessageNumber','Filler6','RejectReasonCodes','Filler7']
                                Header_DeferredPay      = ['JobId','MsgTypeIdentifier','AddendaTypeCode','Filler1','FullTxnAmt','TypeofPlanCode','NumberofInstallments','AmountofInstallment','InstallmentNum','ContractNum','PaymentTypeCode1','PaymentTypeAmt1','PaymentTypeCode2','PaymentTypeAmt2','PaymentTypeCode3','PaymentTypeAmt3','PaymentTypeCode4','PaymentTypeAmt4','PaymentTypeCode5','PaymentTypeAmt5','Filler2','MessageTxnSequenceNumber','AdditionalData','Filler3','TxnIdentifier','Filler4','MessageNumber','Filler5','RejectReasonCodes','FillerorSpecialPaymentTypeIndicator','ReservedField','Filler6']
                                Header_DoubleByteChar   = ['JobId','MsgTypeIdentifier','AddendaTypeCode','LanguageCode','DoubleByteCharCardAccepName','DoubleByteCharCardAccepAddLine1','DoubleByteCharCardAccepAddLine2','DoubleByteCharCardAccepAddLine3','DoubleByteCharCardAccepAddLine4','Filler1','MessageTxnSequenceNumber','Filler2','TxnIdentifier','Filler3','MessageNumber','Filler4','RejectReasonCodes','Filler5']
                                Header_MarketSpeciData  = ['JobId','MsgTypeIdentifier','AddendaTypeCode','LanguageCode','DoubleByteDataLine1','DoubleByteDataLine2','DoubleByteDataLine3','DoubleByteDataLine4','Filler1','Filler2','NameofServEstablishinKatakana','DistrictLocaofServEstablishment','CityNameinKatakana','KatakanaDataLine','RegionalSpeciData','ReservedField1','ReservedField2','ReservedField3','ReservedField4','ReservedField5','ReservedField6','Filler3','MessageTxnSequenceNumber','ReservedField7','ReservedField8','ReservedField9','Filler4','TxnIdentifier','Filler5','MessageNumber','ReservedField10','Filler6','RejectReasonCodes','Filler7']
                                Header_ChipCard         = ['JobId','MsgTypeIdentifier','AddendaTypeCode','Filler1','ICCSystemRelatedData','Filler2','MsgTxnSequenceNum','Filler3','TxnIdentifier','Filler4','MsgNum','Filler5','RejectReasonCodes','Filler6']
                                Header_CorpoPurchase    = ['JobId','MsgTypeIdentifier','AddendaTypeCode','ShiptoName','ContactEmailAddress','ShipToAddressLine1','ShipToAddressLine2','ShiptoCity','ShiptoPostalCode','ShiptoRegionCode','ShiptoCountryCode','ShiptoEUCountry','ShipFromCompanyName','ShipFromAddressLine1','ShipFromAddressLine2','ShipFromCity','ShipFromPostalCode','ShipFromRegionCode','ShipFromCountryCode','ShipFromEUCountry','ShippingCarrier','TotalTaxAmount','Filler1','TaxAuthorityLocation','TaxLiabilityCode','CardAccepInvoiceNum','CardAccepOrigInvoiceNum','CardAccepHeadquartersName','CardAccepHeadquartersNum','ChargeItemAmount1','ShippingCharge','ShippingDate','NetWeight','DutyAmount','ShippingRate','DiscountRate','DiscountAmount','ChargeItemDescription1','OtherChargesAmount','ChargeItemAmount2','NetTxnAmtEuro','TaxTypeCode1','TaxRatePercent1','TaxMonetaryAmt1','TaxExemptCode1','TaxTypeCode2','TaxRatePercent2','TaxMonetaryAmt2','TaxExemptCode2','TaxTypeCode3','TaxRatePercent3','TaxMonetaryAmt3','TaxExemptCode3','TaxTypeCode4','TaxRatePercent4','TaxMonetaryAmount4','TaxExemptCode4','Filler2','Filler3','Filler4','Filler5','Filler6','MessageTxnSequenceNumber','ChargeItemDescription2','ChargeItemAmount3','LineItemDetailKey1','TxnIdentifier','LineItemDetailKey2','MessageNumber','CardmemberReferenceNum','TaxReferenceNum','RequesterName','RequesterEmail','RequesterAddressLine1','RequesterAddressLine2','RequesterCity','RequesterPostalCode','RequesterRegionCode','RequesterCountryCode','BusinessOwnersClassifiCode','ChargeItemDescription3','ChargeItemDescription4','ChargeItemAmt4','ChargeItemQuantity1','ChargeItemQuantity2','ChargeItemQuantity3','ChargeItemQuantity4','Filler7']
                                Header_ExtendFinan      = ['JobId','MsgTypeIdentifier','AddendaTypeCode','ForeignCurrencyFactorAmt','ForeignCurrenFactAmtCurrenCode','ForeignCurrenFactAmtDecimaization','ReservedField1','ReservedField2','ReservedField3','FeeAmount','FeeAmountCurrencyCode','FeeAmountDecimalization','ReservedField4','MessageTxnSequenceNumber','ReservedField5','ReservedField6','ReservedField7','TxnIdentifier','Filler1','MessageNumber','ReservedField8','ReservedField9','TokenLength','Token','WalletProviderID','ReservedField10','ReservedField11','Filler2','RejectReasonCodes','Filler3','FeeAmt_Mod']
                                POS_List                = []
                                ATM_List                = []
                                Fee_Collect_List        = []
                                ATM_Fee_List            = []
                                MarketSpeciData_List    = []
                                DefPaymentPlan_List     = []
                                AirLine_List            = []
                                Retail_Ind_List         = []
                                Insurance_List          = []
                                AutoRental_List         = []
                                Rail_Ind_List           = []
                                Lodging_List            = []
                                Restaurant_List             = []
                                Travel_List                 = []
                                General_List                = []
                                Entertainment_List          = []
                                Corporate_Purchase_List     = []
                                Chip_List                   = []
                                Double_Byte_List            = []
                                Extended_Financial_List     = []
                                Communication_Service_List  = []
                                
                                with codecs.open(InputFilePath, 'r', "cp500") as fo:
                                    while fileline := fo.read(1400):
                                        if not FileInError :
                                            Parse_rec_count += 1
                                            if Parse_rec_count % 10000 == 0:
                                                logger.info(f"Reading Record Number : {Parse_rec_count}",True)
                                            Linenum = fileline
                                            
                                            CombMTI = Linenum[0:4]
                                            #Linenum = Functions.unicode_to_ascii(Linenum,Parse_rec_count)
                                            
                                            match CombMTI:
                                                case '9824':
                                                    TxnDateTime  =  Functions.datetimeconvert(Linenum[111:119],Linenum[119:125])
                                                    InsQuery = "INSERT INTO AMEXClr_IncomingHeader(JobId, MsgTypeIdentifier, TransDate_Org, TransTime_Org, TxnDateTime, ForwardInstIdCode, ActionCode, FileSequenceNumber, MessageNumber, ReceivingInstId, RejectReasonCodes, ReservedField)"
                                                    InsQuery = f"{InsQuery} VALUES( {Inp_Jobid},'{Linenum[0:4]}','{Linenum[111:119]}','{Linenum[119:125]}','{TxnDateTime}','{Linenum[211:222]}','{Linenum[251:254]}','{Linenum[999:1005]}','{Linenum[1031:1039]}','{Linenum[1039:1050]}','{Linenum[1278:1318]}','{Linenum[973:999]}' )"
                                                    SQL_Connect.udf_InsSingleRecIntoDB(Connection_String, InsQuery)
                                                    logger.info("Header 9824 Info: Header stored successfully in database")
                                                    
                                                case '1240':
                                                    if Linenum[141:145] not in ['6011','6010']:
                                                        raw_list = File_Processer.POS(Inp_Jobid, Linenum,Parse_rec_count)
                                                        POS_List.append(raw_list)
                                                    else:
                                                        raw_list = File_Processer.ATM(Inp_Jobid, Linenum,Parse_rec_count)
                                                        ATM_List.append(raw_list)
                                                        
                                                case '1744':
                                                    FileReferenceDate = FileDateFromFile if IsProdFileName == True else ''
                                                    raw_list = File_Processer.Fee_Collect(Inp_Jobid, Linenum, FileReferenceDate, Parse_rec_count)
                                                    Fee_Collect_List.append(raw_list)
                                                    
                                                case '1740':
                                                    raw_list = File_Processer.ATM_Fee(Inp_Jobid, Linenum, Parse_rec_count)
                                                    ATM_Fee_List.append(raw_list)
                                                    
                                                case '9240' | '9340':
                                                    if Linenum[4:6] == '01' :
                                                        raw_list  = [Inp_Jobid,Linenum[0:4],Linenum[4:6],Linenum[6:8],Linenum[8:88],Linenum[88:168],Linenum[168:248],Linenum[248:328],Linenum[328:358],Linenum[358:508],Linenum[508:548],Linenum[548:588],Linenum[588:628],Linenum[628:668],Linenum[668:818],Linenum[818:823],Linenum[823:843],Linenum[843:863] \
                                                                ,Linenum[863:883],Linenum[883:903],Linenum[903:907],Linenum[907:937],Linenum[937:940],Linenum[940:956],Linenum[956:981],Linenum[981:984],Linenum[984:1005],Linenum[1005:1020],Linenum[1020:1031],Linenum[1031:1039],Linenum[1039:1119],Linenum[1119:1278],Linenum[1278:1318],Linenum[1318:1400]]
                                                        
                                                        MarketSpeciData_List.append(raw_list)

                                                    elif Linenum[4:6] == '02' :
                                                        # Deferred Payment Plan
                                                        raw_list = [Inp_Jobid,Linenum[0:4],Linenum[4:6],Linenum[6:524],Linenum[524:539],Linenum[539:543],Linenum[543:547],Linenum[547:562],Linenum[562:566],Linenum[566:580],Linenum[580:582],Linenum[582:597],Linenum[597:599],Linenum[599:614],Linenum[614:616],Linenum[616:631],Linenum[631:633] \
                                                                ,Linenum[633:648],Linenum[648:650],Linenum[650:665],Linenum[665:937],Linenum[937:940],Linenum[940:980],Linenum[980:1005],Linenum[1005:1020],Linenum[1020:1031],Linenum[1031:1039],Linenum[1039:1278],Linenum[1278:1318],Linenum[1318:1319],Linenum[1319:1322],Linenum[1322:1400]]
                                                        DefPaymentPlan_List.append(raw_list)

                                                    elif Linenum[4:6] == '03' :
                                                        
                                                        if Linenum[922:924] == '01' :
                                                            # AirLine
                                                            raw_list = [Inp_Jobid,Linenum[0:4],Linenum[4:6],Linenum[6:7],Linenum[7:10],Linenum[10:18],Linenum[18:21],Linenum[21:29],Linenum[29:32],Linenum[32:40],Linenum[40:43],Linenum[43:51],Linenum[51:66],Linenum[66:81],Linenum[81:96],Linenum[96:111],Linenum[111:112],Linenum[112:127],Linenum[127:142],Linenum[142:157],Linenum[157:172],Linenum[172:200],Linenum[200:203],Linenum[203:228],Linenum[228:242],Linenum[242:249],Linenum[249:252],Linenum[252:270],Linenum[270:273],Linenum[273:298],Linenum[298:319],Linenum[319:322],Linenum[322:324],Linenum[324:326],Linenum[326:329],Linenum[329:333],Linenum[333:336],Linenum[336:338],Linenum[338:340],Linenum[340:343] \
                                                                    ,Linenum[343:347],Linenum[347:350],Linenum[350:352],Linenum[352:354],Linenum[354:357],Linenum[357:361],Linenum[361:364],Linenum[364:366],Linenum[366:368],Linenum[368:371],Linenum[371:375],Linenum[375:376],Linenum[376:378],Linenum[378:380],Linenum[380:382],Linenum[382:396],Linenum[396:404],Linenum[404:436],Linenum[436:444],Linenum[444:458],Linenum[458:516],Linenum[516:517],Linenum[517:518],Linenum[518:519],Linenum[519:520],Linenum[520:521],Linenum[521:922],Linenum[922:924],Linenum[924:937],Linenum[937:940],Linenum[940:1005],Linenum[1005:1020],Linenum[1020:1031],Linenum[1031:1039],Linenum[1039:1278],Linenum[1278:1318],Linenum[1318:1400]]

                                                            AirLine_List.append(raw_list)

                                                        elif Linenum[922:924]=='02':
                                                            # Retail Industry
                                                            raw_list = [Inp_Jobid,Linenum[0:4],Linenum[4:6],Linenum[6:200],Linenum[200:240],Linenum[240:259],Linenum[259:262],Linenum[262:273],Linenum[273:292],Linenum[292:295],Linenum[295:306],Linenum[306:325],Linenum[325:328],Linenum[328:339],Linenum[339:358],Linenum[358:361],Linenum[361:372],Linenum[372:391],Linenum[391:394],Linenum[394:405],Linenum[405:424],Linenum[424:427],Linenum[427:438],Linenum[438:922],Linenum[922:924],Linenum[924:937],Linenum[937:940],Linenum[940:1005],Linenum[1005:1020],Linenum[1020:1031],Linenum[1031:1039],Linenum[1039:1278],Linenum[1278:1318],Linenum[1318:1400]]
                                                            Retail_Ind_List.append(raw_list)


                                                        elif Linenum[922:924] == '04':
                                                            # Insurance Industry
                                                            raw_list = [Inp_Jobid,Linenum[0:4],Linenum[4:6],Linenum[6:200],Linenum[200:223],Linenum[223:246],Linenum[246:253],Linenum[253:276],Linenum[276:301],Linenum[301:331],Linenum[331:922],Linenum[922:924],Linenum[924:937],Linenum[937:940],Linenum[940:1005],Linenum[1005:1020],Linenum[1020:1031],Linenum[1031:1039],Linenum[1039:1278],Linenum[1278:1318],Linenum[1318:1400]]
                                                            Insurance_List.append(raw_list)

                                                        elif Linenum[922:924] == '05':
                                                            # AutoRental
                                                            raw_list = [Inp_Jobid,Linenum[0:4],Linenum[4:6],Linenum[6:200],Linenum[200:214],Linenum[214:234],Linenum[234:252],Linenum[252:255],Linenum[255:258],Linenum[258:276],Linenum[276:279],Linenum[279:282],Linenum[282:290],Linenum[290:296],Linenum[296:314],Linenum[314:317],Linenum[317:320],Linenum[320:328],Linenum[328:334],Linenum[334:335],Linenum[335:343],Linenum[343:369],Linenum[369:377],Linenum[377:383],Linenum[383:391],Linenum[391:397],Linenum[397:417],Linenum[417:437],Linenum[437:457],Linenum[457:495],Linenum[495:922],Linenum[922:924],Linenum[924:937],Linenum[937:940],Linenum[940:1005],Linenum[1005:1020],Linenum[1020:1031],Linenum[1031:1039],Linenum[1039:1043],Linenum[1043:1081],Linenum[1081:1086],Linenum[1086:1087],Linenum[1087:1278],Linenum[1278:1318],Linenum[1318:1400]]
                                                            AutoRental_List.append(raw_list)

                                                        elif Linenum[922:924] == '06':
                                                            # Rail Industry
                                                            raw_list = [Inp_Jobid,Linenum[0:4],Linenum[4:6],Linenum[6:200],Linenum[200:220],Linenum[220:240],Linenum[240:260],Linenum[260:280],Linenum[280:294],Linenum[294:314],Linenum[314:322],Linenum[322:342],Linenum[342:350],Linenum[350:370],Linenum[370:378],Linenum[378:398],Linenum[398:406],Linenum[406:809],Linenum[809:812],Linenum[812:837],Linenum[837:840],Linenum[840:872],Linenum[872:890],Linenum[890:922],Linenum[922:924],Linenum[924:937],Linenum[937:940],Linenum[940:1005],Linenum[1005:1020],Linenum[1020:1031],Linenum[1031:1039],Linenum[1039:1278],Linenum[1278:1318],Linenum[1318:1400]]
                                                            Rail_Ind_List.append(raw_list)

                                                        elif Linenum[922:924] == '11':
                                                            # Lodging Industry
                                                            raw_list = [Inp_Jobid,Linenum[0:4],Linenum[4:6],Linenum[6:200],Linenum[200:201],Linenum[201:202],Linenum[202:210],Linenum[210:216],Linenum[216:224],Linenum[224:230],Linenum[230:232],Linenum[232:247],Linenum[247:273],Linenum[273:285],Linenum[285:293],Linenum[293:301],Linenum[301:316],Linenum[316:331],Linenum[331:346],Linenum[346:348],Linenum[348:350],Linenum[350:352],Linenum[352:922],Linenum[922:924],Linenum[924:937],Linenum[937:940],Linenum[940:1005],Linenum[1005:1020],Linenum[1020:1031],Linenum[1031:1039],Linenum[1039:1278],Linenum[1278:1318],Linenum[1318:1400]]
                                                            Lodging_List.append(raw_list)

                                                        elif Linenum[922:924] == '12':
                                                            # Restaurant Industry
                                                            raw_list = [Inp_Jobid,Linenum[0:4],Linenum[4:6],Linenum[6:200],Linenum[200:220],Linenum[220:235],Linenum[235:255],Linenum[255:270],Linenum[270:290],Linenum[290:305],Linenum[305:325],Linenum[325:340],Linenum[340:922],Linenum[922:924],Linenum[924:937],Linenum[937:940],Linenum[940:1005],Linenum[1005:1020],Linenum[1020:1031],Linenum[1031:1039],Linenum[1039:1278],Linenum[1278:1318],Linenum[1318:1400]]
                                                            Restaurant_List.append(raw_list)

                                                        elif Linenum[922:924] == '13':
                                                            # Communication Services Industry
                                                            raw_list = [Inp_Jobid,Linenum[0:4],Linenum[4:6],Linenum[6:200],Linenum[200:208],Linenum[208:214],Linenum[214:220],Linenum[220:238],Linenum[238:241],Linenum[241:244],Linenum[244:260],Linenum[260:278],Linenum[278:281],Linenum[281:284],Linenum[284:300],Linenum[300:301],Linenum[301:306],Linenum[306:312],Linenum[312:318],Linenum[318:330],Linenum[330:871],Linenum[871:879],Linenum[879:899],Linenum[899:917],Linenum[917:922],Linenum[922:924],Linenum[924:937],Linenum[937:940],Linenum[940:1005],Linenum[1005:1020],Linenum[1020:1031],Linenum[1031:1039],Linenum[1039:1278],Linenum[1278:1318],Linenum[1318:1400]]
                                                            Communication_Service_List.append(raw_list)

                                                        elif Linenum[922:924] == '14':
                                                            # Travel/Cruise Industry
                                                            raw_list = [Inp_Jobid,Linenum[0:4],Linenum[4:6],Linenum[6:13],Linenum[13:14],Linenum[14:22],Linenum[22:47],Linenum[47:55],Linenum[55:63],Linenum[63:71],Linenum[71:79],Linenum[79:82],Linenum[82:85],Linenum[85:88],Linenum[88:91],Linenum[91:93],Linenum[93:95],Linenum[95:97],Linenum[97:99],Linenum[99:103],Linenum[103:107],Linenum[107:111],Linenum[111:115],Linenum[115:117],Linenum[117:119],Linenum[119:121],Linenum[121:123],Linenum[123:131],Linenum[131:139],Linenum[139:154],Linenum[154:156],Linenum[156:176],Linenum[176:179],Linenum[179:182],Linenum[182:200],Linenum[200:203],Linenum[203:217],Linenum[217:922],Linenum[922:924],Linenum[924:937],Linenum[937:940],Linenum[940:1005],Linenum[1005:1020],Linenum[1020:1031],Linenum[1031:1039],Linenum[1039:1278],Linenum[1278:1318],Linenum[1318:1400]]
                                                            Travel_List.append(raw_list)

                                                        elif Linenum[922:924] in ['20','21'] :
                                                            # General Format Industry
                                                            raw_list = [Inp_Jobid,Linenum[0:4],Linenum[4:6],Linenum[6:200],Linenum[200:245],Linenum[245:290],Linenum[290:335],Linenum[335:380],Linenum[380:425],Linenum[425:426],Linenum[426:922],Linenum[922:924],Linenum[924:937],Linenum[937:940],Linenum[940:1005],Linenum[1005:1020],Linenum[1020:1031],Linenum[1031:1039],Linenum[1039:1278],Linenum[1278:1318],Linenum[1318:1400]]
                                                            General_List.append(raw_list)
                                                            
                                                        elif Linenum[922:924] == '22' :
                                                            # Entertainment/Ticketing Industry
                                                            raw_list = [Inp_Jobid,Linenum[0:4],Linenum[4:6],Linenum[6:200],Linenum[200:230],Linenum[230:238],Linenum[238:247],Linenum[247:251],Linenum[251:291],Linenum[291:294],Linenum[294:297],Linenum[297:922],Linenum[922:924],Linenum[924:937],Linenum[937:940],Linenum[940:1005],Linenum[1005:1020],Linenum[1020:1031],Linenum[1031:1039],Linenum[1039:1278],Linenum[1278:1318],Linenum[1318:1400]]
                                                            Entertainment_List.append(raw_list)

                                                        else:
                                                            ErrorReason = f"FILESPLITTOCSV : FILESPLITTOCSV : Unexpected Format Code = {Linenum[922:924]} Received at Record Number = {Parse_rec_count}"
                                                            AMEX_Select_And_Updates.AMEX_Update(5,Connection_String, Inp_Jobid, 'ERROR', ErrorReason)
                                                            logger.debug(ErrorReason,True)
                                                            FileInError = True
                                                            break

                                                    elif Linenum[4:6] == '05' :
                                                        # Corporate Purchasing Card Transaction Detail
                                                        raw_list = [Inp_Jobid,Linenum[0:4],Linenum[4:6],Linenum[6:44],Linenum[44:79],Linenum[79:117],Linenum[117:155],Linenum[155:193],Linenum[193:208],Linenum[208:211],Linenum[211:214],Linenum[214:215],Linenum[215:253],Linenum[253:291],Linenum[291:329],Linenum[329:367],Linenum[367:382],Linenum[382:385],Linenum[385:388],Linenum[388:389],Linenum[389:419],Linenum[419:434],Linenum[434:447],Linenum[447:487],Linenum[487:488],Linenum[488:501],Linenum[501:514],Linenum[514:552],Linenum[552:567],Linenum[567:582],Linenum[582:597],Linenum[597:605],Linenum[605:615],Linenum[615:630],Linenum[630:645],Linenum[645:660],Linenum[660:675],Linenum[675:720],Linenum[720:735],Linenum[735:750],Linenum[750:765],Linenum[765:768],Linenum[768:783],Linenum[783:798],Linenum[798:799],Linenum[799:802],Linenum[802:817],Linenum[817:832],Linenum[832:833],Linenum[833:836],Linenum[836:851],Linenum[851:866],Linenum[866:867],Linenum[867:870],Linenum[870:885],Linenum[885:900],Linenum[900:901],Linenum[901:904],Linenum[904:919],Linenum[919:934],Linenum[934:935],Linenum[935:937],Linenum[937:940],Linenum[940:980],Linenum[980:995],Linenum[995:1005],Linenum[1005:1020],Linenum[1020:1031],Linenum[1031:1039],Linenum[1039:1059],Linenum[1059:1081],Linenum[1081:1119],Linenum[1119:1154],Linenum[1154:1192],Linenum[1192:1230],Linenum[1230:1268],Linenum[1268:1283],Linenum[1283:1286],Linenum[1286:1289],Linenum[1289:1291],Linenum[1291:1331],Linenum[1331:1371],Linenum[1371:1386],Linenum[1386:1389],Linenum[1389:1392],Linenum[1392:1395],Linenum[1395:1398],Linenum[1398:1400]]
                                                        Corporate_Purchase_List.append(raw_list)

                                                    elif Linenum[4:6] == '07' :
                                                        # Chip Card
                                                        #ICCData = Linenum[585:841]
                                                        ICCData = " "
                                                        raw_list = [Inp_Jobid,Linenum[0:4],Linenum[4:6],Linenum[6:585],ICCData,Linenum[841:937],Linenum[937:940],Linenum[940:1005],Linenum[1005:1020],Linenum[1020:1031],Linenum[1031:1039],Linenum[1039:1278],Linenum[1278:1318],Linenum[1318:1400]]
                                                        Chip_List.append(raw_list)

                                                    elif Linenum[4:6] == '08' :
                                                        # Double Byte Character
                                                        raw_list = [Inp_Jobid,Linenum[0:4],Linenum[4:6],Linenum[6:8],Linenum[8:78],Linenum[78:148],Linenum[148:218],Linenum[218:288],Linenum[288:358],Linenum[358:937],Linenum[937:940],Linenum[940:1005],Linenum[1005:1020],Linenum[1020:1031],Linenum[1031:1039],Linenum[1039:1278],Linenum[1278:1318],Linenum[1318:1400]]
                                                        Double_Byte_List.append(raw_list)
                                                        
                                                    elif Linenum[4:6] == '09' :
                                                        # Extended Financial Addendum
                                                        FeeAmt_Mod  = Functions.adddecimal(Linenum[44:59],int(Linenum[62:63]))
                                                        Token = Linenum[1071:1090]
                                                        Token_Last4Digit = str(Token).strip()[-4:]
                                                        Token_Last4Digit = None if len(Token_Last4Digit) == 0 else Token_Last4Digit

                                                        raw_list = [Inp_Jobid,Linenum[0:4],Linenum[4:6],Linenum[6:21],Linenum[21:24],Linenum[24:25],Linenum[25:40],Linenum[40:43],Linenum[43:44],Linenum[44:59],Linenum[59:62],Linenum[62:63],Linenum[63:937],Linenum[937:940],Linenum[940:963],Linenum[963:986],Linenum[986:1005],Linenum[1005:1020],Linenum[1020:1031],Linenum[1031:1039],Linenum[1039:1054],Linenum[1054:1069],Linenum[1069:1071],Token_Last4Digit,Linenum[1090:1092],Linenum[1092:1094],Linenum[1094:1105],Linenum[1105:1278],Linenum[1278:1318],Linenum[1318:1400],FeeAmt_Mod]
                                                        Extended_Financial_List.append(raw_list)

                                                    else:
                                                        ErrorReason = f"FILESPLITTOCSV : Unexpected AddendaTypeCode = {Linenum[4:6]} Received at Record Number = {Parse_rec_count}"
                                                        AMEX_Select_And_Updates.AMEX_Update(5,Connection_String, Inp_Jobid, 'ERROR', ErrorReason)
                                                        logger.debug(ErrorReason,True)
                                                        FileInError = True
                                                        break
                                                    
                                                case '9825':                                                
                                                    TxnDateTime  =  Functions.datetimeconvert(Linenum[111:119],Linenum[119:125])
                                                    InsQuery = "INSERT INTO AMEXClr_IncomingTrailer ( JobId, MsgTypeIdentifier, TransDate_Org, TransTime_Org, TxnDateTime, ForwardInstIdCode, ActionCode, CreditCount, DebitCount, CreditsTotalAmt, DebitsTotalAmt, HashTotalAmt, ExtendedCreditTotalCount, ExtendedDebitTotalCount, FileSequenceNumber, MessageNumber, ReceivingInstId, RejectReasonCodes, ReservedField)"
                                                    InsQuery = f"{InsQuery} VALUES( {Inp_Jobid}, '{Linenum[0:4]}', '{Linenum[111:119]}', '{Linenum[119:125]}', '{TxnDateTime}', '{Linenum[211:222]}', '{Linenum[251:254]}', '{Linenum[938:944]}', '{Linenum[944:950]}', '{Linenum[950:966]}', '{Linenum[966:982]}', '{Linenum[982:999]}', '{Linenum[1005:1013]}', '{Linenum[1013:1021]}', '{Linenum[999:1005]}', '{Linenum[1031:1039]}', '{Linenum[1039:1050]}', '{Linenum[1278:1318]}', '{Linenum[889:938]}')"                                                
                                                    SQL_Connect.udf_InsSingleRecIntoDB(Connection_String, InsQuery)
                                                    LastParseMsgNum = int(Linenum[1031:1039])
                                                    logger.info("Trailer 9825 Info: Trailer stored successfully in database")

                                                case _:
                                                    key = Fernet.generate_key()
                                                    cipher_suite = Fernet(key)
                                                    temp_tet = bytes(Linenum,'utf-8')
                                                    ciphered_text = cipher_suite.encrypt(temp_tet)
                                                    InsQuery = "INSERT INTO AmexUnsupportedRec ( JobId, MsgTypeIdentifier, FileSource, BufferData, DCRKey, MsgNum)"
                                                    InsQuery = f"{InsQuery} VALUES( '{Inp_Jobid}', '{Linenum[0:4]}', '{FileSource}', '{str(ciphered_text)[2:-1]}', '{str(key)[2:-1]}', '{Linenum[1031:1039]}' )"
                                                    SQL_Connect.udf_InsSingleRecIntoDB(Connection_String, InsQuery)
                                                    logger.info("AmexUnsupportedRec MTI Info: Unknow Record Type is Found Please Check AmexUnsupportedRec")

                                            if len(POS_List) >= Insert_Limit_Value:
                                                MultiProcess.insert_to_Sql(Connection_String, POS_List, Header_POS, 'AMEXClr_Interim')
                                                POS_List = []

                                            if len(ATM_List) >= Insert_Limit_Value:
                                                MultiProcess.insert_to_Sql(Connection_String, ATM_List, Header_ATM, 'AMEXClr_Interim')
                                                ATM_List = []

                                            if len(MarketSpeciData_List) >= Insert_Limit_Value:
                                                MultiProcess.insert_to_Sql(Connection_String, MarketSpeciData_List, Header_MarketSpeciData, 'AMEXClr_MarketSpeciData_9240')
                                                MarketSpeciData_List = []

                                            if len(Fee_Collect_List) >= Insert_Limit_Value:
                                                MultiProcess.insert_to_Sql(Connection_String, Fee_Collect_List, Header_FeeCollect, 'AMEXClr_FeeCollect_1744')
                                                Fee_Collect_List = []

                                            if len(ATM_Fee_List) >= Insert_Limit_Value:
                                                MultiProcess.insert_to_Sql(Connection_String, ATM_Fee_List, Header_ATMFee, 'AMEXClr_ATMFee_1740')
                                                ATM_Fee_List = []

                                            if len(DefPaymentPlan_List) >= Insert_Limit_Value:
                                                MultiProcess.insert_to_Sql(Connection_String, DefPaymentPlan_List, Header_DeferredPay, 'AMEXClr_DeferredPay_9240_9340')
                                                DefPaymentPlan_List = []

                                            if len(AirLine_List) >= Insert_Limit_Value:
                                                MultiProcess.insert_to_Sql(Connection_String, AirLine_List, Header_AirIndus, 'AMEXClr_AirIndus_9240_9340')
                                                AirLine_List = []

                                            if len(Retail_Ind_List) >= Insert_Limit_Value:
                                                MultiProcess.insert_to_Sql(Connection_String, Retail_Ind_List, Header_RetailInd, 'AMEXClr_RetailInd_9240_9340')
                                                Retail_Ind_List = []

                                            if len(Insurance_List) >= Insert_Limit_Value:
                                                MultiProcess.insert_to_Sql(Connection_String, Insurance_List, Header_InsurInd, 'AMEXClr_InsurInd_9240_9340')
                                                Insurance_List = []

                                            if len(AutoRental_List) >= Insert_Limit_Value:
                                                MultiProcess.insert_to_Sql(Connection_String, AutoRental_List, Header_AutoRental, 'AMEXClr_AutoRental_9240_9340')
                                                AutoRental_List = []

                                            if len(Rail_Ind_List) >= Insert_Limit_Value:
                                                MultiProcess.insert_to_Sql(Connection_String, Rail_Ind_List, Header_RailInd, 'AMEXClr_RailInd_9240_9340')
                                                Rail_Ind_List = []

                                            if len(Lodging_List) >= Insert_Limit_Value:
                                                MultiProcess.insert_to_Sql(Connection_String, Lodging_List, Header_LodgingInd, 'AMEXClr_LodgingInd_9240_9340')
                                                Lodging_List = []

                                            if len(Restaurant_List) >= Insert_Limit_Value:
                                                MultiProcess.insert_to_Sql(Connection_String, Restaurant_List, Header_RestInd, 'AMEXClr_RestInd_9240_9340')
                                                Restaurant_List = []

                                            if len(Communication_Service_List) >= Insert_Limit_Value:
                                                MultiProcess.insert_to_Sql(Connection_String, Communication_Service_List, Header_CommServ, 'AmexClr_CommServ_9240_9340')
                                                Communication_Service_List = []

                                            if len(Travel_List) >= Insert_Limit_Value:
                                                MultiProcess.insert_to_Sql(Connection_String, Travel_List, Header_TravelCruise, 'AMEXClr_TravelCruise_9240_9340')
                                                Travel_List = []

                                            if len(General_List) >= Insert_Limit_Value:
                                                MultiProcess.insert_to_Sql(Connection_String, General_List, Header_GenForInd, 'AMEXClr_GenForInd_9240_9340')
                                                General_List = []

                                            if len(Entertainment_List) >= Insert_Limit_Value:
                                                MultiProcess.insert_to_Sql(Connection_String, Entertainment_List, Header_EntTickInd, 'AMEXClr_EntTickInd_9240_9340')
                                                Entertainment_List = []

                                            if len(Corporate_Purchase_List) >= Insert_Limit_Value:
                                                MultiProcess.insert_to_Sql(Connection_String, Corporate_Purchase_List, Header_CorpoPurchase, 'AMEXClr_CorpoPurchase_9240')
                                                Corporate_Purchase_List = []

                                            if len(Chip_List) >= Insert_Limit_Value:
                                                MultiProcess.insert_to_Sql(Connection_String, Chip_List, Header_ChipCard, 'AMEXClr_ChipCard_9240')
                                                Chip_List = []

                                            if len(Double_Byte_List) >= Insert_Limit_Value:
                                                MultiProcess.insert_to_Sql(Connection_String, Double_Byte_List, Header_DoubleByteChar,' AMEXClr_DoubleByteChar_9240')
                                                Double_Byte_List = []

                                            if len(Extended_Financial_List) >= Insert_Limit_Value:
                                                MultiProcess.insert_to_Sql(Connection_String, Extended_Financial_List, Header_ExtendFinan, 'AMEXClr_ExtendFinan_9240')
                                                Extended_Financial_List = []
                                                
                                        else:
                                            break

                                if FileInError :
                                    AMEX_Select_And_Updates.AMEX_Update(5,Connection_String, Inp_Jobid, 'ERROR', ErrorReason)
                                    Functions.File_Movement(OutFilePath,EnvVariable_Dict.get('ErrorDir'))
                                    Mail.SendEmail(2, 'Clearing And Settlement', EmailTo, EnvVariable_Dict.get('EmailFrom'),EnvVariable_Dict.get('SMTP_SERVER'),EnvVariable_Dict.get('SMTP_PORT'),Connection_String,Inp_Jobid,FileSource)
                                    sys.exit()
                                    
                                Insert_Limit_Value = 0
                                
                                if len(POS_List) > Insert_Limit_Value:
                                    MultiProcess.insert_to_Sql(Connection_String, POS_List, Header_POS, 'AMEXClr_Interim')
                                    POS_List = []

                                if len(ATM_List) > Insert_Limit_Value:
                                    MultiProcess.insert_to_Sql(Connection_String, ATM_List, Header_ATM, 'AMEXClr_Interim')
                                    ATM_List = []

                                if len(MarketSpeciData_List) > Insert_Limit_Value:
                                    MultiProcess.insert_to_Sql(Connection_String, MarketSpeciData_List, Header_MarketSpeciData, 'AMEXClr_MarketSpeciData_9240')
                                    MarketSpeciData_List = []

                                if len(Fee_Collect_List) > Insert_Limit_Value:
                                    MultiProcess.insert_to_Sql(Connection_String, Fee_Collect_List, Header_FeeCollect, 'AMEXClr_FeeCollect_1744')
                                    Fee_Collect_List = []

                                if len(ATM_Fee_List) > Insert_Limit_Value:
                                    MultiProcess.insert_to_Sql(Connection_String, ATM_Fee_List, Header_ATMFee, 'AMEXClr_ATMFee_1740')
                                    ATM_Fee_List = []

                                if len(DefPaymentPlan_List) > Insert_Limit_Value:
                                    MultiProcess.insert_to_Sql(Connection_String, DefPaymentPlan_List, Header_DeferredPay, 'AMEXClr_DeferredPay_9240_9340')
                                    DefPaymentPlan_List = []

                                if len(AirLine_List) > Insert_Limit_Value:
                                    MultiProcess.insert_to_Sql(Connection_String, AirLine_List, Header_AirIndus, 'AMEXClr_AirIndus_9240_9340')
                                    AirLine_List = []

                                if len(Retail_Ind_List) > Insert_Limit_Value:
                                    MultiProcess.insert_to_Sql(Connection_String, Retail_Ind_List, Header_RetailInd, 'AMEXClr_RetailInd_9240_9340')
                                    Retail_Ind_List = []

                                if len(Insurance_List) > Insert_Limit_Value:
                                    MultiProcess.insert_to_Sql(Connection_String, Insurance_List, Header_InsurInd, 'AMEXClr_InsurInd_9240_9340')
                                    Insurance_List = []

                                if len(AutoRental_List) > Insert_Limit_Value:
                                    MultiProcess.insert_to_Sql(Connection_String, AutoRental_List, Header_AutoRental, 'AMEXClr_AutoRental_9240_9340')
                                    AutoRental_List = []

                                if len(Rail_Ind_List) > Insert_Limit_Value:
                                    MultiProcess.insert_to_Sql(Connection_String, Rail_Ind_List, Header_RailInd, 'AMEXClr_RailInd_9240_9340')
                                    Rail_Ind_List = []

                                if len(Lodging_List) > Insert_Limit_Value:
                                    MultiProcess.insert_to_Sql(Connection_String, Lodging_List, Header_LodgingInd, 'AMEXClr_LodgingInd_9240_9340')
                                    Lodging_List = []

                                if len(Restaurant_List) > Insert_Limit_Value:
                                    MultiProcess.insert_to_Sql(Connection_String, Restaurant_List, Header_RestInd, 'AMEXClr_RestInd_9240_9340')
                                    Restaurant_List = []

                                if len(Communication_Service_List) > Insert_Limit_Value:
                                    MultiProcess.insert_to_Sql(Connection_String, Communication_Service_List, Header_CommServ, 'AmexClr_CommServ_9240_9340')
                                    Communication_Service_List = []

                                if len(Travel_List) > Insert_Limit_Value:
                                    MultiProcess.insert_to_Sql(Connection_String, Travel_List, Header_TravelCruise, 'AMEXClr_TravelCruise_9240_9340')
                                    Travel_List = []

                                if len(General_List) > Insert_Limit_Value:
                                    MultiProcess.insert_to_Sql(Connection_String, General_List, Header_GenForInd, 'AMEXClr_GenForInd_9240_9340')
                                    General_List = []

                                if len(Entertainment_List) > Insert_Limit_Value:
                                    MultiProcess.insert_to_Sql(Connection_String, Entertainment_List, Header_EntTickInd, 'AMEXClr_EntTickInd_9240_9340')
                                    Entertainment_List = []

                                if len(Corporate_Purchase_List) > Insert_Limit_Value:
                                    MultiProcess.insert_to_Sql(Connection_String, Corporate_Purchase_List, Header_CorpoPurchase, 'AMEXClr_CorpoPurchase_9240')
                                    Corporate_Purchase_List = []

                                if len(Chip_List) > Insert_Limit_Value:
                                    MultiProcess.insert_to_Sql(Connection_String, Chip_List, Header_ChipCard, 'AMEXClr_ChipCard_9240')
                                    Chip_List = []

                                if len(Double_Byte_List) > Insert_Limit_Value:
                                    MultiProcess.insert_to_Sql(Connection_String, Double_Byte_List, Header_DoubleByteChar,' AMEXClr_DoubleByteChar_9240')
                                    Double_Byte_List = []

                                if len(Extended_Financial_List) > Insert_Limit_Value:
                                    MultiProcess.insert_to_Sql(Connection_String, Extended_Financial_List, Header_ExtendFinan, 'AMEXClr_ExtendFinan_9240')
                                    Extended_Financial_List = []
                                    
                            except Exception as e:
                                ErrorReason = f"Error Raised PARSING : {e}"
                                ErrorReason = ErrorReason.replace("'","")
                                AMEX_Select_And_Updates.AMEX_Update(5,Connection_String, Inp_Jobid, 'ERROR', ErrorReason)
                                logger.debug(f"Error Raised PARSING : {e}",True )
                                logger.log_exception(*sys.exc_info())
                                sys.exit()
                                
                            if Parse_rec_count == LastParseMsgNum :
                                AMEX_Select_And_Updates.AMEX_Update(3, Connection_String, Inp_Jobid, From = LastParseMsgNum)
                            else:
                                ErrorReason = f"PARSING : All record are not written to CSV OR MessageNumber = {LastParseMsgNum} Mismatch with Total Read Records = {Parse_rec_count}"
                                AMEX_Select_And_Updates.AMEX_Update(5,Connection_String, Inp_Jobid,'ERROR', ErrorReason)
                                Functions.File_Movement(OutFilePath,EnvVariable_Dict.get('ErrorDir'))
                                Mail.SendEmail(2, 'Clearing And Settlement', EmailTo, EnvVariable_Dict.get('EmailFrom'),EnvVariable_Dict.get('SMTP_SERVER'),EnvVariable_Dict.get('SMTP_PORT'),Connection_String,Inp_Jobid,FileSource)
                                logger.error(ErrorReason)
                                sys.exit()

                            current,peak = tracemalloc.get_traced_memory()
                            logger.info("PARSING Memory Trace Current = " + str(current/1024) + " MB And Peak = " + str(peak/1024) + " MB")
                            tracemalloc.clear_traces()
                            
                            res = AMEX_SPCall.AMEX_SPCall(Connection_String, Inp_Jobid, Parse_rec_count, EnvVariable_Dict.get('AMEX_ValidationEnable'), EnvVariable_Dict.get('AMEX_UseCCardOrCCard2'),Stage_StartTime, EmailTo, EnvVariable_Dict.get('EmailFrom'),EnvVariable_Dict.get('SMTP_SERVER'),EnvVariable_Dict.get('SMTP_PORT'), FileSource)
                            
                            if res:
                                current,peak = tracemalloc.get_traced_memory()
                                logger.info("SP Memory Trace Current = " + str(current/1024) + " MB And Peak = " + str(peak/1024) + " MB")
                                tracemalloc.clear_traces()
                                Mail.SendEmail(1, 'Clearing And Settlement', EmailTo, EnvVariable_Dict.get('EmailFrom'),EnvVariable_Dict.get('SMTP_SERVER'),EnvVariable_Dict.get('SMTP_PORT'),Connection_String,Inp_Jobid,FileSource)
                                logger.info(f"******************************* File Processing For Jobid {Inp_Jobid} Completed *******************************", True)
                                
                            else:
                                logger.error('Some Issue occur in AMEX_SPCall Function Please Check')
                                sys.exit()
                            
                            res = AMEX_Select_And_Updates.AMEX_Select(11, Connection_String, ArgVar_1 = FileSource)

                            if res[0][0] > 0:
                                #Resetting Some Variables
                                ErrorReason = ""
                                Parse_rec_count = Rec_Count = 0
                                GotoReadFile = True

                                res = AMEX_SPCall.SP_Call_ChangeFileStatus(Connection_String, FProcess)
                                GotoReadFile = True if res == 1 else False
                                if not GotoReadFile:
                                    logger.debug("SP PR_AMEXChangeFileStatus doesnot return Positive result after executing and some files are in InQueue",True)
                                    break
                            else:                            
                                #Resetting Some Variables
                                ErrorReason = ""
                                Parse_rec_count = Rec_Count = 0
                                GotoReadFile = False                   
        else:
            logger.info("No File Found For Processing")
            time.sleep(EnvVariable_Dict['FileRecheckTime'])