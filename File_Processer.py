import sys
import codecs
import datetime
import Functions
from Logger import Logger

######################################################################################################################################################

logger = Logger()

######################################################################################################################################################

def File_Validate(InputFilePath):
    try:
        FileInError = IsUnexpectedMTI  = Head_Trail_Error = False
        ErrorReason =''
        HeadCount = TrailCount = OtherRecCount = linecounter = TotlaCharInFile = 0
        with codecs.open(InputFilePath, 'r', "cp500") as fo:
            while fileline := fo.read(1400):
                linecounter += 1
                TotlaCharInFile += len(fileline)
                
                if len(fileline) != 1400 :
                    ErrorReason = f"In record number {linecounter}, the record length is {len(fileline)} instead of 1400"
                    logger.debug(ErrorReason)
                    FileInError = True
                    break
                else:
                    raw_record = fileline
                    #Getting MTI to distinguish records
                    CombMTI = raw_record[0:4]
                    match CombMTI:
                        case '9824':
                            HeadCount += 1
                        case '9825':
                            TrailCount += 1
                            LastParsedMsgNum = int(raw_record[1031:1039])
                            
                        case '1240' | '1744' | '1740' | '9340' | '9240':
                            OtherRecCount += 1
                        case _:
                            OtherRecCount += 1
                            IsUnexpectedMTI = True
        if not FileInError:
            if (LastParsedMsgNum != linecounter or HeadCount != 1 or TrailCount != 1):
                Head_Trail_Error = True
                ErrorReason = f"Record Count or Header / Trailer Count is incorrect. TotalRecord = {linecounter}, Header = {HeadCount} ,Trailer = {TrailCount}, TrailerMsgNo. = {LastParsedMsgNum}, TotalCharacterInFile = {TotlaCharInFile} "
                logger.debug(ErrorReason,True)
            elif IsUnexpectedMTI:
                ErrorReason = f"Warning: UnsupportedRec MessageTypeIdentifier Found Please Check AmexUnsupportedRec Table"
                logger.debug(ErrorReason)
            elif OtherRecCount == 0:
                ErrorReason = "Empty File Is Going To Processed"
                logger.debug(ErrorReason)

    except Exception as e:
        logger.debug(e,True)
        logger.log_exception(*sys.exc_info())

    return IsUnexpectedMTI, FileInError, ErrorReason, OtherRecCount, Head_Trail_Error

######################################################################################################################################################

def POS(Inp_Jobid, raw_record, Parse_rec_count):
    try:
        raw_list = []
        cardhash                = Functions.KMSHash(raw_record[6:25].strip())
        card_org                = raw_record[6:25].strip()
        CAName                  = raw_record[277:315].strip()[:24]
        CACity                  = raw_record[391:412].strip()[:20]
        CAAddressLine1          = raw_record[315:353].strip()[:30]
        CANameLocation_Org      = f"{raw_record[277:315]}{raw_record[315:353]}{raw_record[353:391]}{raw_record[391:412]} {raw_record[412:427]} {raw_record[430:433]} {raw_record[427:430]}"
        CANameLocation          = f"{CAName} {CAAddressLine1} {CACity} {raw_record[412:427].strip()} {raw_record[430:433]} {raw_record[427:430]}"
        FPPresentmentAmt        = float(Functions.adddecimal(raw_record[606:621],int(raw_record[639:640])))
        AddAmt1                 = float(Functions.adddecimal(raw_record[652:667],int(raw_record[639:640])))
        AddAmt2                 = float(Functions.adddecimal(raw_record[671:686],int(raw_record[639:640])))
        AddAmt3                 = float(Functions.adddecimal(raw_record[690:705],int(raw_record[639:640])))
        AddAmt4                 = float(Functions.adddecimal(raw_record[709:724],int(raw_record[639:640])))
        AddAmt5                 = float(Functions.adddecimal(raw_record[728:743],int(raw_record[639:640])))
        TxnDateTime             = datetime.datetime.strptime(f'{raw_record[111:119]} {raw_record[119:125]}', '%Y%m%d %H%M%S').strftime('%Y-%m-%d %H:%M:%S')
        NetworkProcessDateTime  = datetime.datetime.strptime(f'{raw_record[786:794]} {raw_record[794:800]}', '%Y%m%d %H%M%S').strftime('%Y-%m-%d %H:%M:%S')
        IssuerSettleDateTime    = datetime.datetime.strptime(f'{raw_record[174:182]} {raw_record[902:908]}', '%Y%m%d %H%M%S').strftime('%Y-%m-%d %H:%M:%S')


        CardExp_Date    = Functions.Conv_Card_Expiriation_Date(raw_record[125:129].strip())
        if CardExp_Date not in [None,""]:
            CardExp_Date  = CardExp_Date.strftime('%Y-%m-%d %H:%M:%S')
        #CrossBorderInd = 'N' if raw_record[427:430] == '840' else 'Y'
        AMEX_AlgorithmID = 1 if raw_record[25:27] in ['00','02','01','06','17'] else -1
        
        FP_ReservedField9 = SP_MsgReasonCode = SP_ReservedField1 = SP_ReservedField2 = SP_FPTxnCurrencyCode = SP_FPTxnDate_Org = SP_FPTxnTime_Org = SP_FPTxnDateTime = ""
        SP_FPNetworkProcessDate = SP_FPNetworkProcessTime = SP_FPNetworkProcessDateTime  = ""

        IssuerNetSettleAmt    = float(Functions.adddecimal(raw_record[568:583],int(raw_record[601:602])))
        IssuerGrossSettleAmt  = float(Functions.adddecimal(raw_record[433:448],int(raw_record[601:602])))
        NetworkRateAmt        = float(int(raw_record[448:463])/100)
        ApprovalCode          = int(raw_record[245:251]) if raw_record[245:251].strip() != '' else 0
        
        if raw_record[166:169] == '200' :
            FP_ReservedField9           = raw_record[169:173]
            TxnAmt                      = float(Functions.adddecimal(raw_record[31:46],int(raw_record[605:606])))
            FPTxnAmt                    = float(Functions.adddecimal(raw_record[185:200],int(raw_record[605:606])))

        elif raw_record[166:169] == '205' :
            SP_MsgReasonCode            = raw_record[169:173]
            SP_ReservedField1           = raw_record[566:567]
            SP_ReservedField2           = raw_record[567:568]
            SP_FPTxnCurrencyCode        = raw_record[602:605]
            SP_FPTxnDate_Org            = raw_record[765:773]
            SP_FPTxnTime_Org            = raw_record[773:779]
            SP_FPNetworkProcessDate     = raw_record[908:916]
            SP_FPNetworkProcessTime     = raw_record[916:922]
            SP_TxnDecimalization        = raw_record[800:801]
            SP_FPNetworkProcessDateTime = datetime.datetime.strptime(f'{SP_FPNetworkProcessDate} {SP_FPNetworkProcessTime}', '%Y%m%d %H%M%S').strftime('%Y-%m-%d %H:%M:%S')
            SP_FPTxnDateTime            = Functions.datetimeconvert(SP_FPTxnDate_Org,SP_FPTxnTime_Org)
            TxnAmt                      = float(Functions.adddecimal(raw_record[31:46],int(raw_record[800:801])))
            FPTxnAmt                    = float(Functions.adddecimal(raw_record[185:200],int(raw_record[605:606])))
            

        raw_list = [Inp_Jobid ,raw_record[0:4] ,raw_record[166:169] ,raw_record[25:27] ,raw_record[25:27] ,raw_record[27:29] ,raw_record[29:31] ,raw_record[4:6] ,raw_record[111:119] ,raw_record[119:125] ,TxnDateTime ,raw_record[31:46] ,TxnAmt ,raw_record[1005:1020] ,raw_record[641:644] ,raw_record[922:924] ,raw_record[141:145] ,raw_record[937:940] ,raw_record[1031:1039] ,raw_record[125:129] ,CardExp_Date ,raw_record[133:141] ,raw_record[896:902] ,IssuerSettleDateTime ,raw_record[174:182] ,raw_record[902:908] ,IssuerGrossSettleAmt ,IssuerNetSettleAmt ,raw_record[433:448] ,raw_record[568:583] ,raw_record[598:601] ,raw_record[601:602] ,raw_record[1039:1050] ,raw_record[924:935] ,FPTxnAmt ,FPPresentmentAmt ,raw_record[185:200] ,raw_record[606:621] ,raw_record[639:640] ,raw_record[636:639] ,raw_record[621:636] ,raw_record[621:636] ,raw_record[605:606] ,raw_record[783:785] ,raw_record[891:893] ,raw_record[1260:1275] ,FP_ReservedField9 ,raw_record[605:606] ,SP_FPTxnDateTime ,SP_FPTxnDate_Org ,SP_FPTxnTime_Org ,SP_FPTxnCurrencyCode \
                    ,raw_record[605:606] ,SP_FPNetworkProcessDateTime ,SP_FPNetworkProcessDate ,SP_FPNetworkProcessTime ,raw_record[980:982] ,raw_record[982:1005] ,SP_ReservedField1 ,SP_ReservedField2 ,SP_MsgReasonCode ,NetworkRateAmt ,raw_record[448:463] ,NetworkProcessDateTime ,raw_record[786:794] ,raw_record[794:800] ,raw_record[254:262] ,raw_record[262:277] ,raw_record[277:315] ,CAName ,raw_record[315:353] ,CAAddressLine1 ,raw_record[353:391] ,raw_record[391:412] ,CACity ,raw_record[412:427] ,raw_record[427:430] ,raw_record[430:433] ,raw_record[940:980] ,raw_record[640:641] ,raw_record[1230:1260] ,raw_record[200:211] ,raw_record[211:222] ,raw_record[222:245] ,raw_record[173:174] ,ApprovalCode ,raw_record[509:511] ,raw_record[511:532] ,raw_record[893:895] ,raw_record[785:786] ,raw_record[154:166] ,raw_record[182:184] ,raw_record[651:652] ,AddAmt1 ,raw_record[652:667] ,raw_record[667:670] ,raw_record[670:671] ,AddAmt2 ,raw_record[671:686] ,raw_record[686:689] ,raw_record[689:690] ,AddAmt3 ,raw_record[690:705] \
                        ,raw_record[705:708] ,raw_record[708:709] ,AddAmt4 ,raw_record[709:724] ,raw_record[724:727] ,raw_record[727:728] ,AddAmt5 ,raw_record[728:743] ,raw_record[743:746] ,raw_record[746:748] ,raw_record[748:763] ,raw_record[935:937] ,raw_record[1020:1022] ,raw_record[1278:1318] ,raw_record[46:61] ,raw_record[61:76] ,raw_record[76:88] ,raw_record[88:103] ,raw_record[103:104] ,raw_record[104:107] ,raw_record[107:111] ,raw_record[145:154] ,raw_record[184:185] ,raw_record[252:254] ,raw_record[463:478] ,raw_record[478:493] ,raw_record[493:508] ,raw_record[508:509] ,raw_record[547:548] ,raw_record[548:563] ,raw_record[563:566] ,raw_record[583:598] ,raw_record[644:647] ,raw_record[647:650] ,raw_record[779:783] ,raw_record[801:891] ,raw_record[1022:1031] ,raw_record[1050:1230] ,raw_record[1275:1278] ,raw_record[1342:1345] ,raw_record[1345:1346] ,raw_record[1349:1364] ,raw_record[1372:1374] ,raw_record[1374:1389] ,raw_record[1389:1393] ,raw_record[1393:1397] ,raw_record[1397:1400] ,card_org[-4:] ,cardhash ,card_org[0:6] \
                            ,AMEX_AlgorithmID ,CANameLocation_Org ,CANameLocation]
        
        raw_list = [None if DE == '' else DE for DE in raw_list]
                
        return raw_list
    
    except Exception as e:
        logger.debug(f'Exception Raised For Record Number {Parse_rec_count}',True)
        logger.debug(e,True)
        logger.log_exception(*sys.exc_info())

######################################################################################################################################################

def ATM(Inp_Jobid, raw_record, Parse_rec_count):
    try:
        raw_list = []
        cardhash                = Functions.KMSHash(raw_record[6:25].strip())
        card_org                = raw_record[6:25].strip()
        CAName                  = raw_record[277:315].strip()[:24]
        CACity                  = raw_record[391:412].strip()[:20]
        CAAddressLine1          = raw_record[315:353].strip()[:30]
        CANameLocation_Org      = f"{raw_record[277:315]}{raw_record[315:353]}{raw_record[353:391]}{raw_record[391:412]} {raw_record[412:427]} {raw_record[430:433]} {raw_record[427:430]}"
        CANameLocation          = f"{CAName} {CAAddressLine1} {CACity} {raw_record[412:427].strip()} {raw_record[430:433]} {raw_record[427:430]}"
        FPPresentmentAmt        = float(Functions.adddecimal(raw_record[606:621],int(raw_record[639:640])))
        AddAmt1                 = float(Functions.adddecimal(raw_record[652:667],int(raw_record[639:640])))
        AddAmt2                 = float(Functions.adddecimal(raw_record[671:686],int(raw_record[639:640])))
        AddAmt3                 = float(Functions.adddecimal(raw_record[690:705],int(raw_record[639:640])))
        AddAmt4                 = float(Functions.adddecimal(raw_record[709:724],int(raw_record[639:640])))
        AddAmt5                 = float(Functions.adddecimal(raw_record[728:743],int(raw_record[639:640])))
        TxnDateTime             = datetime.datetime.strptime(f'{raw_record[111:119]} {raw_record[119:125]}', '%Y%m%d %H%M%S').strftime('%Y-%m-%d %H:%M:%S')
        NetworkProcessDateTime  = datetime.datetime.strptime(f'{raw_record[786:794]} {raw_record[794:800]}', '%Y%m%d %H%M%S').strftime('%Y-%m-%d %H:%M:%S')
        IssuerSettleDateTime    = datetime.datetime.strptime(f'{raw_record[174:182]} {raw_record[902:908]}', '%Y%m%d %H%M%S').strftime('%Y-%m-%d %H:%M:%S')
            
        CardExp_Date    = Functions.Conv_Card_Expiriation_Date(raw_record[125:129].strip())
        if CardExp_Date not in [None,""]:
            CardExp_Date  = CardExp_Date.strftime('%Y-%m-%d %H:%M:%S')
        #CrossBorderInd  = 'N' if raw_record[427:430] == '840' else 'Y'
        AMEX_AlgorithmID = 1 if raw_record[25:27] in ['00','02','01','06','17'] else -1
        
        FP_ReservedField5 = FP_ReservedField30 = FP_IssuerAuthAmt_Org = FP_IssuerAuthAmt = FP_ReservedField17 = SP_MsgReasonCode = SP_FPTxnAmt_Org = SP_FPTxnAmt = ""
        SP_IssuerNetSettleAmt_Org = SP_IssuerNetSettleAmt = SP_FPTxnCurrencyCode = SP_FPTxnDate_Org = SP_FPTxnTime_Org = SP_FPTxnDateTime = SP_TxnDecimalization = ""
        SP_FPNetworkProcessingDate = SP_FPNetworkProcessingTime = SP_FPNetworkProcessDateTime = ""
                
        if raw_record[166:169] == '200':
            FP_ReservedField5           = raw_record[185:200]
            FP_ReservedField30          = raw_record[891:893]
            FP_ReservedField17          = raw_record[602:605]
            FP_IssuerAuthAmt_Org        = raw_record[433:448]
            IssuerGrossSettleAmt_Org    = raw_record[568:583]
            FP_IssuerAuthAmt            = float(Functions.adddecimal(FP_IssuerAuthAmt_Org,int(raw_record[601:602])))
            IssuerGrossSettleAmt        = float(Functions.adddecimal(IssuerGrossSettleAmt_Org,int(raw_record[601:602])))
            TxnAmt                      = float(Functions.adddecimal(raw_record[31:46],int(raw_record[605:606])))

        elif raw_record[166:169] =='205':
            SP_Filler1                  = raw_record[891:893]
            SP_MsgReasonCode            = raw_record[169:173]
            SP_FPTxnCurrencyCode        = raw_record[602:605]
            SP_FPTxnDate_Org            = raw_record[765:773]
            SP_FPTxnTime_Org            = raw_record[773:779]
            SP_FPNetworkProcessingDate  = raw_record[908:916]
            SP_FPNetworkProcessingTime  = raw_record[916:922]
            SP_FPTxnAmt_Org             = raw_record[185:200]
            IssuerGrossSettleAmt_Org    = raw_record[433:448]
            SP_IssuerNetSettleAmt_Org   = raw_record[568:583]
            SP_TxnDecimalization        = raw_record[800:801]
            SP_FPNetworkProcessDateTime = datetime.datetime.strptime(f'{SP_FPNetworkProcessingDate} {SP_FPNetworkProcessingTime}', '%Y%m%d %H%M%S').strftime('%Y-%m-%d %H:%M:%S')
            SP_FPTxnDateTime            = datetime.datetime.strptime(f'{SP_FPTxnDate_Org} {SP_FPTxnTime_Org}', '%Y%m%d %H%M%S').strftime('%Y-%m-%d %H:%M:%S')
            TxnAmt                      = float(Functions.adddecimal(raw_record[31:46],int(SP_TxnDecimalization)))
            SP_FPTxnAmt                 = float(Functions.adddecimal(SP_FPTxnAmt_Org,int(raw_record[605:606])))
            IssuerGrossSettleAmt        = float(Functions.adddecimal(IssuerGrossSettleAmt_Org,int(raw_record[601:602])))
            SP_IssuerNetSettleAmt       = float(Functions.adddecimal(SP_IssuerNetSettleAmt_Org,int(raw_record[601:602])))

        raw_list = [Inp_Jobid, raw_record[0:4], raw_record[166:169], raw_record[25:27], raw_record[25:27], raw_record[27:29], raw_record[29:31], raw_record[4:6], raw_record[111:119], raw_record[119:125], TxnDateTime, raw_record[31:46], TxnAmt, raw_record[1005:1020], raw_record[641:644], raw_record[922:924], raw_record[141:145], raw_record[937:940], raw_record[1031:1039], raw_record[125:129], CardExp_Date, raw_record[133:141], raw_record[896:902], IssuerSettleDateTime, raw_record[174:182], raw_record[902:908], IssuerGrossSettleAmt, SP_IssuerNetSettleAmt, IssuerGrossSettleAmt_Org, SP_IssuerNetSettleAmt_Org, raw_record[598:601], raw_record[601:602], raw_record[1039:1050], raw_record[924:935], SP_FPTxnAmt, FPPresentmentAmt, SP_FPTxnAmt_Org, raw_record[606:621], raw_record[639:640], raw_record[636:639], raw_record[621:636], raw_record[621:636], raw_record[605:606], SP_TxnDecimalization, SP_FPTxnDateTime, SP_FPTxnDate_Org, SP_FPTxnTime_Org, SP_FPTxnCurrencyCode, raw_record[605:606], SP_FPNetworkProcessDateTime, \
                    SP_FPNetworkProcessingDate, SP_FPNetworkProcessingTime, SP_MsgReasonCode, NetworkProcessDateTime, raw_record[786:794], raw_record[794:800], raw_record[254:262], raw_record[262:277], raw_record[277:315], CAName, raw_record[315:353], CAAddressLine1, raw_record[353:391], raw_record[391:412], CACity, raw_record[412:427], raw_record[427:430], raw_record[430:433], raw_record[200:211], raw_record[211:222], raw_record[222:245], raw_record[173:174], raw_record[245:251], raw_record[154:166], raw_record[651:652], AddAmt1, raw_record[652:667], raw_record[667:670], raw_record[670:671], AddAmt2, raw_record[671:686], raw_record[686:689], raw_record[689:690], AddAmt3, raw_record[690:705], raw_record[705:708], raw_record[708:709], AddAmt4, raw_record[709:724], raw_record[724:727], raw_record[727:728], AddAmt5, raw_record[728:743], raw_record[743:746], raw_record[935:937], raw_record[1278:1318], FP_IssuerAuthAmt, FP_IssuerAuthAmt_Org, raw_record[46:61], raw_record[61:76], raw_record[88:103], raw_record[145:154], \
                        FP_ReservedField5, raw_record[252:254], raw_record[463:478], raw_record[478:493], raw_record[493:508], raw_record[508:509], raw_record[547:548], raw_record[548:563], raw_record[563:566], raw_record[566:567], raw_record[567:568], raw_record[583:598], FP_ReservedField17, raw_record[644:647], raw_record[647:650], raw_record[746:747], raw_record[747:762], raw_record[762:765], raw_record[779:786], raw_record[801:816], raw_record[816:831], raw_record[831:846], raw_record[846:861], raw_record[861:876], raw_record[876:891], FP_ReservedField30, raw_record[940:955], raw_record[955:970], raw_record[970:973], raw_record[973:976], raw_record[976:977], raw_record[977:978], raw_record[1050:1260], raw_record[1319:1334], raw_record[1334:1342], raw_record[1349:1364], raw_record[1364:1372], raw_record[1374:1389], raw_record[1389:1393], raw_record[1393:1397], raw_record[1397:1400], card_org[-4:], cardhash, card_org[0:6], AMEX_AlgorithmID, CANameLocation_Org, CANameLocation]
        
        raw_list = [None if DE == '' else DE for DE in raw_list]
                
        return raw_list
    
    except Exception as e:
        logger.debug(f'Exception Raised For Record Number {Parse_rec_count}',True)
        logger.debug(e,True)
        logger.log_exception(*sys.exc_info())

######################################################################################################################################################

def Fee_Collect(Inp_Jobid, raw_record, FileReferenceDate, Parse_rec_count):
    try:
        TxnAmt                  = str(float(Functions.adddecimal(raw_record[31:46],int(raw_record[605:606]))))
        SettleAmt               = str(float(Functions.adddecimal(raw_record[568:583],int(raw_record[601:602]))))
        PresentmentAmt          = str(float(Functions.adddecimal(raw_record[606:621],int(raw_record[639:640]))))
        TxnDateTime             = datetime.datetime.strptime(f'{raw_record[111:119]} {raw_record[119:125]}', '%Y%m%d %H%M%S').strftime('%Y-%m-%d %H:%M:%S')
        SettleDateTime          = datetime.datetime.strptime(f'{raw_record[174:182]} {raw_record[902:908]}', '%Y%m%d %H%M%S').strftime('%Y-%m-%d %H:%M:%S')
        NetworkProcessDateTime  = datetime.datetime.strptime(f'{raw_record[786:794]} {raw_record[794:800]}', '%Y%m%d %H%M%S').strftime('%Y-%m-%d %H:%M:%S')
        DbCrIndicator = 'D' if raw_record[25:27] in ['00','02','01','06','17'] else 'C'                                            
        raw_list = [Inp_Jobid,raw_record[0:4],raw_record[4:25],raw_record[25:27],raw_record[27:29],raw_record[29:31],TxnAmt,raw_record[31:46],raw_record[46:61],raw_record[61:88],raw_record[88:103],raw_record[103:111],raw_record[111:119],raw_record[119:125],TxnDateTime,raw_record[125:166],raw_record[166:169],raw_record[169:171],raw_record[171:174],raw_record[174:182],raw_record[182:185],raw_record[185:200],raw_record[200:211],raw_record[211:222],raw_record[222:245],raw_record[245:340],raw_record[340:392],raw_record[392:433],raw_record[433:435],raw_record[435:567],raw_record[567:568],SettleAmt,raw_record[568:583],raw_record[583:598],raw_record[598:601],raw_record[601:602],raw_record[602:605],raw_record[605:606],PresentmentAmt,raw_record[606:621],raw_record[621:636],raw_record[636:639],raw_record[639:640],raw_record[640:641],raw_record[641:644],raw_record[644:647],raw_record[647:786],raw_record[786:794],raw_record[794:800],NetworkProcessDateTime,raw_record[800:896],raw_record[896:902],raw_record[902:908],SettleDateTime,raw_record[908:924],raw_record[924:935],raw_record[935:937],raw_record[937:940],raw_record[940:1005],raw_record[1005:1020],raw_record[1020:1031],raw_record[1031:1039],raw_record[1039:1050],raw_record[1050:1278],raw_record[1278:1318],raw_record[1318:1400],DbCrIndicator,FileReferenceDate]
        raw_list = [None if DE == '' else DE for DE in raw_list]
        return raw_list
    
    except Exception as e:
        logger.debug(f'Exception Raised For Record Number {Parse_rec_count}',True)
        logger.debug(e,True)
        logger.log_exception(*sys.exc_info())

######################################################################################################################################################

def ATM_Fee(Inp_Jobid, raw_record, Parse_rec_count):
    try:
        card_org                = raw_record[6:25].strip()
        cardhash                = Functions.KMSHash(raw_record[6:25].strip())
        TxnDateTime             = datetime.datetime.strptime(f'{raw_record[111:119]} {raw_record[119:125]}', '%Y%m%d %H%M%S').strftime('%Y-%m-%d %H:%M:%S')
        SettleDateTime          = datetime.datetime.strptime(f'{raw_record[174:182]} {raw_record[902:908]}', '%Y%m%d %H%M%S').strftime('%Y-%m-%d %H:%M:%S')
        NetworkProcessDateTime  = datetime.datetime.strptime(f'{raw_record[786:794]} {raw_record[794:800]}', '%Y%m%d %H%M%S').strftime('%Y-%m-%d %H:%M:%S')
        TxnAmt                  = Functions.adddecimal(raw_record[31:46],int(raw_record[605:606]))
        SettleAmt               = Functions.adddecimal(raw_record[568:583],int(raw_record[601:602]))
        PresentmentAmt          = Functions.adddecimal(raw_record[606:621],int(raw_record[639:640]))

        AMEX_AlgorithmID = '1' if raw_record[25:27] in ['00','02','01','06','17'] else '-1'
        
        raw_list = [Inp_Jobid,raw_record[0:4],raw_record[4:6],cardhash,raw_record[25:27],raw_record[27:29],raw_record[29:31],raw_record[31:46],TxnAmt,raw_record[46:61],raw_record[61:88],raw_record[88:103],raw_record[103:111],raw_record[111:119],raw_record[119:125],TxnDateTime,raw_record[125:166],raw_record[166:169],raw_record[169:174],raw_record[174:182],raw_record[182:200],raw_record[200:211],raw_record[211:222],raw_record[222:245],raw_record[245:340],raw_record[340:433],raw_record[433:435],raw_record[435:567],raw_record[567:568],SettleAmt,raw_record[568:583],raw_record[583:598],raw_record[598:601],raw_record[601:602],raw_record[602:605],raw_record[605:606],PresentmentAmt,raw_record[606:621],raw_record[621:636],raw_record[636:639],raw_record[639:640],raw_record[640:641],raw_record[641:644],raw_record[644:647],raw_record[647:786],raw_record[786:794],raw_record[794:800],NetworkProcessDateTime,raw_record[800:902],raw_record[902:908],SettleDateTime,raw_record[908:924],raw_record[924:935],raw_record[935:937],raw_record[937:940],raw_record[940:1005],raw_record[1005:1020],raw_record[1020:1031],raw_record[1031:1039],raw_record[1039:1050],raw_record[1050:1278],raw_record[1278:1318],raw_record[1318:1400],card_org[-4:],cardhash,card_org[0:6],AMEX_AlgorithmID]
        raw_list = [None if DE == '' else DE for DE in raw_list]
        return raw_list
    
    except Exception as e:
        logger.debug(f'Exception Raised For Record Number {Parse_rec_count}',True)
        logger.debug(e,True)
        logger.log_exception(*sys.exc_info())
