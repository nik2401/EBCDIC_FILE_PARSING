#Import libraries
try:
    import datetime
    import sys
    import smtplib
    import socket
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from Logger import Logger
    import AMEX_Select_And_Updates

    # package should be install, if not [Command: pip install modulename]
except ModuleNotFoundError as err:
    print(str({err.name.strip()})," module is missing, Command to Install - {"" pip install ", str(err.name.strip()), "}")
    sys.exit(1)

######################################################################################################################################################

logger = Logger()

######################################################################################################################################################

def Fill_FileStatus_Table(Connection_String, Inp_JobId, Source):
    global TableEntry
    res = AMEX_Select_And_Updates.AMEX_Select(9,Connection_String, Inp_JobId, Source)
    TableEntry = ''
    TableEntry = TableEntry + \
            """<tr>
                <td><b><h4> """ + str(res[0][0]) + """ </h4></b></td>
                <td><b><h4> """ + str(Inp_JobId) + """ </h4></b></td>
                <td><b><h4> """ + str(res[0][1]) + """ </h4></b></td>
                <td><b><h4> """ + str(res[0][2]) + """ </h4></b></td>
                <td><b><h4> """ + str(res[0][3]) + """ </h4></b></td>
            </tr>"""
    return TableEntry

######################################################################################################################################################

def Fill_DoneFile_Table(Connection_String, Inp_JobId, Source):
    global TableEntry
    
    res = AMEX_Select_And_Updates.AMEX_Select(10,Connection_String, Inp_JobId, Source)

    TableEntry = ''
    Ack_Generated = 'NO'
    Date_Received = res[0][1].strftime('%Y-%m-%d %H:%M:%S') if res[0][1] != 'None' else 'NULL'
    FileDate = res[0][2].strftime('%Y-%m-%d %H:%M:%S') if res[0][2] != 'None' else 'NULL'
    Tot_Rec_Received = '0' if res[0][3] == 'None' else str(res[0][3])
    CCard_Rec = '0' if res[0][4] == 'None' else str(res[0][4])
    

    TableEntry = TableEntry + \
            """<b> Summary: """ + str(res[0][0]) + """ </b>
            <br>
            <table border=""1"">
                <tr>
                    <td><b><h3> Date Received </h3></b></td>
                    <td><b><h3> File Date </h3></b></td>
                    <td><b><h3> File Source </h3></b></td>
                    <td><b><h3> Total Record Received </h3></b></td>
                    <td><b><h3> CCard Record Count </h3></b></td>
                    <td><b><h3> Current Status </h3></b></td>
                    <td><b><h3> Acknowledgment Generated </h3></b></td>
                </tr>
                <tr>
                    <td><b><h3> """ + Date_Received + """ </h3></b></td>
                    <td><b><h3> """ + FileDate + """ </h3></b></td>
                    <td><b><h3> """ + Source + """ </h3></b></td>
                    <td><b><h3> """ + Tot_Rec_Received + """ </h3></b></td>
                    <td><b><h3> """ + CCard_Rec + """ </h3></b></td>
                    <td><b><h3> """ + res[0][5] + """ </h3></b></td>
                    <td><b><h3> """ + Ack_Generated + """ </h3></b></td>
                </tr>
            </table>"""
    
    return TableEntry
 
######################################################################################################################################################

def CreateEmail(FileProcessStage, FileType, Connection_String, Inp_JobId, Source):
    Subject         = f"[CREDIT PROD]--[AMEX]--[Python]--[{FileType}]--[{Sub_File_Success_Error}]"
    temphtml        = ""
    
    if FileProcessStage != 1:
        # File is in ERROR
        temphtml = temphtml + """\
                    <b>Quick Reference: Job Info </b><br><br>
                    <table border=""1"">
                        <tr>
                            <td><b><h3> FileId </h3></b></td>
                            <td><b><h3> JobId </h3></b></td>
                            <td><b><h3> Error Reason </h3></b></td>
                            <td><b><h3> Current Status </h3></b></td>
                            <td><b><h3> Completed Status </h3></b></td>
                        </tr>
                        """ + Fill_FileStatus_Table(Connection_String, Inp_JobId, Source) + """
                    </table>"""
                    
    if FileProcessStage == 1:
        # done file table
        temphtml = temphtml + Fill_DoneFile_Table(Connection_String, Inp_JobId, Source)

    html = """\
        <html>
            <head></head>
            <body>
                <b> """ + str(Subject) +""" </b>
                <br><br>
                <b> Note: The Server Time is operating on : """+ str(datetime.datetime.now())[0:19] +""" </b>
                <br><br>
                <table border=""1"">
                    <tr>
                        <td><b><h3> Responsible Team </h3></b></td>
                        <td><b><h3> Alert Source </h3></b></td>
                        <td><b><h3> Host Name </h3></b></td>
                        <td><b><h3> Server Name </h3></b></td>
                        <td><b><h3> Process Name </h3></b></td>
                    </tr>
                    <tr>
                        <td><b><h4> APP TEAM </h4></b></td>
                        <td><b><h4> PYTHON </h4></b></td>
                        <td><b><h4> """+ str(socket.gethostname()) +""" </h4></b></td>
                        <td><b><h4> CREDIT PROD </h4></b></td>
                        <td><b><h4> """ + str(FileType) + """ </h4></b></td>
                    </tr>
                </table>
                <br>
                """ + temphtml + """
            </body>
        </html>"""
    return html
 
######################################################################################################################################################
    
def SendEmail(Use_Table, FileType, EmailTo, EmailFrom, SMTP_SERVER, SMTP_PORT, Connection_String, Inp_JobId, Source):
    try:
        if len(EmailTo) > 0 and len(EmailFrom) > 0:
                
            global Sub_File_Success_Error
            
            Sub_File_Success_Error = "ERROR" if Use_Table == 2 else 'SUCCESS'
            
            Final_HTML      = CreateEmail(Use_Table, FileType, Connection_String, Inp_JobId, Source)
            Subject         = f"[CREDIT PROD]--[AMEX]--[Python]--[{FileType}]--[{Sub_File_Success_Error}]"
            msg             = MIMEMultipart('alternative')
            msg['Subject']  = Subject
            msg['From']     = EmailFrom
            msg['To']       = ','.join(EmailTo)
            msg.attach(MIMEText(Final_HTML, 'html'))
            s = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
            s.sendmail(EmailFrom, EmailTo, msg.as_string())
            s.quit()
        else:
            pass
        
    except Exception as e:
        logger.debug(f"ERROR During Sending Mail : {e}",True)

######################################################################################################################################################