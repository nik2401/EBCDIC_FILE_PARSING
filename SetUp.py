class SetUp:
    def __init__(self):
        self.SqlOdbcDriver  = "ODBC Driver 17 for SQL Server"
        self.DB_Server_NAME = "BPLDEVDB01"
        self.DBName_CI      = "PRIMARY DATABASE"
        self.SMTP_SERVER    = "EMAIL SERVER NAME"
        self.SMTP_PORT      = 25
        self.EmailFrom      = ""
        self.Temp_EmailTo   = ""

    @classmethod
    def ClearingAndSettlement(cls):
        cls.AMEXFileIN = "F:/Project/AMEX_Python/Dump/AMEXClr/IN/"
        cls.AMEXFileOUT = "F:/Project/AMEX_Python/Dump/AMEXClr/OUT/"
        cls.AMEXFileError = "F:/Project/AMEX_Python/Dump/AMEXClr/ERROR/"
        cls.AMEXFileLog = "F:/Project/AMEX_Python/Dump/AMEXClr/LOG/"
        cls.TxnInsertToDB = 10000
        cls.AMEX_ValidationEnable = 1
        cls.AMEX_UseCCardOrCCard2 = 0
        #Time in Seconds To Recheck File Again if File not Found
        cls.FileRecheckTime = 5
        #Time in Seconds To Recheck the size of file again if file is found
        cls.FileSizeRecheckTime = 5
        return cls
