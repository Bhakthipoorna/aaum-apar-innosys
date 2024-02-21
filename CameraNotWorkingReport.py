def CameraNotWorkingReport(df_sitemas):

    import pymssql
    import pandas as pd
    from datetime import datetime,timedelta
    import pytz
    IST = pytz.timezone('Asia/Kolkata')
    from sqlalchemy import create_engine
    
    Today=pd.to_datetime(datetime.strftime(datetime.now(IST), '%Y-%m-%d'))
    yesterday=datetime.strftime(datetime.now(IST) - timedelta(1), '%Y-%m-%d')

    def transformation(df_sitemas):
        
        conn2=pymssql.connect(host='188.42.97.40',user='aaum_db_usr',password='nfm!DBj!Pu#hZmgThxAoZe$E',database='AAUM_Analytics_DB')
        cur2=conn2.cursor()
        
        BankName=tuple(df_sitemas['BankName'].unique())
        if len(BankName)>1:
            df_cam_issue=pd.read_sql("select TicketId,BankName,SiteName,OpenedOn,ClosedOn FROM CameraNotWorking WHERE BankName IN {} AND ((OpenedOn BETWEEN '{}' AND '{}') OR (ClosedOn>='{}' AND OpenedOn<'{}') OR (ClosedOn IS NULL AND OpenedOn<'{}'))".format(BankName,yesterday,Today,yesterday,Today,Today),conn2)
        else:
            BankName=BankName[0]
            df_cam_issue=pd.read_sql("select TicketId,BankName,SiteName,OpenedOn,ClosedOn FROM CameraNotWorking WHERE BankName='{}' AND ((OpenedOn BETWEEN '{}' AND '{}') OR (ClosedOn>='{}' AND OpenedOn<'{}') OR (ClosedOn IS NULL AND OpenedOn<'{}'))".format(BankName,yesterday,Today,yesterday,Today,Today),conn2)

        cur2.close()
        conn2.close()
        
        return df_cam_issue,BankName
        
            
    df_cam_issue,BankName = transformation(df_sitemas)

    filename = "CameraNotWorking {} - {}.xlsx".format(BankName,yesterday)
    df_cam_issue.to_excel(filename)
   