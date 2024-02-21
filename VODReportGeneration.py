def VODReportGeneration(df_sitemas):
    import pymssql
    import pandas as pd
    import xlsxwriter
    from datetime import datetime,timedelta
    import pytz
    IST = pytz.timezone('Asia/Kolkata')
    from sqlalchemy import create_engine  
    Today=pd.to_datetime(datetime.strftime(datetime.now(IST), '%Y-%m-%d %H:%M:%S'))
    yesterday=datetime.strftime(datetime.now(IST) - timedelta(1), '%Y-%m-%d')
    prior30days=datetime.strftime(datetime.now(IST) - timedelta(30), '%Y-%m-%d')
 
    def transformation(df_sitemas):        
        conn2=pymssql.connect(host='188.42.97.40',user='aaum_db_usr',password='nfm!DBj!Pu#hZmgThxAoZe$E',database='AAUM_Analytics_DB')
        cur2=conn2.cursor()
        BankName=tuple(df_sitemas['BankName'].unique())
        if len(BankName)>1:
            df_status=pd.read_sql("SELECT * from dummy_VOD_report WHERE (Date between '{}' and '{}') AND BankName IN {}".format(prior30days,Today,BankName),conn2)
            df_summary=pd.read_sql("SELECT * from dummy_VOD_report_summary WHERE (Date between '{}' and '{}') AND BankName IN {}".format(prior30days,Today,BankName),conn2)
            df_count=pd.read_sql("SELECT * from dummy_VOD_report_count WHERE Date>='{}' AND BankName IN {}".format(Today,BankName),conn2)                
        else:
            BankName=BankName[0]
            df_status=pd.read_sql("SELECT * from dummy_VOD_report WHERE (Date between '{}' and '{}') AND BankName='{}' ".format(prior30days,Today,BankName),conn2)
            df_summary=pd.read_sql("SELECT * from dummy_VOD_report_summary WHERE (Date between '{}' and '{}') AND BankName='{}' ".format(prior30days,Today,BankName),conn2)
            df_count=pd.read_sql("SELECT * from dummy_VOD_report_count WHERE Date>='{}' AND BankName='{}' ".format(Today,BankName),conn2)        
        cur2.close()
        conn2.close()
        df_status['Date']=pd.to_datetime(df_status['Date'])
        df_status['Date']=df_status['Date'].dt.date
        df_lobby=pd.pivot_table(data=df_status,index='McuId',columns='Date',values='Lobby Camera',aggfunc=lambda x: ' '.join(str(v) for v in x))
        df_lobby=df_lobby.reset_index()
        df_outdoor=pd.pivot_table(data=df_status,index='McuId',columns='Date',values='Outdoor Camera',aggfunc=lambda x: ' '.join(str(v) for v in x))
        df_outdoor=df_outdoor.reset_index()
        df_backroom=pd.pivot_table(data=df_status,index='McuId',columns='Date',values='Backroom Camera',aggfunc=lambda x: ' '.join(str(v) for v in x))
        df_backroom=df_backroom.reset_index()
        df_pinhole=pd.pivot_table(data=df_status,index='McuId',columns='Date',values='Pinhole Camera',aggfunc=lambda x: ' '.join(str(v) for v in x))
        df_pinhole=df_pinhole.reset_index()
        filename = "{} VOD report {}.xlsx".format(BankName,yesterday)
        VOD_Automated_Report = pd.ExcelWriter(filename, engine='xlsxwriter')
        df_summary.to_excel(VOD_Automated_Report, sheet_name='Summary')
        df_lobby.to_excel(VOD_Automated_Report, sheet_name='Lobby')
        df_outdoor.to_excel(VOD_Automated_Report, sheet_name='Outddor')
        df_backroom.to_excel(VOD_Automated_Report, sheet_name='Backroom')
        df_pinhole.to_excel(VOD_Automated_Report, sheet_name='Pinhole')
        df_camera_count.to_excel(VOD_Automated_Report, sheet_name='Lobby%')
        #VOD_Automated_Report.save()
        return VOD_Automated_Report
    VOD_Automated_Report = transformation(df_sitemas)
   
    return VOD_Automated_Report
