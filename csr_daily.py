ServerAddress='103.24.200.157'
UserName='aaum_support'
Password='aaum@123'
DatabaseName='InnoculateCMS'

def critical_sensors_daily(ServerAddress,UserName,Password,DatabaseName):

    import pandas as pd
    import pymssql
    import numpy as np
    import pytz
    from datetime import datetime, timedelta
    from dateutil.relativedelta import relativedelta
    IST = pytz.timezone('Asia/Kolkata')
    from pytz import timezone
    from sqlalchemy import create_engine
    today=datetime.now(IST).date().strftime("%Y-%m-%d")
    yesterday=datetime.strftime(datetime.now(IST) - timedelta(1), '%Y-%m-%d')

    def fetch(ServerAddress,UserName,Password,DatabaseName,yesterday):

        conn = pymssql.connect(server=ServerAddress, user=UserName, password=Password, database=DatabaseName)
        cursor = conn.cursor()
        events = pd.read_sql("SELECT  McuId,GroupNo,UnitNo,EventCode,Msg,AtTime,OpenedOn,ClosedOn FROM lbtEvents WHERE OpenedOn BETWEEN '{}' AND '{}' AND LOWER(ClosedBy)!='system' AND ClosedOn IS NOT NULL AND (Msg!='NC-30' OR Msg!='NC-30 (Mains Power Failure)' OR Msg!='Movement in Lobby for more than 3 Mins') AND (GroupNo='01' AND UnitNo!='33') ".format(yesterday, today),conn)
        df_sitemas = pd.read_sql("select a.s_Name,a.McuId,a.s_Addr_State,a.s_Addr_District,a.s_Addr_Street,a.s_Addr_Zone,b.BankName from lbtSiteMas as a, lbtBankMas as b WHERE a.BankCode=b.BankCode AND a.s_Status=1 AND a.qstatus='True' AND b.qstatus='True' ",conn)
        df_items=pd.read_sql("SELECT * FROM lbtsiteitemsMaster where qStatus='True' ",conn)
        cursor.close()
        conn.close()
        return events,df_sitemas,df_items,yesterday

    def transformation(events,df_sitemas,df_items,yesterday):
        df_items1=df_items[~df_items['ItemName'].isnull()]
        df_items1=df_items1[(df_items1['qStatus']==True) & (df_items1['i_Status']==True)]
        df_items2=df_items1[['McuId']]
        df_items3=pd.merge(df_sitemas['McuId'],df_items2,on='McuId')
        df_items3.drop_duplicates('McuId',keep='first',inplace=True)

        if len(events)==0:
            df_csr_count3=df_items3[["McuId"]]
            timeslots=['12AM~4AM','4AM~8AM','8AM~12PM','12PM~4PM','4PM~8PM','8PM~12AM']
            for i in timeslots:
                if i not in df_csr_count3.columns:
                    df_csr_count3[i]=0
            df_csr_count3=pd.merge(df_sitemas[['BankName','s_Name','McuId']],df_csr_count3,on='McuId')
            df_csr_count3=df_csr_count3[['BankName','s_Name','McuId','12AM~4AM','4AM~8AM','8AM~12PM','12PM~4PM','4PM~8PM','8PM~12AM']]
            df_csr_count3.rename(columns={'s_Name':'SiteName'},inplace=True)
            df_csr_count3['Date']=pd.to_datetime(yesterday)
            df_csr_count3=df_csr_count3[['Date','BankName','SiteName','McuId','12AM~4AM','4AM~8AM','8AM~12PM','12PM~4PM','4PM~8PM','8PM~12AM']]

        else:    
            df_csr_events=events[['McuId','GroupNo','UnitNo','EventCode','Msg','AtTime','OpenedOn','ClosedOn']]
            df_csr_events.drop_duplicates(['McuId','GroupNo','UnitNo','Msg','AtTime'],keep='first',inplace=True)
            df_csr_events2=pd.merge(df_sitemas[['BankName','s_Name','McuId']],df_csr_events,on='McuId',how='left')

            df_csr_events2['hour']=df_csr_events2['OpenedOn'].dt.hour
            df_csr_events2['Date']=df_csr_events2['OpenedOn'].dt.date
            df_csr_events2['HourLabel']=pd.cut(df_csr_events2['hour'],[-1,3,7,11,15,19,23],labels=["12AM~4AM","4AM~8AM","8AM~12PM","12PM~4PM","4PM~8PM","8PM~12AM"])

            df_csr_count=pd.crosstab([df_csr_events2['Date'],df_csr_events2['McuId']],df_csr_events2['HourLabel'])
            df_csr_count = pd.DataFrame((df_csr_count).to_records())
            timeslots=['12AM~4AM','4AM~8AM','8AM~12PM','12PM~4PM','4PM~8PM','8PM~12AM']
            for i in timeslots:
                if i not in df_csr_count.columns:
                    df_csr_count[i]=0

            df_csr_count2=pd.merge(df_sitemas['McuId'],df_csr_count,on='McuId')
            df_csr_count2['Date']=pd.to_datetime(df_csr_count2['Date'])
            df_csr_count3=pd.merge(df_items3,df_csr_count2,on=['McuId'],how='left')
            df_csr_count3=pd.merge(df_sitemas[['BankName','s_Name','McuId']],df_csr_count3,on='McuId')
            df_csr_count3['Date'].fillna(pd.to_datetime(yesterday),inplace=True)
            df_csr_count3.fillna(0,inplace=True)
            df_csr_count3[['12AM~4AM','4AM~8AM','8AM~12PM','12PM~4PM','4PM~8PM','8PM~12AM']] = df_csr_count3[['12AM~4AM','4AM~8AM','8AM~12PM','12PM~4PM','4PM~8PM','8PM~12AM']].astype('int')
            df_csr_count3=df_csr_count3[['Date','BankName','s_Name','McuId','12AM~4AM','4AM~8AM','8AM~12PM','12PM~4PM','4PM~8PM','8PM~12AM']]
            df_csr_count3.rename(columns={'s_Name':'SiteName'},inplace=True)
        return df_csr_count3

    def pushdata(df_csr_count3):

        engine = create_engine('mssql+pymssql://aaum_db_usr:nfm!DBj!Pu#hZmgThxAoZe$E@188.42.97.40/AAUM_Analytics_DB')
        df_csr_count3.to_sql('csr_daily',engine,if_exists='append',index=False, method="multi", chunksize=500)
        engine.dispose()

    events,df_sitemas,df_items,yesterday =  fetch(ServerAddress,UserName,Password,DatabaseName,yesterday)
    df_csr_count3 = transformation(events,df_sitemas,df_items,yesterday)
    pushdata(df_csr_count3)
    print("Successfull")
    #return df_csr_count3
df_csr_count3 =  critical_sensors_daily(ServerAddress,UserName,Password,DatabaseName)
print("Successfull")
