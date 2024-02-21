ServerAddress='103.24.200.157'
UserName='aaum_support'
Password='aaum@123'
DatabaseName='InnoculateNCRIDBI'

def pir_daily(ServerAddress,UserName,Password,DatabaseName):

    import pandas as pd
    import pymssql
    import numpy as np
    import pytz
    from datetime import datetime, timedelta
    from dateutil.relativedelta import relativedelta
    IST = pytz.timezone('Asia/Kolkata')
    from pytz import timezone
    from sqlalchemy import create_engine
    import warnings
    warnings.filterwarnings('ignore')
    today=datetime.now(IST).date().strftime("%Y-%m-%d")
    yesterday=datetime.strftime(datetime.now(IST) - timedelta(1), '%Y-%m-%d')


    def fetch(ServerAddress,UserName,Password,DatabaseName,yesterday):

        conn = pymssql.connect(server=ServerAddress, user=UserName, password=Password, database=DatabaseName)  
        cursor = conn.cursor()
        events = pd.read_sql("SELECT McuId,GroupNo,UnitNo,EventCode,Msg,AtTime,OpenedOn,ClosedOn FROM lbtEvents WHERE OpenedOn BETWEEN '{}' AND '{}' AND GroupNo='01' AND UnitNo='36'".format(yesterday,today),conn)
        df_sitemas = pd.read_sql("select a.s_Name,a.McuId,a.s_Addr_State,a.s_Addr_District,a.s_Addr_Street,a.s_Addr_Zone,b.BankName from lbtSiteMas as a, lbtBankMas as b WHERE a.BankCode=b.BankCode AND a.s_Status=1 AND a.qstatus='True' AND b.qstatus='True' ",conn)
        df_items=pd.read_sql("SELECT * FROM lbtsiteitemsMaster where qStatus='True' ",conn)
        cursor.close()
        conn.close()
        return events,df_sitemas,df_items,yesterday


    def transformation(events,df_sitemas,df_items,yesterday):

        df_items1=df_items[(df_items['qStatus']==True) & (df_items['i_Status']==True)]
        df_items_pir=df_items1[(df_items1['GroupNo']=='01') & (df_items1['UnitNo']=='36')]
        df_items_pir=df_items_pir[~df_items_pir['ItemName'].isnull()]
        df_items_pir1=df_items_pir[['McuId','GroupNo','UnitNo']]
        df_items_pir2=pd.merge(df_sitemas['McuId'],df_items_pir1,on='McuId')

        if len(events)==0:
            df_pir_count3=df_items_pir2[['McuId']]
            timeslots=['12AM~4AM','4AM~8AM','8AM~12PM','12PM~4PM','4PM~8PM','8PM~12AM']
            for i in timeslots:
                if i not in df_pir_count3.columns:
                    df_pir_count3[i]=0
            df_pir_count3['Date']=yesterday
            df_pir_count3=pd.merge(df_sitemas[['BankName','s_Name','McuId']],df_pir_count3,on='McuId')
            df_pir_count3=df_pir_count3[['Date','BankName','s_Name','McuId','12AM~4AM','4AM~8AM','8AM~12PM','12PM~4PM','4PM~8PM','8PM~12AM']]
            df_pir_count3.rename(columns={'s_Name':'SiteName'},inplace=True)
            df_pir_count3['Date']=pd.to_datetime(df_pir_count3['Date'])

        else:
            events = events.drop_duplicates(['McuId','GroupNo','UnitNo','Msg','AtTime'],keep='first')
            pir_events=events[events['Msg']!='Lobby Movement Sensor Tampered. Code: (0136)']

            pir_events=events[['McuId','GroupNo','UnitNo','EventCode','Msg','AtTime','OpenedOn','ClosedOn']]

            pir_events.drop_duplicates(['McuId','GroupNo','UnitNo','Msg','AtTime'],keep='first',inplace=True)

            pir_events['hour']=pir_events['OpenedOn'].dt.hour
            pir_events['Date']=pir_events['OpenedOn'].dt.date
            pir_events['HourLabel']=pd.cut(pir_events['hour'],[-1,3,7,11,15,19,23],labels=["12AM~4AM","4AM~8AM","8AM~12PM","12PM~4PM","4PM~8PM","8PM~12AM"])

            df_pir_count=pd.crosstab([pir_events['Date'],pir_events['McuId']],pir_events['HourLabel'])
            df_pir_count = pd.DataFrame((df_pir_count).to_records())
            timeslots=['12AM~4AM','4AM~8AM','8AM~12PM','12PM~4PM','4PM~8PM','8PM~12AM']
            for i in timeslots:
                if i not in df_pir_count.columns:
                    df_pir_count[i]=0
            df_pir_count2=pd.merge(df_sitemas['McuId'],df_pir_count,on='McuId')
            df_pir_count2['Date']=pd.to_datetime(df_pir_count2['Date'])
            df_pir_count3=pd.merge(df_items_pir2,df_pir_count2,on=['McuId'],how='left')
            df_pir_count3=pd.merge(df_sitemas[['BankName','s_Name','McuId']],df_pir_count3,on='McuId')
            Yesterday=pd.to_datetime(yesterday)
            df_pir_count3['Date'].fillna(Yesterday,inplace=True)
            df_pir_count3.fillna(0,inplace=True)
            df_pir_count3[['12AM~4AM','4AM~8AM','8AM~12PM','12PM~4PM','4PM~8PM','8PM~12AM']] = df_pir_count3[['12AM~4AM','4AM~8AM','8AM~12PM','12PM~4PM','4PM~8PM','8PM~12AM']].astype('int')
            df_pir_count3=df_pir_count3[['Date','BankName','s_Name','McuId','12AM~4AM','4AM~8AM','8AM~12PM','12PM~4PM','4PM~8PM','8PM~12AM']]
            df_pir_count3.rename(columns={'s_Name':'SiteName'},inplace=True)
        return df_pir_count3

    def pushdata(df_pir_count3):
        if len(df_pir_count3)==0:
            print('No record to push')
        else:
            engine = create_engine('mssql+pymssql://aaum_db_usr:nfm!DBj!Pu#hZmgThxAoZe$E@188.42.97.40/AAUM_Analytics_DB')
            df_pir_count3.to_sql('pir_daily',engine,if_exists='append',index=False, method="multi", chunksize=500)
            engine.dispose()

    events,df_sitemas,df_items,yesterday = fetch(ServerAddress,UserName,Password,DatabaseName,yesterday)
    df_pir_count3=transformation(events,df_sitemas,df_items,yesterday)
    #pushdata(df_pir_count3)
    return df_pir_count3
df_pir_count3 = pir_daily(ServerAddress,UserName,Password,DatabaseName)
print("Executed successfully")