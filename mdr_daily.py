ServerAddress='103.24.200.157'
UserName='aaum_support'
Password='aaum@123'
DatabaseName='InnoculateCMS'

def maindoor_daily(ServerAddress,UserName,Password,DatabaseName):

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
        maindoor_events=pd.read_sql("SELECT McuId,GroupNo,UnitNo,EventCode,EventDate,AtTime from lbtEventsALL WHERE AtTime BETWEEN '{}' AND '{}' AND GroupNo='01' AND UnitNo='33' AND EventCode='40' ".format(yesterday,today),conn)
        df_sitemas = pd.read_sql("select a.s_Name,a.McuId,a.s_Addr_State,a.s_Addr_District,a.s_Addr_Street,a.s_Addr_Zone,b.BankName from lbtSiteMas as a, lbtBankMas as b WHERE a.BankCode=b.BankCode AND a.s_Status=1 AND a.qstatus='True' AND b.qstatus='True' ",conn)
        df_items=pd.read_sql("SELECT * FROM lbtsiteitemsMaster where qStatus='True' ",conn)
        cursor.close()
        conn.close()
        return maindoor_events,df_sitemas,df_items,yesterday
    
    def transformation(maindoor_events,df_sitemas,df_items,yesterday):
        
        df_items1=df_items[~df_items['ItemName'].isnull()]
        df_items1=df_items[(df_items['GroupNo']=='01') & (df_items['UnitNo']=='33')]
        df_items2=df_items1[(df_items1['qStatus']==True) & (df_items1['i_Status']==True)]
        df_items3=pd.merge(df_sitemas['McuId'],df_items2,on='McuId')
        
        
        if len(maindoor_events)==0:
            df_count3=df_items3[['McuId']]
            timeslots=['12AM~4AM','4AM~8AM','8AM~12PM','12PM~4PM','4PM~8PM','8PM~12AM']
            for i in timeslots:
                if i not in df_count3.columns:
                    df_count3[i]=0
            df_count3['Date']=yesterday
            df_count3=pd.merge(df_sitemas[['BankName','s_Name','McuId']],df_count3,on='McuId')
            df_count3=df_count3[['Date','BankName','s_Name','McuId','12AM~4AM','4AM~8AM','8AM~12PM','12PM~4PM','4PM~8PM','8PM~12AM']]
            df_count3.rename(columns={'s_Name':'SiteName'},inplace=True)
            df_count3['Date']=pd.to_datetime(df_count3['Date'])
        
        else:
            maindoor_events.drop_duplicates(['McuId','GroupNo','UnitNo','AtTime'],keep='first', inplace=True)
            df_maindoor_events=maindoor_events[['McuId','EventCode','EventDate','AtTime']]
            df_maindoor_events1=pd.merge(df_sitemas[['BankName','s_Name','McuId']],df_maindoor_events,on='McuId',how='left')
            df_maindoor_events1=df_maindoor_events1[~df_maindoor_events1['AtTime'].isnull()]

            df_maindoor_events1['hour']=df_maindoor_events1['AtTime'].dt.hour
            df_maindoor_events1['Date']=df_maindoor_events1['AtTime'].dt.date
            df_maindoor_events1['HourLabel']=pd.cut(df_maindoor_events1['hour'],[-1,3,7,11,15,19,23],labels=["12AM~4AM","4AM~8AM","8AM~12PM","12PM~4PM","4PM~8PM","8PM~12AM"])

            
            df_count=pd.crosstab([df_maindoor_events1['Date'],df_maindoor_events1['McuId']],df_maindoor_events1['HourLabel'])
            df_count = pd.DataFrame((df_count).to_records())

            timeslots=['12AM~4AM','4AM~8AM','8AM~12PM','12PM~4PM','4PM~8PM','8PM~12AM']
            for i in timeslots:
                if i not in df_count.columns:
                    df_count[i]=0
            df_count=pd.merge(df_sitemas['McuId'],df_count,on='McuId')

            df_count['Date']=pd.to_datetime(df_count['Date'])
            df_count1=pd.merge(df_items3[['McuId']],df_count,on=['McuId'],how='left')
            df_count2=df_count1.sort_values(['McuId','Date'])
            df_count2['Date'].fillna(pd.to_datetime(yesterday),inplace=True)
            df_count2.fillna(0,inplace=True)
            df_count['Date']=pd.to_datetime(df_count['Date'])
            df_count3=pd.merge(df_sitemas[['BankName','s_Name','McuId']],df_count2,on='McuId')
            df_count3=df_count3[['Date','BankName','s_Name','McuId','12AM~4AM','4AM~8AM','8AM~12PM','12PM~4PM','4PM~8PM','8PM~12AM']]
            df_count3[['12AM~4AM','4AM~8AM','8AM~12PM','12PM~4PM','4PM~8PM','8PM~12AM']]=df_count3[['12AM~4AM','4AM~8AM','8AM~12PM','12PM~4PM','4PM~8PM','8PM~12AM']].astype('int')
            df_count3.rename(columns={'s_Name':'SiteName'},inplace=True)
            
        return df_count3,df_sitemas
    
    def pushdata(df_count3,df_sitemas):
        sitemas=df_sitemas[['BankName','s_Name','McuId']]
        sitemas.rename(columns={'s_Name':'SiteName'},inplace=True)
        bank=list(sitemas['BankName'].unique())
        conn2=pymssql.connect(host='188.42.97.40',user='aaum_db_usr',password='nfm!DBj!Pu#hZmgThxAoZe$E',database='AAUM_Analytics_DB')
        cur2=conn2.cursor()
        clear_data1="DELETE FROM sitemas WHERE BankName  = %s"
        cur2.executemany(clear_data1, bank)
        conn2.commit()
        conn2.close()
        
        engine = create_engine('mssql+pymssql://aaum_db_usr:nfm!DBj!Pu#hZmgThxAoZe$E@188.42.97.40/AAUM_Analytics_DB')        
        sitemas.to_sql('sitemas',engine,if_exists='append',index=False, method="multi", chunksize=500)  
               
        if len(df_count3)==0:
            print("No data to push")
        else:   
            #df_count3.to_sql('mdr_daily',engine,if_exists='append',index=False, method="multi", chunksize=500)  
            engine.dispose()

    maindoor_events,df_sitemas,df_items,yesterday = fetch(ServerAddress,UserName,Password,DatabaseName,yesterday)
    df_count3,df_sitemas = transformation(maindoor_events,df_sitemas,df_items,yesterday)
    pushdata(df_count3,df_sitemas)
    print('successfull')
    return df_count3
df_count3= maindoor_daily(ServerAddress,UserName,Password,DatabaseName)
print("Excecuted successfully")
