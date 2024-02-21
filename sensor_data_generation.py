def SensorDataGeneration(ServerAddress,UserName,Password,DatabaseName):
    import pymssql
    import pandas as pd
    import numpy as np
    import statistics
    from datetime import datetime
    import pytz
    from sqlalchemy import create_engine
    IST = pytz.timezone('Asia/Kolkata')
    Today=pd.to_datetime(datetime.strftime(datetime.now(IST), '%Y-%m-%d %H:%M:%S'))


    def fetch(ServerAddress,UserName,Password,DatabaseName):
        conn = pymssql.connect(server=ServerAddress, user=UserName, password=Password, database=DatabaseName)
        cursor = conn.cursor()
        df_attendance=pd.read_sql("select * from lbtAttendance",conn)
        df_sitemas = pd.read_sql("select a.s_Name,a.McuId,a.s_Addr_State,a.s_Addr_District,a.s_Addr_Street,a.s_Addr_Zone,b.BankName from lbtSiteMas as a,lbtBankMas as b WHERE a.BankCode=b.BankCode AND a.s_Status=1 AND a.qstatus='True' AND b.qstatus='True' ",conn)
        df_events_dictionary=pd.read_sql("select * from lbtEventsDictionary",conn)
        df_items=pd.read_sql("SELECT * FROM lbtsiteitemsMaster where qStatus='True' ",conn)
        cursor.close()
        conn.close()
        return df_attendance,df_sitemas,df_events_dictionary,df_items,Today

    def transformation(df_attendance,df_sitemas,df_events_dictionary,df_items,Today):
        df_attendance2=df_attendance[((df_attendance['GroupNo']=='01')) & ((df_attendance['EventCode']=='20') | (df_attendance['EventCode']=='40') | (df_attendance['EventCode']=='48') | (df_attendance['EventCode']=='58') | (df_attendance['EventCode']=='59'))]
        df_items1=df_items[df_items['GroupNo']=='01']
        sensors_list=list(df_items1['UnitNo'].unique())
        SensorName=['08','09','10','11','65','66','67','12','13','14','15','75','76','77','32','33','36','56','46','47','48','49','60','61','62']
        
        df_attendance3=df_attendance2[df_attendance2['UnitNo'].isin(SensorName)]
        df_items2=df_items1[df_items1['UnitNo'].isin(SensorName)]
        df_items3=pd.merge(df_sitemas,df_items2,on='McuId',how='left')

        df_attendance3=df_attendance3[~df_attendance3['McuId'].isnull()]
        df_attendance3=df_attendance3[['McuId','GroupNo','UnitNo','EventCode','AtTime']]
        df_all=pd.merge(df_attendance3,df_events_dictionary[['GroupNo','UnitNo','EventCode','EventName']],on=['GroupNo','UnitNo','EventCode'],how='right')
        df_all.drop_duplicates(['McuId','GroupNo','UnitNo','EventCode','AtTime','EventName'],keep='last',inplace=True)
        df_all=df_all[~df_all['AtTime'].isnull()]

        df_all_events=df_all.sort_values(['UnitNo','EventCode','AtTime'])
        df_all_events.drop_duplicates(['McuId','GroupNo','UnitNo','EventCode'],keep='last',inplace=True)
        df_all_events=pd.merge(df_sitemas[['BankName','s_Name','McuId']],df_all_events,on='McuId',how='left')

        df_items4=df_items3[['McuId','GroupNo','UnitNo','ItemName']]
        df_items4.drop_duplicates(['McuId','GroupNo','UnitNo'],keep='last',inplace=True)

        df_items5=df_items1[~df_items1['ItemName'].isnull()]
        df_items6=df_items5.drop_duplicates(['GroupNo','UnitNo'],keep='first')


        df_heartbeat=df_all_events[df_all_events['EventCode']=='20']
        df_trigger=df_all_events[df_all_events['EventCode']!='20']

        df_trigger=df_trigger.sort_values(['McuId','GroupNo','UnitNo','AtTime'])
        df_trigger=df_trigger.drop_duplicates(['McuId','GroupNo','UnitNo'],keep='last')
        df_trigger['LastTrigger']=(Today-df_trigger['AtTime']).astype('timedelta64[h]')
        
        df_heartbeat=df_heartbeat.sort_values(['McuId','GroupNo','UnitNo','AtTime'])
        df_heartbeat=df_heartbeat.drop_duplicates(['McuId','GroupNo','UnitNo'],keep='last')
        df_heartbeat['LastTrigger']=(Today-df_heartbeat['AtTime']).astype('timedelta64[h]')

        df_trigger['bucket']=pd.cut(df_trigger['LastTrigger'],bins=[-100,359,719,2159,4319,8759,999999],labels=["<15days","15-30days","1-3months","3-6months","6-12months",">1year"])
        df_heartbeat['bucket']=pd.cut(df_heartbeat['LastTrigger'],bins=[-100,359,719,2159,4319,8759,999999],labels=["<15days","15-30days","1-3months","3-6months","6-12months",">1year"])

        df_trigger2=df_trigger[['McuId','GroupNo','UnitNo','EventName','bucket']]
        df_trigger3=pd.merge(df_items4[['McuId','GroupNo','UnitNo']],df_trigger2,on=['McuId','GroupNo','UnitNo'],how='left')
        df_trigger4=pd.merge(df_sitemas,df_trigger3,on='McuId')
        df_trigger4=df_trigger4[~df_trigger4['BankName'].isnull()]
        df_trigger4=df_trigger4.drop_duplicates(['McuId','GroupNo','UnitNo'],keep='last')
        df_trigger4.rename(columns={'bucket':'LastTriggerLabel'},inplace=True)
        df_trigger4['LastTriggerLabel']=df_trigger4['LastTriggerLabel'].astype('object')
        df_trigger4['LastTriggerLabel'].fillna('Sensor Installed but No Trigger',inplace=True)
        df_trigger5=pd.merge(df_trigger4,df_items6[['GroupNo','UnitNo','ItemName']],on=['GroupNo','UnitNo'])
        df_trigger_pivot=pd.DataFrame(pd.pivot_table(data=df_trigger5,index=['BankName','s_Name','McuId'],columns=['ItemName'],values=['LastTriggerLabel'],aggfunc=statistics.mode).to_records())

        df_heartbeat2=df_heartbeat[['McuId','GroupNo','UnitNo','EventName','bucket']]
        df_heartbeat3=pd.merge(df_items4[['McuId','GroupNo','UnitNo']],df_heartbeat2,on=['McuId','GroupNo','UnitNo'],how='left')
        df_heartbeat4=pd.merge(df_sitemas,df_heartbeat3,on='McuId')
        df_heartbeat4=df_heartbeat4[~df_heartbeat4['BankName'].isnull()]
        df_heartbeat4=df_heartbeat4.drop_duplicates(['McuId','GroupNo','UnitNo'],keep='last')
        df_heartbeat4.rename(columns={'bucket':'LastTriggerLabel'},inplace=True)
        df_heartbeat4['LastTriggerLabel']=df_heartbeat4['LastTriggerLabel'].astype('object')
        df_heartbeat4['LastTriggerLabel'].fillna('Sensor Installed but No Trigger',inplace=True)
        df_heartbeat5=pd.merge(df_heartbeat4,df_items6[['GroupNo','UnitNo','ItemName']],on=['GroupNo','UnitNo'])
        df_heartbeat_pivot=pd.DataFrame(pd.pivot_table(data=df_heartbeat5,index=['BankName','s_Name','McuId'],columns=['ItemName'],values=['LastTriggerLabel'],aggfunc=statistics.mode).to_records())

        l={}
        for col in df_trigger_pivot.columns[3:]:
            l[col]=col[22:-2]
        df_trigger_pivot=df_trigger_pivot.rename(columns=l)

        l1={}
        for col in df_heartbeat_pivot.columns[3:]:
            l1[col]=col[22:-2]
        df_heartbeat_pivot=df_heartbeat_pivot.rename(columns=l1)

        df_trigger_pivot=pd.merge(df_sitemas[['BankName','s_Name','McuId','s_Addr_Street','s_Addr_District','s_Addr_State','s_Addr_Zone']],df_trigger_pivot,on=['BankName','s_Name','McuId'],how='left')
        df_trigger_pivot.rename(columns={'s_Name':'SiteName','s_Addr_Street':'Address','s_Addr_District':'District','s_Addr_State':'State','s_Addr_Zone':'Zone'},inplace=True)
        df_trigger_pivot=df_trigger_pivot.fillna("Sensor Not Installed")
        df_output=df_trigger_pivot
        
        df_heartbeat_pivot=pd.merge(df_sitemas[['BankName','s_Name','McuId','s_Addr_Street','s_Addr_District','s_Addr_State','s_Addr_Zone']],df_heartbeat_pivot,on=['BankName','s_Name','McuId'],how='left')
        df_heartbeat_pivot.rename(columns={'s_Name':'SiteName','s_Addr_Street':'Address','s_Addr_District':'District','s_Addr_State':'State','s_Addr_Zone':'Zone'},inplace=True)
        df_heartbeat_pivot=df_heartbeat_pivot.fillna("Sensor Not Installed")
        df_output1=df_heartbeat_pivot

        df_output.rename(columns={'Lobby Motion Detector':'Lobby Movement Sensor','Back Room Door Contact Sensor':'Backroom Door Sensor','Battery Bank Location Sensor':'ATM1 Battery Bank Location Sensor','ATM1 Hood Contact Sensor':'ATM1 Hood Sensor','ATM4 Chest Vibration Sensor':'ATM4 Vibration Sensor'},inplace=True)
        df_output1.rename(columns={'Lobby Motion Detector':'Lobby Movement Sensor','Back Room Door Contact Sensor':'Backroom Door Sensor','Battery Bank Location Sensor':'ATM1 Battery Bank Location Sensor','ATM1 Hood Contact Sensor':'ATM1 Hood Sensor','ATM4 Chest Vibration Sensor':'ATM4 Vibration Sensor'},inplace=True)
          
        df_output['Date']=datetime.strftime(Today, '%Y-%m-%d')
        first_column = df_output.pop('Date')
        df_output.insert(0, 'Date', first_column)
        df_output['Date']=pd.to_datetime(df_output['Date'])
        df_output=df_output.replace('Sensor Not Installed',np.nan)

        df_output1['Date']=datetime.strftime(Today, '%Y-%m-%d')
        first_column1 = df_output1.pop('Date')
        df_output1.insert(0, 'Date', first_column)
        df_output1['Date']=pd.to_datetime(df_output1['Date'])
        df_output1=df_output1.replace('Sensor Not Installed',np.nan)
        return df_output,df_output1

    def pushdata(df_output,df_output1,df_sitemas):
        if len(df_output)>0:
            sitemas=df_sitemas[['BankName','s_Name','McuId']]
            sitemas.rename(columns={'s_Name':'SiteName'},inplace=True)
            bank=list(sitemas['BankName'].unique())
            conn2=pymssql.connect(host='188.42.97.40',user='aaum_db_usr',password='nfm!DBj!Pu#hZmgThxAoZe$E',database='AAUM_Analytics_DB')
            cur2=conn2.cursor()
            clear_data1="DELETE FROM SensorPerformance_Trigger WHERE BankName  = %s"
            clear_data2="DELETE FROM SensorPerformance_Heartbeat WHERE BankName  = %s"
            cur2.executemany(clear_data1, bank)
            conn2.commit()
            cur2.executemany(clear_data2, bank)
            conn2.commit()
            conn2.close()
            engine = create_engine('mssql+pymssql://aaum_db_usr:nfm!DBj!Pu#hZmgThxAoZe$E@188.42.97.40/AAUM_Analytics_DB')
            df_output.to_sql('SensorPerformance_Trigger',engine,if_exists='append',index=False, method="multi", chunksize=500)
            df_output1.to_sql('SensorPerformance_Heartbeat',engine,if_exists='append',index=False, method="multi", chunksize=500)
            engine.dispose()

    df_attendance,df_sitemas,df_events_dictionary,df_items,Today=fetch(ServerAddress,UserName,Password,DatabaseName)
    df_output,df_output1=transformation(df_attendance,df_sitemas,df_events_dictionary,df_items,Today)
    pushdata(df_output,df_output1,df_sitemas)
