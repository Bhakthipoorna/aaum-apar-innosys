def SensorPriority(ServerAddress,UserName,Password,DatabaseName):
    import pymssql
    import pandas as pd
    from datetime import datetime, timedelta
    import pytz
    from sqlalchemy import create_engine
    IST = pytz.timezone('Asia/Kolkata')
    Today=pd.to_datetime(datetime.strftime(datetime.now(IST), '%Y-%m-%d %H:%M:%S'))
    prior15days=datetime.strftime(datetime.now(IST) - timedelta(15), '%Y-%m-%d')

    def fetch(ServerAddress,UserName,Password,DatabaseName):

        conn2=pymssql.connect(host=ServerAddress,user=UserName,password=Password,database=DatabaseName)
        cur=conn2.cursor()

        video_rec=pd.read_sql("SELECT Date,BankName,SiteName,McuId,SiteAvailability from videorecording_daily where Date>= '{}'  ".format(prior15days),conn2)
        df_output1=pd.read_sql('Select * from SensorPerformance_Trigger',conn2)
        df_output2=pd.read_sql('Select * from SensorPerformance_Heartbeat',conn2)
        return video_rec,df_output1,df_output2,conn2,cur

    def transform(video_rec,df_output1,df_output2,conn2,cur):
        
        df_site_online=video_rec.groupby(['BankName','SiteName','McuId'])['SiteAvailability'].mean().reset_index()

        df_output3=pd.merge(df_output1,df_site_online,on=['BankName','SiteName','McuId'])
        df_output4=df_output3[df_output3['SiteAvailability']!=0]
        df_output5=df_output4.drop('SiteAvailability',axis=1)
        
        df_output23=pd.merge(df_output2,df_site_online,on=['BankName','SiteName','McuId'])
        df_output24=df_output23[df_output23['SiteAvailability']!=0]
        df_output25=df_output24.drop('SiteAvailability',axis=1)

        df_output6=df_output5.copy()
        df_output5.drop('Priority',axis=1,inplace=True) 
        df_output6.drop('Priority',axis=1,inplace=True)

        df_sensor_problem=df_output5.copy()
        df_sensor_problem.replace({"<15days":None,"15-30days":2,"1-3months":3,"3-6months":4,"6-12months":5,">1year":5,"Sensor Installed but No Trigger":5},inplace=True)
        df_sensor_problem['problemed_sensors']=(25-df_sensor_problem.iloc[:,8:].isnull().sum(axis=1))

        df_output6.replace({"<15days":0,"15-30days":2,"1-3months":3,"3-6months":4,"6-12months":5,">1year":5,"Sensor Installed but No Trigger":5},inplace=True)

        df_output26=df_output25.copy()
        df_output25.drop('Priority',axis=1,inplace=True) 
        df_output26.drop('Priority',axis=1,inplace=True)

        df_sensor_problem2=df_output25.copy()
        df_sensor_problem2.replace({"<15days":None,"15-30days":2,"1-3months":3,"3-6months":4,"6-12months":5,">1year":5,"Sensor Installed but No Trigger":5},inplace=True)
        df_sensor_problem2['problemed_sensors']=(25-df_sensor_problem2.iloc[:,8:].isnull().sum(axis=1))
        df_output26.replace({"<15days":0,"15-30days":2,"1-3months":3,"3-6months":4,"6-12months":5,">1year":5,"Sensor Installed but No Trigger":5},inplace=True)

        df_output6['Risk_score']=df_output6.iloc[:,8:].sum(axis=1)
        df_output6=pd.merge(df_output6,df_sensor_problem[['Date','BankName','SiteName','McuId','problemed_sensors']],on=['Date','BankName','SiteName','McuId'],how='left')
        df_output6['Risk_score']=df_output6['Risk_score']*df_output6['problemed_sensors']
        df_output6=df_output6[df_output6['Risk_score']!=0]
        df_output6['Ranking']=df_output6['Risk_score'].rank(pct=True).round(2)
        df_output6['Ranking']=df_output6['Ranking'].clip(0,1)
        df_output6['Priority']=pd.cut(df_output6['Ranking'],bins=[-1,0.2,0.4,0.6,0.8,1],labels=["P5",'P4','P3','P2','P1'])
        df_output7=pd.merge(df_output5,df_output6[['Date','McuId','Risk_score','Priority']],on=['Date','McuId'])
        df_output7=df_output7.sort_values('Risk_score',ascending=False)
        df_output7.drop('Risk_score',axis=1,inplace=True)

        df_output26['Risk_score']=df_output26.iloc[:,8:].sum(axis=1)
        df_output26=pd.merge(df_output26,df_sensor_problem2[['Date','BankName','SiteName','McuId','problemed_sensors']],on=['Date','BankName','SiteName','McuId'],how='left')
        df_output26['Risk_score']=df_output26['Risk_score']*df_output26['problemed_sensors']

        df_output26=df_output26[df_output26['Risk_score']!=0]
        df_output26['Ranking']=df_output26['Risk_score'].rank(pct=True).round(2)
        df_output26['Ranking']=df_output26['Ranking'].clip(0,1)
        df_output26['Priority']=pd.cut(df_output26['Ranking'],bins=[-1,0.2,0.4,0.6,0.8,1],labels=["P5",'P4','P3','P2','P1'])
        df_output27=pd.merge(df_output25,df_output26[['Date','McuId','Risk_score','Priority']],on=['Date','McuId'])
        df_output27=df_output27.sort_values('Risk_score',ascending=False)
        df_output27.drop('Risk_score',axis=1,inplace=True)

        clear_data="DELETE FROM SensorPerformance_Trigger"
        clear_data1="DELETE FROM SensorPerformance_Heartbeat"
        cur.execute(clear_data)
        conn2.commit()
        cur.execute(clear_data1)
        conn2.commit()
        conn2.close()
        return df_output7,df_output27
    def pushdata(df_output7,df_output27):
        engine = create_engine('mssql+pymssql://aaum_db_usr:nfm!DBj!Pu#hZmgThxAoZe$E@188.42.97.40/AAUM_Analytics_DB')
        df_output7.to_sql('SensorPerformance_Trigger',engine,if_exists='append',index=False, method="multi", chunksize=500)
        df_output27.to_sql('SensorPerformance_Heartbeat',engine,if_exists='append',index=False, method="multi", chunksize=500)
        engine.dispose()
    video_rec,df_output1,df_output2,conn2,cur=fetch(ServerAddress,UserName,Password,DatabaseName)
    df_output7,df_output27=transform(video_rec,df_output1,df_output2,conn2,cur)
    pushdata(df_output7,df_output27)
    return df_output7,df_output27
