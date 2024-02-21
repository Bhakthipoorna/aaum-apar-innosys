ServerAddress='188.42.97.40'
UserName='aaum_db_usr'
Password='nfm!DBj!Pu#hZmgThxAoZe$E'
DatabaseName='AAUM_Analytics_DB'

def main(ServerAddress,UserName,Password,DatabaseName):
    import pymssql
    import pandas as pd
    import numpy as np
    import warnings
    from datetime import datetime, timedelta
    import pytz
    from pytz import timezone 
    from sqlalchemy import create_engine
    warnings.filterwarnings('ignore')
    IST = pytz.timezone('Asia/Kolkata')
    Today=pd.to_datetime(datetime.strftime(datetime.now(IST), '%Y-%m-%d %H:%M:%S'))
    prior15days=datetime.strftime(datetime.now(IST) - timedelta(15), '%Y-%m-%d')

    def fetch_and_transform(ServerAddress,UserName,Password,DatabaseName):

        conn2=pymssql.connect(host=ServerAddress,user=UserName,password=Password,database=DatabaseName)
        cur=conn2.cursor()

        video_rec=pd.read_sql("SELECT Date,BankName,SiteName,McuId,SiteAvailability from videorecording_daily where Date>= '{}'  ".format(prior15days),conn2)
        df_output1=pd.read_sql('Select * from sensor_trigger_performance',conn2)
        df_output2=pd.read_sql('Select * from sensor_heartbeat_performance',conn2)
        
        
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
        df_output6.replace({"<15days":0,"15-30days":2,"1-3months":3,"3-6months":4,"6-12months":5,">1year":5,"Sensor Installed but No Trigger":5},inplace=True)

        df_output26=df_output25.copy()
        df_output25.drop('Priority',axis=1,inplace=True) 
        df_output26.drop('Priority',axis=1,inplace=True)
        df_output26.replace({"<15days":0,"15-30days":2,"1-3months":3,"3-6months":4,"6-12months":5,">1year":5,"Sensor Installed but No Trigger":5},inplace=True)

        df_output6['Risk_score']=df_output6.iloc[:,8:].sum(axis=1)
        df_output6=df_output6[df_output6['Risk_score']!=0]
        df_output6['Ranking']=df_output6['Risk_score'].rank(pct=True).round(2)
        df_output6['Ranking']=df_output6['Ranking'].clip(0,1)
        df_output6['Priority']=pd.cut(df_output6['Ranking'],bins=[-1,0.19,0.39,0.59,0.79,2],labels=["P5",'P4','P3','P2','P1'])
        df_output7=pd.merge(df_output5,df_output6[['Date','McuId','Priority']],on=['Date','McuId'])
        df_output7=df_output7.sort_values('Priority',ascending=False)
        
        df_output26['Risk_score']=df_output26.iloc[:,8:].sum(axis=1)
        df_output26=df_output26[df_output26['Risk_score']!=0]
        df_output26['Ranking']=df_output26['Risk_score'].rank(pct=True).round(2)
        df_output26['Ranking']=df_output26['Ranking'].clip(0,1)
        df_output26['Priority']=pd.cut(df_output26['Ranking'],bins=[-1,0.19,0.39,0.59,0.79,2],labels=["P5",'P4','P3','P2','P1'])
        df_output27=pd.merge(df_output25,df_output26[['Date','McuId','Priority']],on=['Date','McuId'])
        df_output27=df_output27.sort_values('Priority',ascending=False)

        clear_data="DELETE FROM sensor_trigger_performance"
        clear_data1="DELETE FROM sensor_heartbeat_performance"
        cur.execute(clear_data)
        conn2.commit()
        cur.execute(clear_data1)
        conn2.commit()
        engine = create_engine('mssql+pymssql://aaum_db_usr:nfm!DBj!Pu#hZmgThxAoZe$E@188.42.97.40/AAUM_Analytics_DB')
        df_output7.to_sql('sensor_trigger_performance',engine,if_exists='append',index=False, method="multi", chunksize=500)
        df_output27.to_sql('sensor_heartbeat_performance',engine,if_exists='append',index=False, method="multi", chunksize=500)
        
        conn2.close()
        engine.dispose()
        return df_output7,df_output27
    df_output7,df_output27=fetch_and_transform(ServerAddress,UserName,Password,DatabaseName)
    return df_output7,df_output27
df_output7,df_output27=main(ServerAddress,UserName,Password,DatabaseName)
