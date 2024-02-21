
def SitesPriorityDataGenaration(ServerAddress,UserName,Password,DatabaseName):
    import pandas as pd
    import pymssql
    import xlsxwriter
    from datetime import datetime,timedelta
    import pytz
    IST = pytz.timezone('Asia/Kolkata')
    from sqlalchemy import create_engine
    
    Today=pd.to_datetime(datetime.strftime(datetime.now(IST), '%Y-%m-%d %H:%M:%S'))
    prior7days=datetime.strftime(datetime.now(IST) - timedelta(7), '%Y-%m-%d')
 
    def fetch(ServerAddress,UserName,Password,DatabaseName):

        conn = pymssql.connect(server=ServerAddress, user=UserName, password=Password, database=DatabaseName)  
        cursor = conn.cursor()
        df_sitemas = pd.read_sql("select a.s_Name,a.McuId,a.s_Addr_State,a.s_Addr_District,a.s_Addr_Street,a.s_Addr_Zone,b.BankName from lbtSiteMas as a,lbtBankMas as b WHERE a.BankCode=b.BankCode AND a.s_Status=1 AND a.qstatus='True' AND b.qstatus='True' ",conn)
        df_events=pd.read_sql("select McuId,UnitNo,GroupNo,Msg,AtTime,ClosedOn,ClosedBy,ClosingReason from lbtEvents where AtTime  between (GETDATE()-7) and (GETDATE())",conn)
        cursor.close()
        conn.close()


        return df_sitemas, df_events

    def transformation(df_sitemas, df_events):


        df_events=pd.merge(df_sitemas['McuId'],df_events,on='McuId')
        
        df_backroom_events=df_events[(df_events['UnitNo']=='01') & (df_events['GroupNo']=='32')]
        df_backroom_events=df_backroom_events[~df_backroom_events['ClosingReason'].isnull()]

        df_events['ClosedBy']=df_events['ClosedBy'].str.lower()
        df_manual_closure=df_events[df_events['ClosedBy']!='system']
        df_manual_closure=df_events[~df_events['ClosedBy'].isnull()]

        df_pir_events=df_events[(df_events['UnitNo']=='01') & (df_events['GroupNo']=='36')]
                
        df_backroom_events['ClosingReason']=df_backroom_events['ClosingReason'].str.lower()
        df_events_security=df_backroom_events[df_backroom_events['ClosingReason'].str.contains('security')]

        df_sites_with_security=pd.DataFrame(df_events_security['McuId'].unique())
        #df_sites_with_security=df_sites_with_security.reset_index()
        df_sites_with_security.rename(columns={0:'McuId'},inplace=True)
        df_sites_with_security['Security']=1

        df_security=pd.merge(df_sitemas,df_sites_with_security,on='McuId',how='left')
        df_security['Security'].fillna(0,inplace=True)

        df_manual_closure=pd.merge(df_sitemas['McuId'],df_manual_closure,on='McuId')
        df_manual_closure=df_manual_closure[~df_manual_closure['ClosedOn'].isnull()]

        df_manual_closure=df_manual_closure.sort_values('AtTime')
        df_manual_closure.drop_duplicates('McuId',keep='last',inplace=True)

        df_manual_closure['current_time']=pd.to_datetime(datetime.strftime(datetime.now(IST), '%Y-%m-%d %H:%M:%S'))
        df_manual_closure['Last_closure']=(df_manual_closure['current_time']-df_manual_closure['ClosedOn']).astype('timedelta64[m]')

        df_manual_closure=pd.merge(df_sitemas['McuId'],df_manual_closure,on='McuId',how='left')

        df_manual_closure['Rank']=df_manual_closure['Last_closure'].rank(pct=True).round(2)
        df_manual_closure['Rank']=df_manual_closure['Rank'].clip(0,1)
        df_manual_closure['ManualClosure']=pd.cut(df_manual_closure['Rank'],bins=[-1,0.2,0.4,0.6,0.8,1],labels=[5,4,3,2,1])
        df_manual_closure['ManualClosure'].fillna(1,inplace=True)

        df1=pd.merge(df_security[['McuId','Security']],df_manual_closure[['McuId','ManualClosure']],on=['McuId'])

        if len(df_pir_events)>0:

            df_pir_events=pd.merge(df_sitemas['McuId'],df_pir_events,on='McuId')
            df_pir_events=df_pir_events.sort_values('AtTime')
            df_pir_events.drop_duplicates(['McuId','Msg','AtTime'],keep='last',inplace=True)

            df_visits=pd.DataFrame(df_pir_events['McuId'].value_counts())
            df_visits=df_visits.reset_index()
            df_visits.rename(columns={'McuId':'count','index':'McuId'},inplace=True)
            df_visits=pd.merge(df_sitemas['McuId'],df_visits,on='McuId',how='left')
            df_visits.fillna(0,inplace=True)
            df_visits['Tickets_per_day']=(df_visits['count']/10).round(2)

            df_visits['Rank']=df_visits['count'].rank(pct=True).round(2)
            df_visits['Rank']=df_visits['Rank'].clip(0,1)
            df_visits['Footfall']=pd.cut(df_visits['Rank'],bins=[-1,0.2,0.4,0.6,0.8,1],labels=[1,2,3,4,5])

            df2=pd.merge(df1,df_visits[['McuId','Footfall']],on=['McuId'])

            df_pir_events['hour']=df_pir_events['AtTime'].dt.hour
            df_events_night=df_pir_events[df_pir_events['hour'].isin(['22','23','00','01','02','03','04','05'])]

            df_night_visits=pd.DataFrame(df_events_night['McuId'].value_counts())
            df_night_visits=df_night_visits.reset_index()
            df_night_visits.rename(columns={'McuId':'count','index':'McuId'},inplace=True)
            df_night_visits=pd.merge(df_sitemas['McuId'],df_night_visits,on='McuId',how='left')
            df_night_visits.fillna(0,inplace=True)
            df_night_visits['Tickets_per_day']=(df_night_visits['count']/10).round(2)

            df_night_visits['Rank']=df_night_visits['count'].rank(pct=True).round(2)
            df_night_visits['Rank']=df_night_visits['Rank'].clip(0,1)
            df_night_visits['NightVisit']=pd.cut(df_night_visits['Rank'],bins=[-1,0.2,0.4,0.6,0.8,1],labels=[1,2,3,4,5])

            df3=pd.merge(df2,df_night_visits[['McuId','NightVisit']],on=['McuId'])
            df3[['ManualClosure','Footfall','NightVisit']]=df3[['ManualClosure','Footfall','NightVisit']].astype('int')

        else:
            df1[['ManualClosure']]=df1[['ManualClosure']].astype('int')
            df3=pd.DataFrame(data=[['0','0','0']],columns=["McuId","Footfall","NightVisit"])
            df3=pd.merge(df1,df3,on="McuId",how='left')
            df3.fillna(0,inplace=True)
    
    
        df3[['ManualClosure','Footfall','NightVisit']]=df3[['ManualClosure','Footfall','NightVisit']].astype('int')
        df3=pd.merge(df_sitemas[['BankName','s_Name','McuId']],df3,on='McuId',how='left')
        df3.rename(columns={'s_Name':'SiteName'},inplace=True)



        return df3,df_sitemas


    def pushdata(df3,df_sitemas):
        if len(df3)>0:
            bank=list(df_sitemas['BankName'].unique())
            conn2=pymssql.connect(host='188.42.97.40',user='aaum_db_usr',password='nfm!DBj!Pu#hZmgThxAoZe$E',database='AAUM_Analytics_DB')
            cur2=conn2.cursor()
            clear_data1="DELETE FROM Site_Priority WHERE BankName  = %s"
            cur2.executemany(clear_data1, bank)
            conn2.commit()
            conn2.close()
            engine = create_engine('mssql+pymssql://aaum_db_usr:nfm!DBj!Pu#hZmgThxAoZe$E@188.42.97.40/AAUM_Analytics_DB')
            df3.to_sql('Site_Priority',engine,if_exists='append',index=False, method="multi", chunksize=500)
            engine.dispose()
        else:
            pass

    df_sitemas, df_events = fetch(ServerAddress,UserName,Password,DatabaseName)
    df3,df_sitemas = transformation(df_sitemas, df_events)
    pushdata(df3,df_sitemas)
   
