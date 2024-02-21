def CameraNotWorking(df_sitemas,df_vboxattendance):

    import pymssql
    import pandas as pd
    from datetime import datetime,timedelta
    import pytz
    IST = pytz.timezone('Asia/Kolkata')
    from sqlalchemy import create_engine
    
    def transformation(df_sitemas,df_vboxattendance):
        
        conn2=pymssql.connect(host='188.42.97.40',user='aaum_db_usr',password='nfm!DBj!Pu#hZmgThxAoZe$E',database='AAUM_Analytics_DB')
        cur2=conn2.cursor()
        
        BankName=tuple(df_sitemas['BankName'].unique())
        if len(BankName)>1:
            df_cam_issue=pd.read_sql("select TicketId,BankName,SiteName,OpenedOn,ClosedOn from CameraNotWorking where BankName in {} and ClosedOn IS NULL".format(BankName),conn2)
        else:
            BankName=BankName[0]
            df_cam_issue=pd.read_sql("select TicketId,BankName,SiteName,OpenedOn,ClosedOn from CameraNotWorking where BankName='{}' and ClosedOn IS NULL".format(BankName),conn2)

        if len(df_vboxattendance)>0:
            df_vboxattendance.rename(columns={'Vboxid':'VboxId'},inplace=True)
            df_sitemas.rename(columns={'s_Name':'SiteName','VBoxId':'VboxId'},inplace=True)
            
            df_vboxattendance=pd.merge(df_sitemas[['BankName','SiteName','McuId','VboxId']],df_vboxattendance,on='VboxId',how='left')
            
            df_vboxattendance=df_vboxattendance[df_vboxattendance['DeviceId'].isin(['0702','0702R'])]
            df_vboxattendance=df_vboxattendance.sort_values('AtTime')
            df_vboxattendance.drop_duplicates(['VboxId'],keep='last',inplace=True)
            
            df_vboxattendance['last_record']=((pd.to_datetime(datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"))-df_vboxattendance['AtTime'])).astype('timedelta64[m]')
            df_recording=df_vboxattendance[df_vboxattendance['last_record']<5]
            df_not_recording=df_vboxattendance[df_vboxattendance['last_record']>5]
            
            df_not_recording.rename(columns={'AtTime':'OpenedOn','DeviceId':'CameraId'},inplace=True)
            df_not_recording=df_not_recording[['BankName','SiteName','McuId','VboxId','CameraId','OpenedOn']]
            
            df_not_recording_latest=df_not_recording[~df_not_recording['SiteName'].isin(df_cam_issue['SiteName'])]
            df_closed=df_cam_issue[df_cam_issue['SiteName'].isin(df_recording['SiteName'])]
            
        else:
            df_not_recording_latest = pd.DataFrame()
            df_closed = pd.DataFrame()



        return df_not_recording_latest,df_closed,conn2,cur2
        
        
    def pushdata(df_not_recording_latest,df_closed,conn2,cur2):
        
        if len(df_closed)==1:
            cur2.execute(f"UPDATE CameraNotWorking SET ClosedOn=CURRENT_TIMESTAMP WHERE TicketId={df_closed['TicketId'].iloc[0]}")
            conn2.commit()
        elif len(df_closed)>1:
            for i in range(len(df_closed)):
                cur2.execute(f"UPDATE CameraNotWorking SET ClosedOn=CURRENT_TIMESTAMP WHERE TicketId={df_closed['TicketId'].iloc[i]}")
                conn2.commit()
        else:
            pass
        cur2.close()
        conn2.close()
              
        if len(df_not_recording_latest)>0:
            engine = create_engine('mssql+pymssql://aaum_db_usr:nfm!DBj!Pu#hZmgThxAoZe$E@188.42.97.40/AAUM_Analytics_DB')        
            df_not_recording_latest.to_sql('CameraNotWorking',engine,if_exists='append',index=False, method="multi", chunksize=500)
            engine.dispose()
        else:
            pass
        
        
    df_not_recording_latest,df_closed,conn2,cur2 = transformation(df_sitemas,df_vboxattendance)
    pushdata(df_not_recording_latest,df_closed,conn2,cur2)
   
    return df_not_recording_latest,df_closed
