def CameraError(df_vboxattendance,df_sitemas,df_cam):
    import pymssql
    import pandas as pd
    from sqlalchemy import create_engine
    from datetime import datetime
    import pytz
    IST = pytz.timezone('Asia/Kolkata')
        
    def transform(df_vboxattendance,df_sitemas,df_cam):
        BankName=tuple(df_sitemas['BankName'].unique())
        conn2=pymssql.connect(host='188.42.97.40',user='aaum_db_usr',password='nfm!DBj!Pu#hZmgThxAoZe$E',database='AAUM_Analytics_DB')
        cur2=conn2.cursor()
        if len(BankName)>1:
            df_opened_tickets=pd.read_sql("select * from Camera_Issues where BankName in {} and ClosedOn IS NULL".format(BankName),conn2)
        else:
            BankName=BankName[0]
            df_opened_tickets=pd.read_sql("select * from Camera_Issues where BankName='{}' and ClosedOn IS NULL".format(BankName),conn2)

        df_vboxattendance=pd.merge(df_sitemas['VBoxId'],df_vboxattendance,left_on='VBoxId',right_on='Vboxid')
        df_cam=pd.merge(df_sitemas['VBoxId'],df_cam,left_on='VBoxId',right_on='Vboxid',how='left')

        df_vboxattendance=df_vboxattendance[df_vboxattendance['EventCode']=='RECORD']
        df_vboxattendance=df_vboxattendance.sort_values(['Vboxid','DeviceId','AtTime'])
        df_vboxattendance=df_vboxattendance.groupby(['Vboxid','DeviceId']).last()
        df_vboxattendance=df_vboxattendance.reset_index()
        df_vboxattendance.rename(columns={'AtTime':'OpenedOn'},inplace=True)

        df_vbox_latest=df_vboxattendance.groupby('Vboxid').last()
        df_vbox_latest=df_vbox_latest.reset_index()
        df_vbox_latest=df_vbox_latest[['Vboxid','OpenedOn']]
        df_vbox_latest.rename(columns={'OpenedOn':'Latest_recording'},inplace=True)

        df_cam2=df_cam[df_cam['Vboxid'].isin(df_vboxattendance['Vboxid'])]

        df_vboxattendance1=pd.merge(df_cam2[['Vboxid','CameraId','CameraName']],df_vboxattendance,left_on=['Vboxid','CameraId'],right_on=['Vboxid','DeviceId'],how='left')
        df_vboxattendance1=df_vboxattendance1.sort_values(['Vboxid','OpenedOn'])
        df_vboxattendance1=df_vboxattendance1[['Vboxid','CameraId','CameraName','OpenedOn']]
        df_vboxattendance.rename(columns={'Vboxid':'VboxId','DeviceId':'CameraId'},inplace=True)

        df_new_tickets=df_vboxattendance1[df_vboxattendance1['OpenedOn'].isnull()]
        df_new_tickets=pd.merge(df_sitemas[['BankName','s_Name','McuId','VBoxId']],df_new_tickets,left_on='VBoxId',right_on='Vboxid')
        df_new_tickets.rename(columns={'s_Name':'SiteName','Vboxid':'VboxId'},inplace=True)

        df_new_tickets=df_new_tickets[~df_new_tickets.set_index(['VboxId','CameraId','CameraName']).index.isin(df_opened_tickets.set_index(['VboxId','CameraId','CameraName']).index)]
        df_new_tickets['OpenedOn']=pd.to_datetime(datetime.strftime(datetime.now(IST), '%Y-%m-%d %H:%M:%S'))
        df_new_tickets=df_new_tickets[['BankName','SiteName','McuId','VBoxId','CameraId','CameraName','OpenedOn']]
        df_closed_tickets=df_opened_tickets[df_opened_tickets.set_index(['VboxId','CameraId']).index.isin(df_vboxattendance.set_index(['VboxId','CameraId']).index)]
        return df_new_tickets,df_closed_tickets,conn2,cur2
    
    def pushdata(df_new_tickets,df_closed_tickets,conn2,cur2):
        ticketid=tuple(df_closed_tickets['TicketId'].values)

        if len(df_closed_tickets)==1:
            cur2.execute(f"UPDATE Camera_Issues SET ClosedOn=CURRENT_TIMESTAMP WHERE TicketId={df_closed_tickets['TicketId'].iloc[0]}")
            conn2.commit()
        elif len(df_closed_tickets)>1:
            for i in range(len(df_closed_tickets)):
                cur2.execute(f"UPDATE Camera_Issues SET ClosedOn=CURRENT_TIMESTAMP WHERE TicketId={df_closed_tickets['TicketId'].iloc[i]}")
                conn2.commit()
        else:
            pass
        cur2.close()
        conn2.close()
            
        engine = create_engine('mssql+pymssql://aaum_db_usr:nfm!DBj!Pu#hZmgThxAoZe$E@188.42.97.40/AAUM_Analytics_DB')        
        df_new_tickets.to_sql('Camera_Issues',engine,if_exists='append',index=False, method="multi", chunksize=500)
        engine.dispose()

    df_new_tickets,df_closed_tickets,conn2,cur2=transform(df_vboxattendance,df_sitemas,df_cam)
    pushdata(df_new_tickets,df_closed_tickets,conn2,cur2)
   