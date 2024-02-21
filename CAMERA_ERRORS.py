ServerAddress='188.42.97.40'
UserName='aaum_db_usr'
Password='nfm!DBj!Pu#hZmgThxAoZe$E'
DatabaseName='AAUM_Analytics_DB'

def CameraError(ServerAddress,UserName,Password,DatabaseName):
    import pymssql
    import numpy as np
    import pandas as pd
    from sqlalchemy import create_engine
    
    def transform(ServerAddress,UserName,Password,DatabaseName,df_vboxattendance_1,df_cam,df_sitemas):
        
        conn2=pymssql.connect(host=ServerAddress,user=UserName,password=Password,database=DatabaseName)
        cur2=conn2.cursor()
        df_opened_tickets=pd.read_sql('select * from dummy_camera_recording_issue where ClosedOn IS NULL',conn2)
        
        df_vboxattendance_1=pd.merge(df_sitemas['VBoxId'],df_vboxattendance_1,left_on='VBoxId',right_on='Vboxid',how='left')
        df_cam=pd.merge(df_sitemas['VBoxId'],df_cam,left_on='VBoxId',right_on='Vboxid',how='left')
        df_cam=df_cam[df_cam['qStatus']==True]

        df_vboxattendance_1=df_vboxattendance_1[df_vboxattendance_1['EventCode']=='RECORD']
        df_vboxattendance_1=df_vboxattendance_1.sort_values(['Vboxid','DeviceId','AtTime'])
        df_vboxattendance_12=df_vboxattendance_1.groupby(['Vboxid','DeviceId']).last()
        df_vboxattendance_12=df_vboxattendance_12.reset_index()
        df_vboxattendance_12.rename(columns={'AtTime':'OpenedOn'},inplace=True)

        df_vboxattendance_13=pd.merge(df_cam[['Vboxid','CameraId','CameraName']],df_vboxattendance_12,left_on=['Vboxid','CameraId'],right_on=['Vboxid','DeviceId'],how='left')
        df_vboxattendance_13=df_vboxattendance_13.sort_values(['Vboxid','OpenedOn'])

        df_vbox_latest_1=df_vboxattendance_13.groupby('Vboxid').last()
        df_vbox_latest_1=df_vbox_latest_1.reset_index()
        df_vbox_latest_1=df_vbox_latest_1[['Vboxid','OpenedOn']]
        df_vbox_latest_1.rename(columns={'OpenedOn':'Latest_recording'},inplace=True)

        df_vboxattendance_14=pd.merge(df_vboxattendance_13,df_vbox_latest_1,on='Vboxid')
        df_vboxattendance_14['Duration']=(df_vboxattendance_14['Latest_recording']-df_vboxattendance_14['OpenedOn']).astype('timedelta64[m]')
        df_vboxattendance_15=df_vboxattendance_14[df_vboxattendance_14['Duration']>=60]
        df_vboxattendance_15=df_vboxattendance_15[['Vboxid','CameraId','CameraName','OpenedOn']]
        df_vboxattendance_15=pd.merge(df_sitemas,df_vboxattendance_15,left_on='VBoxId',right_on='Vboxid')
        df_vboxattendance_15.rename(columns={'s_Name':'SiteName'},inplace=True)
        df_vboxattendance_15=df_vboxattendance_15[['BankName','SiteName','McuId','Vboxid','CameraId','CameraName','OpenedOn']]

        df_new_ticket=df_vboxattendance_15[~df_vboxattendance_15.set_index(['Vboxid','CameraId']).index.isin(df_opened_tickets.set_index(['Vboxid','CameraId']).index)]
        df_closed_ticket=df_opened_tickets[~df_opened_tickets.set_index(['Vboxid','CameraId']).index.isin(df_vboxattendance_15.set_index(['Vboxid','CameraId']).index)]

        return df_new_ticket,df_closed_ticket,conn2,cur2
    
    def pushdata(df_new_ticket,df_closed_ticket,conn2,cur2):
        
        ticketid=tuple(df_closed_ticket['TicketId'].values)
        for i in range(len(ticketid)):
            cur2.execute(f"UPDATE dummy_camera_recording_issue SET ClosedOn=CURRENT_TIMESTAMP WHERE TicketId={df_closed_ticket['TicketId'].iloc[i]}")
            conn2.commit()
        cur2.close()
        conn2.close()
            
        engine = create_engine('mssql+pymssql://aaum_db_usr:nfm!DBj!Pu#hZmgThxAoZe$E@188.42.97.40/AAUM_Analytics_DB')        
        df_new_ticket.to_sql('dummy_camera_recording_issue',engine,if_exists='append',index=False, method="multi", chunksize=500)
        engine.dispose()

    df_new_ticket,df_closed_ticket,conn2,cur2=transform(ServerAddress,UserName,Password,DatabaseName,df_vboxattendance_1,df_cam,df_sitemas)
    pushdata(df_new_ticket,df_closed_ticket,conn2,cur2)
    
    return df_new_ticket,df_closed_ticket
df_new_ticket,df_closed_ticket=CameraError(ServerAddress,UserName,Password,DatabaseName)