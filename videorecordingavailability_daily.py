ServerAddress='188.42.97.98'
UserName='aaum_support'
Password='aaum@123'
DatabaseName='InnoculateFSSCANARABANK'

def video_recording_daily(ServerAddress,UserName,Password,DatabaseName):
    import pymssql
    import pandas as pd
    import numpy as np
    import warnings
    from dateutil.relativedelta import relativedelta
    from datetime import datetime, timedelta
    import pytz
    from sqlalchemy import create_engine
    IST = pytz.timezone('Asia/Kolkata')
    import warnings
    warnings.filterwarnings('ignore')
    def fetch(ServerAddress,UserName,Password,DatabaseName):
        current_time=datetime.strftime(datetime.now(IST), '%Y-%m-%d %H:%M:%S')
        Today=pd.to_datetime(datetime.strftime(datetime.now(IST), '%Y-%m-%d 00:00:00'))
        yesterday=datetime.strftime(datetime.now(IST) - timedelta(1), '%Y-%m-%d 00:00:00')
        prior1day=datetime.strftime(datetime.now(IST) - timedelta(1), '%Y-%m-%d')
        conn = pymssql.connect(server=ServerAddress, user=UserName, password=Password, database=DatabaseName)  
        cursor = conn.cursor()
        df_video = pd.read_sql("select a.VboxID,a.CameraId,a.FileCount,a.RemoteFileCount,a.AtTime, b.s_Name,b.qStatus,b.BankCode from V3tblCameraRecordingCount as a, lbtSiteMas as b where a.VboxID = b.VBoxId and a.AtTime >= '{}'".format(prior1day), conn)
        df_cam = pd.read_sql("SELECT * FROM tblVideoMas WHERE qstatus='True' " ,conn)
        df_bank = pd.read_sql('SELECT BankCode,BankName FROM lbtBankMas',conn)
        df_mcu_heartbeat=pd.read_sql("select * from McuHBDateTime ",conn)
        events = pd.read_sql("SELECT EventId,McuId,EventType,Msg,AtTime,OpenedOn,ClosedOn FROM lbtEvents WHERE ((ClosedON BETWEEN '{}' AND '{}') AND (OpenedOn < '{}') OR ClosedON IS NULL) AND (Msg='NC-30' or Msg='NC-30 (Mains Power Failure)') AND qStatus='True'".format(prior1day,current_time,Today),conn)
        df_site=pd.read_sql("select a.s_Name,a.VboxID,a.McuId,a.s_Addr_State,b.BankName,a.BankCode,a.Installed_Date from lbtSiteMas as a, lbtBankMas as b WHERE a.BankCode=b.BankCode AND a.qstatus='True' AND b.qstatus='True' AND a.s_Status=1 ",conn)
        df_video = pd.merge(df_video,df_bank,on='BankCode')
        df_videorec = pd.merge(df_video,df_cam,left_on=['VboxID','CameraId'],right_on=['Vboxid','CameraId'])
        df_videorec = df_videorec[df_videorec['qStatus_y']==True]
        df_videorec = df_videorec[df_videorec['qStatus_x']==True]
        df_videorec = df_videorec.drop_duplicates(subset=['VboxID','CameraId','AtTime'])
        df_videorec = df_videorec.sort_values('AtTime')
        cursor.close
        conn.close()
        return df_video,df_cam,df_bank,df_mcu_heartbeat,events,df_site,df_videorec,Today,yesterday,prior1day,DatabaseName

    def transformation(df_video,df_cam,df_bank,df_mcu_heartbeat,events,df_site,df_videorec,Today,yesterday,prior1day,DatabaseName):

        df_sites_cam=pd.merge(df_site,df_cam,left_on='VboxID',right_on='Vboxid',how='left')
        df_sites_cam=df_sites_cam.drop_duplicates(['VboxID','CameraId'],keep='first')
        df_sites_cam=df_sites_cam[['BankName','s_Name','VboxID','McuId','CameraName','CameraId']]

        events.drop_duplicates(['McuId','EventType','AtTime'],inplace=True)
        events1=pd.merge(df_site['McuId'],events,left_on='McuId',right_on='McuId',how='left')
        df_null_events=events1[events1['EventId'].isnull()]
        events2=events1.drop(df_null_events.index)
        events_to_drop=events2[(events2['AtTime']>=Today) & (events2['ClosedOn'].isnull())]
        events3=events2.drop(events_to_drop.index)
        df_open_tickets=events3[events3['ClosedOn'].isnull()]
        
        if DatabaseName=='InnoculateCMS':
            df_mcu_opentickets=pd.merge(df_open_tickets,df_mcu_heartbeat,left_on='McuId',right_on='McuID',how='left')
            df_mcu_opentickets=df_mcu_opentickets[df_mcu_opentickets['OpenedOn']<df_mcu_opentickets['GetDate']]
        else:
            df_mcu_opentickets=pd.merge(df_open_tickets,df_mcu_heartbeat,on='McuId',how='left')
            df_mcu_opentickets=df_mcu_opentickets[df_mcu_opentickets['OpenedOn']<df_mcu_opentickets['ReceivedDate']]

        df_mcu_opentickets=df_mcu_opentickets[['McuId','EventId','EventType','Msg','AtTime','OpenedOn','ClosedOn']]

        events4=events3.merge(df_mcu_opentickets, how='left', indicator=True)
        events4=events4[events4['_merge']=='left_only']
        events4=events4[['McuId','EventId','EventType','Msg','AtTime','OpenedOn','ClosedOn']]

        Events=events4.drop_duplicates(['McuId','EventType','AtTime'])

        df_null=Events[Events['AtTime'].isnull()]
        df_new_events=Events.drop(df_null.index)
        
        df_new_events['today']=Today
        df_new_events['yesterday']=pd.to_datetime(yesterday)
        df_new_events['ClosedOn']=df_new_events['ClosedOn'].fillna(df_new_events['today'])

        df_new_events['ClosedOn'] =np.where(df_new_events['ClosedOn']>Today,df_new_events['today'],df_new_events['ClosedOn'])
        df_new_events['ClosedOn']=pd.to_datetime(df_new_events['ClosedOn'], format='%Y-%m-%d %H:%M:%S')
        df_new_events['AtTime']=np.where(df_new_events['AtTime']<yesterday,df_new_events['yesterday'],df_new_events['AtTime'])
        df_new_events['ClosedOn']=pd.to_datetime(df_new_events['ClosedOn'], format='%Y-%m-%d %H:%M:%S')
        df_new_events['TicketAge']=(df_new_events['ClosedOn']-df_new_events['AtTime']).astype('timedelta64[m]')

        df_new_events=pd.merge(df_new_events,df_site[['McuId','Installed_Date']],how='left')
        df_new_events['Offline%']=((df_new_events['TicketAge']/1440)*100).round(2)
        df_new_events_group=df_new_events.groupby(['McuId']).sum()
        df_new_events_group=df_new_events_group.reset_index()
        df_new_events_group=df_new_events_group[['McuId','TicketAge','Offline%']]
        df_new_events_group.sort_values('Offline%')
        df_offline=pd.merge(df_site[['BankName','s_Name','McuId','VboxID']],df_new_events_group,left_on='McuId',right_on='McuId',how='left')
        df_offline[['TicketAge','Offline%']]=df_offline[['TicketAge','Offline%']].fillna(0)

        df_videorec=df_videorec[['BankName','s_Name','VboxID','CameraId','FileCount','RemoteFileCount','AtTime','CameraName']]
        df_all_recording=pd.merge(df_sites_cam,df_videorec,left_on=['BankName','s_Name','VboxID','CameraName'],right_on=['BankName','s_Name','VboxID','CameraName'],how='left')
        df_all_recording=df_all_recording.sort_values('AtTime')
        df_all_recording=df_all_recording.drop_duplicates(['BankName','s_Name','VboxID','McuId','CameraName'],keep='first')
        df_all_recording=df_all_recording[['BankName','s_Name','McuId','VboxID','CameraName','AtTime','FileCount','RemoteFileCount']]
        df_all_recording['CameraName']=df_all_recording['CameraName'].str.replace(' ', '')
        df_all_recording['RemoteFileCount']=df_all_recording['RemoteFileCount'].fillna(0)
        df_all_recording['FileCount']=df_all_recording['FileCount'].fillna(0)
        df_all_recording['Count']=df_all_recording['FileCount']+df_all_recording['RemoteFileCount']

        df_count=df_all_recording.groupby(['BankName','s_Name','McuId','VboxID','CameraName']).sum()
        df_count=df_count.reset_index()
        df_count=df_count[['BankName','s_Name','VboxID','CameraName','Count']]

        df_pivot=pd.pivot_table(data=df_count,columns='CameraName',index=['VboxID'],values='Count').reset_index()
        camera_list=list(df_count['CameraName'].unique())
        df_pivot.iloc[:,1:]=((df_pivot.iloc[:,1:]/1000)*100).round(2)
        df_pivot.iloc[:,1:]=df_pivot.iloc[:,1:].clip(0,100)
        df_pivot['RecordingAvailablePerc']=(np.mean(df_pivot.iloc[:,1:],axis=1)).round(2)
        df_pivot['Availability Difference']=np.max(df_pivot.iloc[:,1:-1],axis=1)-np.min(df_pivot.iloc[:,1:],axis=1)
        df_pivot['Minimum']=np.min(df_pivot.iloc[:,1:-2],axis=1)

        df_output = pd.merge(df_site[['BankName','s_Name','McuId','VboxID']],df_pivot,on='VboxID')
        df_cam_offline=pd.merge(df_output,df_offline,on=['BankName','s_Name','McuId','VboxID'])
        df_cam_offline['SiteOnlinePerc']=(100-df_cam_offline['Offline%']).round(2)

        df_cam_offline=df_cam_offline.iloc[:, df_cam_offline.columns!='Offline%']
        df_cam_offline=df_cam_offline.iloc[:, df_cam_offline.columns!='TicketAge']
        df_cam_offline['Diff_online_recAvail']=(df_cam_offline['SiteOnlinePerc']-df_cam_offline['RecordingAvailablePerc']).round(2)

        condition = [(df_cam_offline['Diff_online_recAvail']>=50),(df_cam_offline['Diff_online_recAvail']>=20) & (df_cam_offline['Diff_online_recAvail']<50) & (df_cam_offline['Availability Difference']<=20),(df_cam_offline['Diff_online_recAvail']<20),(df_cam_offline['Diff_online_recAvail']>=20) & (df_cam_offline['Diff_online_recAvail']<50) & (df_cam_offline['Availability Difference']>20)]
        values = ['Site Level Video Recording Issue','Site Level Video Recording Issue','No Issue','Camera Issue']
        df_cam_offline['Status'] = np.select(condition, values)  

        df_video_available=df_cam_offline.drop(['Availability Difference','Minimum','Diff_online_recAvail'],axis=1)
        df_video_available['Date']=prior1day
        first_column = df_video_available.pop('Date')
        df_video_available.insert(0, 'Date', first_column)
        df_video_available.rename(columns={'s_Name':'SiteName','RecordingAvailablePerc':'VideoRecAvailability','SiteOnlinePerc':'SiteAvailability'},inplace=True)
        df_site_issue=df_video_available[df_video_available['Status']=='Site Level Video Recording Issue']
        df_no_issue=df_video_available[df_video_available['Status']=='No Issue']   
        df_camera_issue=df_video_available[df_video_available['Status']=='Camera Issue']

        if len(df_camera_issue)>=1:
            df_camera_issue['Remark'] = df_camera_issue.iloc[:,5:-3].idxmin(axis = 1)
            df_camera_issue['Remark'] = df_camera_issue['Remark']+' Issue'
        else:
            df_camera_issue['Remark'] = df_camera_issue['Status'] 
        df_site_issue['Remark']=df_site_issue['Status']
        df_no_issue['Remark']=df_no_issue['Status']
        df_videos_available=pd.concat([df_camera_issue,df_site_issue,df_no_issue],ignore_index=True)
        df_videos_available=df_videos_available.iloc[:,df_videos_available.columns!='Status']
        return df_videos_available

    def pushdata(df_videos_available):
        engine = create_engine('mssql+pymssql://aaum_db_usr:nfm!DBj!Pu#hZmgThxAoZe$E@188.42.97.40/AAUM_Analytics_DB')        
        df_videos_available.to_sql('videorecording_daily',engine,if_exists='append',index=False)
 
    df_video,df_cam,df_bank,df_mcu_heartbeat,events,df_site,df_videorec,Today,yesterday,prior1day,DatabaseName= fetch(ServerAddress,UserName,Password,DatabaseName)
    df_videos_available=transformation(df_video,df_cam,df_bank,df_mcu_heartbeat,events,df_site,df_videorec,Today,yesterday,prior1day,DatabaseName)
    #pushdata(df_videos_available)
    print("succesfull Inside")
    return df_videos_available
df_videos_available= video_recording_daily(ServerAddress,UserName,Password,DatabaseName)
print("succesfull")