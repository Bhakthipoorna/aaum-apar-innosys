def VODDataGeneration(ServerAddress,UserName,Password,DatabaseName):
    import pandas as pd
    import numpy as np
    import pymssql
    from dateutil.relativedelta import relativedelta
    from datetime import datetime, timedelta
    from sqlalchemy import create_engine
    import pytz
    IST = pytz.timezone('Asia/Kolkata')
    Today=pd.to_datetime(datetime.strftime(datetime.now(IST), '%Y-%m-%d %H:%M:%S'))
    yesterday=datetime.strftime(datetime.now(IST) - timedelta(1), '%Y-%m-%d')

    def fetch(ServerAddress,UserName,Password,DatabaseName):
        conn = pymssql.connect(server=ServerAddress, user="aaum_support", password="aaum@123", database=DatabaseName)  
        cursor = conn.cursor()
        df_sitemas = pd.read_sql("select a.s_Name,a.McuId,b.BankName,a.VboxId from lbtSiteMas as a, lbtBankMas as b WHERE a.BankCode=b.BankCode AND a.s_Status=1 AND a.qstatus='True' AND b.qstatus='True' ",conn)
        df_video=pd.read_sql("select VboxId,CameraId,FileCount,RemoteFileCount,AtTime from V3tblCameraRecordingCount where AtTime between '{}' and '{}' ".format(yesterday,Today),conn)
        df_cam = pd.read_sql("SELECT VboxId,CameraName,CameraId FROM tblVideoMas WHERE qstatus='True' " ,conn)     
        cursor.close()
        conn.close()
        return df_sitemas,df_video,df_cam

    def transformation(df_sitemas,df_video,df_cam):
        df_sitemas.rename(columns={'s_Name':'SiteName'},inplace=True)
        df_video=df_video.sort_values('FileCount')
        df_video.drop_duplicates(['VboxId','CameraId'],keep='last',inplace=True)
        df_video['CameraId'].replace({'0702R':'0702'},inplace=True)
        df_video_count=df_video.groupby(['VboxId','CameraId']).count()
        df_video_count=df_video_count.reset_index()
        df_video_count=df_video_count[['VboxId','CameraId','AtTime']]
        df_video_count.rename(columns={'AtTime':'count'},inplace=True)
        df_video_multiple_records=df_video_count[df_video_count['count']>1]
        if len(df_video_multiple_records)>0:
            df_video1=df_video[~df_video['VboxId'].isin(df_video_multiple_records['VboxId'])]
            df_multiple_records=df_video[df_video['VboxId'].isin(df_video_multiple_records['VboxId'])]
            df_multiple_records1=df_multiple_records[df_multiple_records['CameraId']!='0702']
            df_multiple_records2=df_multiple_records[df_multiple_records['CameraId']=='0702']
            df_multiple_records2_latest=df_multiple_records2.sort_values('AtTime')
            df_multiple_records2_latest.drop_duplicates('VboxId',keep='last',inplace=True)
            df_multiple_records2_latest=df_multiple_records2_latest[['VboxId','CameraId','AtTime']]
            df_multiple_records2=df_multiple_records2.groupby(['VboxId']).sum()
            df_multiple_records2=df_multiple_records2.reset_index()
            df_multiple_records2=pd.merge(df_multiple_records2,df_multiple_records2_latest,on='VboxId')
            df_multiple_records3=pd.concat([df_multiple_records1,df_multiple_records2],ignore_index=True)
            df_video1=pd.concat([df_video1,df_multiple_records3],ignore_index=True)
        else:
            df_video1=df_video

        df_video1=pd.merge(df_video1,df_sitemas['VboxId'],on='VboxId')
        df_video1['RemoteFileCount'].fillna(0,inplace=True)
        df_video1['FileCount'].fillna(0,inplace=True)
        df_video1['TotalFile']=df_video1['FileCount']+df_video1['RemoteFileCount']
        df_video1['CameraName']=df_video1['CameraId']
        df_video1['CameraName'].replace({'0701':'Outdoor Camera','0702':'Lobby Camera','0703':'Backroom Camera','0704':'Pinhole Camera','0702R':'Lobby Camera'},inplace=True)
        df_video2=pd.merge(df_cam,df_video1,on=['VboxId','CameraName','CameraId'],how='left') # All the sites installed camera
        df_video2['TotalFile'].fillna(0,inplace=True) # NAN means camera is installed but not record
        df_pivot_table=pd.pivot_table(data=df_video2,index='VboxId',columns='CameraName',values='TotalFile')
        df_pivot_table=df_pivot_table.reset_index()
        camera_names=['Lobby Camera','Backroom Camera','Outdoor Camera','Pinhole Camera']
        for i in camera_names:
            if i not in df_pivot_table.columns:
                df_pivot_table[i]=np.nan               
        df_pivot=df_pivot_table[['VboxId','Backroom Camera','Lobby Camera','Outdoor Camera','Pinhole Camera']]
        df_pivot['Total']=df_pivot.iloc[:,1:].sum(axis=1)
        conditionsOnBackroom = [
                (df_pivot['Backroom Camera']>=860), 
                (df_pivot['Backroom Camera']>=1) & (df_pivot['Backroom Camera']<860),
                (df_pivot['Backroom Camera']==0)]
        valuesOnBackroom = ['YES','PR','NO']
        df_pivot['Backroom Camera'] = np.select(conditionsOnBackroom, valuesOnBackroom)
        conditionsOnLobby = [
                (df_pivot['Lobby Camera']>=860), 
                (df_pivot['Lobby Camera']>=1) & (df_pivot['Lobby Camera']<860),
                (df_pivot['Lobby Camera']==0)]
        valuesOnLobby = ['YES','PR','NO']
        df_pivot['Lobby Camera'] = np.select(conditionsOnLobby, valuesOnLobby)
        conditionsOnOutdoor = [
                (df_pivot['Outdoor Camera']>=860), 
                (df_pivot['Outdoor Camera']>=1) & (df_pivot['Outdoor Camera']<860),
                (df_pivot['Outdoor Camera']==0)]
        valuesOnOutdoor = ['YES','PR','NO']
        df_pivot['Outdoor Camera'] = np.select(conditionsOnOutdoor, valuesOnOutdoor)
        conditionsOnPinhole = [
                (df_pivot['Pinhole Camera']>=860), 
                (df_pivot['Pinhole Camera']>=1) & (df_pivot['Pinhole Camera']<860),
                (df_pivot['Pinhole Camera']==0)]
        valuesOnPinhole = ['YES','PR','NO']
        df_pivot['Pinhole Camera'] = np.select(conditionsOnPinhole, valuesOnPinhole)
        df_pivot['Total'].replace({0:'NO'},inplace=True)
        df_all=pd.merge(df_sitemas,df_pivot,on='VboxId')
        df_all['Date']=yesterday
        df_all=df_all[['Date','BankName','SiteName','McuId','VboxId','Lobby Camera','Backroom Camera','Outdoor Camera','Pinhole Camera','Total']]
        df_all.rename(columns={'Total':'Status'},inplace=True)
        df_all_offline=df_all[df_all['Status']=='NO']
        df_all_not_offline=df_all[df_all['Status']!='NO']
        df_all_not_offline['Status']=df_all_not_offline['Lobby Camera']
        df_output=pd.concat([df_all_offline,df_all_not_offline],ignore_index=True)
        df_output['Date']=pd.to_datetime(df_output['Date'])
        df_all_camera=df_output[['Date','BankName','SiteName','McuId','VboxId','Lobby Camera','Backroom Camera','Outdoor Camera','Pinhole Camera']]
        if len(df_sitemas['BankName'].unique())==1:
            BankName=df_sitemas['BankName'].iloc[0]          
            df_full_record=df_output[df_output['Status']=='YES']
            df_partial_record=df_output[df_output['Status']=='PR']
            df_zero_record=df_output[df_output['Status']=='NO']          
            df_summary=pd.DataFrame([[yesterday,BankName,len(df_output),len(df_full_record),len(df_partial_record),' ',len(df_zero_record)]],columns=['Date','BankName','TotalSites','100%','PartialRecording','TotalDataRecievedSites','ZeroRecording'])
            df_summary['TotalDataRecievedSites']=df_summary['100%']+df_summary['PartialRecording']          
        else:
            BankName1=df_sitemas['BankName'].unique()[0]
            BankName2=df_sitemas['BankName'].unique()[1]            
            df_output1=df_output[df_output['BankName']==BankName1]
            df_output2=df_output[df_output['BankName']==BankName2]           
            df_full_record1=df_output1[df_output1['Status']=='YES']
            df_partial_record1=df_output1[df_output1['Status']=='PR']
            df_zero_record1=df_output1[df_output1['Status']=='NO']            
            df_full_record2=df_output2[df_output2['Status']=='YES']
            df_partial_record2=df_output2[df_output2['Status']=='PR']
            df_zero_record2=df_output2[df_output2['Status']=='NO']            
            df_summary1=pd.DataFrame([[yesterday,BankName1,len(df_output1),len(df_full_record1),len(df_partial_record1),' ',len(df_zero_record1)]],columns=['Date','BankName','TotalSites','100%','PartialRecording','TotalDataRecievedSites','ZeroRecording'])
            df_summary1['TotalDataRecievedSites']=df_summary1['100%']+df_summary1['PartialRecording']            
            df_summary2=pd.DataFrame([[yesterday,BankName2,len(df_output2),len(df_full_record2),len(df_partial_record2),' ',len(df_zero_record2)]],columns=['Date','BankName','TotalSites','100%','PartialRecording','TotalDataRecievedSites','ZeroRecording'])
            df_summary2['TotalDataRecievedSites']=df_summary2['100%']+df_summary2['PartialRecording']           
            df_output=pd.concat([df_output1,df_output2],ignore_index=True)
            df_summary=pd.concat([df_summary1,df_summary2],ignore_index=True)           
        df_pivot_table['Date']=yesterday
        df_camera_count=pd.merge(df_sitemas,df_pivot_table,on='VboxId',how='left')
        df_camera_count=df_camera_count[['Date','BankName','SiteName','McuId','VboxId','Backroom Camera','Lobby Camera','Outdoor Camera','Pinhole Camera']]
        df_camera_count['%']=((df_camera_count['Lobby Camera']/860)*100).round(2)
        df_camera_count['%']=df_camera_count['%'].clip(0,100)
        df_camera_count=df_camera_count.sort_values('%')
        df_camera_count['Date']=pd.to_datetime(df_camera_count['Date'])       
        df_summary['Date']=pd.to_datetime(df_summary['Date'])
        return df_output,df_summary,df_camera_count,df_sitemas        
       
    def pushdata(df_output,df_summary,df_camera_count):       
        engine = create_engine('mssql+pymssql://aaum_db_usr:nfm!DBj!Pu#hZmgThxAoZe$E@188.42.97.40/AAUM_Analytics_DB')        
        df_summary.to_sql('dummy_VOD_report_summary',engine,if_exists='append',index=False, method="multi", chunksize=500)
        df_output.to_sql('dummy_VOD_report',engine,if_exists='append',index=False, method="multi", chunksize=500)
        df_camera_count.to_sql('dummy_VOD_report_count',engine,if_exists='append',index=False, method="multi", chunksize=500)
        engine.dispose()
        
    df_sitemas,df_video,df_cam = fetch(ServerAddress,UserName,Password,DatabaseName)    
    df_output,df_summary,df_camera_count,df_sitemas = transformation(df_sitemas,df_video,df_cam)
    pushdata(df_output,df_summary,df_camera_count)
    
    return df_output,df_summary,df_camera_count,df_sitemas
