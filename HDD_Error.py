def HDDError(df_sitemas,df_harddiskstatus,df_vboxattendance):
    import pymssql
    import numpy as np
    import pandas as pd
    from sqlalchemy import create_engine

    def transform(df_sitemas,df_harddiskstatus,df_vboxattendance):
        BankName=tuple(df_sitemas['BankName'].unique())
        conn2=pymssql.connect(host='188.42.97.40',user='aaum_db_usr',password='nfm!DBj!Pu#hZmgThxAoZe$E',database='AAUM_Analytics_DB')
        cur2=conn2.cursor()
        if len(BankName)>1:
            df_tb=pd.read_sql("select * from HDD_Issues where BankName in {} and ClosedOn IS NULL".format(BankName),conn2)
        else:
            BankName=BankName[0]
            df_tb=pd.read_sql("select * from HDD_Issues where BankName='{}' and ClosedOn IS NULL".format(BankName),conn2)

        df_harddiskstatus=df_harddiskstatus.sort_values(['Attime'])
        df_harddiskstatus.rename(columns={'Attime':'OpenedOn'},inplace=True)
        df_harddiskstatus=df_harddiskstatus.drop_duplicates('Vboxid',keep='last')
        df_harddiskstatus_no_error=df_harddiskstatus[df_harddiskstatus['HDD']=='1']
        df_harddiskstatus=df_harddiskstatus[df_harddiskstatus['HDD']=='0']
        df_harddiskstatus=pd.merge(df_sitemas[['BankName','s_Name','McuId','VBoxId']],df_harddiskstatus,left_on=['VBoxId','McuId'],right_on=['Vboxid','Mcuid'])
        df_harddiskstatus=df_harddiskstatus[['BankName','s_Name','McuId','Vboxid','HDD','Error','OpenedOn']]
        df_harddiskstatus.rename(columns={'s_Name':'SiteName','Vboxid':'VboxId'},inplace=True)
        df_tb.rename(columns={'Vboxid':'VboxId'},inplace=True)
        df_harddiskstatus_no_error.rename(columns={'Vboxid':'VboxId'},inplace=True)

        df_new_tickets=df_harddiskstatus[~df_harddiskstatus.set_index(['VboxId','Error']).index.isin(df_tb.set_index(['VboxId','Error']).index)]
        df_closed_tickets=df_tb[df_tb.set_index(['VboxId','Error']).index.isin(df_harddiskstatus_no_error.set_index(['VboxId','Error']).index)]

        df_tb2=df_tb[~df_tb['VboxId'].isin(df_harddiskstatus_no_error['VboxId'])]

        df_vboxattendance=pd.merge(df_vboxattendance,df_sitemas['VBoxId'],left_on='Vboxid',right_on='VBoxId')
        df_vboxattendance2=df_vboxattendance[df_vboxattendance['EventCode']=='REMOTERECORD']
        df_vboxattendance2=df_vboxattendance2.sort_values(['Vboxid','AtTime'])
        df_vboxattendance2.drop_duplicates('Vboxid',keep='last',inplace=True)
        df_vboxattendance2.rename(columns={'Vboxid':'VboxId','AtTime':'LatestRemoteRecording'},inplace=True)
        df_tb3=pd.merge(df_tb2,df_vboxattendance2[['VboxId','LatestRemoteRecording']],on='VboxId')
        df_tb3=df_tb3[~df_tb3['VboxId'].isin(df_closed_tickets['VboxId'])]
        df_new_tickets=pd.merge(df_new_tickets,df_vboxattendance2[['VboxId','LatestRemoteRecording']],on='VboxId',how='left')

        df_tb3.drop('LatestRemoteRecording_y',axis=1,inplace=True)
        df_tb3.rename(columns={'LatestRemoteRecording_x':'LatestRemoteRecording'},inplace=True)
        df_tb3['LatestRemoteRecording']=df_tb3['LatestRemoteRecording'].astype('str')

        return df_new_tickets,df_closed_tickets,df_tb3,conn2,cur2

    def pushdata(df_new_tickets,df_closed_tickets,df_tb3,conn2,cur2):
                
        ticketid=tuple(df_closed_tickets['TicketId'].values)

        if len(df_closed_tickets)==1:
            cur2.execute(f"UPDATE HDD_Issues SET ClosedOn=CURRENT_TIMESTAMP WHERE TicketId={df_closed_tickets['TicketId'].iloc[i]}")
            conn2.commit()
        elif len(df_closed_tickets)>1:
            for i in range(len(df_closed_tickets)):
                cur2.execute(f"UPDATE HDD_Issues SET ClosedOn=CURRENT_TIMESTAMP WHERE TicketId={df_closed_tickets['TicketId'].iloc[i]}")
                conn2.commit()
        else:
            pass
                    

        if len(df_tb3)==1:
            cur2.execute(f"UPDATE HDD_Issues SET LatestRemoteRecording=CAST('{df_tb3['LatestRemoteRecording'].iloc[i]}' AS DATETIME) WHERE TicketId={df_tb3['TicketId'].iloc[i]}")
            conn2.commit()
        elif len(df_closed_tickets)>1:
            for i in range(len(df_tb3)):
                cur2.execute(f"UPDATE HDD_Issues SET LatestRemoteRecording=CAST('{df_tb3['LatestRemoteRecording'].iloc[i]}' AS DATETIME) WHERE TicketId={df_tb3['TicketId'].iloc[i]}")
                conn2.commit()
        else:
            pass
            
        cur2.close()
        conn2.close()
                
        engine = create_engine('mssql+pymssql://aaum_db_usr:nfm!DBj!Pu#hZmgThxAoZe$E@188.42.97.40/AAUM_Analytics_DB')        
        df_new_tickets.to_sql('HDD_Issues',engine,if_exists='append',index=False, method="multi", chunksize=500)
        engine.dispose()

    df_new_tickets,df_closed_tickets,df_tb3,conn2,cur2 = transform(df_sitemas,df_harddiskstatus,df_vboxattendance)
    pushdata(df_new_tickets,df_closed_tickets,df_tb3,conn2,cur2)
    