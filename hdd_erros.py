ServerAddress='103.24.203.148'
UserName='aaum_support'
Password='aaum@123'
DatabaseName='InnoculateNCRCUB'

def HDDError(ServerAddress,UserName,Password,DatabaseName):
    import pymssql
    import numpy as np
    import pandas as pd
    from sqlalchemy import create_engine

    def fetch(ServerAddress,UserName,Password,DatabaseName):
        conn=pymssql.connect(host=ServerAddress,user=UserName,password=Password,database=DatabaseName)
        cur=conn.cursor()
        df1_harddiskstatus = pd.read_sql("select * from lbtharddiskstatus ",conn)
        df_sitemas = pd.read_sql("select a.s_Name,a.McuId,b.BankName,a.VBoxId from lbtSiteMas as a, lbtBankMas as b WHERE a.BankCode=b.BankCode AND a.s_Status=1 AND a.qstatus='True' AND b.qstatus='True' ",conn)
        cur.close()
        conn.close()
        
        conn2=pymssql.connect(host='188.42.97.40',user='aaum_db_usr',password='nfm!DBj!Pu#hZmgThxAoZe$E',database='AAUM_Analytics_DB')
        cur2=conn2.cursor()
        df_tb=pd.read_sql("select * from dummy_hdd where ClosedOn IS NULL",conn2)
        
        return df1_harddiskstatus,df_sitemas,df_tb,conn2,cur2
    
    def transform(df1_harddiskstatus,df_sitemas,df_tb):
        
        df1_harddiskstatus=df1_harddiskstatus.sort_values(['Attime'])
        df1_harddiskstatus.rename(columns={'Attime':'OpenedOn'},inplace=True)
        df1_harddiskstatus=df1_harddiskstatus.drop_duplicates('Vboxid',keep='last')
        df1_harddiskstatus=df1_harddiskstatus[df1_harddiskstatus['HDD']=='0']
        df1_harddiskstatus=pd.merge(df_sitemas[['BankName','s_Name','McuId','VBoxId']],df1_harddiskstatus,left_on=['VBoxId','McuId'],right_on=['Vboxid','Mcuid'])
        df1_harddiskstatus=df1_harddiskstatus[['BankName','s_Name','McuId','Vboxid','HDD','Error','OpenedOn']]
        df1_harddiskstatus.rename(columns={'s_Name':'SiteName'},inplace=True)


        df_new_tickets=df1_harddiskstatus[~(df1_harddiskstatus['Vboxid'].isin(df_tb['Vboxid']))]
        df_closed_tickets=df_tb[~(df_tb['Vboxid'].isin(df1_harddiskstatus['Vboxid']))]
        return df_new_tickets,df_closed_tickets

    def pushdata(df_new_tickets,df_closed_tickets,conn2,cur2):
        ticketid=tuple(df_closed_tickets['TicketId'].values)
        for i in range(len(ticketid)):
            cur2.execute(f"UPDATE dummy_hdd SET ClosedOn=CURRENT_TIMESTAMP WHERE TicketId={df_closed_tickets['TicketId'].iloc[i]}")
            conn2.commit()
        cur2.close()
        conn2.close()

        engine = create_engine('mssql+pymssql://aaum_db_usr:nfm!DBj!Pu#hZmgThxAoZe$E@188.42.97.40/AAUM_Analytics_DB')        
        df_new_tickets.to_sql('dummy_hdd',engine,if_exists='append',index=False, method="multi", chunksize=500)
        engine.dispose()

    df1_harddiskstatus,df_sitemas,df_tb,conn2,cur2=fetch(ServerAddress,UserName,Password,DatabaseName)
    df_new_tickets,df_closed_tickets=transform(df1_harddiskstatus,df_sitemas,df_tb)
    pushdata(df_new_tickets,df_closed_tickets,conn2,cur2)
    return df_new_tickets,df_closed_tickets

df_new_tickets,df_closed_tickets=hdd_error(ServerAddress,UserName,Password,DatabaseName)