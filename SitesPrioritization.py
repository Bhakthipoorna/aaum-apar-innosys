def SitesPrioritization(ServerAddress,UserName,Password,DatabaseName):

    import pandas as pd
    import numpy as np
    import pymssql
    import xlsxwriter
    from datetime import datetime,timedelta
    import pytz
    IST = pytz.timezone('Asia/Kolkata')
    from sqlalchemy import create_engine
    
    def fetch(ServerAddress,UserName,Password,DatabaseName):

        conn2 = pymssql.connect(server=ServerAddress, user=UserName, password=Password, database=DatabaseName)  
        cursor2 = conn2.cursor()
        df_score=pd.read_sql("select BankName,SiteName,McuId,Security,ManualClosure,Footfall,NightVisit from Site_Priority",conn2)
        return df_score,conn2,cursor2


    def transformation(df_score,conn2,cursor2):

        df_score['Score']=((np.power((df_score['ManualClosure']*df_score['Footfall']*df_score['NightVisit']),1/3))+df_score['Security']).round(2)
        df_score['Rank']=df_score['Score'].rank(pct=True).round(2)
        df_score['Rank']=df_score['Rank'].clip(0,1)
        df_score['SitePriority']=pd.cut(df_score['Rank'],bins=[-1,0.2,0.4,0.6,0.8,1],labels=["S1","S2","S3","S4","S5"])
        df_score.drop(['Score','Rank'],axis=1,inplace=True)

        clear_data="DELETE FROM Site_Priority"
        cursor2.execute(clear_data)
        conn2.commit()
        conn2.close()

        return df_score


    def pushdata(df_score):

        engine = create_engine('mssql+pymssql://aaum_db_usr:nfm!DBj!Pu#hZmgThxAoZe$E@188.42.97.40/AAUM_Analytics_DB')
        df_score.to_sql('Site_Priority',engine,if_exists='append',index=False, method="multi", chunksize=500)
        engine.dispose()
    
    df_score,conn2,cursor2 = fetch(ServerAddress,UserName,Password,DatabaseName)
    df_score = transformation(df_score,conn2,cursor2)
    pushdata(df_score)
   
    return df_score
