tablename='RealTimeSlaViolatedTicket'
subfilter=['All','SOSAck','SOSClose','CriticalAck','CriticalClose']


def events_records(tablename,subfilter,Bank=['All'],Duration=2):
    import pymssql
    import pandas as pd
    import json
    conn = pymssql.connect(server='188.42.97.40', user='aaum_db_usr', password='nfm!DBj!Pu#hZmgThxAoZe$E', database='AAUM_Analytics_DB')  
    cursor = conn.cursor()
    if Bank==['ALL']:
        if subfilter=='All':
            records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} ".format(tablename),conn)
        elif subfilter=='SOSAck':
            records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE EventType='SOS' AND TicketsRequiredAction='TicketToAck' ".format(tablename),conn)
        elif subfilter=='SOSClose':
            records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE EventType='SOS' AND TicketsRequiredAction='TicketToClose' ".format(tablename),conn)
        elif subfilter=='CritcialAck':
            records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE EventType='Critical' AND TicketsRequiredAction='TicketToAck' ".format(tablename),conn)
        elif subfilter=='CritcialClose':
            records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE EventType='Critical' AND TicketsRequiredAction='TicketToClose' ".format(tablename),conn)
        
    else:
        if len(Bank)==1:
            if subfilter=='All':
                records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE BankName='{}' ".format(tablename,Bank[0]),conn)
            elif subfilter=='SOSAck':
                records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE BankName='{}' AND EventType='SOS' AND TicketsRequiredAction='TicketToAck' ".format(tablename,Bank[0]),conn)
            elif subfilter=='SOSClose':
                records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE BankName='{}' AND EventType='SOS' AND TicketsRequiredAction='TicketToClose' ".format(tablename,Bank[0]),conn)
            elif subfilter=='CritcialAck':
                records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE BankName='{}' AND EventType='Critical' AND TicketsRequiredAction='TicketToAck' ".format(tablename,Bank[0]),conn)
            elif subfilter=='CritcialClose':
                records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE BankName='{}' AND EventType='Critical' AND TicketsRequiredAction='TicketToClose' ".format(tablename,Bank[0]),conn)
        
        else:
            Bank=tuple(Bank)
            if subfilter=='All':
                records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} ".format(tablename,Bank[0]),conn)
            elif subfilter=='SOSAck':
                records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE BankName IS {} AND EventType='SOS' AND TicketsRequiredAction='TicketToAck' ".format(tablename,Bank),conn)
            elif subfilter=='SOSClose':
                records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE BankName IS {} AND EventType='SOS' AND TicketsRequiredAction='TicketToClose' ".format(tablename,Bank),conn)
            elif subfilter=='CritcialAck':
                records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE BankName IS {} AND EventType='Critical' AND TicketsRequiredAction='TicketToAck' ".format(tablename,Bank),conn)
            elif subfilter=='CritcialClose':
                records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE BankName IS {} AND EventType='Critical' AND TicketsRequiredAction='TicketToClose' ".format(tablename,Bank),conn)
    conn.close()
    records['OpenedOn'] = records['OpenedOn'].astype('datetime64[s]').map(str)
    return {"records":json.loads(records.to_json(orient='records'))}

