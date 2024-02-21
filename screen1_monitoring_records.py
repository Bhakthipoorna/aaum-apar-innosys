def bank_events_records(tablename,subfilter,Bank=['ALL'],Duration=2):
    import pymssql
    import pandas as pd
    import json
    conn = pymssql.connect(server='188.42.97.40', user='aaum_db_usr', password='nfm!DBj!Pu#hZmgThxAoZe$E', database='AAUM_Analytics_DB')  
    cursor = conn.cursor()
    if tablename in ['pir_events','do2do_events']:
        if Bank==['ALL']:
            if subfilter=='All':
                records=pd.read_sql("SELECT TicketId,BankName,SiteName,OpenedOn,MainDoorOpen FROM {} WHERE   (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(tablename,Duration/24),conn)
            elif subfilter=='>12':
                records=pd.read_sql("SELECT TicketId,BankName,SiteName,OpenedOn,MainDoorOpen FROM {} WHERE MainDoorOpen>=12 and   (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(tablename,Duration/24),conn)
            elif subfilter=='>8':
                records=pd.read_sql("SELECT TicketId,BankName,SiteName,OpenedOn,MainDoorOpen FROM {} WHERE (MainDoorOpen>=8 and MainDoorOpen<12) and   (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(tablename,Duration/24),conn)
            elif subfilter=='>4':
                records=pd.read_sql("SELECT TicketId,BankName,SiteName,OpenedOn,MainDoorOpen FROM {} WHERE (MainDoorOpen>=4 and MainDoorOpen<8) and   (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(tablename,Duration/24),conn)
            elif subfilter=='DoorOff':
                records=pd.read_sql("SELECT TicketId,BankName,SiteName,OpenedOn,MainDoorOpen FROM {} WHERE (MainDoorOpen=0 and [Main Door Sensor]=0) and   (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(tablename,Duration/24),conn)
            elif subfilter=='DoorOpened':
                records=pd.read_sql("SELECT TicketId,BankName,SiteName,OpenedOn,MainDoorOpen FROM {} WHERE (MainDoorOpen=0 and MainDoorStatus='Opened' and [Main Door Sensor]!=0) and   (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(tablename,Duration/24),conn)
        else:
            if len(Bank)==1:
                if subfilter=='All':
                    records=pd.read_sql("SELECT TicketId,BankName,SiteName,OpenedOn,MainDoorOpen FROM {} WHERE BankName='{}' AND   (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(tablename,Bank[0],Duration/24),conn)
                elif subfilter=='>12':
                    records=pd.read_sql("SELECT TicketId,BankName,SiteName,OpenedOn,MainDoorOpen FROM {} WHERE BankName='{}' AND MainDoorOpen>=12 and   (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(tablename,Bank[0],Duration/24),conn)
                elif subfilter=='>8':
                    records=pd.read_sql("SELECT TicketId,BankName,SiteName,OpenedOn,MainDoorOpen FROM {} WHERE BankName='{}' AND (MainDoorOpen>=8 and MainDoorOpen<12) and   (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(tablename,Bank[0],Duration/24),conn)
                elif subfilter=='>4':
                    records=pd.read_sql("SELECT TicketId,BankName,SiteName,OpenedOn,MainDoorOpen FROM {} WHERE BankName='{}' AND (MainDoorOpen>=4 and MainDoorOpen<8) and   (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(tablename,Bank[0],Duration/24),conn)
                elif subfilter=='DoorOff':
                    records=pd.read_sql("SELECT TicketId,BankName,SiteName,OpenedOn,MainDoorOpen FROM {} WHERE BankName='{}' AND (MainDoorOpen=0 and [Main Door Sensor]=0) and   (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(tablename,Bank[0],Duration/24),conn)
                elif subfilter=='DoorOpened':
                    records=pd.read_sql("SELECT TicketId,BankName,SiteName,OpenedOn,MainDoorOpen FROM {} WHERE BankName='{}' AND (MainDoorOpen=0 and MainDoorStatus='Opened' and [Main Door Sensor]!=0) and   (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(tablename,Bank[0],Duration/24),conn)   
            else:
                Bank=tuple(Bank)
                if subfilter=='All':
                    records=pd.read_sql("SELECT TicketId,BankName,SiteName,OpenedOn,MainDoorOpen FROM {} WHERE BankName in {} AND   (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(tablename,Bank,Duration/24),conn)
                elif subfilter=='>12':
                    records=pd.read_sql("SELECT TicketId,BankName,SiteName,OpenedOn,MainDoorOpen FROM {} WHERE BankName in {} AND MainDoorOpen>=12 and   (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(tablename,Bank,Duration/24),conn)
                elif subfilter=='>8':
                    records=pd.read_sql("SELECT TicketId,BankName,SiteName,OpenedOn,MainDoorOpen FROM {} WHERE BankName in {} AND (MainDoorOpen>=8 and MainDoorOpen<12) and   (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(tablename,Bank,Duration/24),conn)
                elif subfilter=='>4':
                    records=pd.read_sql("SELECT TicketId,BankName,SiteName,OpenedOn,MainDoorOpen FROM {} WHERE BankName in {} AND (MainDoorOpen>=4 and MainDoorOpen<8) and   (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(tablename,Bank,Duration/24),conn)
                elif subfilter=='DoorOff':
                    records=pd.read_sql("SELECT TicketId,BankName,SiteName,OpenedOn,MainDoorOpen FROM {} WHERE BankName in {} AND (MainDoorOpen=0 and [Main Door Sensor]=0) and   (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(tablename,Bank,Duration/24),conn)
                elif subfilter=='DoorOpened':
                    records=pd.read_sql("SELECT TicketId,BankName,SiteName,OpenedOn,MainDoorOpen FROM {} WHERE BankName in {} AND (MainDoorOpen=0 and MainDoorStatus='Opened' and [Main Door Sensor]!=0) and   (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(tablename,Bank,Duration/24),conn)   
        conn.close()
        records['OpenedOn'] = records['OpenedOn'].map(str)
        records = pd.merge(records,records.groupby(['SiteName']).size().reset_index(name='SiteTicketCount'),on='SiteName').sort_values(['SiteTicketCount','SiteName','OpenedOn','MainDoorOpen'],ascending=False)
        records['SiteURL'] = "Site Link"

    elif tablename=='RealTimeSlaViolatedTicket':
        if Bank==['ALL']:
            if subfilter=='All':
                records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} ".format(tablename),conn)
            elif subfilter=='SOSAck':
                records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE EventType='SOS' AND TicketsRequiredAction='TicketToAck' ".format(tablename),conn)
            elif subfilter=='SOSClose':
                records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE EventType='SOS' AND TicketsRequiredAction='TicketToClose' ".format(tablename),conn)
            elif subfilter=='CriticalAck':
                records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE EventType='Critical' AND TicketsRequiredAction='TicketToAck' ".format(tablename),conn)
            elif subfilter=='CriticalClose':
                records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE EventType='Critical' AND TicketsRequiredAction='TicketToClose' ".format(tablename),conn)
            
        else:
            if len(Bank)==1:
                if subfilter=='All':
                    records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE BankName='{}' ".format(tablename,Bank[0]),conn)
                elif subfilter=='SOSAck':
                    records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE BankName='{}' AND EventType='SOS' AND TicketsRequiredAction='TicketToAck' ".format(tablename,Bank[0]),conn)
                elif subfilter=='SOSClose':
                    records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE BankName='{}' AND EventType='SOS' AND TicketsRequiredAction='TicketToClose' ".format(tablename,Bank[0]),conn)
                elif subfilter=='CriticalAck':
                    records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE BankName='{}' AND EventType='Critical' AND TicketsRequiredAction='TicketToAck' ".format(tablename,Bank[0]),conn)
                elif subfilter=='CriticalClose':
                    records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE BankName='{}' AND EventType='Critical' AND TicketsRequiredAction='TicketToClose' ".format(tablename,Bank[0]),conn)
            
            else:
                Bank=tuple(Bank)
                if subfilter=='All':
                    records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE BankName in {}".format(tablename,Bank),conn)
                elif subfilter=='SOSAck':
                    records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE BankName in {} AND EventType='SOS' AND TicketsRequiredAction='TicketToAck' ".format(tablename,Bank),conn)
                elif subfilter=='SOSClose':
                    records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE BankName in {} AND EventType='SOS' AND TicketsRequiredAction='TicketToClose' ".format(tablename,Bank),conn)
                elif subfilter=='CriticalAck':
                    records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE BankName in {} AND EventType='Critical' AND TicketsRequiredAction='TicketToAck' ".format(tablename,Bank),conn)
                elif subfilter=='CriticalClose':
                    records=pd.read_sql("SELECT BankName,SiteName,EventId,EventType,Msg,OpenedOn,AgeInMinutes,TicketsRequiredAction FROM {} WHERE BankName in {} AND EventType='Critical' AND TicketsRequiredAction='TicketToClose' ".format(tablename,Bank),conn)
        conn.close()
        records['OpenedOn'] = records['OpenedOn'].astype('datetime64[s]').map(str)
        records = pd.merge(records,records.groupby(['SiteName']).size().reset_index(name='SiteTicketCount'),on='SiteName').sort_values(['SiteTicketCount','SiteName','OpenedOn'],ascending=False)

    elif tablename=='SlaViolationBankwise':
        import pytz
        from datetime import datetime
        from dateutil.relativedelta import relativedelta
        today = pd.to_datetime((datetime.now(pytz.timezone('Asia/Kolkata'))))
        AtTime = (datetime.now(pytz.timezone('Asia/Kolkata'))).strftime("%Y-%m-%d")
        yesterday = pd.to_datetime(today+relativedelta(days=-1)).strftime("%Y-%m-%d")
        last7days =pd.to_datetime(today+relativedelta(days=-7)).strftime("%Y-%m-%d")
        last30days = pd.to_datetime(today+relativedelta(days=-30)).strftime("%Y-%m-%d")
        attime = datetime.now(pytz.timezone('Asia/Kolkata'))
        year=attime.year
        month=attime.month
        year_month=f"{year}-{month}-01"
        if Bank==['ALL']:
            if subfilter=='All':
                records=pd.read_sql("SELECT BankName,(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) AS TotalViolations, SOSAckViolation,SOSCloseViolations,CriticalAckViolation,CriticalCloseViolations FROM {}   WHERE (Date>='{}') ".format(tablename,year_month),conn)
                records=records.groupby(['BankName']).sum().reset_index()
            elif subfilter=='Yesterday':
                records=pd.read_sql("SELECT BankName,(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) AS TotalViolations, SOSAckViolation,SOSCloseViolations,CriticalAckViolation,CriticalCloseViolations FROM {}  WHERE Date='{}' ".format(tablename,yesterday),conn)
                records=records.groupby(['BankName']).sum().reset_index()
            elif subfilter=='Last7Days':
                records=pd.read_sql("SELECT BankName,(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) AS TotalViolations, SOSAckViolation,SOSCloseViolations,CriticalAckViolation,CriticalCloseViolations FROM {}  WHERE Date>='{}' AND Date<'{}' ".format(tablename,last7days,AtTime),conn)
                records=records.groupby(['BankName']).sum().reset_index()
            elif subfilter=='Last30Days':
                records=pd.read_sql("SELECT BankName,(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) AS TotalViolations, SOSAckViolation,SOSCloseViolations,CriticalAckViolation,CriticalCloseViolations FROM {}  WHERE Date>='{}' AND Date<'{}' ".format(tablename,last30days,AtTime),conn)
                records=records.groupby(['BankName']).sum().reset_index()
        else:
            if len(Bank)==1:
                if subfilter=='All':
                    records=pd.read_sql("SELECT '{}' AS BankName, SUM(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) AS TotalViolations, SUM(SOSAckViolation) AS SOSAckViolation,SUM(SOSCloseViolations) AS SOSCloseViolation,SUM(CriticalAckViolation) AS CriticalAckViolation ,SUM(CriticalCloseViolations) AS CriticalCloseViolation FROM {} WHERE BankName='{}' AND (Date>='{}') ".format(Bank[0],tablename,Bank[0],year_month),conn)
                elif subfilter=='Yesterday':
                    records=pd.read_sql("SELECT '{}' AS BankName, SUM(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) AS TotalViolations, SUM(SOSAckViolation) AS SOSAckViolation ,SUM(SOSCloseViolations) AS SOSCloseViolation,SUM(CriticalAckViolation) AS CriticalAckViolation,SUM(CriticalCloseViolations) AS CriticalCloseViolation FROM {} WHERE BankName='{}' AND Date>='{}' ".format(Bank[0],tablename,Bank[0],yesterday),conn)
                elif subfilter=='Last7Days':
                    records=pd.read_sql("SELECT '{}' AS BankName, SUM(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) AS TotalViolations, SUM(SOSAckViolation) AS SOSAckViolation,SUM(SOSCloseViolations) AS SOSCloseViolation,SUM(CriticalAckViolation) AS CriticalAckViolation,SUM(CriticalCloseViolations) AS CriticalCloseViolation FROM {} WHERE BankName='{}' AND Date>='{}' AND Date<'{}' ".format(Bank[0],tablename,Bank[0],last7days,AtTime),conn)
                elif subfilter=='Last30Days':
                    records=pd.read_sql("SELECT '{}' AS BankName, SUM(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) AS TotalViolations, SUM(SOSAckViolation) AS SOSAckViolation,SUM(SOSCloseViolations) AS SOSCloseViolation,SUM(CriticalAckViolation) AS CriticalAckViolation,SUM(CriticalCloseViolations) AS CriticalCloseViolation FROM {} WHERE BankName='{}' AND Date>='{}' AND Date<'{}' ".format(Bank[0],tablename,Bank[0],last30days,AtTime),conn)
            else:
                Bank=tuple(Bank)
                if subfilter=='All':
                    records=pd.read_sql("SELECT Date,BankName,(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) AS TotalViolations, SOSAckViolation, SOSCloseViolations, CriticalAckViolation, CriticalCloseViolations FROM {} WHERE BankName IN {} AND (Date>='{}')".format(tablename,Bank,year_month),conn)
                    records=records.groupby(['BankName']).sum().reset_index()
                elif subfilter=='Yesterday':
                    records=pd.read_sql("SELECT Date,BankName,(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) AS TotalViolations, SOSAckViolation ,SOSCloseViolations, CriticalAckViolation, CriticalCloseViolations FROM {} WHERE BankName IN {} AND Date='{}' ".format(tablename,Bank,yesterday),conn)
                    records=records.groupby(['BankName']).sum().reset_index()
                elif subfilter=='Last7Days':
                    records=pd.read_sql("SELECT Date,BankName,(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) AS TotalViolations, SOSAckViolation, SOSCloseViolations ,CriticalAckViolation, CriticalCloseViolations FROM {}  WHERE BankName IN {} AND Date>='{}' AND Date<'{}' ".format(tablename,Bank,last7days,AtTime),conn)
                    records=records.groupby(['BankName']).sum().reset_index()
                elif subfilter=='Last30Days':
                    records=pd.read_sql("SELECT Date,BankName,(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) AS TotalViolations, SOSAckViolation, SOSCloseViolations, CriticalAckViolation, CriticalCloseViolations FROM {} WHERE BankName IN {} AND Date>='{}' AND Date<'{}' ".format(tablename,Bank,last30days,AtTime),conn)
                    records=records.groupby(['BankName']).sum().reset_index()
        conn.close()
    return {"records":json.loads(records.to_json(orient='records'))}