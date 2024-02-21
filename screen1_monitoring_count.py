def bank_events_count(Bank=["ALL"],Duration=2):
    import pymssql
    import pandas as pd
    from dateutil.relativedelta import relativedelta
    from datetime import datetime
    import pytz
    attime = datetime.now(pytz.timezone('Asia/Kolkata'))
    yesterday = (attime+relativedelta(days=-1)).strftime("%Y-%m-%d")
    last_7days = (attime+relativedelta(days=-7)).strftime("%Y-%m-%d")
    last_30days = (attime+relativedelta(days=-30)).strftime("%Y-%m-%d")
    year=attime.year
    month=attime.month
    year_month=f"{year}-{month}-01"

    conn = pymssql.connect(server='188.42.97.40', user='aaum_db_usr', password='nfm!DBj!Pu#hZmgThxAoZe$E', database='AAUM_Analytics_DB')  
    cursor = conn.cursor()
    if Bank==["ALL"]:
        pir_all=pd.read_sql("SELECT count(*) FROM pir_events WHERE    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL) ".format(Duration/24),conn).iloc[0][0]
        pir_12=pd.read_sql("SELECT count(*) FROM pir_events WHERE MainDoorOpen>=12 and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Duration/24),conn).iloc[0][0]
        pir_8=pd.read_sql("SELECT count(*) FROM pir_events WHERE (MainDoorOpen>=8 and MainDoorOpen<12) and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Duration/24),conn).iloc[0][0]
        pir_4=pd.read_sql("SELECT count(*) FROM pir_events WHERE (MainDoorOpen>=4 and MainDoorOpen<8) and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Duration/24),conn).iloc[0][0]
        pir_dooroff=pd.read_sql("SELECT count(*) FROM pir_events WHERE (MainDoorOpen=0 and [Main Door Sensor]=0) and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Duration/24),conn).iloc[0][0]
        pir_dooropened=pd.read_sql("SELECT count(*) FROM pir_events WHERE (MainDoorOpen=0 and MainDoorStatus='Opened' and [Main Door Sensor]!=0) and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Duration/24),conn).iloc[0][0]
        door_all=pd.read_sql("SELECT count(*) FROM do2do_events WHERE    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Duration/24),conn).iloc[0][0]
        door_12=pd.read_sql("SELECT count(*) FROM do2do_events WHERE MainDoorOpen>=12 and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Duration/24),conn).iloc[0][0]
        door_8=pd.read_sql("SELECT count(*) FROM do2do_events WHERE (MainDoorOpen>=8 and MainDoorOpen<12) and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Duration/24),conn).iloc[0][0]
        door_4=pd.read_sql("SELECT count(*) FROM do2do_events WHERE (MainDoorOpen>=4 and MainDoorOpen<8) and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Duration/24),conn).iloc[0][0]
        total_tickets=pd.read_sql("SELECT count(*) FROM RealTimeSlaViolatedTicket ",conn).iloc[0][0]
        sos_ack=pd.read_sql("SELECT count(*) FROM RealTimeSlaViolatedTicket WHERE EventType='SOS' AND TicketsRequiredAction='TicketToAck' ",conn).iloc[0][0]
        sos_close=pd.read_sql("SELECT count(*) FROM RealTimeSlaViolatedTicket WHERE EventType='SOS' AND TicketsRequiredAction='TicketToClose' ",conn).iloc[0][0]
        critical_ack=pd.read_sql("SELECT count(*) FROM RealTimeSlaViolatedTicket WHERE EventType='Critical' AND TicketsRequiredAction='TicketToAck' ",conn).iloc[0][0]
        critical_close=pd.read_sql("SELECT count(*) FROM RealTimeSlaViolatedTicket WHERE EventType='Critical' AND TicketsRequiredAction='TicketToClose' ",conn).iloc[0][0]
        violated_total=pd.read_sql("SELECT SUM(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) FROM SlaViolationBankwise  WHERE (Date>='{}') ".format(year_month),conn).iloc[0][0]
        violated_yesterday=pd.read_sql("SELECT SUM(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) FROM SlaViolationBankwise WHERE (Date>='{}') ".format(yesterday),conn).iloc[0][0]
        violated_in_last7_days=pd.read_sql("SELECT SUM(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) FROM SlaViolationBankwise WHERE (Date>='{}') ".format(last_7days),conn).iloc[0][0]
        violated_in_last30_days=pd.read_sql("SELECT SUM(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) FROM SlaViolationBankwise WHERE (Date>='{}') ".format(last_30days),conn).iloc[0][0]
    else:
        if len(Bank)==1:
            pir_all=pd.read_sql("SELECT count(*) FROM pir_events WHERE BankName='{}' AND    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Bank[0],Duration/24),conn).iloc[0][0]
            pir_12=pd.read_sql("SELECT count(*) FROM pir_events WHERE BankName='{}' AND MainDoorOpen>=12 and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Bank[0],Duration/24),conn).iloc[0][0]
            pir_8=pd.read_sql("SELECT count(*) FROM pir_events WHERE BankName='{}' AND (MainDoorOpen>=8 and MainDoorOpen<12) and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Bank[0],Duration/24),conn).iloc[0][0]
            pir_4=pd.read_sql("SELECT count(*) FROM pir_events WHERE BankName='{}' AND (MainDoorOpen>=4 and MainDoorOpen<8) and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Bank[0],Duration/24),conn).iloc[0][0]
            pir_dooroff=pd.read_sql("SELECT count(*) FROM pir_events WHERE BankName='{}' AND (MainDoorOpen=0 and [Main Door Sensor]=0) and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Bank[0],Duration/24),conn).iloc[0][0]
            pir_dooropened=pd.read_sql("SELECT count(*) FROM pir_events WHERE BankName='{}' AND (MainDoorOpen=0 and MainDoorStatus='Opened' and [Main Door Sensor]!=0) and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Bank[0],Duration/24),conn).iloc[0][0]
            door_all=pd.read_sql("SELECT count(*) FROM do2do_events WHERE BankName='{}' AND    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Bank[0],Duration/24),conn).iloc[0][0]
            door_12=pd.read_sql("SELECT count(*) FROM do2do_events WHERE BankName='{}' AND MainDoorOpen>=12 and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Bank[0],Duration/24),conn).iloc[0][0]
            door_8=pd.read_sql("SELECT count(*) FROM do2do_events WHERE BankName='{}' AND (MainDoorOpen>=8 and MainDoorOpen<12) and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Bank[0],Duration/24),conn).iloc[0][0]
            door_4=pd.read_sql("SELECT count(*) FROM do2do_events WHERE BankName='{}' AND (MainDoorOpen>=4 and MainDoorOpen<8) and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Bank[0],Duration/24),conn).iloc[0][0]
            total_tickets=pd.read_sql("SELECT count(*) FROM RealTimeSlaViolatedTicket WHERE BankName='{}'".format(Bank[0]),conn).iloc[0][0]
            sos_ack=pd.read_sql("SELECT count(*) FROM RealTimeSlaViolatedTicket WHERE BankName='{}' AND EventTyp='SOS' AND TicketsRequiredAction='TicketToAck'".format(Bank[0]),conn).iloc[0][0]
            sos_close=pd.read_sql("SELECT count(*) FROM RealTimeSlaViolatedTicket WHERE BankName='{}' AND EventType='SOS' AND TicketsRequiredAction='TicketToClose' ".format(Bank[0]),conn).iloc[0][0]
            critical_ack=pd.read_sql("SELECT count(*) FROM RealTimeSlaViolatedTicket WHERE BankName='{}' AND EventType='Critical' AND TicketsRequiredAction='TicketToAck' ".format(Bank[0]),conn).iloc[0][0]
            critical_close=pd.read_sql("SELECT count(*) FROM RealTimeSlaViolatedTicket WHERE BankName='{}' AND EventType='Critical' AND TicketsRequiredAction='TicketToClose' ".format(Bank[0]),conn).iloc[0][0]
            violated_total=pd.read_sql("SELECT SUM(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) FROM SlaViolationBankwise WHERE BankName='{}' (Date>='{}') ".format(Bank[0],year_month),conn).iloc[0][0]
            violated_yesterday=pd.read_sql("SELECT SUM(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) FROM SlaViolationBankwise WHERE BankName='{}' AND (Date>='{}') ".format(Bank[0],yesterday),conn).iloc[0][0]
            violated_in_last7_days=pd.read_sql("SELECT SUM(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) FROM SlaViolationBankwise WHERE BankName='{}' AND (Date>='{}') ".format(Bank[0],last_7days),conn).iloc[0][0]
            violated_in_last30_days=pd.read_sql("SELECT SUM(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) FROM SlaViolationBankwise WHERE BankName='{}' AND (Date>='{}') ".format(Bank[0],last_30days),conn).iloc[0][0]
        else:
            Bank=tuple(Bank)
            pir_all=pd.read_sql("SELECT count(*) FROM pir_events WHERE BankName in {} AND    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Bank,Duration/24),conn).iloc[0][0]
            pir_12=pd.read_sql("SELECT count(*) FROM pir_events WHERE BankName in {} AND MainDoorOpen>=12 and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Bank,Duration/24),conn).iloc[0][0]
            pir_8=pd.read_sql("SELECT count(*) FROM pir_events WHERE BankName in {} AND (MainDoorOpen>=8 and MainDoorOpen<12) and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Bank,Duration/24),conn).iloc[0][0]
            pir_4=pd.read_sql("SELECT count(*) FROM pir_events WHERE BankName in {} AND (MainDoorOpen>=4 and MainDoorOpen<8) and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Bank,Duration/24),conn).iloc[0][0]
            pir_dooroff=pd.read_sql("SELECT count(*) FROM pir_events WHERE BankName in {} AND (MainDoorOpen=0 and [Main Door Sensor]=0) and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Bank,Duration/24),conn).iloc[0][0]
            pir_dooropened=pd.read_sql("SELECT count(*) FROM pir_events WHERE BankName in {} AND (MainDoorOpen=0 and MainDoorStatus='Opened' and [Main Door Sensor]!=0) and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Bank,Duration/24),conn).iloc[0][0]
            door_all=pd.read_sql("SELECT count(*) FROM do2do_events WHERE BankName in {} AND    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Bank,Duration/24),conn).iloc[0][0]
            door_12=pd.read_sql("SELECT count(*) FROM do2do_events WHERE BankName in {} AND MainDoorOpen>=12 and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Bank,Duration/24),conn).iloc[0][0]
            door_8=pd.read_sql("SELECT count(*) FROM do2do_events WHERE BankName in {} AND (MainDoorOpen>=8 and MainDoorOpen<12) and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Bank,Duration/24),conn).iloc[0][0]
            door_4=pd.read_sql("SELECT count(*) FROM do2do_events WHERE BankName in {} AND (MainDoorOpen>=4 and MainDoorOpen<8) and    (OpenedOn>=(CURRENT_TIMESTAMP-{}) and ClosedOn is NULL)".format(Bank,Duration/24),conn).iloc[0][0]
            total_tickets=pd.read_sql("SELECT count(*) FROM RealTimeSlaViolatedTicket WHERE BankName in {} ".format(Bank),conn).iloc[0][0]
            sos_ack=pd.read_sql("SELECT count(*) FROM RealTimeSlaViolatedTicket WHERE BankName in {} AND EventType='SOS' AND TicketsRequiredAction='TicketToAck' ".format(Bank),conn).iloc[0][0]
            sos_close=pd.read_sql("SELECT count(*) FROM RealTimeSlaViolatedTicket WHERE BankName in {} AND EventType='SOS' AND TicketsRequiredAction='TicketToClose' ".format(Bank),conn).iloc[0][0]
            critical_ack=pd.read_sql("SELECT count(*) FROM RealTimeSlaViolatedTicket WHERE BankName in {} AND EventType='Critical' AND TicketsRequiredAction='TicketToAck' ".format(Bank),conn).iloc[0][0]
            critical_close=pd.read_sql("SELECT count(*) FROM RealTimeSlaViolatedTicket WHERE BankName in {} AND EventType='Critical' AND TicketsRequiredAction='TicketToClose' ".format(Bank),conn).iloc[0][0]
            violated_total=pd.read_sql("SELECT SUM(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) FROM SlaViolationBankwise WHERE BankName in {} (Date>='{}') ".format(Bank,year_month),conn).iloc[0][0]
            violated_yesterday=pd.read_sql("SELECT SUM(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) FROM SlaViolationBankwise WHERE BankName in {} AND (Date>='{}') ".format(Bank,yesterday),conn).iloc[0][0]
            violated_in_last7_days=pd.read_sql("SELECT SUM(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) FROM SlaViolationBankwise WHERE BankName in {} AND (Date>='{}') ".format(Bank,last_7days),conn).iloc[0][0]
            violated_in_last30_days=pd.read_sql("SELECT SUM(SOSAckViolation+SOSCloseViolations+CriticalAckViolation+CriticalCloseViolations) FROM SlaViolationBankwise WHERE BankName in {} AND (Date>='{}') ".format(Bank,last_30days),conn).iloc[0][0]
    conn.close()
    if not violated_total:
        violated_total =0
    if not violated_yesterday:
        violated_yesterday =0
    if not violated_in_last7_days:
        violated_in_last7_days =0
    if not violated_in_last30_days:
        violated_in_last30_days =0
    output={"box1":{"title":"Movement in Lobby > 3 min and 1 critical sensor failure","tablename":'pir_events',"subtitle":"Main Door Open Count","total":int(pir_all),"sub-total":{">12":int(pir_12),">8":int(pir_8),">4":int(pir_4),"DoorOff":int(pir_dooroff),"DoorOpened":int(pir_dooropened)}},"box2":{"title":"4 Main door ops in last 15 mins No PIR and 1 critical sensor failure","tablename":"do2do_events","subtitle":"Main Door Open Count","total":int(door_all),"sub-total":{">12":int(door_12),">8":int(door_8),">4":int(door_4)}},"box3":{"title":"Monitoring Events nearing SLA Violation","tablename":"RealTimeSlaViolatedTicket","subtitle":"SLA Category","total":int(total_tickets),"sub-total":{"SOSAck":int(sos_ack),"SOSClose":int(sos_close),"CriticalAck":int(critical_ack),"CriticalClose":int(critical_close)}},"box4":{"title":"SLA Violated on Monitoring","tablename":"SlaViolationBankwise","subtitle":"SLA Violations","total":int(violated_total),"sub-total":{"Yesterday":int(violated_yesterday),"Last7Days":int(violated_in_last7_days),"Last30Days":int(violated_in_last30_days)}}}
    return output
