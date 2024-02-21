def FetchHDDCameraError(ServerAddress,UserName,Password,DatabaseName):
    import pymssql
    import pandas as pd
    def fetch(ServerAddress,UserName,Password,DatabaseName):
        conn=pymssql.connect(host=ServerAddress,user=UserName,password=Password,database=DatabaseName)
        cur=conn.cursor()
        df_sitemas = pd.read_sql("SELECT a.s_Name,a.McuId,b.BankName,a.VBoxId,a.s_Addr_State,a.s_Addr_District,a.s_Addr_Street FROM lbtSiteMas as a, lbtBankMas as b WHERE a.BankCode=b.BankCode AND a.qstatus='True' AND b.qstatus='True' ",conn)
        df_vboxattendance=pd.read_sql("SELECT Vboxid,DeviceId,EventCode,AtTime FROM lbtVboxAttendance WHERE CAST(Attime AS DATE) >= CAST(GETDATE() AS DATE)",conn)
        df_cam = pd.read_sql("SELECT Vboxid,CameraName,CameraId FROM tblVideoMas WHERE qstatus='True' " ,conn)
        df_harddiskstatus = pd.read_sql("SELECT Mcuid,Vboxid,HDD,Error,Attime FROM lbtharddiskstatus WHERE CAST(Attime AS DATE) >= CAST(GETDATE() AS DATE)",conn)
        cur.close()
        conn.close()
        return df_sitemas,df_vboxattendance,df_cam,df_harddiskstatus
    df_sitemas,df_vboxattendance,df_cam,df_harddiskstatus = fetch(ServerAddress,UserName,Password,DatabaseName)
    