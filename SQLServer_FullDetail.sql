CREATE TABLE #tempget (
	[Column 0] [varchar](500) NULL,
	[Column 1] [varchar](50) NULL,
	[Column 2] [varchar](50) NULL,
	[Column 3] [varchar](500) NULL,
	[Column 4] [varchar](500) NULL,
	[Column 5] [varchar](max) NULL,
	[Column 6] [varchar](500) NULL,
	[Column 7] [varchar](max) NULL,
	[Column 8] [varchar](500) NULL,
	[Column 9] [varchar](max) NULL,
	[Column 10] [varchar](50) NULL,
	[Column 11] [varchar](50) NULL,
	[Column 12] [varchar](max) NULL,
	[Column 13] [varchar](50) NULL,
	[Column 14] [varchar](max) NULL,
	[Column 15] [varchar](500) NULL,
	[Column 16] [varchar](50) NULL,
	[Column 17] [varchar](50) NULL,
	[Column 18] [varchar](50) NULL,
	[Column 19] [varchar](max) NULL,
	[Column 20] [varchar](50) NULL,
	[Column 21] [varchar](max) NULL,
	[Column 22] [varchar](max) NULL,
	[Column 23] [varchar](50) NULL,
	[Column 24] [varchar](50) NULL,
	[Column 25] [varchar](50) NULL,
	[Column 26] [varchar](50) NULL,
	[Column 27] [varchar](50) NULL,
	[Column 28] [varchar](50) NULL,
	[Column 29] [varchar](50) NULL,
	[Column 30] [varchar](50) NULL
) 

GO

if ((select value_in_use from sys.configurations where name = 'Ole Automation Procedures') <> 1) and ((select is_srvrolemember('sysadmin')) =1)
begin
exec sp_configure 'show advanced options',1
reconfigure with override
exec sp_configure 'Ole Automation Procedures',1
reconfigure with override
end

GO

	Declare @rdate varchar(50), @hostname varchar(50), @clusterYN varchar(5), @clustername varchar(50),
	@instancename varchar(50), @dbms varchar(50), @edition varchar(50), @Version varchar(50),
	@avamarYesNo varchar(3),@is_sa int
	set nocount on
	select @is_sa=IS_SRVROLEMEMBER('sysadmin')
	Select @rdate= RTRIM(REPLACE(CONVERT( varchar(50),getdate(),101),'/',''))
	select @instancename = CAST(SERVERPROPERTY('servername') as NVARCHAR(100))
	IF @instancename is null 
		set @instancename =  RTRIM(LTRIM(cast(SERVERPROPERTY('ComputerNamePhysicalNetBIOS') as varchar(200))))
	select @hostname = RTRIM(LTRIM(cast(SERVERPROPERTY('ComputerNamePhysicalNetBIOS') as varchar(200))))
	If @hostname is null 
	set @hostname = @instancename

	set @dbms = 'MSSQL'
	DECLARE @SQLedition NVARCHAR(100), @SQLVersion NVARCHAR(100), @SrvVersion NVARCHAR(10)
	set @SQLedition = CAST(SERVERPROPERTY('edition') as NVARCHAR(100))
	set @edition = @SQLedition 
	set @SQLVersion = CAST(SERVERPROPERTY('Productversion') as NVARCHAR(100))
	set @Version =  CASE WHEN LEFT(@SQLversion,2)='8.' then '2000'
						 WHEN LEFT(@SQLversion,2)='9.' then '2005'
						 WHEN LEFT(@SQLversion,5)='10.50' then '2008 R2'
						 WHEN LEFT(@SQLversion,2)='10' then '2008'
						 WHEN LEFT(@SQLversion,2)='11' then '2012'
						 WHEN LEFT(@SQLversion,2)='12' then '2014'
						 WHEN LEFT(@SQLversion,2)='13' then '2016'
						 WHEN LEFT(@SQLversion,2)='14' then '2017'
						 WHEN LEFT(@SQLversion,2)='15' then '2019'
						ELSE RTRIM(@SQLVersion)
					end
set @SrvVersion=CAST(CAST(LEFT(@SQLVersion,CHARINDEX('.',@SQLVersion)-1) as INT)*10 as NVARCHAR(10))
--select @SrvVersion

select @is_sa [Is_SA], @rdate [rdate], @instancename [instancename], @hostname [hostname],@SrvVersion [SrvVersion] into ##hostinstinfo

set @clusterYN='No'
If SERVERPROPERTY('IsClustered')=1
	SET @clusterYN='Yes'
set @avamarYesNo ='No'
Declare @AVcount int, @Localcount int
set @AVcount=0
set @Localcount=0
select @AVcount= count(*)  from msdb.[dbo].[backupset] B (NOLOCK)
 inner join msdb.[dbo].[backupmediafamily] f on f.media_set_id = B.media_set_id
 where Physical_device_name not like '%:\%' and  (backup_finish_date) > getdate() - 10
 and Physical_device_name not like 'vd_%' 
 and type='D'
IF @AVcount <> 0 set @avamarYesNo ='Yes'

select @Localcount= count(*)  from msdb.[dbo].[backupset] B (NOLOCK)
 inner join msdb.[dbo].[backupmediafamily] f on f.media_set_id = B.media_set_id
 where Physical_device_name like '%:\%' and  (backup_finish_date) > getdate() - 10
 and type='D'

DECLARE       @DBEngineLogin       VARCHAR(100)
DECLARE       @AgentLogin          VARCHAR(100)
EXECUTE       master.dbo.xp_instance_regread
              @rootkey      = N'HKEY_LOCAL_MACHINE',
              @key          = N'SYSTEM\CurrentControlSet\Services\MSSQLServer',
              @value_name   = N'ObjectName',
              @value        = @DBEngineLogin OUTPUT
EXECUTE       master.dbo.xp_instance_regread
              @rootkey      = N'HKEY_LOCAL_MACHINE',
              @key          = N'SYSTEM\CurrentControlSet\Services\SQLServerAgent',
              @value_name   = N'ObjectName',
              @value        = @AgentLogin OUTPUT
declare @maxRam varchar(50), @minRam varchar(50)

DECLARE @jobhistory_max_rows INT ,
        @jobhistory_max_rows_per_job INT ,
		@jobhistory_min_time datetime
 
 
EXECUTE master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE',
                                        N'SOFTWARE\Microsoft\MSSQLServer\SQLServerAgent',
                                        N'JobHistoryMaxRows',
                                        @jobhistory_max_rows OUTPUT,
                                        N'no_output'
SELECT @jobhistory_max_rows = ISNULL(@jobhistory_max_rows, -1)
 
 
EXECUTE master.dbo.xp_instance_regread N'HKEY_LOCAL_MACHINE',
                                        N'SOFTWARE\Microsoft\MSSQLServer\SQLServerAgent',
                                        N'JobHistoryMaxRowsPerJob',
                                        @jobhistory_max_rows_per_job OUTPUT,
                                        N'no_output'
 
if @is_sa=1 
SELECT @jobhistory_min_time = min(msdb.dbo.agent_datetime(run_date, run_time)) from [msdb].[dbo].[sysjobhistory]
else
SELECT @jobhistory_min_time = min(CAST(CAST(run_date AS VARCHAR(12)) + CAST(run_time AS VARCHAR(12)) AS BIGINT)) from [msdb].[dbo].[sysjobhistory]

IF @Version<>'2000'
Begin
	select @maxRam= convert(varchar(50),value_in_use) from SYS.CONFIGURATIONS (NOLOCK)
	where name = 'max server memory (MB)'

	select @minRam = convert(varchar(50),value_in_use)  from SYS.CONFIGURATIONS (NOLOCK)
	where name = 'min server memory (MB)'

end
declare @windows varchar(15), @windwosVer char(4)
SELECT @windwosVer  = CASE when CHARINDEX('Windows NT', @@VERSION) <> 0 then
                                 RIGHT(SUBSTRING(@@VERSION, CHARINDEX('Windows NT', @@VERSION), 14), 3)
                           when CHARINDEX('10.0 <X64>',@@version) <> 0 then '10.' 
                           when CHARINDEX('6.3 <X64>',@@version) <> 0 then '6.3' 
                                 ELSE 'xxxx' end
set @windows =  CASE     WHEN @windwosVer='10.' then 'Win-2016'
                                        WHEN @windwosVer='6.3' then 'Win-2012R2'
                         WHEN @windwosVer='6.2' then 'Win-2012'
                                        WHEN @windwosVer='6.1' then 'Win-2008R2'
                                        WHEN @windwosVer='6.0' then 'Win-2008'
                                        WHEN @windwosVer='5.2' then 'Win-2003'
                                        WHEN @windwosVer='5.0' then 'Win-2000'
                                        ELSE 'Win-'+RTRIM(@windwosVer)
                                 end
SET @edition = RTRIM(@edition)+':'+RTRIM(@windows)

--declare @vmtype varchar(20)

--if @Version not in ('2000','2005','2008')
--select @vmtype=virtual_machine_type_desc from master.sys.dm_os_sys_info
--else
--select @vmtype='NA'

INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],[Column 5]
           ,[Column 6],[Column 7],[Column 8],[Column 9],[Column 10]
           ,[Column 11],[Column 12],[Column 13],[Column 14],[Column 15]
		   ,[Column 17],[Column 19],[Column 20],[Column 21]
		   ,[Column 22],[Column 23],/*Adding last used column*/[Column 27])
select  'ServerInfo_01', rdate,instancename, hostname,@edition ,
		@Version,@SQLVersion, @clusterYN,@DBEngineLogin, @AgentLogin
		,@minRam,@maxRam,@avamarYesNo,convert(varchar(50),@AVcount),convert(varchar(50),@Localcount),max_workers_count
		,DEFAULT_DOMAIN()
		,convert(varchar(200),SERVERPROPERTY('ErrorLogFileName')),@jobhistory_max_rows, @jobhistory_max_rows_per_job,@jobhistory_min_time
		,Is_SA,pwdcompare('ourN'+replace(@Version,' R2','')+'pass',(select password_hash from sys.sql_logins where name ='sa')) SAPWD_Chk
		FROM ##hostinstinfo,sys.dm_os_sys_info 

GO

update #tempget set [Column 18] = (select sqlserver_start_time from sys.dm_os_sys_info) where [Column 0]='ServerInfo_01'

GO

declare @SrvVersion NVARCHAR(100)
select @SrvVersion=SrvVersion from ##hostinstinfo
update #tempget set [Column 16] = CASE @SrvVersion when '80' then NULL when '90' then NULL else SUBSTRING(convert(varchar(50),SYSDATETIMEOFFSET()),30,5) end
where [Column 0]='ServerInfo_01'

GO

select * into #tempreginfo from sys.dm_server_registry

if (select SrvVersion from ##hostinstinfo) not in (80,90)
begin
declare @tcpip varchar(16),@tcpport varchar(10),@tcpdport varchar(10)
if exists (select * from #tempreginfo where registry_key like '%SuperSocketNetLib\Tcp\IP%' and value_name='Enabled' and value_data=1)
begin
select @tcpport=cast(dsr2.value_data as varchar(10)) from #tempreginfo dsr1 join #tempreginfo dsr2 on dsr1.registry_key=dsr2.registry_key
where dsr1.registry_key like '%SuperSocketNetLib\Tcp\%' and dsr1.value_name='Enabled' and dsr1.value_data=1 and dsr2.value_name='TcpPort'
select @tcpdport=cast(dsr2.value_data as varchar(10)) from #tempreginfo dsr1 join #tempreginfo dsr2 on dsr1.registry_key=dsr2.registry_key
where dsr1.registry_key like '%SuperSocketNetLib\Tcp\%' and dsr1.value_name='Enabled' and dsr1.value_data=1 and dsr2.value_name='TcpDynamicPorts'
select @tcpip=cast(dsr2.value_data as varchar(16)) from #tempreginfo dsr1 join #tempreginfo dsr2 on dsr1.registry_key=dsr2.registry_key
where dsr1.registry_key like '%SuperSocketNetLib\Tcp\%' and dsr1.value_name='Enabled' and dsr1.value_data=1 and dsr2.value_name='IpAddress'
--select @tcpip,@tcpport,@tcpdport
end
else begin
declare @tcp varchar(10),@tcpd varchar(10)
select @tcpport=cast(nullif(value_data,'') as varchar(10)) from #tempreginfo where registry_key like '%SuperSocketNetLib\Tcp\IPAll' and value_name='TcpPort'
select @tcpdport=cast(nullif(value_data,'') as varchar(10)) from #tempreginfo where registry_key like '%SuperSocketNetLib\Tcp\IPAll' and value_name='TcpDynamicPorts'
select @tcpip='HostIP'
--select @tcpport,@tcpdport
--select @tcpport=isnull(isnull(@tcpd,@tcp),'1433'),@tcpip='HostIP'
end

update #tempget set [Column 24] = @tcpip , [Column 25]=@tcpport, [Column 26]=@tcpdport
where [Column 0]='ServerInfo_01'
end

INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],[Column 5])
select  'ServerProtocols', rdate,instancename, hostname,t.* from ##hostinstinfo,
(select case 
when registry_key like '%SuperSocketNetLib\NP' then 'NamedPipes'
when registry_key like '%SuperSocketNetLib\SM' then 'SharedMemory'
when registry_key like '%SuperSocketNetLib\Via' then 'VIA'
when registry_key like '%SuperSocketNetLib\tcp' then 'TCP/IP'
end [Protocol],
case value_data when 0 then 'False' when 1 then 'True' end [Enabled]
from #tempreginfo where registry_key like '%SuperSocketNetLib\%' and value_name ='Enabled') t
where t.[Protocol] is not null

drop table #tempreginfo

GO

CREATE table ##temp
 (
 Dbname varchar(100),
 name varchar(100),
 Filename varchar(200),
 groupid int,
 SizeMB decimal(18,2),
 SpaceUsed decimal(18,2),
 FreeSpace decimal(18,2)
 )

insert into ##temp (dbname,name,filename,groupid,sizemb)
select '['+A.name+']',rtrim(B.name),rtrim(B.filename),B.groupid,CAST(CEILING(((CONVERT(decimal(25,2),b.size) / 1024) * 8)) AS varchar(15)) as SizeMB
from sysdatabases A, sysaltfiles B
where A.dbid = B.dbid 

--update ##temp set SpaceUsed=CAST(FILEPROPERTY(name, 'SpaceUsed')/128 as DECIMAL(18,2)) where Dbname=DB_NAME()
--update ##temp set FreeSpace=SizeMB-SpaceUsed

declare @dbName varchar(255),@strSQL nvarchar(2000)
declare ListDbs cursor for
select name from master..sysdatabases (NOLOCK) 
open ListDbs
fetch next from ListDbs into @dbName
while @@fetch_status = 0
begin
select @strSQL = ' use [' + @dbname + ']
update ##temp set SpaceUsed=CAST(FILEPROPERTY(name, ''SpaceUsed'')/128 as DECIMAL(18,2)) where Dbname=''['+@dbname+']'''
--print @strSQL
Begin Try
	exec sp_executesql @strSQL
End Try
Begin Catch
End Catch
fetch next from ListDbs	into @dbName
end
close ListDbs
deallocate ListDbs

update ##temp set FreeSpace=SizeMB-SpaceUsed
--select * from ##temp
GO

IF OBJECT_ID(N'tempdb..#DBCC_Results') IS NOT NULL
BEGIN
    DROP TABLE #DBCC_Results
END

CREATE TABLE #DBCC_Results (ParentObject varchar(100), Object varchar(100), Field varchar(100), Value varchar(100))
CREATE TABLE #temp_dbcc (dbname varchar(100), lastdate varchar(100))

INSERT INTO #DBCC_Results
EXEC sp_msForEachdb @command1 = 'DBCC DBINFO (''?'') WITH TABLERESULTS,NO_INFOMSGS'

ALTER TABLE #DBCC_Results ADD ID INT IDENTITY(1, 1)
GO -- GO required here
INSERT INTO #temp_dbcc
SELECT r.Value as [Database], r2.Value as CheckDB_LastKnownGood
FROM #DBCC_Results r
INNER JOIN #DBCC_Results r2
ON r2.ID = (SELECT MIN(ID) FROM #DBCC_Results WHERE Field = 'dbi_dbccLastKnownGood' AND ID > r.ID)
WHERE r.Field = 'dbi_dbname'
ORDER BY r.Value

DROP TABLE #DBCC_Results
GO

create table #alldbdump
(Dbname sysname, 
DumpType varchar(5), 
dumpDate datetime,
dumpfile varchar(max)) 
/* -- select CONVERT(varchar(20),DatabasePropertyEx(M.name,'IsAutoClose')),
	--CONVERT(varchar(20),DatabasePropertyEx(M.name,'IsAutoShrink')),
 -- name,* from master..sysdatabases M*/
insert into #alldbdump (Dbname ,DumpType , dumpDate,dumpfile)
select Dbname ,DumpType , dumpDate,dumpfile from (SELECT DISTINCT Database_name dbname, physical_device_name dumpfile,backup_finish_date dumpdate,type dumptype
,row_number() OVER (PARTITION BY Database_name,Type ORDER BY backup_finish_date DESC) rn
FROM msdb.[dbo].[backupset] bs (NOLOCK) join msdb.[dbo].backupmediafamily bmf (NOLOCK)
on bs.media_set_id = bmf.media_set_id) t where t.rn=1

GO

INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],[Column 5]
           ,[Column 6],[Column 7],[Column 8],[Column 9],[Column 10]
           ,[Column 11],[Column 12],[Column 13],[Column 14] ,[Column 15]
		   ,[Column 16],[Column 17],[Column 18],[Column 19],[Column 20],[Column 21],[Column 22]
		   ,[Column 23],[Column 24],[Column 25],[Column 26],[Column 27])
select  'DBInfo_01',rdate,instancename,hostname,M.name
          ,CONVERT(varchar(20),DatabasePropertyEx(M.name,'Status'))
		  ,CONVERT(varchar(20),DatabasePropertyEx(M.name,'Recovery')),
		F.dumpDate, T.dumpDate,d.SizeMB 
		,d.SpaceUsed ,d.FreeSpace,d.[Filename],suser_sname( M.owner_sid ),CONVERT(varchar(20),DatabasePropertyEx(M.name,'IsAutoShrink')),compatibility_level,SrvVersion,
		M.user_access_desc,M.state_desc,M.is_broker_enabled,--M.is_auto_shrink_on,M.is_auto_close_on,
		case when (M.database_id in (1,2,3,4) OR M.is_distributor=1 OR M.name like 'ReportServer%') then 'SystemDB' else 'UserDB' end [DBType]
		,F.dumpfile,T.dumpfile
		,CASE CONVERT(varchar(20),DatabasePropertyEx(M.name,'IsInStandBy')) WHEN 1 THEN 'STANDBY' WHEN 0 THEN 'NO STANDBY' END
		,CONVERT(varchar(20),DatabasePropertyEx(M.name,'Updateability')),CONVERT(varchar(20),DatabasePropertyEx(M.name,'UserAccess'))
		,db.lastdate,M.create_date
from ##hostinstinfo,master.sys.databases M (NOLOCK) 
left outer join ##temp d on d.Dbname = '['+M.name +']'
left outer join 
#alldbdump F on d.Dbname = '['+F.Dbname +']' and F.DumpType ='D'
left outer join #alldbdump T on d.Dbname = '['+T.Dbname +']' and T.DumpType ='L'
left outer join #temp_dbcc db on d.Dbname='['+db.dbname+']'
order by d.Dbname

drop table ##temp
drop table #alldbdump
drop table #temp_dbcc
GO

declare @inst varchar(50),@srv varchar(50)
select @inst=CAST(ISNULL(SERVERPROPERTY('Instancename'),'MSSQLSERVER') AS VARCHAR)
select @srv=ISNULL(@@SERVERNAME,CAST(SERVERPROPERTY('MachineName') AS VARCHAR))
if (charindex('\',@srv) <> 0) select @srv=LEFT(@srv,charindex('\',@srv)-1)
INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],[Column 5])
--select 'CMDB_CI',rdate,instancename,hostname,@inst+'@'+hostname,name from ##hostinstinfo,master.sys.databases
select 'CMDB_CI',rdate,instancename,hostname,@inst+'@'+hostname,name from ##hostinstinfo,master.sys.databases
union
select 'CMDB_CI',rdate,instancename,hostname,@inst+'@'+NodeName,name from ##hostinstinfo,master.sys.dm_os_cluster_nodes,master.sys.databases where nodename not in (hostname)

GO
/*-- select suser_sname( sid ),* from master..sysdatabases
-- part 2 over
-- Part 3
-- SQLAgent_Job_Repl.sql*/
if (select Is_SA from ##hostinstinfo) = 1
begin
select  LEFT(j.Name,100) as "Job Name", LEFT(REPLACE(j.description,CHAR(13),' '),50) as "Job Description", c.name as "Job Category",
	msdb.dbo.agent_datetime(h.run_date, h.run_time) [LastRunTime], 
	((h.run_duration/10000*3600 + (h.run_duration/100)%100*60 + h.run_duration%100 + 31 )/60) [DurationMins],
case h.run_status 
when 0 then 'Failed' 
when 1 then 'Successful' 
when 3 then 'Cancelled' 
when 4 then 'In Progress'
end as JobStatus,
suser_sname(j.owner_sid) as JobOwner,j.enabled,
date_created,date_modified 
into #jobStatus
from msdb..sysJobHistory h (NOLOCK), msdb..sysJobs j (NOLOCK), msdb..syscategories c (NOLOCK)
where j.job_id = h.job_id and j.category_id=c.category_id
and h.step_id = 0
and h.run_date = 
(select max(hi.run_date) from msdb..sysJobHistory hi where h.job_id = hi.job_id and hi.step_id = 0)
and h.run_time =
(select max(hj.run_time) from msdb..sysJobHistory hj where h.job_id = hj.job_id and hj.step_id = 0 and
hj.run_date=h.run_date)
order by 1

UPDATE #jobStatus set Jobstatus='Running' where [Job Name] in (SELECT distinct J.name
          FROM msdb.dbo.sysjobs J 
          JOIN msdb.dbo.sysjobactivity A 
              ON A.job_id=J.job_id 
          WHERE 
          A.run_requested_date IS NOT NULL 
          AND A.stop_execution_date IS NULL
         )

INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],[Column 5]
           ,[Column 6],[Column 7],[Column 8],[Column 9],[Column 10],[Column 11]
		   ,[Column 12],[Column 13] )
select  distinct 'Job Status',rdate, instancename ,hostname,  [Job Name]
	,[Job Description],LastRunTime,DurationMins,
          JobStatus,JobOwner,enabled,date_created,date_modified,[Job Category] 

from ##hostinstinfo, #jobStatus
order by [Job Name]
drop table #jobStatus
end
GO

/*--category
--select * from msdb..sysJobs order by name

-- select @@servername -- FTC-WCEFMHDB501
-- select distinct j.Name , J.job_ID from msdb..sysJobs j
--left outer join msdb..sysJobHistory h on j.job_id = h.job_id
--where h.job_id is not null
--select * from msdb..sysJobHistory hj where job_id='FA1F2617-B14C-4662-B2DB-D23B2951745F'
--or job_id='B491A3AC-BD96-45A0-9A9C-8582360E0C4A'
--or job_id='1D5E126D-1AD1-46B9-B5DC-02EA57558965'
--order by run_date desc

--sp_help_job
-- running job
--exec msdb.dbo.sp_help_job @execution_status=1 

-- DBEng (Moodys MIT)
-- Nilesh Patel 9/25/2014

--select * from master..sysdatabases where name like 'distribu%'
--declare @dbName varchar(255)
--	declare @strSQL nvarchar(2000)
------------	declare ListDbs cursor
------------	for
------------		select name from master..sysdatabases (NOLOCK) where category=16--name like 'distribu%' 
------------	open ListDbs
------------	fetch next
------------		from ListDbs
------------		into @dbName
------------	while @@fetch_status = 0
------------	begin
------------	select @strSQL = 
------------	'
------------		use ' + @dbname + '
------------SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED 
------------SELECT DISTINCT  ''Repl_Source!'',
------------srv.srvname publication_server ,''!'' 
------------, p.publisher_db ,''!''
------------, p.publication publication_name ,''!''
------------, ss.srvname subscription_server ,''!''
------------, s.subscriber_db ,''!''
------------, la.name AS repl_agent_job_name ,''!''
------------, da.name AS d_repl_agent_job_name ,''!''
------------,''!'',''!'',''!'',''!'',''!'',''!'',''!''
------------FROM MSpublications p 
------------JOIN MSsubscriptions s ON p.publication_id = s.publication_id 
------------JOIN master..sysservers ss ON s.subscriber_id = ss.srvid 
------------JOIN master..sysservers srv ON srv.srvid = p.publisher_id 
------------left outer JOIN[dbo].[MSlogreader_agents] la ON la.publisher_id = p.publisher_id and p.publisher_db=la.publisher_db
--------------     AND la.id = s.publication_id 
------------left outer JOIN [dbo].[MSdistribution_agents] da ON s.subscriber_db=da.subscriber_db
------------     AND da.subscriber_id = s.subscriber_id AND da.publication = p.publication
------------ORDER BY 1,2,3 
------------	'
------------	exec sp_executesql @strSQL
	
------------	fetch next
------------		from ListDbs
------------		into @dbName
------------	end
------------	close ListDbs
------------	deallocate ListDbs
	
	-- Part 3 over
	
	--go
	-- part 4 
	-- only beyond 2000
--SET NOCOUNT ON
--use master
--go
--sp_configure  'show advanced options', 1
--Go 
-- Reconfigure with override
-- Go 
--sp_configure 'Ole Automation Procedures' ,1
--go 
--RECONFIGURE with override
--go
--sp_configure  'show advanced options', 0
--Go 
-- Reconfigure with override
*/

DECLARE @hr int
DECLARE @fso int
DECLARE @drive char(1)
DECLARE @odrive int
DECLARE @TotalSize varchar(20) DECLARE @MB Numeric ; SET @MB = 1048576
CREATE TABLE #drives (drive char(1) PRIMARY KEY, FreeSpace int NULL,
TotalSize int NULL) INSERT #drives(drive,FreeSpace) EXEC
master.dbo.xp_fixeddrives EXEC @hr=sp_OACreate
'Scripting.FileSystemObject',@fso OUT IF @hr <> 0 EXEC sp_OAGetErrorInfo
@fso
DECLARE dcur CURSOR LOCAL FAST_FORWARD
FOR SELECT drive from #drives ORDER by drive
OPEN dcur FETCH NEXT FROM dcur INTO @drive
WHILE @@FETCH_STATUS=0
BEGIN
EXEC @hr = sp_OAMethod @fso,'GetDrive', @odrive OUT, @drive
IF @hr <> 0 EXEC sp_OAGetErrorInfo @fso EXEC @hr =
sp_OAGetProperty
@odrive,'TotalSize', @TotalSize OUT IF @hr <> 0 EXEC sp_OAGetErrorInfo
@odrive UPDATE #drives SET TotalSize=@TotalSize/@MB WHERE
drive=@drive FETCH NEXT FROM dcur INTO @drive
End
Close dcur
DEALLOCATE dcur
EXEC @hr=sp_OADestroy @fso IF @hr <> 0 EXEC sp_OAGetErrorInfo @fso

INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],[Column 5],[Column 6]) 

SELECT distinct 'Disk Status',rdate,instancename ,hostname,  drive,TotalSize as 'Total(MB)', FreeSpace as 'Free(MB)' FROM ##hostinstinfo,#drives
ORDER BY drive 
drop table #drives
declare @cpucount varchar(50), @totalRam varchar(50)

GO

--create table #SVer(ID int,  Name  sysname, Internal_Value int, Value nvarchar(512))
--insert #SVer exec master.dbo.xp_msver

declare @memtotal varchar(20),@memavail varchar(20),@logcpu varchar(10),@phycpu varchar(10)

if (select SrvVersion from ##hostinstinfo) not in ('80','90')
select @memtotal=total_physical_memory_kb/1024,@memavail=available_physical_memory_kb/1024 from sys.dm_os_sys_memory

select @logcpu=cpu_count,@phycpu=cpu_count/hyperthread_ratio from sys.dm_os_sys_info

INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],[Column 5]) 
		                   
SELECT 'Memory Status',rdate,instancename ,hostname, @memtotal,@memavail from ##hostinstinfo
--	FROM #SVer
--WHere Name = 'PhysicalMemory'
union
SELECT 'CPU Status',rdate,instancename ,hostname, @logcpu,@phycpu from ##hostinstinfo
--FROM #SVer
--WHere Name = 'ProcessorCount'

--drop table #SVer                

/*--IF @Version<>'2000'
--	Begin
--	 select 'CPU Status',@rdate,@instancename ,@hostname,  '!',
--	 cpu_count from sys.dm_os_sys_info

--	 select 'Memory Status',@rdate,@instancename ,@hostname,  '!',
--		CONVERT(decimal(8,2),physical_memory_in_bytes/(1024*1024*1024.00)),
--		'!'
--	from sys.dm_os_sys_info
--	end

--DROP TABLE #drives 
--go
-- added on 3/24/2015 by nilesh --main script from Naren
-- and link server info by nilesh
--drop table ##tempaccess*/

GO

CREATE table #tempaccess
 (Rtype varchar(10),
access_class varchar(100),
LoginName varchar(100),
Username varchar(100),
Dbname varchar(100),
Permission varchar(100),
OptionGD varchar(5),
createdate varchar(100),
modifydate varchar(100),
passcheck varchar(2))
 
 insert into #tempaccess
SELECT 'Access','ServerRole' as class_desc,p.name LoginName,'NULL',isnull(p.default_database_name,'master') DefDB,r.name RoleName,'NULL'
,p.create_date, p.modify_date,pwdcompare(s.name,password_hash)
FROM sys.server_principals r inner join sys.server_role_members m  
on r.principal_id = m.role_principal_id 
right outer join sys.server_principals p on p.principal_id = m.member_principal_id
left outer join sys.sql_logins s on p.name=s.name

 insert into #tempaccess
select 'Access',perm.class_desc,prin.name LoginName,'NULL','NULL',perm.permission_name SvrExPerm,perm.state [Option]
,prin.create_date, prin.modify_date,pwdcompare(s.name,password_hash)
from sys.server_permissions perm join sys.server_principals prin 
on perm.grantee_principal_id = prin.principal_id
left outer join sys.sql_logins s on prin.name=s.name
where perm.type not in ('CL','CO','COSQ')

 insert into #tempaccess
exec master..sp_msforeachdb'
select ''Access'',''DBRole'' as class_desc,ISNULL(l.name,''[OrphanUser]'') as LoginName,b.name as UserName, ''?'' as DBName, c.name as RoleName,''NULL''
,c.createdate,c.updatedate,pwdcompare(s.name,password_hash)
from [?].dbo.sysmembers a join [?].dbo.sysusers b on a.memberuid = b.uid
join [?].dbo.sysusers c on a.groupuid = c.uid 
left outer join master.dbo.syslogins l on l.sid=b.sid 
left outer join master.sys.sql_logins s on l.name=s.name
where b.issqlrole=0;'

 insert into #tempaccess
exec sp_msforeachdb'
select ''Access'',perm.class_desc ,''DBUSER'',prin.name UserName,''?'' DBName,perm.permission_name DBExPerm,perm.state [Option]
,prin.create_date,prin.modify_date,pwdcompare(s.name,password_hash)
from sys.database_permissions perm join sys.database_principals prin 
on perm.grantee_principal_id = prin.principal_id
join master.dbo.syslogins l on l.sid=prin.sid 
left outer join master.sys.sql_logins s on l.name=s.name
where perm.type not in (''SL'',''CO'',''EX'') and prin.name != ''public'''

INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],[Column 5]
           ,[Column 6],[Column 7],[Column 8],[Column 9],[Column 10],[Column 11],[Column 12] )

select Rtype ,rdate, instancename ,hostname, access_class , LoginName , 
Username , Dbname ,  Permission , OptionGD, createdate, modifydate ,passcheck
 from  ##hostinstinfo,#tempaccess

drop table #tempaccess

GO

INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],[Column 5]
           ,[Column 6],[Column 7],[Column 8],[Column 9],[Column 10],[Column 11]) 
select 'LinkedS',rdate, instancename ,hostname,
srvname,srvproduct,providername,datasource,uses_self_credential,remote_name,L.modify_date,S.schemadate 
from ##hostinstinfo,sysservers S left outer join 
sys.[linked_logins] L on S.srvid = L.server_id
where S.srvid <> 0

GO

INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],[Column 5]
           ,[Column 6],[Column 7], [Column 8] )
select  'Config',rdate, instancename , hostname, 
name, convert(varchar(20),minimum), convert(varchar(20),maximum), convert(varchar(20),value), convert(varchar(20),value_in_use)
FROM ##hostinstinfo,sys.configurations 

GO

if exists (select name from master..sysobjects where name='dm_hadr_database_replica_states')
begin
INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],[Column 5]
           ,[Column 6],[Column 7],[Column 8],[Column 9],[Column 10])
select 'AG Config',rdate,instancename,hostname,G.name 'AG name',
DB.name 'DBName', primary_replica, r.replica_server_name,Rs.synchronization_state_desc 'Replica state', 
RS.synchronization_health_desc + case when is_local=0 then ' Secondary' else ' Primary' end as 'Replica Health' ,RS.last_commit_time  
From ##hostinstinfo, [sys].[dm_hadr_database_replica_states] RS
inner join [sys].[dm_hadr_availability_group_states] GS on GS.group_id = Rs.group_id 
inner join sys.availability_groups G on G.group_ID = RS.group_ID
inner join sys.databases db on db.database_id = RS.database_id
inner join sys.availability_replicas R on R.replica_id = RS.replica_id 
end

GO

if exists (select name from sysdatabases where name='distribution')
begin
INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],[Column 5],[Column 6],[Column 7],[Column 8],
           [Column 9],[Column 10],[Column 11],[Column 12],[Column 13],[Column 14])
select 'ReplInfo',rdate,instancename,hostname,case publication_type when 0 then 'Transactional' when 1 then 'Snapshot' when 2 then 'Merge' end Rep_Type,
ss.srvname subscriber, p.publication,p.publisher_db,p.publication_id,a.article,a.source_owner,
a.source_object,a.destination_object,s.subscriber_db,working_directory
from ##hostinstinfo,msdb.[dbo].MSdistpublishers,distribution..msarticles a join distribution..mspublications p on a.publication_id=p.publication_id
join distribution..mssubscriptions s on a.article_id=s.article_id
join master..sysservers ss on s.subscriber_id=ss.srvid
order by p.publication_id,a.source_owner,a.article
end

GO

----------Log Shipping Info on Primary
INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],[Column 5]
           ,[Column 6],[Column 7],[Column 8],[Column 9],[Column 10],[Column 11])
select 'LS Prim',rdate,instancename,hostname,mp.primary_server,mp.primary_database,ps.secondary_server,ps.secondary_database,pd.last_backup_date,
pd.backup_directory,pd.monitor_server,datediff(mi,pd.last_backup_date,getdate()) backup_latency
from ##hostinstinfo,msdb..log_shipping_monitor_primary mp join msdb..log_shipping_primary_secondaries ps on mp.primary_id=ps.primary_id
join msdb..log_shipping_primary_databases pd on ps.primary_id=pd.primary_id

GO

----------Log Shipping Info on Secondary
INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],[Column 5]
           ,[Column 6],[Column 7],[Column 8],[Column 9],[Column 10],[Column 11],[Column 12],[Column 13])
select 'LS Sec',rdate,instancename,hostname,s.primary_server,s.primary_database,s.backup_source_directory,s.backup_destination_directory,
s.last_copied_date,sd.last_restored_date,ms.restore_threshold,ms.last_restored_latency,
datediff(mi,s.last_copied_date,getdate()) copy_latency,datediff(mi,sd.last_restored_date,getdate()) restore_latency
from ##hostinstinfo,msdb..log_shipping_monitor_secondary ms join msdb..log_shipping_secondary_databases sd on ms.secondary_id=sd.secondary_id
join msdb..log_shipping_secondary s on sd.secondary_id=s.secondary_id

GO

IF (select SrvVersion from ##hostinstinfo) not in ('80','90')
Begin
INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],[Column 5]
           ,[Column 6],[Column 7],[Column 8],[Column 9])
select 'Services',rdate,instancename,hostname,servicename,startup_type_desc,status_desc,service_account,last_startup_time,is_clustered 
from ##hostinstinfo,sys.dm_server_services
end

GO

INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],[Column 5]
           ,[Column 6],[Column 7],[Column 8],[Column 9],[Column 10],[Column 11])
select 'DBMailAccount',rdate,instancename,hostname,p.name,p.description,a.name,a.description,a.email_address,a.display_name,a.replyto_address,pp.is_default
from ##hostinstinfo,[msdb].[dbo].[sysmail_profile] p join [msdb].[dbo].[sysmail_profileaccount] pa on p.profile_id=pa.profile_id
join [msdb].[dbo].[sysmail_account] a on pa.account_id=a.account_id
join [msdb].[dbo].[sysmail_principalprofile] pp on p.profile_id=pp.profile_id

GO

IF exists (select 1 from sysdatabases where name='MDYDBA2') 
	--IF exists (select 1 from MDYDBA2..sysobjects where name like 'D%LEvents') 
Begin
INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],[Column 5]
           ,[Column 6],[Column 7])
select RType,rdate,instancename,hostname,loginname,count,mintime,maxtime from ##hostinstinfo ,
(select 'DDLAudit' RType,'AllData' loginname,count(*) count,MIN(eventdate) mintime,Max(eventdate) maxtime from [MDYDBA2].[dbo].[DDLEvents] UNION
SELECT 'DDLAudit' RType,LoginName,count(*) count,MIN(eventdate) mintime,Max(eventdate) maxtime FROM [MDYDBA2].[dbo].[DDLEvents] Group by LoginName UNION
select 'DMLAudit' RType,'AllData' loginname,count(*) count,MIN(eventdate) mintime,Max(eventdate) maxtime from [MDYDBA2].[dbo].[DMLEvents] UNION
SELECT 'DMLAudit' RType,LoginName,count(*) count,MIN(eventdate) mintime,Max(eventdate) maxtime FROM [MDYDBA2].[dbo].[DMLEvents] Group by LoginName ) t;

End

GO

IF exists (select 1 from sysdatabases where name='MDYDBA2') 
	IF exists (select 1 from MDYDBA2..sysobjects where name like 'LoggedinEvents') 
Begin
INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],[Column 5]
           ,[Column 6],[Column 7])
select RType,rdate,instancename,hostname,loginname,count,mintime,maxtime from ##hostinstinfo ,
(select 'LoginHistory' Rtype,LoginName,sum(logCounts) count,min(EventDate) mintime,max(EventDate) maxtime from [MDYDBA2].[dbo].[LoggedinEvents] Group by LoginName ) t;

End

GO

IF (OBJECT_ID('tempdb..#invalidlogins') IS NOT NULL)
 BEGIN
 DROP TABLE #invalidlogins
 END 

 IF (OBJECT_ID('tempdb..#invalidlogins_dbinfo') IS NOT NULL)
 BEGIN
 DROP TABLE #invalidlogins_dbinfo
 END 

CREATE TABLE #invalidlogins(
ACCTSID VARBINARY(100),
NTLOGIN SYSNAME)

CREATE TABLE #invalidlogins_dbinfo(
dbname varchar(500),
name varchar(500),
[type] char(20),
[schema] varchar(100),
ACCTSID VARBINARY(85))

GO

INSERT INTO #invalidlogins
 EXEC sys.sp_validatelogins

GO

if (select count(*) from #invalidlogins) > 0 
begin
USE master
insert into #invalidlogins_dbinfo
exec sp_msforeachdb 'select ''?'' [dbname],dp.name [username],''[dbuser]'' [type],NULL,dp.sid from [?].sys.database_principals dp join #invalidlogins ti on dp.sid=ti.ACCTSID;'

insert into #invalidlogins_dbinfo
exec sp_msforeachdb 'select ''?'' [dbname],do.name [object],''[sch.obj]'' [type],dss.name [schema],ddp.sid from [?].sys.schemas dss join [?].sys.database_principals ddp on dss.schema_id=ddp.principal_id join #invalidlogins ti on ddp.sid=ti.ACCTSID left outer join [?].sys.objects do on dss.schema_id=do.schema_id;'

end

GO

INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],
		    [Column 5],[Column 6],[Column 7])
SELECT 'InvalidLogins',rdate, instancename ,hostname, t.* from ##hostinstinfo,(
SELECT NTLOGIN,'[InvalidLogin]' type,NULL name,NULL name2 FROM #invalidlogins
UNION
SELECT NTLOGIN,type,name,NULL name2 FROM #invalidlogins ti join 
(select name,owner_sid,'[dbowner]' type from master.sys.databases
union
select name,owner_sid,'[jobowner]' type from msdb..sysjobs) sd on ti.ACCTSID=sd.owner_sid
union
select NTLOGIN,type,dbname,case when name is null then [schema] when [schema] is null then name else [schema]+'.'+name end from #invalidlogins_dbinfo tid join #invalidlogins ti on tid.ACCTSID=ti.ACCTSID
) t order by t.NTLOGIN
GO

drop table #invalidlogins
GO

INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4])
select 'TempDB_Objects',rdate,instancename,hostname,count(*) from ##hostinstinfo , tempdb..sysobjects where xtype='U' and name not like '#%'
group by rdate,instancename,hostname

GO

INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],[Column 5],[Column 6],[Column 7])
select 'DBA_Check',rdate,instancename,hostname,name,step_name,enabled,'"'+command+'"' [text] from ##hostinstinfo ,[msdb].[dbo].[sysjobsteps] js 
join [msdb].[dbo].[sysjobs] j on j.job_id=js.job_id where command like '%MoodysSyntelGlobal%@Moodys.com%'

GO

INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],[Column 5])
select 'DBA_Check',rdate,instancename,hostname,name ,'"'+text+'"' [text]
FROM ##hostinstinfo , msdb.dbo.syscomments c
INNER JOIN msdb.dbo.sysobjects o ON c.id=o.id
where c.text like '%MoodysSyntelGlobal%@Moodys.com%'

GO
--if @@SERVERNAME like '%PTC%' or @@SERVERNAME not like '%2%'

Declare @hostname varchar(50)
select @hostname = hostname from ##hostinstinfo
if @hostname like '%PTC%' or RIGHT(@hostname  , 3) not like '2%'

BEGIN
-- added on 6/14/2019 by Nilesh/Rohit to get top 10 
-- to get top 10 for 3 types of last 7 days 
-- drop table #tmp
select qs.*,st.*,
SUBSTRING(st.TEXT, (qs.statement_start_offset/2)+1,((CASE qs.statement_end_offset WHEN -1 THEN DATALENGTH(st.TEXT)
		ELSE qs.statement_end_offset END - qs.statement_start_offset)/2)+1) SQL_Query,
DB_NAME(CONVERT(INT,CASE spd.value when 32767 then 1 else spd.value end)) [DBName],
CONVERT(INT,spo.value) [ObjID]
into #tmp
FROM sys.dm_exec_query_stats  qs
CROSS APPLY sys.dm_exec_sql_text(sql_handle) st
CROSS APPLY sys.dm_exec_plan_attributes(qs.plan_handle) spd
CROSS APPLY sys.dm_exec_plan_attributes(qs.plan_handle) spo
WHERE sql_handle IS NOT NULL 
AND last_execution_time >= getdate()- 7
AND st.TEXT not like '%dm_exec_query_stats%CROSS APPLY%dm_exec_sql_text%'
AND spd.attribute='dbid' AND spo.attribute='objectid'
and total_worker_time > 500000  -- above 500 milliseconds

--> Top 10 CPU Start
SET ROWCOUNT 10
INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],[Column 5],[Column 6],[Column 7],[Column 8],[Column 9],[Column 10],
			[Column 11],[Column 12],[Column 13],[Column 21],[Column 22])
--	Note 21 & 22 as Maxvarchar
select '1. Top 10 CPU'   RTYPE ,instancename, hostname,  creation_time , 		last_execution_time,
		       (total_worker_time+0.0)/1000000 TotalCPUSeconds, (total_worker_time+0.0)/(execution_count*1000000) CPUperExec,
	execution_count ExecCount,     total_logical_reads TotalLogicalRead,total_logical_writes TotalLogicalWrite, 
	total_Physical_reads TotalPhysicalRead,last_physical_reads LastPhysicalRead, 
	isnull(DBName,'') DBName, 	isnull(str(ObjID,20),'') ObjectID,
	isnull(SUBSTRING(REPLACE(REPLACE(REPLACE(REPLACE(SQL_Query,',', ';'),CHAR(13), ''),CHAR(10), ''),'  ',' '),1,2000),'') tSQL	,
    isnull(SUBSTRING(REPLACE(REPLACE(REPLACE(REPLACE(TEXT,',', ';'),CHAR(13), ''),CHAR(10), ''),'  ',' '),1,2000),'') ParentCode
FROM #tmp, ##hostinstinfo
--%%DBNAMEFILTER%%
ORDER BY total_worker_time DESC

--> Top 10 I/O Start
INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],[Column 5],[Column 6],[Column 7],[Column 8],[Column 9],[Column 10],
			[Column 11],[Column 12],[Column 13],[Column 21],[Column 22])
select  '2. Top 10 I/O' RTYPE ,instancename, hostname,  creation_time , 		last_execution_time,
		       (total_worker_time+0.0)/1000000 TotalCPUSeconds, (total_worker_time+0.0)/(execution_count*1000000) CPUperExec,
	execution_count ExecCount,     total_logical_reads TotalLogicalRead,total_logical_writes TotalLogicalWrite, 
	total_Physical_reads TotalPhysicalRead,last_physical_reads LastPhysicalRead, 
	isnull(DBName,'') DBName, 	isnull(str(ObjID,20),'') ObjectID,
	isnull(SUBSTRING(REPLACE(REPLACE(REPLACE(REPLACE(SQL_Query,',', ';'),CHAR(13), ''),CHAR(10), ''),'  ',' '),1,2000),'') tSQL	,
    isnull(SUBSTRING(REPLACE(REPLACE(REPLACE(REPLACE(TEXT,',', ';'),CHAR(13), ''),CHAR(10), ''),'  ',' '),1,2000),'') ParentCode
FROM #tmp , ##hostinstinfo
WHERE total_logical_reads+total_logical_writes > 0
AND sql_handle IS NOT NULL --and last_execution_time >= getdate() - 7
--%%DBNAMEFILTER%%
ORDER BY (total_logical_reads+total_logical_writes)/(execution_count+0.0) DESC

--> Top 10 I/O Start
INSERT INTO #tempget
           ([Column 0],[Column 1],[Column 2],[Column 3],[Column 4],[Column 5],[Column 6],[Column 7],[Column 8],[Column 9],[Column 10],
			[Column 11],[Column 12],[Column 13],[Column 21],[Column 22])select  '3. Top 10 PhysicalRead' RTYPE ,instancename, hostname,  creation_time , 		last_execution_time,
		       (total_worker_time+0.0)/1000000 TotalCPUSeconds, (total_worker_time+0.0)/(execution_count*1000000) CPUperExec,
	execution_count ExecCount,     total_logical_reads TotalLogicalRead,total_logical_writes TotalLogicalWrite, 
	total_Physical_reads TotalPhysicalRead,last_physical_reads LastPhysicalRead, 
	isnull(DBName,'') DBName, 	isnull(str(ObjID,20),'') ObjectID,
	isnull(SUBSTRING(REPLACE(REPLACE(REPLACE(REPLACE(SQL_Query,',', ';'),CHAR(13), ''),CHAR(10), ''),'  ',' '),1,2000),'') tSQL	,
    isnull(SUBSTRING(REPLACE(REPLACE(REPLACE(REPLACE(TEXT,',', ';'),CHAR(13), ''),CHAR(10), ''),'  ',' '),1,2000),'') ParentCode
FROM #tmp , ##hostinstinfo
WHERE total_Physical_reads > 0
AND sql_handle IS NOT NULL --and last_execution_time >= getdate() - 7
ORDER BY total_Physical_reads desc

SET ROWCOUNT 0
END

select @@servername [instance],*  from #tempget order by [Column 0]
--select distinct [Column 0] from #tempget

drop table #tempget 
drop table ##hostinstinfo

--drop table ##temp
--drop table #jobStatus
--drop table #drives
--drop table #tempaccess
--drop table #alldbdump
