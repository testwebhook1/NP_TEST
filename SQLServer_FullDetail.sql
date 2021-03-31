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
