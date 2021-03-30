create or replace PROCEDURE                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   SP_PROFILING_GENERAL (
   p_schema  IN VARCHAR2,
   p_table  IN VARCHAR2,
   p_column  IN VARCHAR2,
   p_from_date IN VARCHAR2,
   p_to_date IN VARCHAR2,
   p_datepart IN VARCHAR2, -- DW, DH  daily or hourly
   p_oncolumn1 IN VARCHAR2,
   p_oncolumn2 IN VARCHAR2,
   p_forvalue IN VARCHAR2, p_processYN in VARCHAR2
)
AUTHID CURRENT_USER
--- written by Nilehs Patel and started coding on 2/15
-- added MQ_STATS on 3/6/19 by Nilesh
--Paramters 
--from & to date
--schema/table
--DW or DH  on which Column1, Column2

--for which column value
-- Min,Max,Avg, Base counts
-- adding VROP_DIRECT 7/10/2019
-- adding controlM job history 7/31

AS
 --  v_schema   user_tables.table_name%TYPE;
 --  v_table   user_tables.table_name%TYPE;
 --  v_columns user_tab_cols.column_name%TYPE;

row_count NUMBER;
 v_today TIMESTAMP;
last_max_time TIMESTAMP;
r_today TIMESTAMP;
ln_loop_count   NUMBER (6,0)  := 0;
 r_count NUMBER:=0;
del_count NUMBER :=0;

loc_stmt      VARCHAR2(700);
p_table_notbl varchar2(50);

BEGIN
dbms_output.put_line('p_from_date - '||p_from_date ) ;
dbms_output.put_line('p_to_date - '||p_to_date ) ;
dbms_output.put_line('*** parameters checking ***   p_processYN :- '||p_processYN );  

p_table_notbl := UPPER(RTRIM(SUBSTR(p_table,1,INSTR(p_table,'_TBL',1)-1)));
dbms_output.put_line(p_table ||': parameter table name->'|| p_table_notbl);
loc_stmt := ' select count(*) '|| --DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||
    ' FROM '||DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema)||'.'||DBMS_ASSERT.SIMPLE_SQL_NAME(p_table)||
    ' WHERE ROWNUM <= 1 AND '|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||' > TO_DATE('''||p_from_date||''',''MM/DD/YYYY'') ' 
    ||     ' AND '|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||'  <  TO_DATE('''||p_to_date||''',''MM/DD/YYYY'') ' ;

--    ' WHERE ROWNUM <= 1 AND '|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||' > ''' ||p_from_date ||''''
--    ||     ' AND '|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||'  < ''' ||p_to_date ||'''';

dbms_output.put_line('t-sql - '||loc_stmt ) ;
EXECUTE IMMEDIATE loc_stmt INTO row_count ;
--EXECUTE IMMEDIATE loc_stmt  INTO row_count USING TO_DATE(p_from_date,'MM/DD/YYYY'), TO_DATE(p_to_date,'MM/DD/YYYY');
--EXECUTE IMMEDIATE loc_stmt  INTO row_count USING p_from_date,p_to_date;

      dbms_output.put_line('*** parameters are ok ***   p_processYN :- '||p_processYN );  

BEGIN  --A

--DECLARE     v_today TIMESTAMP (6) :=NULL ;
    BEGIN  --B
    SELECT SYSDATE
      INTO v_today
      FROM DUAL;
   DBMS_OUTPUT.PUT_LINE('Begin prod sysdate: ' || v_today);
     END; --B
-- select * from SPLUNK_LOGS.HOSTLOGS_TBL where LENGTH(HOSTNAME) > 50

      loc_stmt := 'TRUNCATE table CMDB_INVENTORY.TEMP_ROW_DATA REUSE STORAGE  ' ; -- assuming only one basline process running
dbms_output.put_line('truncate TEMP_ROW_DATA t-sql - ' ||loc_stmt) ;
    EXECUTE IMMEDIATE  loc_stmt ;-- 'truncate table CMDB_INVENTORY.TEMP_ROW_DATA REUSE STORAGE;'; -- error hence deleting data
  INSERT INTO CMDB_INVENTORY.DATA_CONTROL_PROFILE
   (	DATASOURCE, LAST_PROFILE_TIME, TABLENAME, LAST_COUNTS,MAX_DATA_TIME, MIN_DATA_TIME,STATUS, FREQUENCY,COMMENTS ) 
SELECT DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema),v_today, '*',0,TO_DATE(p_to_date,'MM/DD/YYYY') , TO_DATE(p_from_date,'MM/DD/YYYY') ,'InProgress-01','Weekly',null from DUAL;
--loc_stmt := ' delete CMDB_INVENTORY.TEMP_ROW_DATA where 1=1';
--dbms_output.put_line('t-sql delete all data from temp table - '||loc_stmt ) ;
--EXECUTE IMMEDIATE loc_stmt;
     
-- create table CMDB_INVENTORY.TEMP_ROW_DATA as
--a
 IF  p_table = 'NETSTAT_TBL'  THEN
  loc_stmt := '  INSERT  INTO CMDB_INVENTORY.TEMP_ROW_DATA
    ( SCHEMANAME , TABLENAME , EVENT_TIME , ONCOL01 , ONCOL02 ,RVALUE  )
                   select '''|| 
                    DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema)||''' as SCHEMANAME,''' ||
                    DBMS_ASSERT.SIMPLE_SQL_NAME(p_table) ||'''as TABLENAME,' ||
                    DBMS_ASSERT.SIMPLE_SQL_NAME(p_column)  ||' as EVENT_TIME,' ||
          DBMS_ASSERT.SIMPLE_SQL_NAME(p_oncolumn1)||'||''**''||SOURCE_IP||''**''||SOURCE_PORT as ONCOL01,' ||
          DBMS_ASSERT.SIMPLE_SQL_NAME(p_oncolumn2)||'||''**''||TARGET_IP||''**''||TARGET_PORT  as ONCOL2 ,' || 
          '1 as RVALUE' || 
    ' FROM '||DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema)||'.'||DBMS_ASSERT.SIMPLE_SQL_NAME(p_table)||
    ' WHERE '|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||' > TO_DATE('''||p_from_date||''',''MM/DD/YYYY'') ' 
    ||     ' AND '|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||'  <  TO_DATE('''||p_to_date||''',''MM/DD/YYYY'') ' ;
             dbms_output.put_line('t-sql2 - '||loc_stmt ) ;
     EXECUTE IMMEDIATE loc_stmt;       
END IF;
IF  p_table = 'VROP_DIRECT_TBL'  THEN
  loc_stmt := 'INSERT  INTO CMDB_INVENTORY.TEMP_ROW_DATA
    ( SCHEMANAME , TABLENAME , EVENT_TIME , ONCOL01 , ONCOL02 ,RVALUE  )
    select '''|| 
                    DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema)||'''  as SCHEMANAME,''' ||
                    DBMS_ASSERT.SIMPLE_SQL_NAME(p_table) ||'''  as TABLENAME,' ||
                    DBMS_ASSERT.SIMPLE_SQL_NAME(p_column)  ||' as EVENT_TIME,' ||
                  'HOSTNAME as ONCOL01,' ||
                   '''CPU_USAGE_PERC'' as ONCOL2 ,' ||
                  'CPU_USAGE_PERC'  ||' as RVALUE'||
      ' FROM '||DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema)||'.'||DBMS_ASSERT.SIMPLE_SQL_NAME(p_table)||
    ' WHERE '|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||' > TO_DATE('''||p_from_date||''',''MM/DD/YYYY'') ' 
    ||     ' AND '|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||'  <  TO_DATE('''||p_to_date||''',''MM/DD/YYYY'') ' ;
       
       dbms_output.put_line('t-sql2 - '||loc_stmt ) ;
       
       EXECUTE IMMEDIATE loc_stmt;
         loc_stmt := 'INSERT  INTO CMDB_INVENTORY.TEMP_ROW_DATA
    ( SCHEMANAME , TABLENAME , EVENT_TIME , ONCOL01 , ONCOL02 ,RVALUE  )
    select '''|| 
                    DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema)||'''  as SCHEMANAME,''' ||
                    DBMS_ASSERT.SIMPLE_SQL_NAME(p_table) ||'''  as TABLENAME,' ||
                    DBMS_ASSERT.SIMPLE_SQL_NAME(p_column)  ||' as EVENT_TIME,' ||
                   'HOSTNAME as ONCOL01,' ||
                   '''DISK_LATENCY'' as ONCOL2 ,' ||
                   'DISK_MAXLATENCY_MILLISEC'  ||' as RVALUE'||
      ' FROM '||DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema)||'.'||DBMS_ASSERT.SIMPLE_SQL_NAME(p_table)||
    ' WHERE '|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||' > TO_DATE('''||p_from_date||''',''MM/DD/YYYY'') ' 
    ||     ' AND '|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||'  <  TO_DATE('''||p_to_date||''',''MM/DD/YYYY'') ' ;
           
       dbms_output.put_line('t-sql2 - '||loc_stmt ) ;
       
       EXECUTE IMMEDIATE loc_stmt;
END IF;
-- added 7/31/2019
--removed jobID and added NVL condition for Hostname 8/7/2019 by Nilesh
IF  p_table = 'JOBS_HISTORY_TBL'  THEN
  loc_stmt := 'INSERT  INTO CMDB_INVENTORY.TEMP_ROW_DATA
    ( SCHEMANAME , TABLENAME , EVENT_TIME , ONCOL01 , ONCOL02 ,RVALUE  )
    select '''|| 
                    DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema)||'''  as SCHEMANAME,''' ||
                    DBMS_ASSERT.SIMPLE_SQL_NAME(p_table) ||'''  as TABLENAME,' ||
                    DBMS_ASSERT.SIMPLE_SQL_NAME(p_column)  ||' as EVENT_TIME,' ||
--                  '''JobID-''||RTRIM(Job_ID)||'':Parent-''||RTRIM(parent_table)||'':App-''||RTRIM(application) as ONCOL01,' ||
                  '''Parent-''||RTRIM(parent_table)||'':App-''||RTRIM(application) as ONCOL01,' ||
                  '''Group-''||RTRIM(group_name)||'':Job-''||RTRIM(JOB_NAME)||'':Host-''||RTRIM(NVL(HOSTNAME,''NULL'')) as ONCOL02,' ||
                  'RUN_TIME_SECONDS'  ||' as RVALUE'||
      ' FROM '||DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema)||'.'||DBMS_ASSERT.SIMPLE_SQL_NAME(p_table)||
    ' WHERE '|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||' > TO_DATE('''||p_from_date||''',''MM/DD/YYYY'') ' 
    ||     ' AND '|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||'  <  TO_DATE('''||p_to_date||''',''MM/DD/YYYY'') ' ;
       
              
       dbms_output.put_line('t-sql2 - '||loc_stmt ) ;
       
       EXECUTE IMMEDIATE loc_stmt;
       
END IF;
-- added 8/9/2019
-- DB_INFORMATICA.VW_INFA_DETAIL  (IPC/IDQ/CDC)
IF  p_table = 'VW_INFA_DETAIL'  THEN
  loc_stmt := 'INSERT  INTO CMDB_INVENTORY.TEMP_ROW_DATA
    ( SCHEMANAME , TABLENAME , EVENT_TIME , ONCOL01 , ONCOL02 ,RVALUE  )
    select '''|| 
                    DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema)||'''  as SCHEMANAME,''' ||
                    DBMS_ASSERT.SIMPLE_SQL_NAME(p_table) ||'''  as TABLENAME,' ||
                    DBMS_ASSERT.SIMPLE_SQL_NAME(p_column)  ||' as EVENT_TIME,' ||
--                  '''JobID-''||RTRIM(Job_ID)||'':Parent-''||RTRIM(parent_table)||'':App-''||RTRIM(application) as ONCOL01,' ||
                  '''INFA-''||RTRIM(RTYPE)||'':HOST-''||RTRIM(HOSTNAME) as ONCOL01,' ||
                  '''P-''||RTRIM(PROCESS_NAME) as ONCOL02,' ||
                  'CPU_USAGE'  ||' as RVALUE'||
      ' FROM '||DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema)||'.'||DBMS_ASSERT.SIMPLE_SQL_NAME(p_table)||
    ' WHERE '|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||' > TO_DATE('''||p_from_date||''',''MM/DD/YYYY'') ' 
    ||     ' AND '|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||'  <  TO_DATE('''||p_to_date||''',''MM/DD/YYYY'') ' ;
       
       dbms_output.put_line('t-sql2 - '||loc_stmt ) ;
     EXECUTE IMMEDIATE loc_stmt;       
       
END IF;
-- added 8/22/2019
-- JB_STATS. many tavles/view 
IF  p_schema = 'JB_STATS'  THEN
   dbms_output.put_line('inserting in temp for VW_JBOSSGC for CollectionTime') ;
 INSERT  INTO CMDB_INVENTORY.TEMP_ROW_DATA  ( SCHEMANAME , TABLENAME , EVENT_TIME , ONCOL01 , ONCOL02 ,RVALUE  )
    select 'JB_STATS', 'VW_JBOSSGC', EVENT_TIME, 
          HOSTNAME||':'|| recordtype, 'CollectionTime', ROUND(collectiontime / collectioncount,0)
        FROM JB_STATS.VW_JBOSSGC 
        WHERE EVENT_TIME > TO_DATE(p_from_date,'MM/DD/YYYY') AND EVENT_TIME  <  TO_DATE(p_to_date,'MM/DD/YYYY') 
        and collectioncount > 0;
        commit;
       
   dbms_output.put_line('inserting in temp for VW_JBOSSOS for FreePhysicalPercentage') ;
    INSERT  INTO CMDB_INVENTORY.TEMP_ROW_DATA
    ( SCHEMANAME , TABLENAME , EVENT_TIME , ONCOL01 , ONCOL02 ,RVALUE  )
    select 'JB_STATS', 'VW_JBOSSOS', EVENT_TIME, 
          HOSTNAME, 'FreePhysicalPercentage', ROUND((FREEPHYSICALMEMORYSIZE / TOTALPHYSICALMEMORYSIZE)*100,0)
        FROM JB_STATS.VW_JBOSSOS  
      WHERE EVENT_TIME > TO_DATE(p_from_date,'MM/DD/YYYY') AND EVENT_TIME  <  TO_DATE(p_to_date,'MM/DD/YYYY') ;
        commit;

   dbms_output.put_line('inserting in temp for VW_JBOSSDS for AverageCreationTime') ;
    INSERT  INTO CMDB_INVENTORY.TEMP_ROW_DATA
    ( SCHEMANAME , TABLENAME , EVENT_TIME , ONCOL01 , ONCOL02 ,RVALUE  )
  
    select 'JB_STATS', 'VW_JBOSSDS', EVENT_TIME, 
          HOSTNAME||':'||DSNAME, 'AverageCreationTime', AverageCreationTime
        FROM JB_STATS.VW_JBOSSDS    
        WHERE EVENT_TIME > TO_DATE(p_from_date,'MM/DD/YYYY') AND EVENT_TIME  <  TO_DATE(p_to_date,'MM/DD/YYYY') 
        and AverageCreationTime > 0 and AverageCreationTime < 10000;
        commit;

END IF;

--added on 8/23
IF  p_schema = 'MQ_STATS'  THEN
   dbms_output.put_line('inserting in temp for VW_IIBHOURLY for CPUSecondsPerThread') ;
     INSERT  INTO CMDB_INVENTORY.TEMP_ROW_DATA  ( SCHEMANAME , TABLENAME , EVENT_TIME , ONCOL01 , ONCOL02 ,RVALUE  )
       select 'MQ_STATS', 'VW_IIBHOURLY', EVENT_TIME, 
          NODENAME||':'|| APPLICATIONNAME, 'CPUSecondsPerThread', ROUND((MAXIMUMCPUTIME / NUMBEROFTHREADSINPOOL)/1000,2)
        FROM MQ_STATS.VW_IIBHOURLY 
        WHERE EVENT_TIME > TO_DATE(p_from_date,'MM/DD/YYYY') AND EVENT_TIME  <  TO_DATE(p_to_date,'MM/DD/YYYY') 
        and MAXIMUMCPUTIME > 0;
        commit;
       
   dbms_output.put_line('inserting in temp for VW_IIBHOURLY for FreePhysicalPercentage') ;
    INSERT  INTO CMDB_INVENTORY.TEMP_ROW_DATA
    ( SCHEMANAME , TABLENAME , EVENT_TIME , ONCOL01 , ONCOL02 ,RVALUE  )
    select 'MQ_STATS', 'VW_IIBHOURLY', EVENT_TIME, 
          NODENAME||':'|| APPLICATIONNAME, 'TotalBackout', TOTALNUMBEROFBACKOUTS
        FROM MQ_STATS.VW_IIBHOURLY 
        WHERE EVENT_TIME > TO_DATE(p_from_date,'MM/DD/YYYY') AND EVENT_TIME  <  TO_DATE(p_to_date,'MM/DD/YYYY') 
        and TOTALNUMBEROFBACKOUTS > 0;  commit;

   dbms_output.put_line('inserting in temp for VW_IIBHOURLY for AverageCreationTime') ;
    INSERT  INTO CMDB_INVENTORY.TEMP_ROW_DATA
    ( SCHEMANAME , TABLENAME , EVENT_TIME , ONCOL01 , ONCOL02 ,RVALUE  )
  
select 'MQ_STATS', 'VW_IIBHOURLY', EVENT_TIME, 
          NODENAME||':'|| APPLICATIONNAME, 'MaxThread', TIMESMAXIMUMNUMBEROFTHREADSREACHED
        FROM MQ_STATS.VW_IIBHOURLY 
        WHERE EVENT_TIME > TO_DATE(p_from_date,'MM/DD/YYYY') AND EVENT_TIME  <  TO_DATE(p_to_date,'MM/DD/YYYY') 
        and TIMESMAXIMUMNUMBEROFTHREADSREACHED > 0;
        
        commit;

END IF;
     
 --SELECT count(1) INTO row_count FROM SPLUNK_LOGS.HOSTLOGS_TBL
 --    WHERE  -- SUBSTR(event_time,1,2) = '20' and 
 --    (EVENT_TIME)  < sysdate - keepdays;
     
select count(*) into row_count from CMDB_INVENTORY.TEMP_ROW_DATA;
      dbms_output.put_line('Enough rows to process > 0 = '||row_count);  

  BEGIN --B
  IF( row_count>0) and   p_processYN = 'Y' THEN
    BEGIN --C
 UPDATE CMDB_INVENTORY.DATA_CONTROL_PROFILE SET LAST_COUNTS = row_count , STATUS = 'InProgress-02' WHERE
   	DATASOURCE = DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema) and LAST_PROFILE_TIME = v_today  ;

-- delete from basline if data exists 
loc_stmt := ' delete CMDB_INVENTORY.ALL_PROFILING_TBL where
            RTRIM(SCHEMANAME) = RTRIM('''||DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema)||''') AND RTRIM(TABLENAME) = RTRIM('''||DBMS_ASSERT.SIMPLE_SQL_NAME(p_table)||''')';
            
loc_stmt := 'ALTER TABLE CMDB_INVENTORY.ALL_PROFILING_TBL TRUNCATE PARTITION '|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema);
dbms_output.put_line('t-sql3 remove old baseline - '||loc_stmt ) ;
    SELECT SYSDATE
      INTO r_today
      FROM DUAL;
   --select v_count
    DBMS_OUTPUT.PUT_LINE('process start date: ' || r_today);

     EXECUTE IMMEDIATE loc_stmt;
     
commit;

BEGIN -- F

INSERT   INTO CMDB_INVENTORY.ALL_PROFILING_TBL
    ( SCHEMANAME ,TABLENAME , PROFILEBASE , PROFILEON , BASE_TIME , SUMVALUE , MINVALUE ,MAXVALUE ,MIN_TIME , MAX_TIME,COUNTS  )
select  RTRIM(SCHEMANAME) , RTRIM(TABLENAME) ,  RTRIM(ONCOL01) , RTRIM(ONCOL02) , to_char(EVENT_TIME,'d-HH24')  , SUM(RVALUE ), MIN(RVALUE ), MAX(RVALUE ), min(EVENT_TIME), MAX(EVENT_TIME),count(*)
from CMDB_INVENTORY.TEMP_ROW_DATA      
group by  RTRIM(SCHEMANAME) , RTRIM(TABLENAME) ,  RTRIM(ONCOL01) , RTRIM(ONCOL02) ,to_char(EVENT_TIME,'d-HH24') ;
commit;
INSERT   INTO CMDB_INVENTORY.ALL_PROFILING_TBL
    ( SCHEMANAME ,TABLENAME , PROFILEBASE , PROFILEON , BASE_TIME , SUMVALUE , MINVALUE ,MAXVALUE ,MIN_TIME , MAX_TIME,COUNTS  )
select  RTRIM(SCHEMANAME) , RTRIM(TABLENAME) ,  RTRIM(ONCOL01) , RTRIM(ONCOL02)  , to_char(EVENT_TIME,'D')  , SUM(RVALUE ), MIN(RVALUE ), MAX(RVALUE ), min(EVENT_TIME), MAX(EVENT_TIME),count(*)
from CMDB_INVENTORY.TEMP_ROW_DATA      
group by  RTRIM(SCHEMANAME) , RTRIM(TABLENAME) ,  RTRIM(ONCOL01) , RTRIM(ONCOL02) ,to_char(EVENT_TIME,'D') ;
commit;

INSERT   INTO CMDB_INVENTORY.ALL_PROFILING_TBL
    ( SCHEMANAME ,TABLENAME , PROFILEBASE , PROFILEON , BASE_TIME , SUMVALUE , MINVALUE ,MAXVALUE ,MIN_TIME , MAX_TIME,COUNTS  )
select RTRIM(SCHEMANAME) , RTRIM(TABLENAME) ,  RTRIM(ONCOL01) , RTRIM(ONCOL02) , to_char(EVENT_TIME,'YYYY-MON')  , SUM(RVALUE ), MIN(RVALUE ), MAX(RVALUE ), min(EVENT_TIME), MAX(EVENT_TIME),count(*)
from CMDB_INVENTORY.TEMP_ROW_DATA      
group by  RTRIM(SCHEMANAME) , RTRIM(TABLENAME) ,  RTRIM(ONCOL01) , RTRIM(ONCOL02)  ,to_char(EVENT_TIME,'YYYY-MON') ;
commit;
 UPDATE CMDB_INVENTORY.DATA_CONTROL_PROFILE SET  STATUS = 'Completed' WHERE
   	DATASOURCE = DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema) and LAST_PROFILE_TIME = v_today ;


    SELECT SYSDATE
      INTO v_today
      FROM DUAL;
  DBMS_OUTPUT.PUT_LINE('end   date: ' || v_today);
END; -- F

-- deletion over
     
 --     BEGIN  --D
 --     UPDATE CMDB_INVENTORY.DATA_CONTROL_ARCHIVE
 --       SET LAST_COUNTS          = row_count,
 --           LAST_DELETION_TIME = v_today,
  --         LAST_MAX_DATA_TIME = p_to_date,
 --           LAST_MIN_DATA_TIME = p_from_date,
 --           STATUS               = 'SUCCESS', COMMENTS = 'Actual deleted '|| TO_CHAR(del_count)
 --           WHERE DATASOURCE = p_schema and TABLENAME = p_table_notbl;
 --     END;  --D
      
      commit;
       dbms_output.put_line('Data populated baseline for '||p_table );    
      END;  --C
  ELSE
 --   dbms_output.put_line('LESS THAN 10 ');    
    dbms_output.put_line('not enough rows to delete '||p_table|| ' table < 1 = '||row_count);     
  END IF; 
  
  END; --B
--Execute the procedure  
END ; --A

EXCEPTION
   WHEN others
   THEN
      dbms_output.put_line('No data or no access or invalid parameters - check above t-sql' );
      dbms_output.put_line('parameters are not ok');
      dbms_output.put_line('    schema name-->' ||p_schema );
      dbms_output.put_line('     Table name -->' || p_table );
      dbms_output.put_line('    Column name -->'|| p_column );
      DBMS_OUTPUT.PUT_LINE ('    '||SQLERRM);
      DBMS_OUTPUT.PUT_LINE ('    '||SQLCODE);
       
END SP_PROFILING_GENERAL;