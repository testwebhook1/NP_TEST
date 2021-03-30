create or replace PROCEDURE                                                                                                                                         SP_BASELINE_GENERAL (
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
loc_stmt := ' select COUNT(*)'|| --DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||
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
END IF;
IF  p_table = 'VROP_TBL'  THEN
  loc_stmt := 'INSERT  INTO CMDB_INVENTORY.TEMP_ROW_DATA
    ( SCHEMANAME , TABLENAME , EVENT_TIME , ONCOL01 , ONCOL02 ,RVALUE  )
    select '''|| 
                    DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema)||'''  as SCHEMANAME,''' ||
                    DBMS_ASSERT.SIMPLE_SQL_NAME(p_table) ||'''  as TABLENAME,' ||
                    DBMS_ASSERT.SIMPLE_SQL_NAME(p_column)  ||' as EVENT_TIME,' ||
                  ' HOSTNAME as ONCOL01,' ||
                   'STAT_GROUP ||''**''||STAT_NAME  as ONCOL2 ,' ||
                  DBMS_ASSERT.SIMPLE_SQL_NAME(p_forvalue)  ||' as RVALUE'||
      ' FROM '||DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema)||'.'||DBMS_ASSERT.SIMPLE_SQL_NAME(p_table)||
    ' WHERE '|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||' > TO_DATE('''||p_from_date||''',''MM/DD/YYYY'') ' 
    ||     ' AND '|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||'  <  TO_DATE('''||p_to_date||''',''MM/DD/YYYY'') ' ;

END IF;
IF  p_table = 'RAW_HOURLY_TBL'  THEN
  loc_stmt := 'INSERT  INTO CMDB_INVENTORY.TEMP_ROW_DATA
    ( SCHEMANAME , TABLENAME , EVENT_TIME , ONCOL01 , ONCOL02 ,RVALUE  )
    select '''|| 
                    DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema)||'''  as SCHEMANAME,''' ||
                    DBMS_ASSERT.SIMPLE_SQL_NAME(p_table) ||'''  as TABLENAME,' ||
                    DBMS_ASSERT.SIMPLE_SQL_NAME(p_column)  ||' as EVENT_TIME,' ||
                  ' INSTANCENAME as ONCOL01,' ||
                   'APPLICATIONNAME ||''**TOTALNUMOFINPUTMSG'' as ONCOL2 ,' ||
                  'TOTALNUMOFINPUTMSG'  ||' as RVALUE'||
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
                  ' INSTANCENAME as ONCOL01,' ||
                   'APPLICATIONNAME ||''**MAXCPUTIME'' as ONCOL2 ,' ||
                  'MAXCPUTIME'  ||' as RVALUE'||
      ' FROM '||DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema)||'.'||DBMS_ASSERT.SIMPLE_SQL_NAME(p_table)||
    ' WHERE '|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||' > TO_DATE('''||p_from_date||''',''MM/DD/YYYY'') ' 
    ||     ' AND '|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||'  <  TO_DATE('''||p_to_date||''',''MM/DD/YYYY'') ' ;
END IF;

dbms_output.put_line('t-sql2 - '||loc_stmt ) ;

     EXECUTE IMMEDIATE loc_stmt;
     
 --SELECT count(1) INTO row_count FROM SPLUNK_LOGS.HOSTLOGS_TBL
 --    WHERE  -- SUBSTR(event_time,1,2) = '20' and 
 --    (EVENT_TIME)  < sysdate - keepdays;
     
select count(*) into row_count from CMDB_INVENTORY.TEMP_ROW_DATA;
      dbms_output.put_line('Enough rows to process > 0 = '||row_count);  

  BEGIN --B
  IF( row_count>0) and   p_processYN = 'Y' THEN
    BEGIN --C

-- delete from basline if data exists 
loc_stmt := ' delete CMDB_INVENTORY.ALL_PROFILING_TBL where
            RTRIM(SCHEMANAME) = RTRIM('''||DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema)||''') AND RTRIM(TABLENAME) = RTRIM('''||DBMS_ASSERT.SIMPLE_SQL_NAME(p_table)||''')';
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
       
END SP_BASELINE_GENERAL;