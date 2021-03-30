create or replace PROCEDURE                  "SP_ARCHIVE_GENERAL" (
   p_schema  IN VARCHAR2,
   p_table  IN VARCHAR2,
   p_column IN VARCHAR2 --,    p_value  IN VARCHAR2
)
--- written by Nilehs Patel and tested in non-prod on SG_LEGACY_LOGS NETSTAT table on 2/5/2019e
-- exception for no data found add 05/09/2019 by nilesh
-- error message updated with control Archive data check w/o TBL
AUTHID CURRENT_USER AS
   v_schema   user_tables.table_name%TYPE;
   v_table   user_tables.table_name%TYPE;
   v_columns user_tab_cols.column_name%TYPE;
keepdays NUMBER :=500;
row_count NUMBER;
max_time TIMESTAMP;
min_time TIMESTAMP;
 v_today TIMESTAMP;
last_max_time TIMESTAMP;
r_today TIMESTAMP;
ln_loop_count   NUMBER (6,0)  := 0;
 r_count NUMBER:=0;
del_count NUMBER :=0;
batchsize   NUMBER (6,0)  := 500;
loc_stmt      VARCHAR2(200);
p_table_notbl varchar2(50);

BEGIN

p_table_notbl := UPPER(RTRIM(SUBSTR(p_table,1,INSTR(p_table,'_TBL',1)-1)));
dbms_output.put_line(p_table ||': parameter table name->'|| p_table_notbl);

dbms_output.put_line('t-sql - select '|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||' FROM '||DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema)||'.'||DBMS_ASSERT.SIMPLE_SQL_NAME(p_table)||
' WHERE ROWNUM <= 1');

EXECUTE IMMEDIATE 
      'select '|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||' FROM '||DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema)||'.'||DBMS_ASSERT.SIMPLE_SQL_NAME(p_table)||
' WHERE ROWNUM <=2';

      dbms_output.put_line('*** parameters ok *** ');  

BEGIN  --A

--DECLARE     v_today TIMESTAMP (6) :=NULL ;
    BEGIN  --B
    SELECT SYSDATE
      INTO v_today
      FROM DUAL;
 --  DBMS_OUTPUT.PUT_LINE('sysdate: ' || v_today);
     END; --B
-- select * from SPLUNK_LOGS.HOSTLOGS_TBL where LENGTH(HOSTNAME) > 50
--a
loc_stmt := 'select  MAX('|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||') 
FROM '||DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema)||'.'||DBMS_ASSERT.SIMPLE_SQL_NAME(p_table);
dbms_output.put_line('t-sql - '||loc_stmt ) ;
EXECUTE IMMEDIATE loc_stmt INTO max_time ;
--b

--a
loc_stmt := 'select  MIN('|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||') 
FROM '||DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema)||'.'||DBMS_ASSERT.SIMPLE_SQL_NAME(p_table);
dbms_output.put_line('t-sql - '||loc_stmt ) ;
EXECUTE IMMEDIATE loc_stmt INTO min_time ;
--b  

      dbms_output.put_line('Min datetime = '||min_time);  
      dbms_output.put_line('Max datetime = '||max_time);  

      SELECT DAYS_TOKEEP INTO keepdays FROM CMDB_INVENTORY.DATA_CONTROL_ARCHIVE
                     WHERE DATASOURCE = p_schema and TABLENAME = p_table_notbl;
      SELECT BATCH_QTY INTO  batchsize FROM CMDB_INVENTORY.DATA_CONTROL_ARCHIVE
                     WHERE DATASOURCE = p_schema and TABLENAME = p_table_notbl;
If batchsize = 0 
  then batchsize:=5000;
end if;

      dbms_output.put_line('Days to keep = '||keepdays);  
      dbms_output.put_line('Batch Size = '||batchsize);  

--a
loc_stmt := 'select  count(1) 
FROM '||DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema)||'.'||DBMS_ASSERT.SIMPLE_SQL_NAME(p_table)||
  ' WHERE '|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||' < sysdate  - '|| keepdays;
dbms_output.put_line('t-sql - '||loc_stmt ) ;
EXECUTE IMMEDIATE loc_stmt INTO row_count ;

 --SELECT count(1) INTO row_count FROM SPLUNK_LOGS.HOSTLOGS_TBL
 --    WHERE  -- SUBSTR(event_time,1,2) = '20' and 
 --    (EVENT_TIME)  < sysdate - keepdays;

--exec select count(*) into :row_count from dual;
      dbms_output.put_line(p_schema ||'.'|| p_table ||' rows to be deleted - '||row_count); 
      dbms_output.put_line('Enough rows to process > 0 = '||row_count);  

  BEGIN --B
  IF( row_count>=0)THEN
    BEGIN --C
-- delete in batch

BEGIN
    SELECT SYSDATE
      INTO r_today
      FROM DUAL;
   --select v_count
    DBMS_OUTPUT.PUT_LINE('start date: ' || r_today);

loop
  ln_loop_count:=ln_loop_count+1;
  exit when ln_loop_count = 10 ;

--delete SPLUNK_LOGS.HOSTLOGS_TBL where (EVENT_TIME)  < sysdate - keepdays
--and ROWNUM <= batchsize; 
--r_count:= SQL%ROWCOUNT;
loc_stmt := 'DELETE  
FROM '||DBMS_ASSERT.SIMPLE_SQL_NAME(p_schema)||'.'||DBMS_ASSERT.SIMPLE_SQL_NAME(p_table)||
  ' WHERE '|| DBMS_ASSERT.SIMPLE_SQL_NAME(p_column) ||' < sysdate  - '|| keepdays || ' and ROWNUM <= '||batchsize;
dbms_output.put_line('t-sql - '||loc_stmt ) ;

 EXECUTE IMMEDIATE loc_stmt ;--INTO r_count ;
r_count:= SQL%ROWCOUNT;

 DBMS_OUTPUT.PUT_LINE(r_count);
del_count:=del_count+ r_count;
commit;
 SELECT SYSDATE
      INTO r_today
      FROM DUAL;
   dbms_output.put_line('loop# = '||ln_loop_count|| ' : ' ||r_today || ' : deleted - ' || del_count);  
 -- DBMS_LOCK.Sleep( 5 );
    EXIT when r_count = 0;
end loop;


    SELECT SYSDATE
      INTO v_today
      FROM DUAL;
  DBMS_OUTPUT.PUT_LINE('end   date: ' || v_today);
END;

-- deletion over

      BEGIN  --D
      UPDATE CMDB_INVENTORY.DATA_CONTROL_ARCHIVE
        SET LAST_COUNTS          = row_count,
            LAST_DELETION_TIME = v_today,
           LAST_MAX_DATA_TIME = max_time,
            LAST_MIN_DATA_TIME = min_time,
            STATUS               = 'SUCCESS', COMMENTS = 'Actual deleted '|| TO_CHAR(del_count)
            WHERE DATASOURCE = p_schema and TABLENAME = p_table_notbl;
      END;  --D

      commit;
       dbms_output.put_line('Data deleted successfully for '||p_table );    
      END;  --C
  ELSE
 --   dbms_output.put_line('LESS THAN 10 ');    
    dbms_output.put_line('not enough rows to delete '||p_table|| ' table < 1 = '||row_count);     
  END IF; 

  END; --B
--Execute the procedure  
END ; --A

EXCEPTION
   WHEN NO_DATA_FOUND THEN
            dbms_output.put_line('No data to be  deleted / check DATA_CONTROL_ARCHIVE w/o should be _TBL' );
   WHEN others
   THEN
      dbms_output.put_line('No data or no access or invalid parameters - check above t-sql' );
      dbms_output.put_line('parameters are not ok');
      dbms_output.put_line('    schema name-->' ||p_schema );
      dbms_output.put_line('     Table name -->' || p_table );
      dbms_output.put_line('    Column name -->'|| p_column );
      DBMS_OUTPUT.PUT_LINE ('    '||SQLERRM);
      DBMS_OUTPUT.PUT_LINE ('    '||SQLCODE);

END SP_ARCHIVE_GENERAL;

