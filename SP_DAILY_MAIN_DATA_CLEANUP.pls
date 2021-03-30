create or replace PROCEDURE                  "SP_DAILY_MAIN_DATA_CLEANUP" 
-- created by nilesh for Master deletion SP which will call all other deletion procs
-- 2/6/2019 
-- added  3/27     MQ_STATS.SP_REFRESH_FROM_STG_RAW_HOURLY_TBL();
-- 06/04/2019 - calling DVL top 10 
 -- 06/14/2019 - calling VROP_DIRECT in non pord only
 -- 08/07 calling CONTROLM_LOGS.SP_JOBS_HISTORY_CHECKS 
 -- 08/22 calling Informatica and Jboss check 
 -- 08/23 calling MQ_STATS.SP_IIBHOURLY_CHECKS
-- added 3/31/2020 for Job History deletion by Nilesh
-- remove code 3/31/2020 of execution MQ_STATS.SP_REFRESH_FROM_STG_RAW_HOURLY_TBL
-- added 4/1/2020 for JB_STATS KAFKA_INCOMING deletion by Nilesh
-- added 4/2/2020 for MQ_STATS KAFKA_INCOMING deletion by Nilesh
-- added 4/7/2020 for TOMCAT_STATS KAFKA_INCOMING deletion by Nilesh

AUTHID CURRENT_USER AS  

  v_today TIMESTAMP;
  P_SCHEMA VARCHAR2(200);
  P_TABLE VARCHAR2(200);
  P_COLUMN VARCHAR2(200);
  im_exist number(1);
   p_from_date  timestamp; 
   p_to_date  timestamp; --VARCHAR2 (20);
   p_datepart  VARCHAR2 (1); -- H, D, M
   loc_stmt vARCHAR2(500);
BEGIN
    BEGIN
    SELECT SYSDATE
      INTO v_today
      FROM DUAL;
 --  DBMS_OUTPUT.PUT_LINE('sysdate: ' || v_today);
    END;
BEGIN
DBMS_OUTPUT.PUT_LINE('calling DVL top 10 collection '||v_today);
END;

BEGIN
     CMDB_INVENTORY.SP_DVL_TOP10_TBL();
     DBMS_OUTPUT.PUT_LINE('** OVER **'||v_today);
 END;

begin
  select case 
           when exists( select *  FROM all_objects WHERE
                   OBJECT_NAME like  '%SP_REFRESH_FROM_STG_DBMW_CONFIG_DATA_TBL%' and OWNER = 'ORA_DEV_CAS01'
     )
           then 1
           else 0
         end  into im_exist
  from dual;
  DBMS_OUTPUT.PUT_LINE('----***----');
  DBMS_OUTPUT.PUT_LINE('SP_REFRESH_FROM_STG_DBMW_CONFIG_DATA_TBL: ' || im_exist);
END;
IF im_exist = 1 Then

  BEGIN
       DBMS_OUTPUT.PUT_LINE('starting CAS Config data process  '||v_today);
 --      ORA_DEV_CAS01."SP_REFRESH_FROM_STG_DBMW_CONFIG_DATA_TBL"();
        loc_stmt := 'CALL ORA_DEV_CAS01.SP_REFRESH_FROM_STG_DBMW_CONFIG_DATA_TBL() ';
        dbms_output.put_line(' execute config only in non-prod  t-sql - ' ||loc_stmt) ;
            EXECUTE IMMEDIATE  loc_stmt ;-- 
        dbms_output.put_line(' executed config data processing ') ;
  END;

end if;

 BEGIN
      DBMS_OUTPUT.PUT_LINE('starting TOMCAT  KAFKA incoming process  '||v_today);
      TOMCAT_STATS.SP_REFRESH_FROM_STG_KAFKA_INCOMING_TBL();
     commit;
     DBMS_OUTPUT.PUT_LINE('** OVER **'||v_today);
 END; 
 BEGIN
      DBMS_OUTPUT.PUT_LINE('starting JB_STATS KAFKA incoming process  '||v_today);
      JB_STATS.SP_REFRESH_FROM_STG_KAFKA_INCOMING_TBL();
     commit;
     DBMS_OUTPUT.PUT_LINE('** OVER **'||v_today);
 END; 
 BEGIN
      DBMS_OUTPUT.PUT_LINE('starting MQ_STATS  KAFKA incoming process  '||v_today);
      MQ_STATS.SP_REFRESH_FROM_STG_KAFKA_INCOMING_TBL();
     commit;
     DBMS_OUTPUT.PUT_LINE('** OVER **'||v_today);
 END; 

  DBMS_OUTPUT.PUT_LINE('starting anomaly checks '||v_today);
----ANOMALY DAILY SELECT TRUNC(SYSDATE-1)                 start_date,           TRUNC(SYSDATE-1) + 86399 / 86400 end_date    FROM dual;

  p_from_date := TRUNC(SYSDATE-1); -- TO_DATE('04/05/2019','MM/DD/YYYY');
  p_to_date :=  TRUNC(SYSDATE-1) + 86399 / 86400  ; -- TO_DATE('04/16/2019','MM/DD/YYYY');
  p_datepart := 'D';
  DBMS_OUTPUT.PUT_LINE('starting VROP anomaly checks on day '||v_today ||' from '||p_from_date || ' to ' || p_to_date );
  VROP_STATS.SP_VROP_DIRECT_CHECKS(
    p_from_date => p_from_date,
    p_to_date => p_to_date,
    p_datepart => p_datepart
  );
----
  p_from_date := TRUNC(SYSDATE-1); -- TO_DATE('04/05/2019','MM/DD/YYYY');
  p_to_date :=  TRUNC(SYSDATE-1) + 86399 / 86400  ; -- TO_DATE('04/16/2019','MM/DD/YYYY');
  p_datepart := 'D';
  DBMS_OUTPUT.PUT_LINE('starting ControlM Jobs anomaly checks on day '||v_today ||' from '||p_from_date || ' to ' || p_to_date );

 CONTROLM_LOGS.SP_JOBS_HISTORY_CHECKS(
    p_from_date => p_from_date,
    p_to_date => p_to_date,
    p_datepart => p_datepart
  );
--

  p_from_date := TRUNC(SYSDATE-1); -- TO_DATE('04/05/2019','MM/DD/YYYY');
  p_to_date :=  TRUNC(SYSDATE-1) + 86399 / 86400  ; -- TO_DATE('04/16/2019','MM/DD/YYYY');
  p_datepart := 'D';
  DBMS_OUTPUT.PUT_LINE('starting Informatica OS Anomaly checks on day '||v_today ||' from '||p_from_date || ' to ' || p_to_date );

 DB_INFORMATICA.SP_VW_INFA_DETAIL_CHECKS(
    p_from_date => p_from_date,
    p_to_date => p_to_date,
    p_datepart => p_datepart
  );
  --
  p_from_date := TRUNC(SYSDATE-1); -- TO_DATE('04/05/2019','MM/DD/YYYY');
  p_to_date :=  TRUNC(SYSDATE-1) + 86399 / 86400  ; -- TO_DATE('04/16/2019','MM/DD/YYYY');
  p_datepart := 'D';
  DBMS_OUTPUT.PUT_LINE('starting Jboss 3 anomaly checks on day '||v_today ||' from '||p_from_date || ' to ' || p_to_date );

 JB_STATS.SP_JOBSS_CHECKS(
    p_from_date => p_from_date,
    p_to_date => p_to_date,
    p_datepart => p_datepart
  );
 --MQ_STATS.SP_IIBHOURLY_CHECKS
 p_from_date := TRUNC(SYSDATE-1); -- TO_DATE('04/05/2019','MM/DD/YYYY');
  p_to_date :=  TRUNC(SYSDATE-1) + 86399 / 86400  ; -- TO_DATE('04/16/2019','MM/DD/YYYY');
  p_datepart := 'D';
  DBMS_OUTPUT.PUT_LINE('starting MQ IIBHOURLY 3 anomaly checks on day '||v_today ||' from '||p_from_date || ' to ' || p_to_date );

 MQ_STATS.SP_IIBHOURLY_CHECKS(
    p_from_date => p_from_date,
    p_to_date => p_to_date,
    p_datepart => p_datepart
  );

 DBMS_OUTPUT.PUT_LINE('Anomaly check over  '||v_today);
-----
    DBMS_OUTPUT.PUT_LINE('starting data cleanup master '||v_today);
    DBMS_OUTPUT.PUT_LINE('calling SG_LEGACY_LOGS-NETSTAT_TBL data cleanup '||v_today);

BEGIN  -- arch
  P_SCHEMA := 'SG_LEGACY_LOGS';
  P_TABLE := 'NETSTAT_TBL';
  P_COLUMN := 'EVENT_TIME';

  CMDB_INVENTORY.SP_ARCHIVE_GENERAL(
    P_SCHEMA => P_SCHEMA,
    P_TABLE => P_TABLE,
    P_COLUMN => P_COLUMN
  );
     DBMS_OUTPUT.PUT_LINE('** OVER **'||v_today);
END;  -- arch

       DBMS_OUTPUT.PUT_LINE('calling SG_LOGS-NETSTAT_TBL data cleanup '||v_today);

BEGIN  -- arch
  P_SCHEMA := 'SG_LOGS';
  P_TABLE := 'NETSTAT_TBL';
  P_COLUMN := 'EVENT_TIME';

  CMDB_INVENTORY.SP_ARCHIVE_GENERAL(
    P_SCHEMA => P_SCHEMA,
    P_TABLE => P_TABLE,
    P_COLUMN => P_COLUMN
  );
     DBMS_OUTPUT.PUT_LINE('** OVER **'||v_today);
     END;  -- arch  

   DBMS_OUTPUT.PUT_LINE('calling SPLUNK_LOGS-HOSTLOGS_TBL data cleanup '||v_today);

BEGIN  -- arch
  P_SCHEMA := 'SPLUNK_LOGS';
  P_TABLE := 'HOSTLOGS_TBL';
  P_COLUMN := 'EVENT_TIME';

  CMDB_INVENTORY.SP_ARCHIVE_GENERAL(
    P_SCHEMA => P_SCHEMA,
    P_TABLE => P_TABLE,
    P_COLUMN => P_COLUMN
  );
     DBMS_OUTPUT.PUT_LINE('** OVER **'||v_today);
END;  -- arch

  DBMS_OUTPUT.PUT_LINE('calling VROP_STATS-VROP_TBL data cleanup '||v_today);

BEGIN  -- arch
  P_SCHEMA := 'VROP_STATS';
  P_TABLE := 'VROP_DIRECT_TBL';
  P_COLUMN := 'EVENT_TIME';

  CMDB_INVENTORY.SP_ARCHIVE_GENERAL(
    P_SCHEMA => P_SCHEMA,
    P_TABLE => P_TABLE,
    P_COLUMN => P_COLUMN
  );
     DBMS_OUTPUT.PUT_LINE('** OVER **'||v_today);
END;  -- arch  


-- added 3/31/2020 for Job History deletion by Nilesh
      DBMS_OUTPUT.PUT_LINE('calling CONTROLM_LOG JOBS_HISTORY_TBL data cleanup '||v_today);

 BEGIN  -- arch
  P_SCHEMA := 'CONTROLM_LOGS';
  P_TABLE := 'JOBS_HISTORY_TBL';
  P_COLUMN := 'END_TIME';  -- or event_time as per table

  CMDB_INVENTORY.SP_ARCHIVE_GENERAL(
    P_SCHEMA => P_SCHEMA,
    P_TABLE => P_TABLE,
    P_COLUMN => P_COLUMN
  );
     DBMS_OUTPUT.PUT_LINE('** OVER **'||v_today);
END;  -- arch 

-- added 4/1/2020 for JB_STATS KAFKA_INCOMING deletion by Nilesh
      DBMS_OUTPUT.PUT_LINE('calling JB_STATS KAFKA_INCOMING_TBL data cleanup '||v_today);

BEGIN  -- arch
  P_SCHEMA := 'JB_STATS';
  P_TABLE := 'KAFKA_INCOMING_TBL';
  P_COLUMN := 'EVENT_TIME' ;--'END_TIME';  -- or event_time as per table

  CMDB_INVENTORY.SP_ARCHIVE_GENERAL(
    P_SCHEMA => P_SCHEMA,
    P_TABLE => P_TABLE,
    P_COLUMN => P_COLUMN
  );
     DBMS_OUTPUT.PUT_LINE('** OVER **'||v_today);
END;  -- arch  

-- added 4/2/2020 for MQ_STATS KAFKA_INCOMING deletion by Nilesh
      DBMS_OUTPUT.PUT_LINE('calling MQ_STATS KAFKA_INCOMING_TBL data cleanup '||v_today);

BEGIN  -- arch
  P_SCHEMA := 'MQ_STATS';
  P_TABLE := 'KAFKA_INCOMING_TBL';
  P_COLUMN := 'EVENT_TIME' ;--'END_TIME';  -- or event_time as per table

  CMDB_INVENTORY.SP_ARCHIVE_GENERAL(
    P_SCHEMA => P_SCHEMA,
    P_TABLE => P_TABLE,
    P_COLUMN => P_COLUMN
  );
     DBMS_OUTPUT.PUT_LINE('** OVER **'||v_today);
END;  -- arch  
--TOMCAT_STATS 
-- added 4/7/2020 for TOMCAT_STATS KAFKA_INCOMING deletion by Nilesh
      DBMS_OUTPUT.PUT_LINE('calling TOMCAT_STATS KAFKA_INCOMING_TBL data cleanup '||v_today);

BEGIN  -- arch
  P_SCHEMA := 'TOMCAT_STATS';
  P_TABLE := 'KAFKA_INCOMING_TBL';
  P_COLUMN := 'EVENT_TIME' ;--'END_TIME';  -- or event_time as per table

  CMDB_INVENTORY.SP_ARCHIVE_GENERAL(
    P_SCHEMA => P_SCHEMA,
    P_TABLE => P_TABLE,
    P_COLUMN => P_COLUMN
  );
     DBMS_OUTPUT.PUT_LINE('** OVER **'||v_today);
END;  -- arch  

--BEGIN
--UTL_MAIL.SEND(sender     => 'oracle@moodys.com'
--                , recipients => 'Lisa.Zhu@moodys.com,NileshR.Patel@moodys.com'
--                , subject    => 'Client is Lisa -- Testmail from CAUDV01'
--                , message    => 'Hello from SQLDeveloper_CMDB -- CAUDV01');
--                end;

END;
