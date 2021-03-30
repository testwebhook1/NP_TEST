create or replace Procedure                                                                       SP_MONTHLY_MAIN_ROLLOVER
-- created by nilesh for Master rollover SP to call indiividual SP
-- 2/6/2019
-- added splunk, SG_Legacy  & SG on 3/4 by Nilesh   
AUTHID CURRENT_USER AS 
  v_today TIMESTAMP;
BEGIN
    BEGIN
    SELECT SYSDATE
      INTO v_today
      FROM DUAL;
 --  DBMS_OUTPUT.PUT_LINE('sysdate: ' || v_today);
    END;
    DBMS_OUTPUT.PUT_LINE('Starting data rollup master SP '||v_today);
    DBMS_OUTPUT.PUT_LINE('Calling Splunk data rollup '||v_today);
      
BEGIN
  --SPLUNK_LOGS."SP_ROLLOVER_HOSTLOGS_TBL"();
SPLUNK_LOGS.SP_ROLLOVER_HOSTLOGS_TBL;

END;
--select * from CMDB_INVENTORY.DATA_CONTROL_ROLLOVER_LOGS

    DBMS_OUTPUT.PUT_LINE('Calling Social Graph Legacy rollup '||v_today);

--set serveroutput on   --- took < 1 min in non-prod & prod  Legacy to roll-up one month Dec-2018
BEGIN
  --SPLUNK_LOGS."SP_ROLLOVER_HOSTLOGS_TBL"();
  SG_LEGACY_LOGS.SP_ROLLOVER_NETSTAT_TBL;
END;

--set serveroutput on   --- took < 3 min in non-prod & prod transformation to roll-up one month Dec-2018
    DBMS_OUTPUT.PUT_LINE('Calling Social Graph rollup '||v_today);
BEGIN
    SG_LOGS.SP_ROLLOVER_NETSTAT_TBL;
END;
--select * from CMDB_INVENTORY.DATA_CONTROL_ROLLOVER
--select YYYY_MON,count(*) ,MIN(last_time), MAX(last_time) from "SG_LOGS"."NETSTAT_HISTORY_TBL" group by YYYY_MON order by YYYY_MON

END;