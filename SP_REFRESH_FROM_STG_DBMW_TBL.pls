create or replace Procedure                                                                                                                                                                                                                                                                                                            sp_refresh_from_STG_DBMW_TBL
-- created by nilesh to refresh data once enough rows in STG table
-- 08/22/2018
-- change/add alias after access reviewd on 2/22
-- change from 6000+ to 3500+ with new code from Satyendra to match Khsuhal's inventory 7_27_2020 by Nilesh
-- also added 2 new columns DBMW_CLASS, SUPPORTED_BY on 7_27_2020 by Nilesh

AUTHID CURRENT_USER AS 

row_count NUMBER;
max_time TIMESTAMP;
 v_today TIMESTAMP;
BEGIN

--DECLARE     v_today TIMESTAMP (6) :=NULL ;
    BEGIN
    SELECT SYSDATE
      INTO v_today
      FROM DUAL;
 --  DBMS_OUTPUT.PUT_LINE('sysdate: ' || v_today);
  END;

--DECLARE     max_time TIMESTAMP (6) :=NULL ;
    BEGIN
      select MAX(EVENT_TIME) INTO max_time from CMDB_INVENTORY.DBMW_TBL_STG;
    END;
    
select count(1) into row_count from CMDB_INVENTORY.DBMW_TBL_STG;
-- SELECT NUM_ROWS INTO row_count FROM All_tables where  table_name = 'DBMW_TBL_STG';
--exec select count(*) into :row_count from dual;
dbms_output.put_line('MORE THAN 3500 - '||row_count ); 


  BEGIN
  IF( row_count>=3500)THEN
    BEGIN
      dbms_output.put_line('Enough rows to replace table >= 3500 = '||row_count);  
      
    DELETE CMDB_INVENTORY.DBMW_TBL;
 
       dbms_output.put_line('deleted all data from main table');  
 
        INSERT   INTO CMDB_INVENTORY.DBMW_TBL
    (
      TCP_PORT ,
      EVENT_TIME ,
      INSTANCENAME ,
      SUBCATEGORY ,
      VIRTUAL_IP2 ,
      VIRTUAL_IP1 ,
      DBMW_APP_NAME ,
      HOST_SYSID ,
      HOSTNAME ,
      PRODUCT_VERSION ,
      SUPPORT_GROUP ,
      CATEGORY ,
      MONITROING_TOOL ,
      CLUSTER_TRUE ,
      LOAD_BALANCER_TRUE ,
      SOX_TRUE ,
      INSTANCE_SYSID ,
      IP_ADDRESS ,
      DBMW_DESCRIPTION ,
      MONITORING_SERVICE ,
      PRODUCT ,
      VIRTUAL_RECORD ,
      RESTRICTED_TRUE ,
      OWNED_BY ,
      REPLICATION_APP ,
      AGED ,
      INSTALL_STATUS ,
      REPLICATION_DB ,
      ENVIRONMENT,
      DBMW_CLASS, SUPPORTED_BY
    )
     SELECT DISTINCT  TCP_PORT ,
      EVENT_TIME ,
      UPPER(LTRIM(RTRIM(INSTANCENAME))) ,
      UPPER(SUBCATEGORY) ,
      VIRTUAL_IP2 ,
      VIRTUAL_IP1 ,
      UPPER(DBMW_APP_NAME) ,
      HOST_SYSID ,
      UPPER(LTRIM(RTRIM(SUBSTR(INSTANCENAME,INSTR(INSTANCENAME, '@', -1, 1)+1,50)))) ,
      UPPER(PRODUCT_VERSION) ,
      UPPER(SUPPORT_GROUP) ,
      UPPER(CATEGORY) ,
      UPPER(MONITROING_TOOL) ,
      UPPER(CLUSTER_TRUE) ,
      (LOAD_BALANCER_TRUE),
      UPPER(SOX_TRUE) ,
      INSTANCE_SYSID ,
      IP_ADDRESS ,
      UPPER(DBMW_DESCRIPTION) ,
      UPPER(MONITORING_SERVICE) ,
      UPPER(PRODUCT) ,
      UPPER(VIRTUAL_RECORD) ,
      UPPER(RESTRICTED_TRUE) ,
      UPPER(OWNED_BY) ,
      UPPER(REPLICATION_APP) ,
      UPPER(AGED),
      UPPER(INSTALL_STATUS) ,
      REPLICATION_DB ,
      UPPER(ENVIRONMENT) ,
      UPPER(DBMW_CLASS), UPPER(SUPPORTED_BY)
      from CMDB_INVENTORY.DBMW_TBL_STG 
      WHERE support_group is not null;
      
        BEGIN
      UPDATE CMDB_INVENTORY.DATA_CONTROL_INFO
        SET LAST_COUNTS          = row_count,
            LAST_COLLECTION_TIME = v_today,
            LAST_MAX_DATA_TIME = max_time,
            STATUS               = 'SUCCESS'
              WHERE DATASOURCE = 'CMDB_INVENTORY' and TABLENAME = 'DBMW';
      END;
      
      commit;
         dbms_output.put_line('data processed successfully DBMW data.' );   
      END;
  ELSE
 --   dbms_output.put_line('LESS THAN 10 ');    
    dbms_output.put_line('not enough rows to replace with DBMW_TBL_STG table < 6000 = '||row_count);     
  END IF; 
  END;
--Execute the procedure  
EXCEPTION
   WHEN others
   THEN
      DBMS_OUTPUT.PUT_LINE ('    '||SQLERRM);
      DBMS_OUTPUT.PUT_LINE ('    '||SQLCODE);
END ;