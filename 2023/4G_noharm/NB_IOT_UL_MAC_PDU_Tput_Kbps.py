# Databricks notebook source
KPI_gold = 'NB_IOT_UL_MAC_PDU_Tput_Kbps_KPI_gold'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(CalendarDate) as Min_Day , max(CalendarDate) as Max_Day ,min(hour24) as Min_Hour ,max(hour24) as Max_Hour FROM 
# MAGIC (
# MAGIC         select CalendarDate,hour24    from  NBIoTAirMACULandDLpackets_lte_hr_silver
# MAGIC )k

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vz_conf_ecell_silver limit 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC with v_conf_ecell_silver as (
# MAGIC select CLUSTER_NAME,REPLACE(ENODEB_ID,'VZ_','eNB_') ENODEB_ID,concat('NBIoT_cNum',ECELL_ID) ECELL_ID,DL_BANDWIDTH,FREQUENCY_BAND_INDICATOR,MARKET_NAME,NETWORK_NAME,SUB_MARKET_ID,SUB_MARKET_NAME from vz_conf_ecell_silver
# MAGIC )
# MAGIC select 
# MAGIC CLUSTER_NAME,ENODEB_ID,ECELL_ID,DL_BANDWIDTH,FREQUENCY_BAND_INDICATOR,MARKET_NAME,NETWORK_NAME,SUB_MARKET_ID,SUB_MARKET_NAME,
# MAGIC cast(CalendarDate as date) as Daily,
# MAGIC         hour24 as Hourly,NE_ID,LOCATION,
# MAGIC NBIoT_AirMacULByte NB_IOT_UL_MAC_PDU_Tput_Kbps_NUM,
# MAGIC NBIoT_AirMacULByteCnt NB_IOT_UL_MAC_PDU_Tput_Kbps_DENOM
# MAGIC from (
# MAGIC 	select 
# MAGIC 	CalendarDate,hour24,NE_ID,LOCATION,	LOCATION2,
# MAGIC 	sum(NBIoT_AirMacDLByte) NBIoT_AirMacDLByte,
# MAGIC 	sum(NBIoT_AirMacDLByteCnt) NBIoT_AirMacDLByteCnt,
# MAGIC 	sum(NBIoT_AirMacULByte) NBIoT_AirMacULByte,
# MAGIC 	sum(NBIoT_AirMacULByteCnt) NBIoT_AirMacULByteCnt
# MAGIC 	from (
# MAGIC 			select CalendarDate,hour24,NE_ID,LOCATION,split(LOCATION,'/')[0] LOCATION2,
# MAGIC 			sum(NBIoT_AirMacDLByte_Kbytes_SUM) NBIoT_AirMacDLByte,
# MAGIC 			sum(NBIoT_AirMacDLByteCnt_count_SUM) NBIoT_AirMacDLByteCnt,
# MAGIC 			sum(NBIoT_AirMacULByte_Kbytes_SUM) NBIoT_AirMacULByte,
# MAGIC 			sum(NBIoT_AirMacULByteCnt_count_SUM) NBIoT_AirMacULByteCnt
# MAGIC 			-- select *
# MAGIC 			from 
# MAGIC 			hive_metastore.`default`.NBIoTAirMACULandDLpackets_lte_hr_silver
# MAGIC 			--where  CalendarDate='2022-12-19' 
# MAGIC 			group by CalendarDate,hour24,NE_ID,LOCATION
# MAGIC 	) y group by CalendarDate,hour24,NE_ID,LOCATION,LOCATION2
# MAGIC ) x
# MAGIC INNER join v_conf_ecell_silver b  on b.ENODEB_ID=x.NE_ID and b.ECELL_ID=x.LOCATION2 

# COMMAND ----------

gold_kpi = _sqldf
display(gold_kpi)

# COMMAND ----------

Gold_exists = spark.catalog.tableExists(KPI_gold)
print("GOLD exist: " + str(Gold_exists))
  
if str(Gold_exists) == 'True':
    print("GOLD exists Overwriting GOLD") 
    gold_kpi.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(KPI_gold)
else:
    print("GOLD didn't Exist Saving Data as GOLD") 
    gold_kpi.write.format("delta").saveAsTable(KPI_gold)

# COMMAND ----------

spark.sql(f'OPTIMIZE {KPI_gold}')

# COMMAND ----------

sqdf2 = spark.sql(f'select * from  {KPI_gold}')
display(sqdf2)
