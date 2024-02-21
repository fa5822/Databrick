# Databricks notebook source
KPI_gold = 'NBIoT_Total_ERAB_Drop_Rate_KPI_gold'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(CalendarDate) as Min_Day , max(CalendarDate) as Max_Day ,min(hour24) as Min_Hour ,max(hour24) as Max_Hour FROM 
# MAGIC (
# MAGIC         select CalendarDate,hour24    from  nbiotcalldrop_lte_hr_silver
# MAGIC )k

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vz_conf_ecell_silver limit 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC  with v_conf_ecell_silver as (
# MAGIC select CLUSTER_NAME,REPLACE(ENODEB_ID,'VZ_','eNB_') ENODEB_ID,concat('NBIoT_cNum',ECELL_ID) ECELL_ID,DL_BANDWIDTH,FREQUENCY_BAND_INDICATOR,MARKET_NAME,NETWORK_NAME,SUB_MARKET_ID,SUB_MARKET_NAME from vz_conf_ecell_silver
# MAGIC )
# MAGIC select 
# MAGIC CLUSTER_NAME,ENODEB_ID,ECELL_ID,DL_BANDWIDTH,FREQUENCY_BAND_INDICATOR,MARKET_NAME,NETWORK_NAME,SUB_MARKET_ID,SUB_MARKET_NAME,
# MAGIC cast(CalendarDate as date) as Daily,
# MAGIC         hour24 as Hourly,NE_ID,LOCATION,
# MAGIC NBIoT_Total_ERAB_Drop_Rate_NUM ,
# MAGIC (NBIoT_EstabInitSuccNbr+NBIoT_EstabAddSuccNbr) NBIoT_Total_ERAB_Drop_Rate_DENOM
# MAGIC from (
# MAGIC 	select 
# MAGIC 	CalendarDate,hour24,NE_ID,LOCATION,	
# MAGIC 	sum(NBIoT_Total_ERAB_Drop_Rate_NUM) NBIoT_Total_ERAB_Drop_Rate_NUM,
# MAGIC 	sum(NBIoT_EstabInitSuccNbr) NBIoT_EstabInitSuccNbr,
# MAGIC 	sum(NBIoT_EstabAddSuccNbr) NBIoT_EstabAddSuccNbr
# MAGIC 	from (
# MAGIC 				select CalendarDate,hour24,NE_ID,split(LOCATION,'/')[0] LOCATION,
# MAGIC 						sum(NBIoT_CallDrop_NccbArqMaxReTransmission_count_SUM+
# MAGIC 						NBIoT_CallDrop_NccbDspAuditMacCallRelease_count_SUM+
# MAGIC 						NBIoT_CallDrop_NccbDspAuditRlcCallRelease_count_SUM+
# MAGIC 						NBIoT_CallDrop_NccbDspAuditRlcMacCallRelease_count_SUM+
# MAGIC 						NBIoT_CallDrop_NccbRadioLinkFailure_count_SUM+
# MAGIC 						NBIoT_CallDrop_NccbRcvCellReleaseIndFromNcmb_count_SUM+
# MAGIC 						NBIoT_CallDrop_NccbRcvResetRequestFromNcmb_count_SUM+
# MAGIC 						NBIoT_CallDrop_NccbS1SctpOutOfService_count_SUM+
# MAGIC 						NBIoT_CallDrop_NccbTmoutRrcConnectionReconfig_count_SUM+
# MAGIC 						NBIoT_CallDrop_NccbTmoutRrcConnectionReestablish_count_SUM) NBIoT_Total_ERAB_Drop_Rate_NUM,
# MAGIC 						0 NBIoT_EstabInitSuccNbr,
# MAGIC 						0 NBIoT_EstabAddSuccNbr
# MAGIC 				-- select *
# MAGIC 				from 
# MAGIC 				hive_metastore.`default`.nbiotcalldrop_lte_hr_silver    -- NBIoT_cNum6/MO_SIGNAL
# MAGIC 				--where  CalendarDate='2022-12-18' 
# MAGIC 				group by CalendarDate,hour24,NE_ID,split(LOCATION,'/')[0]
# MAGIC 			union all
# MAGIC 				select CalendarDate,hour24,NE_ID,split(LOCATION,'/')[0] LOCATION,
# MAGIC 				0 NBIoT_Total_ERAB_Drop_Rate_NUM,
# MAGIC 				sum(NBIoT_EstabInitSuccNbr_count_SUM) NBIoT_EstabInitSuccNbr,  -- NBIoT_cNum1/MT_ACCESS/QCI2
# MAGIC 				0 NBIoT_EstabAddSuccNbr
# MAGIC 				-- select *
# MAGIC 				from 
# MAGIC 				hive_metastore.`default`.nbioterabsetup_lte_hr_silver
# MAGIC 				--where  CalendarDate='2022-12-18' 
# MAGIC 				group by CalendarDate,hour24,NE_ID,split(LOCATION,'/')[0]
# MAGIC 			union all
# MAGIC 				select CalendarDate,hour24,NE_ID, split(LOCATION,'/')[0] LOCATION,
# MAGIC 				0 NBIoT_Total_ERAB_Drop_Rate_NUM,
# MAGIC 				0 NBIoT_EstabInitSuccNbr,
# MAGIC 				sum(NBIoT_EstabAddSuccNbr_count_SUM) NBIoT_EstabAddSuccNbr
# MAGIC 				-- select *
# MAGIC 				from 
# MAGIC 				hive_metastore.`default`.nbioterabsetupadd_lte_hr_silver    -- NBIoT_cNum2/QCI9
# MAGIC 				--where  CalendarDate='2022-12-18' 
# MAGIC 				group by CalendarDate,hour24,NE_ID,split(LOCATION,'/')[0]
# MAGIC 	) y group by CalendarDate,hour24,NE_ID,LOCATION
# MAGIC ) x
# MAGIC INNER join v_conf_ecell_silver b  on b.ENODEB_ID=x.NE_ID and b.ECELL_ID=x.LOCATION

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
