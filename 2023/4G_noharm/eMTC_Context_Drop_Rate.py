# Databricks notebook source
KPI_gold = 'eMTC_Context_Drop_Rate_KPI_gold'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(CalendarDate) as Min_Day , max(CalendarDate) as Max_Day ,min(hour24) as Min_Hour ,max(hour24) as Max_Hour FROM 
# MAGIC (
# MAGIC         select CalendarDate,hour24    from  emtccalldrop_lte_hr_silver
# MAGIC )k

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vz_conf_ecell_silver limit 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emtccalldrop_lte_hr_silver limit 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emtcueassociatedlogicals1connectionestablishment_lte_hr_silver limit 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC  with v_conf_ecell_silver as (
# MAGIC select CLUSTER_NAME,REPLACE(ENODEB_ID,'VZ_','eNB_') ENODEB_ID,concat('cNum',ECELL_ID) ECELL_ID,DL_BANDWIDTH,FREQUENCY_BAND_INDICATOR,MARKET_NAME,NETWORK_NAME,SUB_MARKET_ID,SUB_MARKET_NAME from vz_conf_ecell_silver
# MAGIC )
# MAGIC select 
# MAGIC CLUSTER_NAME,ENODEB_ID,ECELL_ID,DL_BANDWIDTH,FREQUENCY_BAND_INDICATOR,MARKET_NAME,NETWORK_NAME,SUB_MARKET_ID,SUB_MARKET_NAME,
# MAGIC Daily,
# MAGIC Hourly,NE_ID,LOCATION,
# MAGIC eMTC_Context_Drop_Rate_NUM,
# MAGIC eMTC_Context_Drop_Rate_DENOM
# MAGIC from (
# MAGIC 	select 
# MAGIC 	cast(CalendarDate as date) as Daily,
# MAGIC 	hour24 as Hourly,NE_ID,LOCATION,	split(LOCATION,'/')[0] xLOCATION,
# MAGIC 	sum(eMTC_Context_Drop_Rate_NUM) eMTC_Context_Drop_Rate_NUM,
# MAGIC 	sum(eMTC_Context_Drop_Rate_DENOM) eMTC_Context_Drop_Rate_DENOM
# MAGIC 	from (
# MAGIC 				select CalendarDate,hour24,NE_ID,LOCATION,
# MAGIC 						sum(eMTC_CallDrop_EccbDspAuditRlcMacCallRelease_count_SUM+
# MAGIC 							eMTC_CallDrop_EccbRcvResetRequestFromEcmb_count_SUM+
# MAGIC 							eMTC_CallDrop_EccbRcvCellReleaseIndFromEcmb_count_SUM+
# MAGIC 							eMTC_CallDrop_EccbRadioLinkFailure_count_SUM+
# MAGIC 							eMTC_CallDrop_EccbDspAuditMacCallRelease_count_SUM+
# MAGIC 							eMTC_CallDrop_EccbArqMaxReTransmission_count_SUM+
# MAGIC 							eMTC_CallDrop_EccbDspAuditRlcCallRelease_count_SUM+
# MAGIC 							eMTC_CallDrop_EccbTmoutRrcConnectionReconfig_count_SUM+
# MAGIC 							eMTC_CallDrop_EccbTmoutRrcConnectionReestablish_count_SUM+
# MAGIC 							eMTC_CallDrop_EccbS1SctpOutOfService_count_SUM) eMTC_Context_Drop_Rate_NUM,
# MAGIC 						0 eMTC_Context_Drop_Rate_DENOM
# MAGIC 				from 
# MAGIC 				emtccalldrop_lte_hr_silver
# MAGIC 				group by CalendarDate,hour24,NE_ID,LOCATION
# MAGIC 			union all
# MAGIC 				select CalendarDate,hour24,NE_ID,LOCATION,
# MAGIC 				0 eMTC_Context_Drop_Rate_NUM,
# MAGIC 				sum(eMTC_S1ConnEstabSucc_count_SUM) eMTC_Context_Drop_Rate_DENOM
# MAGIC 				from 
# MAGIC 				emtcueassociatedlogicals1connectionestablishment_lte_hr_silver
# MAGIC 				group by CalendarDate,hour24,NE_ID,LOCATION
# MAGIC 	) y group by CalendarDate,hour24,NE_ID,LOCATION
# MAGIC ) x
# MAGIC INNER join v_conf_ecell_silver b  on b.ENODEB_ID=x.NE_ID and b.ECELL_ID=x.xLOCATION

# COMMAND ----------

# %sql
# select * from eMTC_Context_Drop_Rate_KPI_gold where NE_ID = eNB_5**** and Ecell_ID = cNum* and FREQUENCY_BAND_INDICATOR = 13 and daily = '23-05-01'

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
