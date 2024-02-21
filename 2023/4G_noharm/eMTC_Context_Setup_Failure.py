# Databricks notebook source
KPI_gold = 'eMTC_Context_Setup_Failure_KPI_gold'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(CalendarDate) as Min_Day , max(CalendarDate) as Max_Day ,min(hour24) as Min_Hour ,max(hour24) as Max_Hour FROM 
# MAGIC (
# MAGIC         select CalendarDate,hour24    from  emtcueassociatedlogicals1connectionestablishment_lte_hr_silver
# MAGIC )k

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vz_conf_ecell_silver limit 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC  with v_conf_ecell_silver as (
# MAGIC select CLUSTER_NAME,REPLACE(ENODEB_ID,'VZ_','eNB_') ENODEB_ID,concat('cNum',ECELL_ID) ECELL_ID,DL_BANDWIDTH,FREQUENCY_BAND_INDICATOR,MARKET_NAME,NETWORK_NAME,SUB_MARKET_ID,SUB_MARKET_NAME from vz_conf_ecell_silver
# MAGIC )
# MAGIC select 
# MAGIC CLUSTER_NAME,ENODEB_ID,ECELL_ID,DL_BANDWIDTH,FREQUENCY_BAND_INDICATOR,MARKET_NAME,NETWORK_NAME,SUB_MARKET_ID,SUB_MARKET_NAME,
# MAGIC Daily,hourly,NE_ID,LOCATION,
# MAGIC (eMTC_S1ConnEstabAtt-eMTC_S1ConnEstabSucc)  eMTC_Context_Setup_Failure_NUM,
# MAGIC eMTC_S1ConnEstabAtt eMTC_Context_Setup_Failure_DENOM
# MAGIC from (
# MAGIC 	select 
# MAGIC 	cast(CalendarDate as date) as Daily,
# MAGIC 	hour24 as Hourly,NE_ID,LOCATION,	split(LOCATION,'/')[0] xLOCATION,
# MAGIC 	sum(eMTC_S1ConnEstabAtt) eMTC_S1ConnEstabAtt,
# MAGIC 	sum(eMTC_S1ConnEstabSucc) eMTC_S1ConnEstabSucc
# MAGIC 	from (
# MAGIC 				select CalendarDate,hour24,NE_ID,LOCATION,
# MAGIC 					sum(eMTC_S1ConnEstabAtt_count_SUM) eMTC_S1ConnEstabAtt,
# MAGIC 					sum(eMTC_S1ConnEstabSucc_count_SUM) eMTC_S1ConnEstabSucc
# MAGIC 				-- select *
# MAGIC 				from 
# MAGIC 				emtcueassociatedlogicals1connectionestablishment_lte_hr_silver    -- cNum2/MO_VOICE_CALL
# MAGIC 				--where  CalendarDate='2022-12-18' 
# MAGIC 				group by CalendarDate,hour24,NE_ID,LOCATION
# MAGIC 	) y group by CalendarDate,hour24,NE_ID,LOCATION
# MAGIC ) x
# MAGIC INNER join v_conf_ecell_silver b  on b.ENODEB_ID=x.NE_ID and b.ECELL_ID=x.xLOCATION

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
