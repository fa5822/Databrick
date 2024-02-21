# Databricks notebook source
KPI_gold = 'eMTC_Total_ERAB_Setup_Failure_Rate_KPI_gold'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(CalendarDate) as Min_Day , max(CalendarDate) as Max_Day ,min(hour24) as Min_Hour ,max(hour24) as Max_Hour FROM 
# MAGIC (
# MAGIC         select CalendarDate,hour24    from  eMTCERABSetupAdd_lte_hr_silver
# MAGIC )k

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(CalendarDate) as Min_Day , max(CalendarDate) as Max_Day ,min(hour24) as Min_Hour ,max(hour24) as Max_Hour FROM 
# MAGIC (
# MAGIC         select CalendarDate,hour24    from  eMTCERABSetup_lte_hr_silver
# MAGIC )k

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vz_conf_ecell_silver limit 1000

# COMMAND ----------

# MAGIC %sql
# MAGIC with v_conf_ecell_silver as (
# MAGIC select CLUSTER_NAME,REPLACE(ENODEB_ID,'VZ_','eNB_') ENODEB_ID,concat('cNum',ECELL_ID) ECELL_ID,DL_BANDWIDTH,FREQUENCY_BAND_INDICATOR,MARKET_NAME,NETWORK_NAME,SUB_MARKET_ID,SUB_MARKET_NAME from vz_conf_ecell_silver
# MAGIC )
# MAGIC select 
# MAGIC CLUSTER_NAME,ENODEB_ID,ECELL_ID,DL_BANDWIDTH,FREQUENCY_BAND_INDICATOR,MARKET_NAME,NETWORK_NAME,SUB_MARKET_ID,SUB_MARKET_NAME,
# MAGIC cast(CalendarDate as date) as Daily,
# MAGIC         hour24 as Hourly,NE_ID,LOCATION,
# MAGIC (eMTC_EstabInitAttNbr + eMTC_EstabAddAttNbr - eMTC_ErabAddFailNbr_CpCcInteraction -  eMTC_EstabInitSuccNbr -  eMTC_EstabAddSuccNbr) eMTC_Total_ERAB_Setup_Failure_Rate_NUM,
# MAGIC (eMTC_EstabInitAttNbr + eMTC_EstabAddAttNbr - eMTC_ErabAddFailNbr_CpCcInteraction) eMTC_Total_ERAB_Setup_Failure_Rate_DENOM
# MAGIC from (
# MAGIC 	select 
# MAGIC 	CalendarDate,hour24,NE_ID,LOCATION,	LOCATION2,
# MAGIC 	sum(eMTC_EstabInitAttNbr) eMTC_EstabInitAttNbr,
# MAGIC 	sum(eMTC_EstabInitSuccNbr) eMTC_EstabInitSuccNbr,
# MAGIC 	sum(eMTC_EstabAddAttNbr) eMTC_EstabAddAttNbr,
# MAGIC 	sum(eMTC_ErabAddFailNbr_CpCcInteraction) eMTC_ErabAddFailNbr_CpCcInteraction,
# MAGIC 	sum(eMTC_EstabAddSuccNbr) eMTC_EstabAddSuccNbr
# MAGIC 	from (
# MAGIC 			select CalendarDate,hour24,NE_ID,concat(Split(LOCATION,'/')[0],'/',Split(LOCATION,'/')[2]) LOCATION,Split(LOCATION,'/')[0] LOCATION2,
# MAGIC 			sum(eMTC_EstabInitAttNbr_count_SUM) eMTC_EstabInitAttNbr,
# MAGIC 			sum(eMTC_EstabInitSuccNbr_count_SUM) eMTC_EstabInitSuccNbr,
# MAGIC 			0 eMTC_EstabAddAttNbr,
# MAGIC 			0 eMTC_ErabAddFailNbr_CpCcInteraction,
# MAGIC 			0 eMTC_EstabAddSuccNbr
# MAGIC 			-- select *
# MAGIC 			from 
# MAGIC 			hive_metastore.`default`.eMTCERABSetup_lte_hr_silver
# MAGIC 			--where  CalendarDate='2022-12-18' 
# MAGIC 			group by CalendarDate,hour24,NE_ID,LOCATION		
# MAGIC 			union all
# MAGIC 			select CalendarDate,hour24,NE_ID,LOCATION,Split(LOCATION,'/')[0] LOCATION2,
# MAGIC 			0 eMTC_EstabInitAttNbr,
# MAGIC 			0 eMTC_EstabInitSuccNbr,
# MAGIC 			sum(eMTC_EstabAddAttNbr_count_SUM) eMTC_EstabAddAttNbr,
# MAGIC 			sum(eMTC_ErabAddFailNbr_CpCcInteraction_count_SUM) eMTC_ErabAddFailNbr_CpCcInteraction,
# MAGIC 			sum(eMTC_EstabAddSuccNbr_count_SUM) eMTC_EstabAddSuccNbr
# MAGIC 			-- select *
# MAGIC 			from 
# MAGIC 			eMTCERABSetupAdd_lte_hr_silver
# MAGIC 			--where  CalendarDate='2022-12-18' 
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

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from EMTC_Total_ERAB_Setup_Failure_Rate_KPI_gold where enodeb_id = 'eNB_100006' and ECELL_ID = 'cNum1' and Daily = '2023-05-01' and Hourly = '19'
