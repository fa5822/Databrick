# Databricks notebook source
KPI_gold = 'PUSCH_SINR_KPI_gold'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT min(CalendarDate) as Min_Day , max(CalendarDate) as Max_Day ,min(hour24) as Min_Hour ,max(hour24) as Max_Hour FROM 
# MAGIC (
# MAGIC         select CalendarDate,hour24    from  SINRDistribution_lte_hr_silver
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
# MAGIC cast(CalendarDate as date) as Daily,
# MAGIC         hour24 as Hourly,NE_ID,LOCATION,
# MAGIC PUSCH_SINR_NUM,
# MAGIC PUSCH_SINR_DENOM
# MAGIC from (
# MAGIC 	select 
# MAGIC 	CalendarDate,hour24,NE_ID,LOCATION,	
# MAGIC 	sum(PUSCH_SINR_NUM) PUSCH_SINR_NUM,
# MAGIC 	sum(PUSCH_SINR_DENOM) PUSCH_SINR_DENOM
# MAGIC 	from (
# MAGIC 			select CalendarDate,hour24,NE_ID,LOCATION,
# MAGIC 			sum(
# MAGIC 			SinrDistULWbPreComp_Bin0_count_SUM*power(10,-9/10) +
# MAGIC 			SinrDistULWbPreComp_Bin1_count_SUM*power(10,-7/10) +
# MAGIC 			SinrDistULWbPreComp_Bin2_count_SUM*power(10,-5/10) +
# MAGIC 			SinrDistULWbPreComp_Bin3_count_SUM*power(10,-3/10) +
# MAGIC 			SinrDistULWbPreComp_Bin4_count_SUM*power(10,-1/10) +
# MAGIC 			SinrDistULWbPreComp_Bin5_count_SUM*power(10,1/10) +
# MAGIC 			SinrDistULWbPreComp_Bin6_count_SUM*power(10,3/10) +
# MAGIC 			SinrDistULWbPreComp_Bin7_count_SUM*power(10,5/10) +
# MAGIC 			SinrDistULWbPreComp_Bin8_count_SUM*power(10,7/10) +
# MAGIC 			SinrDistULWbPreComp_Bin9_count_SUM*power(10,9/10) +
# MAGIC 			SinrDistULWbPreComp_Bin10_count_SUM*power(10,11/10) +
# MAGIC 			SinrDistULWbPreComp_Bin11_count_SUM*power(10,13/10) +
# MAGIC 			SinrDistULWbPreComp_Bin12_count_SUM*power(10,15/10) +
# MAGIC 			SinrDistULWbPreComp_Bin13_count_SUM*power(10,17/10) +
# MAGIC 			SinrDistULWbPreComp_Bin14_count_SUM*power(10,19/10) +
# MAGIC 			SinrDistULWbPreComp_Bin15_count_SUM*power(10,21/10) +
# MAGIC 			SinrDistULWbPreComp_Bin16_count_SUM*power(10,23/10) +
# MAGIC 			SinrDistULWbPreComp_Bin17_count_SUM*power(10,25/10) +
# MAGIC 			SinrDistULWbPreComp_Bin18_count_SUM*power(10,27/10) +
# MAGIC 			SinrDistULWbPreComp_Bin19_count_SUM*power(10,29/10) 
# MAGIC 			) PUSCH_SINR_NUM 
# MAGIC 			,
# MAGIC 			sum(
# MAGIC 			SinrDistULWbPreComp_Bin0_count_SUM+
# MAGIC 			SinrDistULWbPreComp_Bin1_count_SUM+
# MAGIC 			SinrDistULWbPreComp_Bin2_count_SUM+
# MAGIC 			SinrDistULWbPreComp_Bin3_count_SUM+
# MAGIC 			SinrDistULWbPreComp_Bin4_count_SUM+
# MAGIC 			SinrDistULWbPreComp_Bin5_count_SUM+
# MAGIC 			SinrDistULWbPreComp_Bin6_count_SUM+
# MAGIC 			SinrDistULWbPreComp_Bin7_count_SUM+
# MAGIC 			SinrDistULWbPreComp_Bin8_count_SUM+
# MAGIC 			SinrDistULWbPreComp_Bin9_count_SUM+
# MAGIC 			SinrDistULWbPreComp_Bin10_count_SUM+
# MAGIC 			SinrDistULWbPreComp_Bin11_count_SUM+
# MAGIC 			SinrDistULWbPreComp_Bin12_count_SUM+
# MAGIC 			SinrDistULWbPreComp_Bin13_count_SUM+
# MAGIC 			SinrDistULWbPreComp_Bin14_count_SUM+
# MAGIC 			SinrDistULWbPreComp_Bin15_count_SUM+
# MAGIC 			SinrDistULWbPreComp_Bin16_count_SUM+
# MAGIC 			SinrDistULWbPreComp_Bin17_count_SUM+
# MAGIC 			SinrDistULWbPreComp_Bin18_count_SUM+
# MAGIC 			SinrDistULWbPreComp_Bin19_count_SUM
# MAGIC 			) PUSCH_SINR_DENOM
# MAGIC 			-- select *
# MAGIC 			from 
# MAGIC 			SINRDistribution_lte_hr_silver
# MAGIC 			--where  CalendarDate='2022-12-18' 
# MAGIC 			group by CalendarDate,hour24,NE_ID,LOCATION
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
