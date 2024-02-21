# Databricks notebook source
KPI_gold = 'VoLTE_IARate_KPI_gold'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from rrcconnectionestablishmentmessage_lte_hr_silver limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from erabsetup_lte_hr_silver limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from erabsetupadd_lte_hr_silver limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM vz_conf_ecell_silver limit 100

# COMMAND ----------

# MAGIC %sql
# MAGIC  with v_conf_enodeb_silver as (
# MAGIC         select distinct CLUSTER_NAME,REPLACE(ENODEB_ID,'VZ_','eNB_') ENODEB_ID,MARKET_NAME,NETWORK_NAME,SUB_MARKET_ID,SUB_MARKET_NAME, FREQUENCY_BAND_INDICATOR from `vz_conf_ecell_silver`
# MAGIC         )
# MAGIC         select 
# MAGIC         CLUSTER_NAME,ENODEB_ID,MARKET_NAME,NETWORK_NAME,SUB_MARKET_ID,SUB_MARKET_NAME,FREQUENCY_BAND_INDICATOR,
# MAGIC 		CalendarDay,
# MAGIC         cast(CalendarDate as date) as Daily,
# MAGIC 		hour24 as Hourly,NE_ID,
# MAGIC 		SIP_Call_Attempts,
# MAGIC 		SIP_Network_Effective_Calls,
# MAGIC 		RrcEstabMsg_RrcConnectionRequest,
# MAGIC 		RrcEstabMsg_RrcConnectionSetupComplete,
# MAGIC 		EstabInitAttNbr_QCI1,
# MAGIC 		EstabInitSuccNbr_QCI1,
# MAGIC 		EstabInitAttNbr_QCI5,
# MAGIC 		EstabInitSuccNbr_QCI5,
# MAGIC 		EstabAddAttNbr_QCI1,
# MAGIC 		ErabAddFailNbr_CpCcInteraction_QCI1,
# MAGIC 		EstabAddSuccNbr_QCI1,
# MAGIC 		EstabAddAttNbr_QCI5,
# MAGIC 		ErabAddFailNbr_CpCcInteraction_QCI5,
# MAGIC 		EstabAddSuccNbr_QCI5,
# MAGIC         (  100.0 *  (  1.0 -  (  (  (  (  1.0 -  ( RRC_Conn_Fail_Rate_pct /  100.0 )  )  *  (  1.0 -  ( ERABSetupFailureRateQCI5_pct /  100.0 )  )  )  * (  1.0 -  ( ERABSetupFailureRateQCI1_pct /  100.0 )  )  )  *  (S1U_SIP_SEER /  100.0 )  )  )  ) VoLTE_IARate_NUM,
# MAGIC 		1 VoLTE_IARate_DENOM
# MAGIC         from (
# MAGIC             select 
# MAGIC             		CalendarDate,CalendarDay,hour24,NE_ID,	
# MAGIC             		sum(SIP_Call_Attempts) SIP_Call_Attempts,
# MAGIC 		            sum(SIP_Network_Effective_Calls) SIP_Network_Effective_Calls,
# MAGIC             		sum(RrcEstabMsg_RrcConnectionRequest) RrcEstabMsg_RrcConnectionRequest,
# MAGIC                 	sum(RrcEstabMsg_RrcConnectionSetupComplete) RrcEstabMsg_RrcConnectionSetupComplete,
# MAGIC             		sum(EstabInitAttNbr_QCI1) EstabInitAttNbr_QCI1,
# MAGIC                     sum(EstabInitSuccNbr_QCI1) EstabInitSuccNbr_QCI1,
# MAGIC                     sum(EstabInitAttNbr_QCI5) EstabInitAttNbr_QCI5,
# MAGIC                     sum(EstabInitSuccNbr_QCI5) EstabInitSuccNbr_QCI5,
# MAGIC                     sum(EstabAddAttNbr_QCI1) EstabAddAttNbr_QCI1,
# MAGIC                     sum(ErabAddFailNbr_CpCcInteraction_QCI1) ErabAddFailNbr_CpCcInteraction_QCI1,
# MAGIC                     sum(EstabAddSuccNbr_QCI1) EstabAddSuccNbr_QCI1,
# MAGIC                     sum(EstabAddAttNbr_QCI5) EstabAddAttNbr_QCI5,
# MAGIC                     sum(ErabAddFailNbr_CpCcInteraction_QCI5) ErabAddFailNbr_CpCcInteraction_QCI5,
# MAGIC                     sum(EstabAddSuccNbr_QCI5) EstabAddSuccNbr_QCI5,
# MAGIC                     100*sum(SIP_Network_Effective_Calls)/sum(SIP_Call_Attempts) S1U_SIP_SEER,
# MAGIC                     100*sum(RrcEstabMsg_RrcConnectionRequest-RrcEstabMsg_RrcConnectionSetupComplete)  /  sum(RrcEstabMsg_RrcConnectionRequest) RRC_Conn_Fail_Rate_pct,
# MAGIC                     100*(sum(EstabInitAttNbr_QCI1+EstabAddAttNbr_QCI1-ErabAddFailNbr_CpCcInteraction_QCI1)-sum(EstabInitSuccNbr_QCI1 + EstabAddSuccNbr_QCI1 ))/sum(EstabInitAttNbr_QCI1+EstabAddAttNbr_QCI1-ErabAddFailNbr_CpCcInteraction_QCI1) ERABSetupFailureRateQCI1_pct,
# MAGIC                     100*(sum(EstabInitAttNbr_QCI5+EstabAddAttNbr_QCI5-ErabAddFailNbr_CpCcInteraction_QCI5)-sum(EstabInitSuccNbr_QCI5 + EstabAddSuccNbr_QCI5 ))/sum(EstabInitAttNbr_QCI5+EstabAddAttNbr_QCI5-ErabAddFailNbr_CpCcInteraction_QCI5) ERABSetupFailureRateQCI5_pct
# MAGIC             from (
# MAGIC             			select date_format(to_date(day, 'MM/dd/yyyy'), 'yyyy-MM-dd') as CalendarDate, date_format(to_date(day, 'MM/dd/yyyy'), 'EEEE') as CalendarDay, cast(hr as int)+1 as hour24,concat('eNB_',ENODEB) NE_ID, 
# MAGIC 					            sum(SIP_Call_Attempts) SIP_Call_Attempts,
# MAGIC 					            sum(SIP_Network_Effective_Calls) SIP_Network_Effective_Calls,
# MAGIC 			                	0 RrcEstabMsg_RrcConnectionRequest,
# MAGIC 			                	0 RrcEstabMsg_RrcConnectionSetupComplete,
# MAGIC 			                    0 EstabInitAttNbr_QCI1,
# MAGIC 			                    0 EstabInitSuccNbr_QCI1,
# MAGIC 			                    0 EstabInitAttNbr_QCI5,
# MAGIC 			                    0 EstabInitSuccNbr_QCI5,
# MAGIC 			                    0 EstabAddAttNbr_QCI1,
# MAGIC 			                    0 ErabAddFailNbr_CpCcInteraction_QCI1,
# MAGIC 			                    0 EstabAddSuccNbr_QCI1,
# MAGIC 			                    0 EstabAddAttNbr_QCI5,
# MAGIC 			                    0 ErabAddFailNbr_CpCcInteraction_QCI5,
# MAGIC 			                    0 EstabAddSuccNbr_QCI5
# MAGIC 			                    from 
# MAGIC 			                    snap.enb_iris.snap_enb_iris_kpis_hourly_bronze_old
# MAGIC 			                    group by `DAY`,CalendarDay,HR,ENODEB		
# MAGIC 			                union all
# MAGIC 			                select CalendarDate,CalendarDay,hour24,NE_ID,
# MAGIC 			                	0 SIP_Call_Attempts,
# MAGIC 					          	0 SIP_Network_Effective_Calls,
# MAGIC 			                	sum(RrcEstabMsg_RrcConnectionRequest_count_SUM) RrcEstabMsg_RrcConnectionRequest,
# MAGIC 			                	sum(RrcEstabMsg_RrcConnectionSetupComplete_count_SUM) RrcEstabMsg_RrcConnectionSetupComplete,
# MAGIC 			                    0 EstabInitAttNbr_QCI1,
# MAGIC 			                    0 EstabInitSuccNbr_QCI1,
# MAGIC 			                    0 EstabInitAttNbr_QCI5,
# MAGIC 			                    0 EstabInitSuccNbr_QCI5,
# MAGIC 			                    0 EstabAddAttNbr_QCI1,
# MAGIC 			                    0 ErabAddFailNbr_CpCcInteraction_QCI1,
# MAGIC 			                    0 EstabAddSuccNbr_QCI1,
# MAGIC 			                    0 EstabAddAttNbr_QCI5,
# MAGIC 			                    0 ErabAddFailNbr_CpCcInteraction_QCI5,
# MAGIC 			                    0 EstabAddSuccNbr_QCI5
# MAGIC 			                    from 
# MAGIC 			                    rrcconnectionestablishmentmessage_lte_hr_silver
# MAGIC 			                    group by CalendarDate,CalendarDay,hour24,NE_ID		
# MAGIC 			                union all    
# MAGIC 			            	select CalendarDate,CalendarDay,hour24,NE_ID,
# MAGIC 			            		0 SIP_Call_Attempts,
# MAGIC 					          	0 SIP_Network_Effective_Calls,
# MAGIC 			            		0 RrcEstabMsg_RrcConnectionRequest,
# MAGIC 			                	0 RrcEstabMsg_RrcConnectionSetupComplete,
# MAGIC 			                    case when split (LOCATION,'/')[2]= 'QCI1' then sum(EstabInitAttNbr_count_SUM) end EstabInitAttNbr_QCI1,
# MAGIC 			                    case when split (LOCATION,'/')[2]= 'QCI1' then sum(EstabInitSuccNbr_count_SUM) end EstabInitSuccNbr_QCI1,
# MAGIC 			                    case when split (LOCATION,'/')[2]= 'QCI5' then sum(EstabInitAttNbr_count_SUM) end EstabInitAttNbr_QCI5,
# MAGIC 			                    case when split (LOCATION,'/')[2]= 'QCI5' then sum(EstabInitSuccNbr_count_SUM) end EstabInitSuccNbr_QCI5,
# MAGIC 			                    0 EstabAddAttNbr_QCI1,
# MAGIC 			                    0 ErabAddFailNbr_CpCcInteraction_QCI1,
# MAGIC 			                    0 EstabAddSuccNbr_QCI1,
# MAGIC 			                    0 EstabAddAttNbr_QCI5,
# MAGIC 			                    0 ErabAddFailNbr_CpCcInteraction_QCI5,
# MAGIC 			                    0 EstabAddSuccNbr_QCI5
# MAGIC 			                    from 
# MAGIC 			                    erabsetup_lte_hr_silver
# MAGIC 			                    where  split (LOCATION,'/')[2] in ('QCI1','QCI5') 
# MAGIC 			                    group by CalendarDate,CalendarDay,hour24,NE_ID,LOCATION		
# MAGIC 			                union all
# MAGIC 			                    select CalendarDate,CalendarDay,hour24,NE_ID,	
# MAGIC 			                    0 SIP_Call_Attempts,
# MAGIC 					          	0 SIP_Network_Effective_Calls,
# MAGIC 			                    0 RrcEstabMsg_RrcConnectionRequest,
# MAGIC 			                	0 RrcEstabMsg_RrcConnectionSetupComplete,
# MAGIC 			                    0 EstabInitAttNbr_QCI1,
# MAGIC 			                    0 EstabInitSuccNbr_QCI1,
# MAGIC 			                    0 EstabInitAttNbr_QCI5,
# MAGIC 			                    0 EstabInitSuccNbr_QCI5,
# MAGIC 								case when split (LOCATION,'/')[1]= 'QCI1' then sum(EstabAddAttNbr_count_SUM) end EstabAddAttNbr_QCI1,
# MAGIC 								case when split (LOCATION,'/')[1]= 'QCI1' then sum(ErabAddFailNbr_CpCcInteraction_count_SUM) end ErabAddFailNbr_CpCcInteraction_QCI1,
# MAGIC 								case when split (LOCATION,'/')[1]= 'QCI1' then sum(EstabAddSuccNbr_count_SUM) end EstabAddSuccNbr_QCI1,
# MAGIC 								case when split (LOCATION,'/')[1]= 'QCI5' then sum(EstabAddAttNbr_count_SUM) end EstabAddAttNbr_QCI5,
# MAGIC 								case when split (LOCATION,'/')[1]= 'QCI5' then sum(ErabAddFailNbr_CpCcInteraction_count_SUM) end ErabAddFailNbr_CpCcInteraction_QCI5,
# MAGIC 								case when split (LOCATION,'/')[1]= 'QCI5' then sum(EstabAddSuccNbr_count_SUM) end EstabAddSuccNbr_QCI5
# MAGIC 			                    from 
# MAGIC 			                    erabsetupadd_lte_hr_silver
# MAGIC 			                    where  split (LOCATION,'/')[1] in ('QCI1','QCI5') 
# MAGIC 			                    group by CalendarDate,CalendarDay,hour24,NE_ID,LOCATION		
# MAGIC             ) y group by CalendarDate,CalendarDay,hour24,NE_ID
# MAGIC         ) x
# MAGIC INNER join v_conf_enodeb_silver b  on b.ENODEB_ID=x.NE_ID 

# COMMAND ----------

gold_kpi = _sqldf

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
# MAGIC select * from volte_iarate_kpi_gold where VoLTE_IARate_NUM is not null
