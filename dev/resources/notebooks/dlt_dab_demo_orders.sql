-- Databricks notebook source
--create streaming table
CREATE OR REFRESH STREAMING TABLE st_order
AS
SELECT * FROM STREAM(samples.tpch.orders)

-- COMMAND ----------

--Create MV
CREATE OR REFRESH MATERIALIZED VIEW agg_order
AS
SELECT count(o_orderkey) as cnt_orders,o_orderstatus from LIVE.st_order 
group by o_orderstatus

-- COMMAND ----------

-- select * from `test-neha-catalog`.dev_kriti_d_bhardwajbronze.st_order
