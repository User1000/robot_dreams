#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark.sql import SparkSession
import pyspark.sql.functions as F

import psycopg2
from hdfs import InsecureClient

import os
from datetime import datetime


# In[2]:


spark = SparkSession.builder    .config('spark.driver.extraClassPath'
            , '/mnt/share/postgresql-42.3.1.jar')\
    .master('local')\
    .appName("lesson")\
    .getOrCreate()


# In[3]:


pg_url = "jdbc:postgresql://localhost:5432/postgres"
pg_properties = {"user": "pguser", "password": "secret"}


# In[61]:


film_df = spark.read.jdbc(pg_url, table="film", properties = pg_properties)
film_category_df = spark.read.jdbc(pg_url, table="film_category", properties = pg_properties)
actor_df = spark.read.jdbc(pg_url, table="actor", properties = pg_properties)
film_actor_df = spark.read.jdbc(pg_url, table="film_actor", properties = pg_properties)
inventory_df = spark.read.jdbc(pg_url, table="inventory", properties = pg_properties)
rental_df = spark.read.jdbc(pg_url, table="rental", properties = pg_properties)
payment_df = spark.read.jdbc(pg_url, table="payment", properties = pg_properties)
city_df = spark.read.jdbc(pg_url, table="city", properties = pg_properties)
address_df = spark.read.jdbc(pg_url, table="address", properties = pg_properties)
customer_df = spark.read.jdbc(pg_url, table="customer", properties = pg_properties)


# In[26]:


# вывести количество фильмов в каждой категории, отсортировать по убыванию.

category_df    .join(film_category_df, 'category_id')    .groupBy('name')    .count()    .orderBy(F.desc('count'))    .show()


# In[35]:


# вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.

actor_df    .join(film_actor_df, 'actor_id')    .join(inventory_df, 'film_id')    .join(rental_df, 'inventory_id')    .groupBy('actor_id', 'first_name', 'last_name')    .agg(F.countDistinct(F.col('rental_id')).alias('rental_count'))    .orderBy(F.desc('rental_count'))    .limit(10)    .show()


# In[40]:


# вывести категорию фильмов, на которую потратили больше всего денег.

category_df    .join(film_category_df, 'category_id')    .join(inventory_df, 'film_id')    .join(rental_df, 'inventory_id')    .join(payment_df, 'rental_id')    .groupBy('category_id')    .agg(F.sum('amount').alias('category_amount'))    .orderBy(F.desc('category_amount'))    .limit(10)    .show()


# In[44]:


# вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN.

film_df    .join(inventory_df, 'film_id', 'leftanti')    .select('title')    .show()


# In[53]:


# вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.


from pyspark.sql.window import Window
from pyspark.sql.functions import rank
windowSpec  = Window.partitionBy().orderBy("films_count")

actor_df    .join(film_actor_df, 'actor_id')    .join(film_category_df, 'film_id')    .join(category_df, 'category_id')    .where("name = 'Children'")    .groupBy('actor_id', 'first_name', 'last_name')    .agg(F.count('film_id').alias('films_count'))    .withColumn("rank",rank().over(windowSpec))    .where('rank <= 3')    .select('actor_id', 'first_name', 'last_name')    .show()


# In[65]:


# вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию.

city_df    .join(address_df, 'city_id')    .join(customer_df, 'address_id')    .groupBy('city_id')    .agg(
        F.count(F.when(F.col('active') == 1, F.lit(1)).otherwise(F.lit(None))).alias('active_count'),
        F.count(F.when(F.col('active') == 0, F.lit(1)).otherwise(F.lit(None))).alias('inactive_count')
    )\
    .orderBy(F.desc('active_count'))\
    .show()


# In[80]:


#вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), 
#и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.

(category_df
     .join(film_category_df, 'category_id', 'left')
     .join(inventory_df, 'film_id', 'left')
     .join(rental_df, 'inventory_id', 'left')
     .join(customer_df, 'customer_id', 'left')
     .join(address_df, 'address_id', 'left')
     .join(city_df, 'city_id', 'inner')
     .where('city like "a%"')
     .groupBy('category_id')
     .agg(
         F.sum((F.col('return_date').cast("long") - F.col('rental_date').cast("long")) / 3600).alias('hours')
     )
     .orderBy(F.desc('hours'))
     .limit(1)
).unionByName(
category_df
     .join(film_category_df, 'category_id', 'left')
     .join(inventory_df, 'film_id', 'left')
     .join(rental_df, 'inventory_id', 'left')
     .join(customer_df, 'customer_id', 'left')
     .join(address_df, 'address_id', 'left')
     .join(city_df, 'city_id', 'inner')
     .where('city like "%-%"')
     .groupBy('category_id')
     .agg(
         F.sum((F.col('return_date').cast("long") - F.col('rental_date').cast("long")) / 3600).alias('hours')
     )
     .orderBy(F.desc('hours'))
     .limit(1)
).show()

