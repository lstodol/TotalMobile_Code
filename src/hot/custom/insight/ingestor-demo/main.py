# Databricks notebook source
from storage.repository import new_repository
from usecase.interactor import new_interactor
from handler.processor import new_processor

# COMMAND ----------

r = new_repository()
i = new_interactor(r)
p = new_processor(i)


# COMMAND ----------

try:
    p.handler()
except Exception as e:
    raise e