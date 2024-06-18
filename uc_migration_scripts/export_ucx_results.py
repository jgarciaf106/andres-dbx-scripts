# Databricks notebook source
# MAGIC %md
# MAGIC ### Instructions:
# MAGIC 1. Use DBR 14 all purpose cluster
# MAGIC 1. Hit "Run all" button and wait for completion
# MAGIC 1. Go to the bottom of the notebook and click the download link

# COMMAND ----------

# DBTITLE 1,Installing Packages
# MAGIC %pip install openpyxl
# MAGIC %pip install databricks-sdk --upgrade -q -q -q

# COMMAND ----------

# DBTITLE 1,Restart Python Library
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Import Libraries and Databricks SDK
# import libraries
import json, re, time, os, re, shutil
import pandas as pd
import openpyxl
from databricks.sdk import WorkspaceClient

# COMMAND ----------

# DBTITLE 1,UCX Assessment Export
class UCXAssessment:
    def __init__(self):
        self.workspace_client = WorkspaceClient()
        self.workspace_id = self.workspace_client.get_workspace_id()
        self.host = f"https://{self.workspace_client.dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()}"
        self.tmp_path = "/Workspace/Applications/ucx/ucx_results"
        self.download_path = "/dbfs/FileStore/ucx_results"
        self.file_name = "ucx_assessment_results.xlsx"

    def _get_ucx_assessment_queries(self):
        all_queries = self.workspace_client.queries.list(q="[UCX] UCX")

        additional_queries = [
            {
                "id": -1,
                "name": "[UCX] UCX Assessment (Main) - 01_1_ucx_permissions.sql;",
                "query": "SELECT * FROM hive_metastore.ucx.permissions",
            },
            {
                "id": -2,
                "name": "[UCX] UCX Assessment (Main) - 02_2_ucx_grants.sql",
                "query": "SELECT * FROM hive_metastore.ucx.grants;",
            },
            {
                "id": -3,
                "name": "[UCX] UCX Assessment (Main) - 03_3_ucx_groups.sql",
                "query": "SELECT * FROM hive_metastore.ucx.groups;",
            },
        ]

        ucx_assessment_queries = []
        for query in all_queries:
            ## The double blank after UCX is not a typo, please leave as is
            if (
                query.name.startswith("[UCX] UCX  Assessment (Main) - ")
                and "count" not in query.name
                and "compatibility" not in query.name
            ):
                ## extract only the attributes needed
                extracted = {
                    key: getattr(query, key) for key in ("id", "query", "name")
                }
                ucx_assessment_queries.append(extracted)

        ucx_assessment_queries.extend(additional_queries)

        return ucx_assessment_queries

    def _prepare_directories(self):
        if not os.path.exists(self.tmp_path):
            os.makedirs(self.tmp_path)

        if not os.path.exists(self.download_path):
            os.makedirs(self.download_path)

    def _cleanup(self, tmp_file_path):
        shutil.move(tmp_file_path, f"{self.download_path}/{self.file_name}")
        shutil.rmtree(self.tmp_path)

    def export_results(self):

        self._prepare_directories()

        tmp_file_path = f"{self.tmp_path}/{self.file_name}"
        extract_name_pattern = re.compile(r"- \d+_\d+_(.*)\.sql")
        results = self._get_ucx_assessment_queries()

        try:
            with pd.ExcelWriter(tmp_file_path, engine="openpyxl") as writer:
                for result in results:
                    match = extract_name_pattern.search(result["name"])
                    if match:
                        sheet_name = match.group(1)
                        sdf = spark.sql(result["query"])
                        if sdf.count() > 0:
                            df = sdf.toPandas()
                            df.to_excel(writer, sheet_name=sheet_name, index=False)

            self._cleanup(tmp_file_path)

            download_link = (
                f"{self.host}/files/ucx_results/{self.file_name}?o={self.workspace_id}"
            )

            displayHTML(
                f"""
                <h2>Click to Export the Results</h2>
                <a href='{download_link}' target='_blank' download>Export UCX Results</a>
            """
            )
        except IndexError:
            print("No data to export at this time")

# COMMAND ----------

# run the notebook
assessment = UCXAssessment()
assessment.export_results()
