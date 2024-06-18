# Databricks notebook source
# DBTITLE 1,Upgrade Databricks-SDK Installation
# MAGIC %pip install databricks-sdk --upgrade -q -q -q

# COMMAND ----------

# DBTITLE 1,Python Library Restart
dbutils.library.restartPython()

# COMMAND ----------

# DBTITLE 1,Library Imports and Databricks Setup
# import libraries
import json, re, time, os, re, shutil
import pandas as pd
from databricks.sdk import WorkspaceClient
from pyspark.sql.functions import count, col

# COMMAND ----------

# DBTITLE 1,Query Manager
class QueryManager:
    def __init__(self, database):
        self.database = database
        self.queries = self._set_ucx_queries()

    def select_statement(self, query_name):
        try:
            query_to_run = next(
                query["query"] for query in self.queries if query["name"] == query_name
            )
            return spark.sql(query_to_run)
        except Exception as e:
            print(f"An error occurred while executing the query: {query_name}")
            print(f"Error details: {str(e)}")

    def _set_ucx_queries(self):
        queries = [
            {
                "name": "permissions",
                "query": f"SELECT * FROM hive_metastore.{self.database}.permissions",
            },
            {
                "name": "grants",
                "query": f"SELECT * FROM hive_metastore.{self.database}.grants;",
            },
            {
                "name": "groups",
                "query": f"SELECT * FROM hive_metastore.{self.database}groups;",
            },
            {
                "name": "migration_type_count",
                "query": f"""
                    WITH tables as (
                        SELECT
                            object_type AS type,
                            CASE
                            WHEN UPPER(table_format) IN (
                                'CSV',
                                'JSON',
                                'AVRO',
                                'ORC',
                                'TEXT',
                                'COM.DATABRICKS.SPARK.CSV',
                                'COM.DATABRICKS.SPARK.XML',
                                'XML'
                            ) THEN 'CSV/JSON/AVRO/ORC/TEXT/XML/SPARK.CSV/SPARK.XML'
                            WHEN UPPER(table_format) IN (
                                'MYSQL',
                                'SQLSERVER',
                                'SNOWFLAKE',
                                'ORG.APACHE.SPARK.SQL.JDBC',
                                'POSTGRESQL'
                            ) THEN 'JDBC'
                            ELSE UPPER(table_format)
                            END AS format,
                            CASE
                            WHEN STARTSWITH(location, "dbfs:/mnt") THEN "DBFS MOUNT"
                            WHEN STARTSWITH(location, "/dbfs/mnt") THEN "DBFS MOUNT"
                            WHEN STARTSWITH(location, "dbfs:/databricks-datasets") THEN "Databricks Demo Dataset"
                            WHEN STARTSWITH(location, "/dbfs/databricks-datasets") THEN "Databricks Demo Dataset"
                            WHEN STARTSWITH(location, "dbfs:/") THEN "DBFS ROOT"
                            WHEN STARTSWITH(location, "/dbfs/") THEN "DBFS ROOT"
                            WHEN STARTSWITH(location, "wasb") THEN "UNSUPPORTED"
                            WHEN STARTSWITH(location, "adl") THEN "UNSUPPORTED"
                            ELSE "EXTERNAL"
                            END AS storage,
                            IF(table_format = "DELTA", "Yes", "No") AS is_delta,
                            location
                        FROM
                            hive_metastore.{self.database}.tables
                        ),
                        table_by_group AS (
                        SELECT
                            CASE 
                                WHEN storage = 'DBFS ROOT' THEN 'DEEP CLONE/CTAS'
                                WHEN type = 'MANAGED' AND format = 'HIVE' AND storage = 'EXTERNAL' THEN 'DEEP CLONE/CTAS'
                                WHEN type = 'EXTERNAL' AND format = 'HIVE' AND storage = 'EXTERNAL' THEN 'DEEP CLONE/CTAS'
                                WHEN type = 'MANAGED' AND format = 'HIVE' AND storage = 'DBFS MOUNT' THEN 'DEEP CLONE/CTAS'
                                WHEN type = 'EXTERNAL' AND format = 'HIVE' AND storage = 'DBFS MOUNT' THEN 'DEEP CLONE/CTAS'
                                WHEN type = 'MANAGED' AND format = 'DELTA' AND storage = 'EXTERNAL' THEN 'SYNC'
                                WHEN type = 'MANAGED' AND format = 'DELTA' AND storage = 'DBFS MOUNT' THEN 'SYNC'
                                WHEN type = 'MANAGED' AND format = 'PARQUET' AND storage = 'EXTERNAL' THEN 'SYNC'
                                WHEN type = 'MANAGED' AND format = 'PARQUET' AND storage = 'DBFS MOUNT' THEN 'SYNC'
                                WHEN type = 'MANAGED' AND format = 'CSV/JSON/AVRO/ORC/TEXT/XML/SPARK.CSV/SPARK.XML' AND storage = 'EXTERNAL' THEN 'SYNC'
                                WHEN type = 'MANAGED' AND format = 'CSV/JSON/AVRO/ORC/TEXT/XML/SPARK.CSV/SPARK.XML' AND storage = 'DBFS MOUNT' THEN 'SYNC'
                                WHEN type = 'EXTERNAL' AND format = 'DELTA' AND storage = 'DBFS MOUNT' THEN 'SYNC'
                                WHEN type = 'EXTERNAL' AND format = 'PARQUET' AND storage = 'EXTERNAL' THEN 'SYNC'
                                WHEN type = 'EXTERNAL' AND format = 'PARQUET' AND storage = 'DBFS MOUNT' THEN 'SYNC'
                                WHEN type = 'EXTERNAL' AND format = 'CSV/JSON/AVRO/ORC/TEXT/XML/SPARK.CSV/SPARK.XML' AND storage = 'EXTERNAL' THEN 'SYNC'
                                WHEN type = 'EXTERNAL' AND format = 'CSV/JSON/AVRO/ORC/TEXT/XML/SPARK.CSV/SPARK.XML' AND storage = 'DBFS MOUNT' THEN 'SYNC'
                                WHEN format = 'JDBC' THEN 'LAKE HOUSE FEDERATION (READ-ONLY)' 
                                WHEN storage = 'UNSUPPORTED' THEN 'CTAS' 
                                WHEN type = 'VIEW' THEN 'CREATE VIEW' 
                                ELSE 'ANALYZE MANUALLY'
                            END AS migration_type,
                            type,
                            format,
                            storage
                            FROM
                            tables
                        )
                        SELECT
                        DISTINCT
                        migration_type,
                        COUNT(*) OVER(PARTITION BY migration_type) AS table_count
                        FROM table_by_group;
            """,
            },
        ]

        return queries

# COMMAND ----------

# DBTITLE 1,Documentation Retrieval
class Documentation:
    def __init__(self):
        self.workspace_client = WorkspaceClient()
        self.config = self.workspace_client.config
        self.official_docs = {
            "MIGRATION OVERVIEW": {
                "azure": "https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/migrate",
                "aws": "https://docs.databricks.com/en/data-governance/unity-catalog/migrate.html#hive-to-unity-catalog-migration-options",
                "gcp": "https://docs.gcp.databricks.com/en/data-governance/unity-catalog/migrate.html?_ga=2.33846248.89958740.1718399394-1511206587.1708537116",
            },
            "MIGRATION OPTIONS": "https://www.databricks.com/blog/migrating-tables-hive-metastore-unity-catalog-metastore",
            "ANALYZE MANUALLY": "https://www.databricks.com/blog/migrating-tables-hive-metastore-unity-catalog-metastore",
            "SYNC": {
                "azure": "https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-aux-sync",
                "aws": "https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-sync.html",
                "gcp": "https://docs.gcp.databricks.com/en/sql/language-manual/sql-ref-syntax-aux-sync.html?_ga=2.38021348.6961092.1717540940-1511206587.1708537116",
            },
            "DEEP CLONE": {
                "azure": "https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/delta-clone",
                "aws": "https://docs.databricks.com/en/sql/language-manual/delta-clone.html",
                "gcp": "https://docs.gcp.databricks.com/en/sql/language-manual/delta-clone.html",
            },
            "CTAS": {
                "azure": "https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-create-table-using",
                "aws": "https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html",
                "gcp": "https://docs.gcp.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-create-table-using.html",
            },
            "CREATE VIEW": {
                "azure": "https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/create-views",
                "aws": "https://docs.databricks.com/en/data-governance/unity-catalog/create-views.html",
                "gcp": "https://docs.gcp.databricks.com/en/data-governance/unity-catalog/create-views.html",
            },
            "LAKEHOUSE FEDERATION (READ-ONLY)": {
                "azure": "https://learn.microsoft.com/en-us/azure/databricks/query-federation/",
                "aws": "https://docs.databricks.com/en/query-federation/index.html",
                "gcp": "https://docs.gcp.databricks.com/en/query-federation/index.html",
            },
            "uc_privileges": {
                "azure": "https://learn.microsoft.com/en-us/azure/databricks/data-governance/unity-catalog/manage-privileges/privileges",
                "aws": "https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html",
                "gcp": "https://docs.gcp.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html",
            },
            "sql_use_catalog": {
                "azure": "https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-syntax-ddl-use-catalog",
                "aws": "https://docs.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-use-catalog.html",
                "gcp": "https://docs.gcp.databricks.com/en/sql/language-manual/sql-ref-syntax-ddl-use-catalog.html",
            },
            "uc_volumes": {
                "azure": "https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/sql-ref-volumes",
                "aws": "https://docs.databricks.com/en/sql/language-manual/sql-ref-volumes.html",
                "gcp": "https://docs.gcp.databricks.com/en/sql/language-manual/sql-ref-volumes.html",
            },
            "dbr_migration_tool": "https://dbrmg.databricks.com/",
        }

    def get_url_by_cloud_provider(self, doc_key):
        doc_urls = self.official_docs.get(doc_key)

        if isinstance(doc_urls, dict):
            if self.config.is_azure:
                return doc_urls.get("azure")
            elif self.config.is_aws:
                return doc_urls.get("aws")
            elif self.config.is_gcp:
                return doc_urls.get("gcp")
        return doc_urls

# COMMAND ----------

# DBTITLE 1,Report Generator
class UCXReportGenerator:
    def __init__(self, database):
        self.qm = QueryManager(database)
        self.docs = Documentation()
        self.html_report = ""

    def _get_html_overview(self):
        overview = self.docs.get_url_by_cloud_provider("MIGRATION OVERVIEW")
        blog = self.docs.get_url_by_cloud_provider("MIGRATION OPTIONS")

        html = f"""<li>For a better understing of migrating into Unity Catalog <a href={overview}>this article</a> will provide a great overview of the steps required for a smooth migration.</li><li>This <a href={blog}>article</a> works as a complementary read to understand the different flavors of table migration.</li>"""

        return html

    def _get_html_tables_and_views(self):
        html = ""
        html_options = {
            "SYNC": """<li>Your workspace has <strong>{input_table_count}</strong> external tables. <strong>No need data replication is needed to migrate those.</strong> You will be able to migrate them to UC using the <strong><a href={input_official_doc}>SYNC command</a></strong> (as long as you keep them external, which is fine)</li>""",
            "CREATE VIEW": """<li>Your workspace has <strong>{input_table_count}</strong> views. Migrating views to UC is not hard, in general:<ul><li>Execute <strong>DESCRIBE TABLE EXTENDED</strong> against each view to obtain the View Original Text or View Text definition.</li><li>Execute <strong><a href={input_official_doc}>CREATE VIEW</a></strong> definition (you must know what's going to be the target UC catalog and schema for each view)</li></ul></li>
            """,
            "DEEP CLONE/CTAS": """<li>Your workspace has <strong>{input_table_count}</strong> tables that <strong>requires data replication</strong>. You will be able to migrate them to UC using the <strong><a href={input_official_doc_one}>DEEP CLONE command</a></strong> or <strong><a href={input_official_doc_two}>CTAS Command</a></strong> depending on the specific format scenario. Please use migration matrix summary for specific scenarios and options to approach the migration.</li>""",
            "CTAS": """<li>Your workspace has <strong>{input_table_count}</strong> unsupport format tables. <strong>These tables require data replication.</strong> You will be able to migrate them to UC using the <strong><a href={input_official_doc}>CTAS command.</a></strong></li>""",
            "LAKEHOUSE FEDERATION (READ-ONLY)": """<li>Your workspace has <strong>{input_table_count}</strong> external tables. <strong>No need data replication is needed to migrate those.</strong> You will be able to migrate them to UC using the <strong><a href={input_official_doc}>SYNC command</a></strong> (as long as you keep them external, which is fine)</li>""",
            "ANALYZE MANUALLY": """<li>Your workspace has <strong>{input_table_count}</strong> tables that will require additional analysis from your team. Based on the output of analysis you can determine which is the best approach to migrate this group of tables. Remember you can use the <a href={input_official_doc}><em>How to upgrade your Hive tables to Unity Catalog blog</em></a> to facilitate your analisys.</li>""",
        }

        migration_type_count = (
            self.qm.select_statement("migration_type_count")
            .orderBy("table_count", ascending=False)
            .collect()
        )

        for mtc in migration_type_count:
            table_count = mtc["table_count"]
            migration_type = mtc["migration_type"]
            option = html_options.get(migration_type)

            if option is None:
                continue  # Skip if no HTML option is found

            if migration_type == "DEEP CLONE/CTAS":
                deep_clone_doc = self.docs.get_url_by_cloud_provider("DEEP CLONE")
                ctas_doc = self.docs.get_url_by_cloud_provider("CTAS")
                html += option.format(
                    input_table_count=table_count,
                    input_official_doc_one=deep_clone_doc,
                    input_official_doc_two=ctas_doc,
                )
            else:
                doc = self.docs.get_url_by_cloud_provider(migration_type)
                html += option.format(
                    input_table_count=table_count, input_official_doc=doc
                )

        return html

    def _get_html_tables_and_view_rec(self):
        azure_rec = ""
        aws_rec = ""
        if self.docs.config.is_azure:
            azure_rec = "<li>If data is in <strong>ADLS 1</strong> or <strong>wasb</strong> then <strong>CTAS</strong> is required (Azure only)</li>"
        if self.docs.config.is_aws:
            aws_rec = "<ul><li>Also <strong>SYNC</strong> needs to be run by a cluster with instance profile (AWS only).</li></ul>"

        html = f"""
                <li><strong>SYNC</strong> requires UC external location to exist beforehand, pointing to the location or a parent location.{aws_rec}</li>           
                <li><strong>SYNC</strong> forces to maintain same table name. If table name change is required, use <strong>CTAS</strong> instead of <strong>SYNC</strong>.</li>
                <li><strong>SYNC</strong> when the source table HMS type = <strong>MANAGED</strong> requires some extra steps. See steps above.</li>
                <li><strong>SYNC</strong> with a table with mount location as source uses the real location (not the mnt point) for the new location. This is nice because we want to sunset those mount points.</li>
                <li><strong>UNSUPPORTED</strong> Tables, indicates that may break downstream or upstream dependencies if not accounted for.</li>
                <li>All data in <strong>DBFS ROOT</strong> needs to be moved somewhere else.</li>
                <li><strong>DEEP CLONing</strong> a parquet table creates a Delta Table, not parquet. This may not be what the customer wants in some cases. Ie if some external dependency assumes parquet format.</li>
                <li><a href="https://www.databricks.com/blog/migrating-tables-hive-metastore-unity-catalog-metastore">Scala code to convert HMS managed to HMS external is in the Appendix section of this article. Just need to <strong>USE CATALOG</strong> hive_metastore; before running the Scala.</a></li>
                <li><strong>DEEP CLONE</strong> keeps metadata while <strong>CTAS</strong> loses it, creating a brand new table.</li>
                <li><strong>DEEP CLONE</strong> also preserves the existing partitioning scheme. If that is not the desired outcome, please use CTAS.</li>
                {azure_rec}
                <li>If the <strong>HMS</strong> table is non-Delta+partitioned, and you register as UC external, there will be a performance penalty in the sense that UC does not store external table partition metadata for non-Delta tables. More info <a href="https://docs.google.com/presentation/d/1U7_66n8vHf0esvRGzXZ229idlhLepGGrn8Tq5jBPmCs/edit#slide=id.g2b253d1c1b3_0_5107">here</a> and <a href="https://databricks.slack.com/archives/C027U33QZ9R/p1685021517795909">here</a>.</li>
        """

        return html

    def _get_html_storage_locations(self):
        html = "<li>WIP</li>"
        return html

    def _get_html_mount_points(self):
        html = "<li>WIP</li>"
        return html

    def _get_html_compute(self):
        html = "<li>WIP</li>"
        return html

    def _get_html_dlt(self):
        html = "<li>WIP</li>"
        return html

    def _get_html_identity_and_access(self):
        html = "<li>WIP</li>"
        return html

    def _set_html_report(self):
        self.html_report = f"""
            <!DOCTYPE html>
            <html lang="en">
            <head>
                <meta charset="UTF-8">
                <meta name="viewport" content="width=device-width, initial-scale=1.0">
                <title>Migration Analysis</title>
                <style>
                    body {{
                        
                        font-size: 14px;
                        margin: 0;
                        padding: 0;
                        background-color: #f4f4f4;
                        color: #333;
                    }}
                    header, footer {{
                        background-color: #12262e;
                        color: white;
                        text-align: center;
                        padding: 1em 0;
                    }}
                    .container {{
                        width: 80%;
                        margin: 0 auto;
                        padding: 20px;
                        background-color: white;
                        box-shadow: 0 0 10px rgba(0, 0, 0, 0.1);
                    }}
                    h2 {{
                        color: #12262e;
                    }}
                    ul {{
                        list-style-type: none;
                        padding: 0;
                    }}
                    ul li {{
                        margin: 10px 0;
                    }}
                    ul li li {{
                        list-style-type: disc;
                        margin-left: 20px; /* Optional: adjust the indentation of nested list items */
                    }}
                    table {{
                        width: 100%;
                        border-collapse: collapse;
                        margin: 20px 0;
                    }}
                    table, th, td {{
                        border: 1px solid #ddd;
                        text-align: left !important;
                    }}
                    th, td {{
                        word-wrap:break-word;
                        padding: 10px;
                    }}
                    th {{
                        background-color: #12262e;
                        color: white;
                    }}
                    a {{
                        color: #12262e;
                        text-decoration: underline;
                    }}
                    a:hover {{
                        text-decoration: underline;
                    }}
                    tbody tr {{
                        display: none;
                    }}

                    tbody tr.expanded {{
                        display: table-row;
                    }}

                    button {{
                        background-color: #12262e;
                        border: none;
                        color: white;
                        padding: 5px 10px;
                        text-align: center;
                        text-decoration: none;
                        cursor: pointer;
                        }}
                </style>
            </head>
            <body>
                <header>
                    <h1>Migration Analysis Report</h1>
                </header>
                <div class="container">
                    <h2>Overview</h2>
                    <ul>
                        {self._get_html_overview()}
                    </ul>

                    <h2>Tables and Views</h2>
                    <ul>                      
                        {self._get_html_tables_and_views()}
                    </ul>

                    <h3>Additional Recommendations:</h3>
                    <ul>
                        {self._get_html_tables_and_view_rec()}
                    </ul>

                    <h2>Storage Locations</h2>
                    <ul>
                        {self._get_html_storage_locations()}
                    </ul>

                    <h2>Mount Points</h2>
                    <ul>
                        {self._get_html_mount_points()}
                    </ul>

                    <h2>Clusters, Jobs, Submits</h2>
                    <ul>
                       {self._get_html_compute()}
                    </ul>

                    <h2>DLT</h2>
                    <ul>
                       {self._get_html_dlt()}
                    </ul>

                    <h2>Permissions/Grants/Groups</h2>
                    <ul>
                        {self._get_html_identity_and_access()}
                    </ul>

                    <br>       
                    <p>Besides the recommendations, we are sharing with you a <a href="https://docs.google.com/spreadsheets/d/1zAAI1aWGej45iUeG0z_pT-nSXx9uGAJgh9Mh7ZfNf7c/edit?usp=sharing">project plan template</a> and a <a href="https://docs.google.com/spreadsheets/d/1l7hF2DzYGekSD3nON_osZ1pMD7x94170qXF3cw6VKwA/edit?usp=sharing">matrix summary for your table scenarios</a>.</p>
                </div>
                <footer>
                    <p>&copy; 2024 Databricks. Shared Technical Services (STS).</p>
                </footer>
                <!-- Uncomment to enable table expand/collapse 
                <script>
                    document.addEventListener('DOMContentLoaded', () => {{
                        const tables = [
                            {{ button: 'tablesByTypeButton', table: 'tablesByTypeTable' }},
                            {{ button: 'storageLocationButton', table: 'storageLocationTable' }},
                            {{ button: 'mountPointButton', table: 'mountPointTable' }}
                        ];

                        tables.forEach(({{ button, table }}) => {{
                            const toggleButton = document.getElementById(button);
                            const tableRows = document.querySelectorAll(`.${{table}} tbody tr`);
                            let isExpanded = false;

                            toggleButton.addEventListener('click', () => {{
                                if (isExpanded) {{
                                    tableRows.forEach(row => row.classList.remove('expanded'));
                                    toggleButton.textContent = 'Expand All';
                                }} else {{
                                    tableRows.forEach(row => row.classList.add('expanded'));
                                    toggleButton.textContent = 'Collapse All';
                                }}
                                isExpanded = !isExpanded;
                            }});
                        }});
                    }});
                </script>
                -->
            </body>
            </html>
        """

    def render_report(self):
        self._set_html_report()
        displayHTML(self.html_report)

# COMMAND ----------

ucx_report = UCXReportGenerator("ucx_andres_garcia")
ucx_report.render_report()
