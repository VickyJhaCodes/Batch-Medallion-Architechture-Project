# Databricks notebook source
# MAGIC %run /Workspace/DeltaLake/Utilities/CommonFunctions
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Mounting to Source DataLake Storage Mnt Point Name: /mnt/source_datalake_storage_gen2

# COMMAND ----------

# Step 1: Define variables
storage_account_name = f_get_secret("sourcedatalakename")
container_name = f_get_secret("sourcedatalakecontainername")
mount_point = "/mnt/source_datalake_storage_gen2"

# Azure AD service principal credentials
client_id = f_get_secret("client-id1")  # Service Principal Application ID
tenant_id = f_get_secret("tenant-id")    # Azure Directory (Tenant) ID
client_secret = f_get_secret("client-secret")      # Service Principal Client Secret

# Step 2: Configure the connection
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# Step 3: Mount the Blob Storage container
try:
    dbutils.fs.mount(
        source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs=configs
    )
    print(f"Successfully mounted {container_name} to {mount_point}")
except Exception as e:
    print(f"Error: {str(e)}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Mounting to Source Blob Storage Mnt Point Name: /mnt/source_blob_storage

# COMMAND ----------

# Parameters
client_id = f_get_secret("client-id1")
client_secret = f_get_secret("client-secret")
tenant_id = f_get_secret("tenant-id")
storage_account_name = f_get_secret("sourceBlobStorageAccountName")
container_name = f_get_secret("sourceBlobStorageContainerName")
mount_point = "/mnt/source_blob_storage"

# Construct the OAuth endpoint
endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"

# Scope to access the storage account
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": endpoint
}

# Mount the storage
try:
    dbutils.fs.mount(
        source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs=configs
    )
    print(f"Mounted successfully at {mount_point}")
except Exception as e:
    print(f"Error mounting storage: {e}")


# COMMAND ----------

# MAGIC %md 
# MAGIC ## Mounting to Sink Bronze Data Lake Storage Mnt Point Name: /mnt/sink_bronze_datalake_storage_gen2

# COMMAND ----------

# Step 1: Define variables
storage_account_name = f_get_secret("sinkdatalakestoragename")
container_name = f_get_secret("sinkdatalakebronzecontainername")
mount_point = "/mnt/sink_bronze_datalake_storage_gen2"

# Azure AD service principal credentials
client_id = f_get_secret("client-id1")  # Service Principal Application ID
tenant_id = f_get_secret("tenant-id")    # Azure Directory (Tenant) ID
client_secret = f_get_secret("client-secret")      # Service Principal Client Secret

# Step 2: Configure the connection
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# Step 3: Mount the Blob Storage container
try:
    dbutils.fs.mount(
        source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs=configs
    )
    print(f"Successfully mounted {container_name} to {mount_point}")
except Exception as e:
    print(f"Error: {str(e)}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Mounting to Sink Silver Data Lake Storage Mnt Point Name: /mnt/_sink_silver_datalake_storage_gen2_

# COMMAND ----------

# Step 1: Define variables
storage_account_name = f_get_secret("sinkdatalakestoragename")
container_name = f_get_secret("sinkdatalakesilvercontainername")
mount_point = "/mnt/sink_silver_datalake_storage_gen2"

# Azure AD service principal credentials
client_id = f_get_secret("client-id1")  # Service Principal Application ID
tenant_id = f_get_secret("tenant-id")    # Azure Directory (Tenant) ID
client_secret = f_get_secret("client-secret")      # Service Principal Client Secret

# Step 2: Configure the connection
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# Step 3: Mount the Blob Storage container
try:
    dbutils.fs.mount(
        source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs=configs
    )
    print(f"Successfully mounted {container_name} to {mount_point}")
except Exception as e:
    print(f"Error: {str(e)}")

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Mounting to Sink Gold Data Lake Storage Mnt Point Name: /mnt/_sink_gold_datalake_storage_gen2_

# COMMAND ----------

# Step 1: Define variables
storage_account_name = f_get_secret("sinkdatalakestoragename")
container_name = f_get_secret("sinkdatalakegoldcontainername")
mount_point = "/mnt/sink_gold_datalake_storage_gen2"

# Azure AD service principal credentials
client_id = f_get_secret("client-id1")  # Service Principal Application ID
tenant_id = f_get_secret("tenant-id")    # Azure Directory (Tenant) ID
client_secret = f_get_secret("client-secret")      # Service Principal Client Secret

# Step 2: Configure the connection
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# Step 3: Mount the Blob Storage container
try:
    dbutils.fs.mount(
        source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point=mount_point,
        extra_configs=configs
    )
    print(f"Successfully mounted {container_name} to {mount_point}")
except Exception as e:
    print(f"Error: {str(e)}")

# COMMAND ----------

#New Silver One
# Step 1: Define variables
storage_account_name_new = "sinkdatalakestorage"
container_name_new = "silver"
mount_point_new = "/mnt/new_sink_sil_layer_datalake_storage_gen2"

# Azure AD service principal credentials
client_id = f_get_secret("client-id1")  # Service Principal Application ID
tenant_id = f_get_secret("tenant-id")    # Azure Directory (Tenant) ID
client_secret = f_get_secret("client-secret")      # Service Principal Client Secret

# Step 2: Configure the connection
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": client_id,
    "fs.azure.account.oauth2.client.secret": client_secret,
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# Step 3: Mount the Blob Storage container
try:
    dbutils.fs.mount(
        source=f"abfss://{container_name_new}@{storage_account_name_new}.dfs.core.windows.net/",
        mount_point=mount_point_new,
        extra_configs=configs
    )
    print(f"Successfully mounted {container_name_new} to {mount_point_new}")
except Exception as e:
    print(f"Error: {str(e)}")

# COMMAND ----------

dbutils.fs.unmount('/mnt/new_sink_silverlayer_datalake_storage_gen2')

# COMMAND ----------


