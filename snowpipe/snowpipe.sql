-- create the format for the json data
CREATE OR REPLACE FILE FORMAT json_load_format TYPE = 'JSON';

-- Create the stage table that loads the Json data
CREATE OR REPLACE TABLE cloudquery.public.raw_azure_storage_containers_data (data VARIANT);

-- Create the stage 
CREATE OR REPLACE STAGE s3_stage_from_azure
  URL='s3://cq-poc/connectors/azure/azure_storage_containers/'
  STORAGE_INTEGRATION = s3_snowflake_integration_for_azure
  FILE_FORMAT = json_load_format;

-- if there are already existing files in the path, will have to copy manually by running the following command
COPY INTO cloudquery.public.raw_azure_storage_containers_data
FROM @s3_stage_from_azure;

-- show pipes
-- Update the ARN in Event Triggers of S3 Bucket
SHOW PIPES

-- Create pipe 
CREATE OR REPLACE PIPE s3_to_snf_from_azure_pipe
AUTO_INGEST = TRUE
AS
COPY INTO cloudquery.public.raw_azure_storage_containers_data
FROM @s3_stage_from_azure
FILE_FORMAT = 'json_load_format';

-- Create the data landing table
CREATE OR REPLACE TABLE cloudquery.public.azure_storage_containers(
_cq_id STRING,
_cq_parent_id STRING,
_cq_source_name STRING,
_cq_sync_time TIMESTAMP,
extended_location STRING(255),
id STRING(255),
identity STRING,
kind STRING,
location STRING,
name STRING,
properties_accessTier STRING,
properties_allowBlobPublicAccess BOOLEAN,
properties_allowCrossTenantReplication BOOLEAN,
properties_allowSharedKeyAccess BOOLEAN,
properties_creationTime TIMESTAMP,
properties_defaultToOAuthAuthentication BOOLEAN,
properties_dnsEndpointType STRING,
properties_encryption_keySource STRING,
properties_encryption_requireInfrastructureEncryption BOOLEAN,
properties_encryption_services_blob_enabled BOOLEAN,
properties_encryption_services_blob_keyType STRING,
properties_encryption_services_blob_lastEnabledTime TIMESTAMP,
properties_encryption_services_file_enabled BOOLEAN,
properties_encryption_services_file_keyType STRING,
properties_encryption_services_file_lastEnabledTime TIMESTAMP,
properties_keyCreationTime_key1 TIMESTAMP,
properties_keyCreationTime_key2 TIMESTAMP,
properties_largeFileSharesState STRING,
properties_minimumTlsVersion STRING,
properties_networkAcls_bypass STRING,
properties_networkAcls_defaultAction STRING,
properties_primaryEndpoints_blob STRING,
properties_primaryEndpoints_dfs STRING,
properties_primaryEndpoints_file STRING,
properties_primaryEndpoints_queue STRING,
properties_primaryEndpoints_table STRING,
properties_primaryEndpoints_web STRING,
properties_primaryLocation STRING,
properties_privateEndpointConnections STRING,
properties_provisioningState STRING,
properties_publicNetworkAccess STRING,
properties_secondaryEndpoints_blob STRING,
properties_secondaryEndpoints_dfs STRING,
properties_secondaryEndpoints_queue STRING,
properties_secondaryEndpoints_table STRING,
properties_secondaryEndpoints_web STRING,
properties_secondaryLocation STRING,
properties_statusOfPrimary STRING,
properties_statusOfSecondary STRING,
properties_supportsHttpsTrafficOnly BOOLEAN,
sku_name STRING,
sku_tier STRING,
subscription_id STRING,
tags STRING,
type STRING
);

-- create the stream to load the streaming data that is being loaded into the bucket recently
 
CREATE OR REPLACE STREAM azure_storage_containers_stream
ON TABLE raw_azure_storage_containers_data
SHOW_INITIAL_ROWS = FALSE;


-- create the task so taht it automatically puts the data from streaming into target path

CREATE OR REPLACE TASK automate_insert_into_target
WAREHOUSE = 'PRECOG'
SCHEDULE = 'USING CRON * * * * * UTC'
AS
INSERT INTO azure_storage_containers
SELECT 
  $1:"_cq_id"::varchar AS _cq_id,
  $1:"_cq_parent_id"::varchar AS _cq_parent_id,
  $1:"_cq_source_name"::varchar AS _cq_source_name,
  $1:"_cq_sync_time"::varchar AS _cq_sync_time,
  $1:"extended_location"::varchar AS extended_location,
  $1:"id"::varchar AS id,
  $1:"identity"::varchar AS identity,
  $1:"kind"::varchar AS kind,
  $1:"location"::varchar AS location,
  $1:"name"::varchar AS name,
  $1:"properties_accessTier"::varchar AS properties_accessTier,
  $1:"properties_allowBlobPublicAccess"::varchar AS properties_allowBlobPublicAccess,
  $1:"properties_allowCrossTenantReplication"::varchar AS properties_allowCrossTenantReplication,
  $1:"properties_allowSharedKeyAccess"::varchar AS properties_allowSharedKeyAccess,
  $1:"properties_creationTime"::varchar AS properties_creationTime,
  $1:"properties_defaultToOAuthAuthentication"::varchar AS properties_defaultToOAuthAuthentication,
  $1:"properties_dnsEndpointType"::varchar AS properties_dnsEndpointType,
  $1:"properties_encryption_keySource"::varchar AS properties_encryption_keySource,
  $1:"properties_encryption_requireInfrastructureEncryption"::varchar AS properties_encryption_requireInfrastructureEncryption,
  $1:"properties_encryption_services_blob_enabled"::varchar AS properties_encryption_services_blob_enabled,
  $1:"properties_encryption_services_blob_keyType"::varchar AS properties_encryption_services_blob_keyType,
  $1:"properties_encryption_services_blob_lastEnabledTime"::varchar AS properties_encryption_services_blob_lastEnabledTime,
  $1:"properties_encryption_services_file_enabled"::varchar AS properties_encryption_services_file_enabled,
  $1:"properties_encryption_services_file_keyType"::varchar AS properties_encryption_services_file_keyType,
  $1:"properties_encryption_services_file_lastEnabledTime"::varchar AS properties_encryption_services_file_lastEnabledTime,
  $1:"properties_keyCreationTime_key1"::varchar AS properties_keyCreationTime_key1,
  $1:"properties_keyCreationTime_key2"::varchar AS properties_keyCreationTime_key2,
  $1:"properties_largeFileSharesState"::varchar AS properties_largeFileSharesState,
  $1:"properties_minimumTlsVersion"::varchar AS properties_minimumTlsVersion,
  $1:"properties_networkAcls_bypass"::varchar AS properties_networkAcls_bypass,
  $1:"properties_networkAcls_defaultAction"::varchar AS properties_networkAcls_defaultAction,
  $1:"properties_primaryEndpoints_blob"::varchar AS properties_primaryEndpoints_blob,
  $1:"properties_primaryEndpoints_dfs"::varchar AS properties_primaryEndpoints_dfs,
  $1:"properties_primaryEndpoints_file"::varchar AS properties_primaryEndpoints_file,
  $1:"properties_primaryEndpoints_queue"::varchar AS properties_primaryEndpoints_queue,
  $1:"properties_primaryEndpoints_table"::varchar AS properties_primaryEndpoints_table,
  $1:"properties_primaryEndpoints_web"::varchar AS properties_primaryEndpoints_web,
  $1:"properties_primaryLocation"::varchar AS properties_primaryLocation,
  $1:"properties_privateEndpointConnections"::varchar AS properties_privateEndpointConnections,
  $1:"properties_provisioningState"::varchar AS properties_provisioningState,
  $1:"properties_publicNetworkAccess"::varchar AS properties_publicNetworkAccess,
  $1:"properties_secondaryEndpoints_blob"::varchar AS properties_secondaryEndpoints_blob,
  $1:"properties_secondaryEndpoints_dfs"::varchar AS properties_secondaryEndpoints_dfs,
  $1:"properties_secondaryEndpoints_queue"::varchar AS properties_secondaryEndpoints_queue,
  $1:"properties_secondaryEndpoints_table"::varchar AS properties_secondaryEndpoints_table,
  $1:"properties_secondaryEndpoints_web"::varchar AS properties_secondaryEndpoints_web,
  $1:"properties_secondaryLocation"::varchar AS properties_secondaryLocation,
  $1:"properties_statusOfPrimary"::varchar AS properties_statusOfPrimary,
  $1:"properties_statusOfSecondary"::varchar AS properties_statusOfSecondary,
  $1:"properties_supportsHttpsTrafficOnly"::varchar AS properties_supportsHttpsTrafficOnly,
  $1:"sku_name"::varchar AS sku_name,
  $1:"sku_tier"::varchar AS sku_tier,
  $1:"subscription_id"::varchar AS subscription_id,
  $1:"tags"::varchar AS tags,
  $1:"type"::varchar AS type
FROM azure_storage_containers_stream;

ALTER TASK automate_insert_into_target resume;

SELECT * FROM azure_storage_containers;