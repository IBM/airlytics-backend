CREATE EXTERNAL TABLE `databasename.tablename`(
  `eventid` string,
  `appversion` string,
  `schemaversion` string,
  `productid` string,
  `name` string,
  `eventtime` bigint,
  `sessionid` string,
  `userid` string,
  `platform` string,
  `event` string,
  `recievedtimestamp` bigint,
  `source` string,
  `devicetime` bigint,
  `receivedtime` bigint,
  `offset` bigint,
  `shard` smallint)
PARTITIONED BY (partition smallint, day string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://adl/rawdata/topicName'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'projection.day.format'='yyyy-MM-dd',
  'projection.day.interval.unit'='DAYS',
  'projection.day.range'='2020-04-01, NOW',
  'projection.day.type'='date',
  'projection.enabled'='true',
  'projection.partition.range'='0,99',
  'projection.partition.type'='integer')