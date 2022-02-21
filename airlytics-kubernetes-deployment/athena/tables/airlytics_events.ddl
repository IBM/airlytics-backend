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
  `sessionStartTime` bigint,
  `previousvalues` string,
  `devUser` boolean,
  `cd.customfimentionfield` string,
  `eventname.eventfield` string,
  `nullvaluefields` string,
  `writetime` bigint,
  `devicetime` bigint,
  `receivedtime` bigint,
  `source` string)
PARTITIONED BY (shard smallint, day string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://ald/parquet/topicName'
TBLPROPERTIES (
  'has_encrypted_data'='false',
  'projection.day.format'='yyyy-MM-dd',
  'projection.day.interval.unit'='DAYS',
  'projection.day.range'='2020-04-01, NOW',
  'projection.day.type'='date',
  'projection.enabled'='true',
  'projection.shard.range'='0,999',
  'projection.shard.type'='integer')