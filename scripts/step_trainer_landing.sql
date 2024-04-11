CREATE EXTERNAL TABLE IF NOT EXISTS `stedi-v1`.`step_trainer_landing` (
  `sensorReadingTime` string,
  `serialNumber` string,
  `distanceFromObject` int
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://step-trainer-landing-v1/'
TBLPROPERTIES ('classification' = 'json');