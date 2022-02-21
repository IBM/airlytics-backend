This Utility is to run queries on the analytics data.
<br>It generates reports of the Test that were run.
<br>For running the utility one need to set correctly the testng.xml file resides of the "resources/queryValidator"
<br>There are 2 existing query handlers : 
<br>
<br> *MySqlQueryHandler - handler for accessing mySql db 
<br> *AthenaAWSQueryHandler - handler for accessing Athena on AWS db
<br>
<br> For running the MySqlQueryHandler you need to have a mySql database and to specify the following parameters:
        \<parameter name = "dbName" value="*some existing schema on the mySQL db\*"/>
        \<parameter name = "tableName" value="*name of success events table\*"/>
        \<parameter name = "errorTableName" value="*name of failure events table\*"/>
