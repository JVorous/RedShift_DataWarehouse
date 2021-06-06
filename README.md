# Data_Modeling_With_AWS_Redshift
<h1>Dependencies</h1>
<ul>
    <li>configparser</li>
    <li>psycopg2</li>
    <li>sql_queries (included in files)</li>
    <li>AWS Redshift with S3</li>
</ul>

<h1>Project</h1>
A music streaming startup, Sparkify, has grown their user base and song database and want to move 
their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user 
activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, 
stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to 
continue finding insights in what songs their users are listening to. You'll be able to test your database 
and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results 
with their expected results.

<h1>Steps</h1>
<ol>
    <li>Configure dwh.cfg file for your specific connection parameters</li>
    <li>Run create_tables.py -- will use statements from sql_queries.py to drop and create appropriate tables</li>
    <li>run etl.py -- imports data from json files into target database for analysis.</li>
</ol> 

<h1>Data sets</h1>
<ul>
    <li>Song data: s3://udacity-dend/song_data</li>
    <li>Log data: s3://udacity-dend/log_data</li>
    <li>Log data json path: s3://udacity-dend/log_json_path.json</li>
</ul>
