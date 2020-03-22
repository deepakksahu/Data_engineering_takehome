
    Solution to Technical Task for a position of Data Engineer @ CrossLend

** [Description](https://github.com/crosslend/data_engineer_coding_exercise) :**

We would like to propose that you familiarize yourself
with the housing market in Berlin, and hence suggest that you to build  
a data pipeline to integrate the data for the flats available for rent  
in Berlin. The  objective of this pipeline is to deliver data to the  
analytics layer for data science research.

**Approach:**
1. created an class which will consist of all the API calls.
2. In the **main.py** create an object for the class and use the same
   for any api call.
3. Modularise the code according to their functioning.
4. Added logging for better debugging.
5. Added required comments to increase the readability of the code.
6. Added the Docker file to containerize the solution.
7. Added all the dependency in requirements.txt
8. Writing the DF to s3 as well as postgres.
9. Added the airflow DAG consider we have k8s to spin pods.

**Command: **  
`python main.py
--config '{"key":"dffbab93-44e9-41c2-bfff-6bab66c89b6c", "s3_output_location": "s3://crosslen_datalake/homeloans/",
"user":"sysadmin", "password":"sysadmin@123", "host":"127.0.0.1",
"port":"5432", "database":"postgres_db"}'`

**Config**  
*A variable to be created in Airflow named: immobilienscout24_conf*

    {"key":"dffbab93-44e9-41c2-bfff-6bab66c89b6c",
    "s3_output_location": "s3://crosslen_datalake/homeloans/",  
    "user":"sysadmin",  
    "password":"sysadmin@123",  
    "host":"127.0.0.1", "port":"5432",  
    "database":"postgres_db"}

**Deployment:**  
Image (named *flat-data-ingestion*) to be build using Jenkins and and  
update in the airflow variable  *crosslend_images_config*

Schedule in the DAG is for everyday.

*P.S : All the scheduling assumption as per the current env. I am  
working. This can very accordingly.*

*P.P.S : The host am using is not working anymore. Hence the write to  
destination part is needs more testing.*
