
Technical Task for a position of Data Engineer @ CrossLend
Dear candidate

As part of the recruitment process, we would like to ask you to complete the following assessment which should take not more than couple of evenings of your time. Please do your best and aim for an end-to-end MVP solution.

As a result of the exercise, we expect to receive a link to the github or gitlab repo with a working executable code base instructions on how to run it, and with a detailed report (as markdown README.md file) containing justification of your tech choices.

Description
We would like to propose that you familiarize yourself with the housing market in Berlin, and hence suggest that you to build a data pipeline to integrate the data for the flats available for rent in Berlin. The objective of this pipeline is to deliver data to the analytics layer for data science research.

In order to achieve the objective, we would like to propose the following approach for you:

Build a stateless service to fetch raw data from the source using the set of API end-points. The service objectives:

Authenticate with API (you will be provided with required access keys)
Fetch all available data
Store unchanged raw data to the data persistence layer (e.g. on disk) partitioned by date
Flatten nested json from the API response payload
Write data to the "hot storage" (e.g. PostgreSQL)
Build a stateless service to map/transform raw data to align with the analytics layer SLA. The service objectives:

Connect to the "hot storage"
Read the mapping/transformation SQL queries
Execute transformation SQL queries
It is recommended that you design the pipeline to be stateless with independent services. The pipeline should operate on schedule with daily trigger. Optionally, you can propose an orchestration solution to trigger the pipeline.