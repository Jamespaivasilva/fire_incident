Assignment:
A customer needs to analyze a dataset of fire incidents in the city of San Francisco. In
order to do so, it requests you to make the data available in a data warehouse and create
a model to run dynamic queries efficiently.

Requirements:
The copy of the dataset in the data warehouse should reflect exactly the current
state of the data at the source.
For the sake of this exercise, assume that the dataset is updated daily at the
source.
The business intelligence team needs to run queries that aggregate these
incidents along the following dimensions: time period, district, battalion.

**Description of the steps and solution provided:**
(1) Provisioner a Mysql server for this porpuse.
(2) Created IAM role to allow access into AWS.
(3) Upload the Data into the Data Lake on AWS S3.
(4) Download the dataset from AWS S3 using a Python script and load the data into the table.
(5) Generated a DWH model, create a fact table and 3 dimensions Time,District and Battalian.
(7) Validated.
(8) Added 4 example of metrics that can be extract from the model.

