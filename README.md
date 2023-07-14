# This is the steps for STEDI project 

## Create IAM Role for read from data lake s3 with glue jobs

![iam_role](./image/Captura%20de%20Pantalla%202023-07-14%20a%20la(s)%204.03.31%20a.m..png)

## Copy files from stedi project starter to camilo-Data-Lake landing zone for accelerometer, customer and step_trainer


### Accelerometer

![accelerometer](./image/Captura%20de%20Pantalla%202023-07-13%20a%20la(s)%2011.57.37%20p.m..png)


### Customer

![customer](./image/Captura%20de%20Pantalla%202023-07-14%20a%20la(s)%2012.07.23%20a.m..png)

### Step_trainer

![step_trainer](./image/Captura%20de%20Pantalla%202023-07-14%20a%20la(s)%2012.09.43%20a.m..png)


## Create glue tables for lakehouse 

### Customer

![create_customer_table](./image/Captura%20de%20Pantalla%202023-07-14%20a%20la(s)%2012.58.17%20a.m..png)


Script glue table customer

[Customer sql](./sql/customer_landing.sql)

Create glue crawler for sync date from datalake zone to glue table

![Customer glue crawler](./image/Captura%20de%20Pantalla%202023-07-14%20a%20la(s)%2012.58.17%20a.m..png)

### Accelerometer

![create_accelerometer_table](./image/Captura%20de%20Pantalla%202023-07-14%20a%20la(s)%201.19.10%20a.m..png)


Script glue table customer

[Accelerometer sql](./sql/accelerometer_landing.sql)

Create glue crawler for sync date from datalake zone to glue table

![Accelerometer glue crawler](./image/Captura%20de%20Pantalla%202023-07-14%20a%20la(s)%201.13.00%20a.m..png)

### Preview Customer & Accelerometer

Athena query result

* Accelerometer

![Accelerometer](./image/Captura%20de%20Pantalla%202023-07-14%20a%20la(s)%201.21.53%20a.m..png)

* Customer

![Customer](./image/Captura%20de%20Pantalla%202023-07-14%20a%20la(s)%2011.16.59%20a.m..png)

## Spark glue job for transfor customer from landing to trusted

### Customer glue job execution

* Spark glue Job Customer trusted
[Spark_glue_job_customer_trusted](./glue/customer_trusted-etl-spark.py)

![Customer_job_execution](./image/Captura%20de%20Pantalla%202023-07-14%20a%20la(s)%202.02.45%20a.m..png)

* Athena query result customer trusted

![customer trusted athena](./image/customer_trusted.png)

* spark glue Job accelerometer trusted

[spark_job_accelerometer_trusted](./glue/accelerometer_trusted.py)

![accelerometer_job_execution_job_trusted](./image/Captura%20de%20Pantalla%202023-07-14%20a%20la(s)%208.34.07%20a.m..png)

* Athena query result accelerometer trusted

![Accelerometer_trusted](./image/accelerometer_trusted.png)


* Spark glue job customer curated

[Spark_glue_job_customer_curated](./glue/customer_curated-etl-spark.py)

![customer_curated_execution_job](./image/Captura%20de%20Pantalla%202023-07-14%20a%20la(s)%203.49.00%20a.m..png)

* Athena query result customer curated
![customer_curated](./image/customer_curated.png)

* Spark glue job step_trainer

[spark-glue-job-step-trainer](./glue/step_trainer_trusted-etl-apark.py)

![step_trainer_trusted_job](./image/Captura%20de%20Pantalla%202023-07-14%20a%20la(s)%204.22.23%20a.m..png)


