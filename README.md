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

![Customer](./image/Captura%20de%20Pantalla%202023-07-14%20a%20la(s)%201.25.05%20a.m..png)
