# Project: STEDI Human Balance Analytics

## Introduction

Spark and AWS Glue allow you to process data from multiple sources, categorize the data, and curate it to be queried in the future for multiple purposes. As a data engineer on the STEDI Step Trainer team, you'll need to extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS so that Data Scientists can train the learning model. 

## Project Details

The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:

- trains the user to do a STEDI balance exercise;
- and has sensors on the device that collect data to train a machine-learning algorithm to detect steps;
- has a companion mobile app that collects customer data and interacts with the device sensors.

STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.

## Implementation

### Landing Zone

**Glue Tables**:

- [customer_landing.sql](script/customer_landing.sql)
- [accelerometer_landing.sql](script/accelerometer_landing.sql)
- [step_trainer_trusted.sql](script/step_trainer_trusted.sql)

**Athena**:
Landing Zone data query results

*Customer Landing*:

<figure>
  <img src="docs/customer_landing.png" alt="Customer Landing data" width=60% height=60%>
</figure>

*Accelerometer Landing*:

<figure>
  <img src="docs/accelerometer_landing.png" alt="Accelerometer Landing data" width=60% height=60%>
</figure>

*Step Trainer Landing*:

<figure>
  <img src="docs/step_trainer_landing.png" alt="Accelerometer Landing data" width=60% height=60%>
</figure>

### Trusted Zone

**Glue job scripts**:

- [customer_trusted.py](scripts/customer_trusted.py)
- [accelerometer_trusted.py](scripts/accelerometer_trusted.py)

**Athena**:
Trusted Zone Query results:

*Customer*:
<figure>
  <img src="docs/customer_trusted.png" alt="Customer Truested data" width=60% height=60%>
</figure>

*Accelerometer*:
<figure>
  <img src="docs/accelerometer_trusted.png" alt="Customer Truested data" width=60% height=60%>
</figure>

*Step Trainer*:

<figure>
  <img src="docs/step_trainer_trusted.png" alt="Customer Truested data" width=60% height=60%>
</figure>
### Curated Zone

*Customer*:
<figure>
  <img src="docs/customer_curated.png" alt="Customer Truested data" width=60% height=60%>
</figure>

*Machine learning Curated*:
<figure>
  <img src="docs/machine_learning_curated.png" alt="Customer Truested data" width=90% height=90%>
</figure>



**Glue job scripts**:

- [customer_curated.py](scripts/customer_curated.py)
- [machine_learning_curated.py](scripts/machine_learning_curated.py)