# Walkme Take Home Assignment

You are given a list of Avro Parquet with the schema described in the following [file](schema.avsc).

Files represent some user activity. Activities have the following constraints:

* `userId` uniquely describes user
* `environment` describes an id of environment that given user had activity on
* `activityType` is an optional field that describes type of the particular activity it can be one of `IDLE`, `ACTIVE`, `SPECIAL`, `ADDITIONAL`.
* `startTimestamp` describes epoch timestamp of the activity start
* `endTimestamp` optional field describes epoch timestamp of the activity end
* All activities are tied to particular day, which means that if given activity has no `endTimestamp`, it is assumed to be end of day.

## Existing Architecture

You can assume that there is an existing HTTP API with the following endpoint:
```agsl
GET /testEnvironments/{userId}
```
If the `userId` has any test environments defined endpoint will return the JSON describing the test environment, otherwise it will return `404`. 

The JSON response from the endpoint is as follows:
```
{
 "userId": String,
 "environment": String,
 "activeFrom": String
 "activeUntil": String
}
```
Each test environment is automatically promoted to production environment after `activeUntil` and after that time it should not be considered test anymore. Similarly, before `activeFrom` environment should not be considered as test environment.

Both `activeFrom` and `activeUntil` are dates in format "YYYY-mm-DD"

In order to run the Test API for the provided test data You can use the [script](start-api.sh). You can check that the api works by using `GET /testEnvironments/161xyVtzx3`.
## Goal

Your goal is to create a batch job in Spark/Flink or any other distributed framework of Your choice. Example data that has the same formatting and partitioning as the original data can be found [here](data). 

The requirements are as follows:
1. The job should calculate daily activity aggregates for every user per environment and write them to specified output path.
2. The job should work for specified date range.
2. The job should ignore the activites that happened on **active** test environments.
3. The job might be used by different departments, that care about different activity types, so the job should have possibility to ignore one or more activity types based on configuration.


## What matters

For this task what matters the most is to have properly designed pipeline with good code quality and error handling with some level of testing it's not required to overdo the tests though. 

If You feel there is something that can be done better but it would require a lot of time, feel free to add a writeup in ReadMe about this.