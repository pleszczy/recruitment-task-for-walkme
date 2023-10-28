# Walkme Take Home Assignment

You are given a list of Avro Parquet with the schema described in the following [file](src/main/avro/schema.avsc).

Files represent some user activity. Activities have the following constraints:

* `userId` uniquely describes user
* `environment` describes an id of environment that given user had activity on
* `activityType` is an optional field that describes type of the particular activity it can be one
  of `IDLE`, `ACTIVE`, `SPECIAL`, `ADDITIONAL`.
* `startTimestamp` describes epoch timestamp of the activity start
* `endTimestamp` optional field describes epoch timestamp of the activity end
* All activities are tied to particular day, which means that if given activity has no `endTimestamp`, it is assumed to
  be end of day.

## Existing Architecture

You can assume that there is an existing HTTP API with the following endpoint:

```agsl
GET /testEnvironments/{userId}
```

If the `userId` has any test environments defined endpoint will return the JSON describing the test environment,
otherwise it will return `404`.

The JSON response from the endpoint is as follows:

```
{
 "userId": String,
 "environment": String,
 "activeFrom": String
 "activeUntil": String
}
```

Each test environment is automatically promoted to production environment after `activeUntil` and after that time it
should not be considered test anymore.
Similarly, before `activeFrom` environment should not be considered as test environment.

Both `activeFrom` and `activeUntil` are dates in format "YYYY-mm-DD"

To run the Test API for the provided test data You can use the [script](start-api.sh).You can check that the
api works by using `GET /testEnvironments/161xyVtzx3`.

```shell
http GET localhost:8080/testEnvironments/161xyVtzx3
```

```
HTTP/1.1 200
Content-Type: application/json;charset=UTF-8
Date: Wed, 25 Oct 2023 16:00:58 GMT
Transfer-Encoding: chunked

{
    "activeFrom": "2022-02-23",
    "activeUntil": "2023-02-24",
    "environment": "third",
    "userId": "161xyVtzx3"
}
```

## Goal

Your goal is to create a batch job in Spark/Flink or any other distributed framework of Your choice.
Example data that has the same formatting and partitioning as the original data can be found [here](data).

The requirements are as follows:

1. The job should calculate daily activity aggregates for every user per environment and write them to a specified
   output
   path.
2. The job should work for specified date range.
   under Java 172. The job should ignore the activities that happened in **active** test environments.
3. The job might be used by different departments that care about different activity types, so the job should have
   the possibility to ignore one or more activity types based on configuration.

## What matters

For this task, what matters the most is to have a properly designed pipeline with good code quality and error handling
with
some level of testing. It's not required to overdo the tests though.

If You feel there is something that can be done better, but it would require a lot of time, feel free to add a writeup
in
ReadMe about this.

# Discussing solution

## Personal Goals

Venturing into the world of Flink for the first time, my primary objective is to learn and grow. I view these
recruitment tasks as valuable learning opportunities and consistently share my experiences
on [my GitHub](https://github.com/pleszczy).

## How to run

Using default input/output data paths

```shell
./gradlew run --args="--excludedActivityTypes ACTIVE,IDLE"
```

Overriding input/output data paths

```shell
./gradlew run --args="--excludedActivityTypes ACTIVE,IDLE --input /data --output /output/daily-activity"
```

## Requirements

- JDK 17

## Implementation Details and Notes

### 1. Mismatch Between Avro and Parquet Schemas:

- **Issue**: AvroSerializer incorrectly mapped the activity column value to the startTimestamp column.
- **Root Cause**: Discrepancy between Avro and Parquet schema structures.
- **Resolution**: Adjusted the Avro schema to match the Parquet schema.

  ![img.png](img.png)
  ![img_1.png](img_1.png)

### 2. Default Value Anomaly in `endTimestamp`:

- **Issue**: Avro schema set the default value for `endTimestamp` as the string "null".
- **Resolution**: Modified the Avro schema to use the actual `null` value.

### 3. Java 17 Compatibility with Flink:

- **Issue**: Due to Flink's utilization of reflection for serializing UDFs and data (through Kryo), issues can arise if
  UDFs or data types incorporate JDK classes.
- **Resolution**: Allowed access to necessary JDK classes using VM options
  e.g. `--add-opens java.base/java.lang=ALL-UNNAMED`.

  ![img_2.png](img_2.png)

### 4. OkHttp3 Infinite Retrying Issue

- **Issue**: OkHttp3 retries indefinitely when the API is down.
- **Resolution**: No solution has been found as of now. I suspect a Jigsaw strict module system.

### 5. Kryo Serializer and Java Records

- **Issue**: Kryo serializer does not support java records
- **Resolution**: Record support was added to kryo in https://github.com/EsotericSoftware/kryo/pull/766 but flink is
  using a much older version. Upgraded kryo dependency to the latest version.

`java.lang.IllegalArgumentException: Unable to create serializer "com.esotericsoftware.kryo.serializers.FieldSerializer" for class: com.walkme.usecases.PrepareDailyActivityAggregates$AggregateActivity`

### 6. lambda methods don't provide enough information for automatic type extraction when Java generics are involved

- **Issue**: An issue forcing you to use verbose anonymous classes instead of smaller lambdas
- **Resolution**: No solution has been found as of now, other than using value objects instead of generic Tuples
- **Resources**: https://nightlies.apache.org/flink/flink-docs-release-1.19/docs/dev/datastream/java_lambdas/

### 7. Activity Duration Verification

- **Note**: Verify whether an activity can extend over multiple days. If it's limited to just one day, we can simplify
  the logic and reduce computational complexity.

### 8. Git configured to point to a private repository gitlab.walkmernd.com

**Resolution**: Use a public GitHub account to host the project and setup CI

## Project Structure Overview

This project aspires to adhere to the `Clean Architecture` principles. However, due to time constraints, its
implementation is still ongoing and not yet complete.

### `src/mainavro`

Houses AVRO schemas.

#### `adapters`

Interfaces with external frameworks and libraries, ensuring decoupling of core application logic from specific external
implementations.

##### `frameworks`

- **flink**: Contains classes related to the Flink framework.
- **jackson**: Holds configurations for the Jackson library.
- **okhttp3**: Configuration and utility classes for OkHttp3.
- **resilence4j**: Configurations and classes for the Resilience4j library.

#### `repositories`

Handles data abstraction, whether from databases, file systems, or other external sources.

#### `common`

Utilities and helpers for the application like `TimeUtils`.

#### `entities`

Encapsulates the core business logic and domain.

- **ActivityAccumulator**: Represents accumulated activities over certain criteria.
- **Environment**: Represents a test environment.

#### `exceptions`

Central hub for all custom exceptions.

- **FetchEnvironmentException**: Thrown for issues retrieving environment data.

#### `use-cases`

Holds application-specific operations and business rules.

- **AggregateDailyActivities**: Use-case for aggregating daily activities.
- **CalculateDailyActivityAggregates**: Use-case for the calculation of daily activity aggregates.
- **FilterOutActivitiesInActiveTestEnvironment**: Use-case for filtering activities in active test environments.
- **FilterOutExcludedActivityTypes**: Use-case for the filtering specific activity types based on input criteria.

### Root Level

- **App**: Main application class.
- **AppModule**: Application module configurations.