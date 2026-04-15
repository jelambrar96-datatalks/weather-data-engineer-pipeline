# Project Evaluation Criteria

Here's the evaluation rubric from the DataTalks.Club Data Engineering Zoomcamp project page:

## Project Evaluation Rubric

| Category                                          | 0 Points                                      | 2 Points                                                                       | 4 Points                                                                                                   |
| ------------------------------------------------- | --------------------------------------------- | ------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------- |
| **Problem description**                           | Problem is not described                      | Problem is described but shortly or not clearly                                | Problem is well described and it's clear what the problem the project solves                               |
| **Cloud**                                         | Cloud is not used, things run only locally    | The project is developed in the cloud                                          | The project is developed in the cloud and IaC tools are used                                               |
| **Data ingestion – Batch/Workflow orchestration** | No workflow orchestration                     | Partial workflow orchestration: some steps are orchestrated, some run manually | End-to-end pipeline: multiple steps in the DAG, uploading data to data lake                                |
| **Data ingestion – Stream**                       | No streaming system (like Kafka, Pulsar, etc) | A simple pipeline with one consumer and one producer                           | Using consumer/producers and streaming technologies (like Kafka streaming, Spark streaming, Flink, etc)    |
| **Data warehouse**                                | No DWH is used                                | Tables are created in DWH, but not optimized                                   | Tables are partitioned and clustered in a way that makes sense for the upstream queries (with explanation) |
| **Transformations** (dbt, Spark, etc)             | No transformations                            | Simple SQL transformation (no dbt or similar tools)                            | Transformations are defined with dbt, Spark or similar technologies                                        |
| **Dashboard**                                     | No dashboard                                  | A dashboard with 1 tile                                                        | A dashboard with 2 tiles                                                                                   |
| **Reproducibility**                               | No instructions how to run the code at all    | Some instructions are there, but they are not complete                         | Instructions are clear, it's easy to run the code, and the code works                                      |

> **Note:** For peer reviewing, you must evaluate **3 projects** of your peers to receive points, and you get **3 extra points** for each evaluation.
