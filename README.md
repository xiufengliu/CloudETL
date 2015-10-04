CloudETL: A Scalable Dimensional ETL for Hive
====================

# Introduction

Extract-Transform-Load (ETL) programs process data from sources into data warehouses (DWs). Due to the rapid growth of data volumes, there is an increasing demand for systems that can scale on demand. Recently, much attention has been given to MapReduce which is a framework for highly parallel handling of massive data sets in cloud environments. The MapReduce-based Hive has been proposed as an RDBMS-like system for DWs and provides good and scalable analytical features. It is, however, still challenging to do proper dimensional ETL processing with (relational) Hive; for example, the concept of slowly changing dimensions (SCDs) is not supported (and due to lacking support for UPDATEs they are also very complicated to handle manually). To remedy this, we here implement the cloud-enabled ETL framework CloudETL. CloudETL uses Hadoop to parallelize the ETL execution and to process data into Hive. The user defines the ETL process by means of high-level constructs and transformations and does not have to worry about the technical details of MapReduce. CloudETL provides built-in support for different dimensional concepts, including star schemas and SCDs. We have performed extensive performance study using realistic data amounts and compare with other cloud-enabled systems. The results show that CloudETL has good scalability and outperforms the dimensional ETL capabilities of Hive both with respect to performance and programmer productivity. For example, Hive uses 3.9 times as long to load an SCD in one of our experiments and needs 112 statements while CloudETL only needs 4.

# Installation

* Environment requirements:
> Java 1.6
> Hadoop 0.21.0
> Hive 0.8.0

# The example of implementing a parallel ETL for star schema:


# Publications

- X. Liu, C. Thomsen and T. B. Pedersen, CloudETL: Scalable Dimensional ETL for Hadoop and Hive, TR-30, Department of Computer Science, Aalborg University, 2012 (PDF).
