
# Mason Spark

This repository contains various implementations of mason execution engine jobs which use a spark configuration.

Requirements: 

```
docker, sbt (scala)
```

Run 

```./build```

Which builds the jar file and packages it into the mason-spark docker image.   mason-spark is derived from the google spark operator image and is based on 

```scala 2.11.12 and spark 2.4.5 ```

by default.  More versions will be supported later but for now ensure that you are using these.

# Current Job Implementations

1.  MergeJob:  Merges all the files in a single specified path to a single schema
    - Currently Supports:
        - Parquet
        - CSV
        - JSON
        - JSON lines
    - Does not yet support mixed formats