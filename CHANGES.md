# UTILS #

### Version plugin] prepare release 0.2.6 ###

### Version 0.2.6 ###
* ISSUE [72](https://github.com/bigdatagenomics/bdg-utils/pull/72): Remove dependency on Spark internal Logging

### Version 0.2.5 ###
* ISSUE [67](https://github.com/bigdatagenomics/bdg-utils/pull/67): Modifying pom.xml files to move back to Scala 2.10 for development.

### Version SNAPSHOT ###
* ISSUE [64](https://github.com/bigdatagenomics/bdg-utils/pull/64): Add exception for maven-javadoc-plugin in move_to_scala_2.11.sh
* ISSUE [63](https://github.com/bigdatagenomics/bdg-utils/pull/63): Remove extraneous git-commit-id-plugin configuration
* ISSUE [62](https://github.com/bigdatagenomics/bdg-utils/pull/62): Update spark, hadoop, and parquet dependency versions
* ISSUE [60](https://github.com/bigdatagenomics/bdg-utils/pull/60): Various small util fixes :

### Version 0.2.3 ###
* ISSUE [51](https://github.com/bigdatagenomics/bdg-utils/pull/51): Added "Driver Time" column to represent the total time in the driver
* ISSUE [49](https://github.com/bigdatagenomics/bdg-utils/pull/49): Adding method for hashing down a whole RDD.
* ISSUE [48](https://github.com/bigdatagenomics/bdg-utils/pull/48): Fix minor code style and doc errors and warnings
* ISSUE [47](https://github.com/bigdatagenomics/bdg-utils/pull/47): [utils-46] removing duplicate declaration of spark-core dependency
* ISSUE [45](https://github.com/bigdatagenomics/bdg-utils/pull/45): [utils-44] updating release tag name format to more closely match artifact name

### Version 0.2.2 ###
* ISSUE [42](https://github.com/bigdatagenomics/bdg-utils/pull/42): [utils-42] Upgrading to the org.apache.parquet release of Parquet.
* ISSUE [39](https://github.com/bigdatagenomics/bdg-utils/pull/39): Updated docs to add correct parameter for printing metrics

### Version 0.2.1 ###
* ISSUE [38](https://github.com/bigdatagenomics/bdg-utils/pull/38): kill `job` argument to BDGSparkCommand

### Version 0.2.0 ###
* ISSUE [36](https://github.com/bigdatagenomics/bdg-utils/pull/36): Clean up for release
* ISSUE [35](https://github.com/bigdatagenomics/bdg-utils/pull/35): Fix problem with nested RDD operations that causes negative durations
* ISSUE [34](https://github.com/bigdatagenomics/bdg-utils/pull/34): Cross compile to Scala 2.10 and 2.11.
* ISSUE [31](https://github.com/bigdatagenomics/bdg-utils/pull/31): Move normalization code from ADAM
* ISSUE [33](https://github.com/bigdatagenomics/bdg-utils/pull/33): [ADAM-634] Remove bdg-utils-parquet
* ISSUE [32](https://github.com/bigdatagenomics/bdg-utils/pull/32): Adding gzip codec.
* ISSUE [30](https://github.com/bigdatagenomics/bdg-utils/pull/30): Move CLI code from ADAM
* ISSUE [14](https://github.com/bigdatagenomics/bdg-utils/pull/14): [bdg-utils-13] Added code to implement a poisson mixture model.

### Version 0.1.2 ###
* ISSUE [28](https://github.com/bigdatagenomics/bdg-utils/pull/28): Instrumented some new RDD operations from Spark 1.2
* ISSUE [26](https://github.com/bigdatagenomics/bdg-utils/pull/26): [bdg-utils-25] Upgrade to Spark 1.2.0.
* ISSUE [27](https://github.com/bigdatagenomics/bdg-utils/pull/27): Added documentation for instrumentation to README.md
* ISSUE [24](https://github.com/bigdatagenomics/bdg-utils/pull/24): Remove Timers object from bdg-utils as it contains ADAM-specific timers

### Version 0.1.1 ###
* ISSUE [22](https://github.com/bigdatagenomics/bdg-utils/pull/22): Porting a fix to Parquet from ADAM.

### Version 0.1.0 ###
* ISSUE [21](https://github.com/bigdatagenomics/bdg-utils/pull/21): [bdg-utils-20] Port unit testing suite fixes over from ADAM.
* ISSUE [19](https://github.com/bigdatagenomics/bdg-utils/pull/19): Added more granular instrumentation to bdg-utils.

### Version 0.0.1 ###
* ISSUE [15](https://github.com/bigdatagenomics/bdg-utils/pull/15): [bdg-utils-9] Silence logging during unit tests.
* ISSUE [10](https://github.com/bigdatagenomics/bdg-utils/pull/10): [bdg-utils-3] Moving over the Parquet classes.
* ISSUE [12](https://github.com/bigdatagenomics/bdg-utils/pull/12): [bdg-utils-11] Adding testFile to SparkFunUtils
* ISSUE [8](https://github.com/bigdatagenomics/bdg-utils/pull/8): [bdg-utils-6] Migrates metrics code from ADAM
* ISSUE [7](https://github.com/bigdatagenomics/bdg-utils/pull/7): [bdg-utils-2] Migrates minhashing code from PacMin.
* ISSUE [5](https://github.com/bigdatagenomics/bdg-utils/pull/5): Initial setup
