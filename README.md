utils
=========

General (non-omics) code used across BDG products. Apache 2 licensed.

# Usage

## Instrumentation

The bdg-utils project contains functionality for instrumenting Spark operations, as well as Scala function calls.
Function calls can be recorded both in the Spark Driver, and in the Spark Workers.

The following is an example of some instrumentation output from the
[ADAM Project](https://github.com/bigdatagenomics/adam):

```
Timings
+------------------------------------------------------------+--------------+----------------+-------------+----------+----------------+----------------+----------------+
|                           Metric                           | Worker Total |  Driver Total  | Driver Only |  Count   |      Mean      |      Min       |      Max       |
+------------------------------------------------------------+--------------+----------------+-------------+----------+----------------+----------------+----------------+
| └─ Base Quality Recalibration                              |            - | 2 mins 36 secs |   644.45 ms |        1 | 2 mins 36 secs | 2 mins 36 secs | 2 mins 36 secs |
|     ├─ map at DecadentRead.scala:50                        |            - |        6.72 ms |           - |        1 |        6.72 ms |        6.72 ms |        6.72 ms |
|     │   └─ function call                                   |   21.45 secs |              - |           - |   747308 |       28.71 µs |              0 |      183.24 ms |
|     ├─ filter at BaseQualityRecalibration.scala:71         |            - |       20.09 ms |           - |        1 |       20.09 ms |       20.09 ms |       20.09 ms |
|     │   └─ function call                                   |    170.74 ms |              - |           - |   373654 |         456 ns |              0 |        1.37 ms |
|     ├─ flatMap at BaseQualityRecalibration.scala:71        |            - |        9.88 ms |           - |        1 |        9.88 ms |        9.88 ms |        9.88 ms |
|     │   └─ function call                                   |   46.17 secs |              - |           - |   363277 |      127.08 µs |          69 µs |       54.42 ms |
|     ├─ map at BaseQualityRecalibration.scala:82            |            - |        5.37 ms |           - |        1 |        5.37 ms |        5.37 ms |        5.37 ms |
|     │   └─ function call                                   |    6.58 secs |              - |           - | 33940138 |         193 ns |              0 |       44.63 ms |
|     ├─ aggregate at BaseQualityRecalibration.scala:83      |            - | 2 mins 35 secs |           - |        1 | 2 mins 35 secs | 2 mins 35 secs | 2 mins 35 secs |
|     │   └─ function call                                   |   34.49 secs |              - |           - | 33940139 |        1.02 µs |              0 |      510.07 ms |
|     │       └─ Observation Accumulator: seq                |   26.91 secs |              - |           - | 33940138 |         792 ns |              0 |      510.07 ms |
|     └─ map at BaseQualityRecalibration.scala:95            |            - |       71.76 ms |           - |        1 |       71.76 ms |       71.76 ms |       71.76 ms |
|         └─ function call                                   |   57.08 secs |              - |           - |   373654 |      152.76 µs |          89 µs |       39.02 ms |
|             └─ Recalibrate Read                            |   56.85 secs |              - |           - |   373654 |      152.14 µs |          88 µs |       39.01 ms |
|                 └─ Compute Quality Score                   |   51.48 secs |              - |           - |   373654 |      137.76 µs |          75 µs |        34.4 ms |
|                     └─ Get Extra Values                    |   19.01 secs |              - |           - |   373654 |       50.88 µs |          22 µs |       33.31 ms |
| └─ Save File In ADAM Format                                |            - | 2 mins 33 secs |     89.8 ms |        1 | 2 mins 33 secs | 2 mins 33 secs | 2 mins 33 secs |
|     ├─ map at ADAMRDDFunctions.scala:73                    |            - |       30.45 ms |           - |        1 |       30.45 ms |       30.45 ms |       30.45 ms |
|     │   └─ function call                                   |     126.9 ms |              - |           - |   373654 |         339 ns |              0 |          65 µs |
|     └─ saveAsNewAPIHadoopFile at ADAMRDDFunctions.scala:75 |            - | 2 mins 33 secs |           - |        1 | 2 mins 33 secs | 2 mins 33 secs | 2 mins 33 secs |
|         └─ Write ADAM Record                               |   12.22 secs |              - |           - |   373654 |       32.71 µs |              0 |      359.63 ms |
+------------------------------------------------------------+--------------+----------------+-------------+----------+----------------+----------------+----------------+

Spark Operations
+----------+-----------------------------------------------------+---------------+----------------+----------------+----------+
| Sequence |                      Operation                      | Is New Stage? | Stage Duration |  Driver Total  | Stage ID |
+----------+-----------------------------------------------------+---------------+----------------+----------------+----------+
| 1        | map at DecadentRead.scala:50                        | false         |              - |        6.72 ms | -        |
| 2        | filter at BaseQualityRecalibration.scala:71         | false         |              - |       20.09 ms | -        |
| 3        | flatMap at BaseQualityRecalibration.scala:71        | false         |              - |        9.88 ms | -        |
| 4        | map at BaseQualityRecalibration.scala:82            | false         |              - |        5.37 ms | -        |
| 5        | aggregate at BaseQualityRecalibration.scala:83      | true          | 2 mins 35 secs | 2 mins 35 secs | 1        |
| 6        | map at BaseQualityRecalibration.scala:95            | false         |              - |       71.76 ms | -        |
| 7        | map at ADAMRDDFunctions.scala:73                    | false         |              - |       30.45 ms | -        |
| 8        | saveAsNewAPIHadoopFile at ADAMRDDFunctions.scala:75 | true          | 2 mins 32 secs | 2 mins 33 secs | 2        |
+----------+-----------------------------------------------------+---------------+----------------+----------------+----------+
```

### Basic Usage

First, initialize the `Metrics` object and create a Spark listener:

```scala
import org.bdgenomics.utils.instrumentation.Metrics
import org.bdgenomics.utils.instrumentation.{RecordedMetrics, MetricsListener}
Metrics.initialize(sparkContext)
val metricsListener = new MetricsListener(new RecordedMetrics())
sparkContext.addSparkListener(metricsListener)
```

Metrics collection is turned on by calling the `Metrics.initialize` method. Calling the `initialize` method also resets any
previously-recorded metrics, so it is advisable to call it at the start of every Spark job. It is currently also necessary to
create a Spark listener and register this in the Spark context (though this requirement may be removed in future
versions).

Then, to instrument a Spark RDD called `rdd`:

```scala
import org.apache.spark.rdd.MetricsContext._
val instrumentedRDD = rdd.instrument()
```

When any operations are performed on `instrumentedRDD` the RDD operation will be instrumented, along
with any functions that the operation uses. All subsequent RDD operations will be instrumented until
the `unInstrument` method is called on an RDD. For example, consider the following code:

```scala
val array = instrumentedRDD.map(_+1).keyBy(_%2).groupByKey().collect()
val writer = new PrintWriter(new OutputStreamWriter(System.out))
Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
writer.close()
```

This will result in output like the following:

```
Timings
+------------------------------------------+--------------+--------------+-------------+-------+----------+----------+----------+
|                  Metric                  | Worker Total | Driver Total | Driver Only | Count |   Mean   |   Min    |   Max    |
+------------------------------------------+--------------+--------------+-------------+-------+----------+----------+----------+
| └─ map at BdgUtilsTester.scala:26        |            - |     82.84 ms |           - |     1 | 82.84 ms | 82.84 ms | 82.84 ms |
|     └─ function call                     |       321 µs |            - |           - |     5 |  64.2 µs |     8 µs |   284 µs |
| └─ keyBy at BdgUtilsTester.scala:26      |            - |     11.52 ms |           - |     1 | 11.52 ms | 11.52 ms | 11.52 ms |
|     └─ function call                     |        63 µs |            - |           - |     5 |  12.6 µs |     7 µs |    34 µs |
| └─ groupByKey at BdgUtilsTester.scala:26 |            - |     41.63 ms |           - |     1 | 41.63 ms | 41.63 ms | 41.63 ms |
| └─ collect at BdgUtilsTester.scala:26    |            - |     354.3 ms |           - |     1 | 354.3 ms | 354.3 ms | 354.3 ms |
+------------------------------------------+--------------+--------------+-------------+-------+----------+----------+----------+

Spark Operations
+----------+---------------------------------------+---------------+----------------+--------------+----------+
| Sequence |               Operation               | Is New Stage? | Stage Duration | Driver Total | Stage ID |
+----------+---------------------------------------+---------------+----------------+--------------+----------+
| 1        | map at BdgUtilsTester.scala:26        | false         |              - |     82.84 ms | -        |
| 2        | keyBy at BdgUtilsTester.scala:26      | true          |         124 ms |     11.52 ms | 0        |
| 3        | groupByKey at BdgUtilsTester.scala:26 | false         |              - |     41.63 ms | -        |
| 4        | collect at BdgUtilsTester.scala:26    | true          |          53 ms |     354.3 ms | 1        |
+----------+---------------------------------------+---------------+----------------+--------------+----------+
```

#### Timings Table

The "Timings" table contains each Spark operation, as well as timings for each of the functions that the Spark
operations use. This table has 3 columns representing the total time for a particular entry, as follows:

 * The "Worker Total" column is the total time spent in the Spark worker nodes (in other words, within an RDD operation).
 * The "Driver Total" column is the total time spent in the Spark driver program (in other words, outside of any RDD
 operations), including time spent waiting for Spark operations to complete. Since Spark executes operations lazily,
 this column can be misleading. Typically the driver program will only wait for operations that return data back to
 the driver program. Therefore, to investigate the performance of the driver program itself, it is typically better
 to use the "Driver Only" column.
 * The "Driver Only" column is the total time spent in the driver program, _excluding_ any time spent waiting for
 Spark operations to complete. Note that in the above example the "Driver Only" column is always empty, as nothing in
 the driver program has been instrumented. See "Instrumenting Function Calls" for details of how to instrument the
 driver program.

For an explanation of the difference between the Spark driver program and worker nodes see the
[Spark Cluster Mode Overview](http://spark.apache.org/docs/latest/cluster-overview.html).

#### Spark Operations Table

The "Spark Operations" table contains more details about the Spark operations. The operations are displayed in the order in which
they were executed, and are labelled with the location in the driver program from which they are called.
In addition, the following columns are shown:

 * The "Is New Stage?" column specifies whether a particular operation required the creation of a new "stage" in Spark.
 A stage is a group of operations that must be completed before subsequent operations on the same data set can proceed.
 (for example, a [shuffle operation](http://spark.apache.org/docs/latest/programming-guide.html#shuffle-operations)).
 * The "Stage Duration" column specifies the total duration of a particular stage. That is, is represents the
 duration of a particular entry, plus *all of the preceeding entries until the end of the previous stage*, for a particular
 data set. It is not possible to obtain the time for individual operations within a stage - however it is possible to obtain
 timings for functions that are executed by these operations. See the description of the "Timings" table (above) for more information.
 * The "Driver Total" column is the total time spent waiting for an operation to complete in the Spark driver program. This
 matches the corresponding column in the "Timings" table. See the description of that table (above) for further details.
 * The "Stage ID" column is Spark's internal stage ID.

**IMPORTANT**: When using bdg-utils instrumentation it is not a good idea to import `SparkContext._`, as the implicit
conversions in there may conflict with those required for instrumentation. Instead it is better to import
`MetricsContext._` and import only the specific parts of `SparkContext` that are required for your application.
Avoid importing `rddToPairRDDFunctions` and `rddToOrderedRDDFunctions` from `SparkContext` as they will conflict
with the corresponding methods in `MetricsContext`.

### Instrumenting Function Calls

As well as instrumenting top-level functions used Spark operations, it is possible to instrument nested function calls.
For example, consider the following code:

```scala
object MyTimers extends Metrics {
  val DriverFunctionTopLevel = timer("Driver Function Top Level")
  val DriverFunctionNested = timer("Driver Function Nested")
  val WorkerFunctionTopLevel = timer("Worker Function Top Level")
  val WorkerFunctionNested = timer("Worker Function Nested")
}

import MyTimers._

DriverFunctionTopLevel.time {
  DriverFunctionNested.time {
    val array = instrumentedRDD.map(e => {
      WorkerFunctionTopLevel.time {
        WorkerFunctionNested.time {
          e+1
        }
      }
    }).collect()
  }
}

val writer = new PrintWriter(new OutputStreamWriter(System.out, "UTF-8"))
Metrics.print(writer, Some(metricsListener.metrics.sparkMetrics.stageTimes))
writer.close()
```

This will result in output like the following:

```
Timings
+-----------------------------------------------+--------------+--------------+-------------+-------+-----------+-----------+-----------+
|                    Metric                     | Worker Total | Driver Total | Driver Only | Count |   Mean    |    Min    |    Max    |
+-----------------------------------------------+--------------+--------------+-------------+-------+-----------+-----------+-----------+
| └─ Driver Function Top Level                  |            - |    604.27 ms |   168.11 ms |     1 | 604.27 ms | 604.27 ms | 604.27 ms |
|     └─ Driver Function Nested                 |            - |    497.71 ms |    61.55 ms |     1 | 497.71 ms | 497.71 ms | 497.71 ms |
|         ├─ map at NestedTester.scala:40       |            - |     75.39 ms |           - |     1 |  75.39 ms |  75.39 ms |  75.39 ms |
|         │   └─ function call                  |    173.29 ms |            - |           - |     5 |  34.66 ms |  30.84 ms |  35.71 ms |
|         │       └─ Worker Function Top Level  |     166.5 ms |            - |           - |     5 |   33.3 ms |  30.79 ms |  33.99 ms |
|         │           └─ Worker Function Nested |    103.54 ms |            - |           - |     5 |  20.71 ms |  20.21 ms |  20.83 ms |
|         └─ collect at NestedTester.scala:48   |            - |    360.78 ms |           - |     1 | 360.78 ms | 360.78 ms | 360.78 ms |
+-----------------------------------------------+--------------+--------------+-------------+-------+-----------+-----------+-----------+
```

We can see that a tree structure representing the nested function calls, along with their timings, is displayed.

Note that the "Worker Total" column is populated only for function calls within RDD operations, and the "Driver Total" column is populated
for RDD operations themselves, and any operations outside them. The "Driver Only" column is populated just for function calls outside
RDD operations. See the description of the "Timings" table above for further details on these columns.

### Instrumenting File Saving Operations

It is possible to instrument file-saving operations in Spark by using a custom Hadoop `OutputFormat`. For example,
consider the following code:

```scala
import org.apache.spark.rdd.InstrumentedOutputFormat
class InstrumentedAvroParquetOutputFormat extends InstrumentedOutputFormat[Void, IndexedRecord] {
  override def outputFormatClass(): Class[_ <: NewOutputFormat[Void, IndexedRecord]] = classOf[AvroParquetOutputFormat]
  override def timerName(): String = "Write Avro Record"
}
```

This class extends the `InstrumentedOutputFormat` class to add instrumentation around the
`AvroParquetOutputFormat` class. Every record written will be instrumented with the timer name returned
from the `timerName` method.

After extending `InstrumentedOutputFormat` the regular `saveAs*HadoopFile` methods can be used on an instrumented RDD:

```scala
instrumentedRDD.saveAsNewAPIHadoopFile(filePath,
  classOf[java.lang.Void], manifest[T].runtimeClass.asInstanceOf[Class[T]], classOf[InstrumentedAvroParquetOutputFormat],
  ContextUtil.getConfiguration(job))
```

Note that the RDD must have be instrumented (see "Basic Usage" above).

Unfortunately it is not currently possible to instrument file reading, only writing.

### Additional Spark Statistics

It is possible to get additional metrics about Spark tasks. For example, consider the following code:

```scala
import org.bdgenomics.utils.instrumentation.{RecordedMetrics, MetricsListener}
val metricsListener = new MetricsListener(new RecordedMetrics())
sparkContext.addSparkListener(metricsListener)

val array = instrumentedRDD.map(_+1).keyBy(_%2).groupByKey().collect()
val writer = new PrintWriter(new OutputStreamWriter(System.out, "UTF-8"))
metricsListener.metrics.sparkMetrics.print(writer)
writer.close()
```

This will result in output similar to this:

```
Task Timings
+-------------------------------+------------+-------+--------+-------+-------+
|            Metric             | Total Time | Count |  Mean  |  Min  |  Max  |
+-------------------------------+------------+-------+--------+-------+-------+
| Task Duration                 |     128 ms |     2 |  64 ms | 46 ms | 82 ms |
| Executor Run Time             |      86 ms |     2 |  43 ms | 42 ms | 44 ms |
| Executor Deserialization Time |      15 ms |     2 | 7.5 ms |  1 ms | 14 ms |
| Result Serialization Time     |       2 ms |     2 |   1 ms |     0 |  2 ms |
+-------------------------------+------------+-------+--------+-------+-------+

Task Timings By Host
+-------------------------------+-----------+------------+-------+--------+-------+-------+
|            Metric             |   Host    | Total Time | Count |  Mean  |  Min  |  Max  |
+-------------------------------+-----------+------------+-------+--------+-------+-------+
| Task Duration                 | localhost |     128 ms |     2 |  64 ms | 46 ms | 82 ms |
| Executor Run Time             | localhost |      86 ms |     2 |  43 ms | 42 ms | 44 ms |
| Executor Deserialization Time | localhost |      15 ms |     2 | 7.5 ms |  1 ms | 14 ms |
| Result Serialization Time     | localhost |       2 ms |     2 |   1 ms |     0 |  2 ms |
+-------------------------------+-----------+------------+-------+--------+-------+-------+

Task Timings By Stage
+-------------------------------+----------------------------+------------+-------+-------+-------+-------+
|            Metric             |      Stage ID & Name       | Total Time | Count | Mean  |  Min  |  Max  |
+-------------------------------+----------------------------+------------+-------+-------+-------+-------+
| Task Duration                 | 1: keyBy at Ins.scala:30   |      82 ms |     1 | 82 ms | 82 ms | 82 ms |
| Task Duration                 | 0: collect at Ins.scala:30 |      46 ms |     1 | 46 ms | 46 ms | 46 ms |
| Executor Run Time             | 1: keyBy at Ins.scala:30   |      44 ms |     1 | 44 ms | 44 ms | 44 ms |
| Executor Run Time             | 0: collect at Ins.scala:30 |      42 ms |     1 | 42 ms | 42 ms | 42 ms |
| Executor Deserialization Time | 1: keyBy at Ins.scala:30   |      14 ms |     1 | 14 ms | 14 ms | 14 ms |
| Executor Deserialization Time | 0: collect at Ins.scala:30 |       1 ms |     1 |  1 ms |  1 ms |  1 ms |
| Result Serialization Time     | 0: collect at Ins.scala:30 |       2 ms |     1 |  2 ms |  2 ms |  2 ms |
| Result Serialization Time     | 1: keyBy at Ins.scala:30   |          0 |     1 |     0 |     0 |     0 |
+-------------------------------+----------------------------+------------+-------+-------+-------+-------+
```

The tables contain times for various parts of executing a Spark task, as well as the same timings broken down by
host and Spark Stage.

### Performance

The overhead of instrumenting a function call has been measured at around 120 nanoseconds on an Intel i7-3720QM
processor. The overhead of calling the instrumentation code when no metrics are being recorded (the
`Metrics.initialize` method has not be called) is negligible.

### Lifecycle and Threading

Calling the `Metrics.initialize` method turns on metrics collection only for the *calling thread*. Therefore, if the
Driver application is multi-threaded it is necessary to make this call in every thread that requires instrumentation.

Calling the `Metrics.initialize` method resets any previously-recorded Metrics, so it is strongly advised to call
this at the start of each Spark job, otherwise metrics can "leak" between jobs.

If an application does not want to record metrics, it can simply avoid calling the `Metrics.initialize` method.
This is useful for applications that want to avoid recording metrics in certain situations; it is not necessary to
modify any code, just avoid calling the `initialize` method. Attempting to record metrics when the `initialize`
method has not been called will not produce an error, and incurs negligible overhead. However, attempting to
call the `Metrics.print` method will produce an error in this case.

If the application has previously turned on metrics collection, it can be turned off for a particular thread by
calling the `Metrics.stopRecording` method. Calling `uninstrument` on an RDD is not enough to stop metrics collection,
since metrics will still be collected in the Spark Driver. It is always necessary to call the
`Metrics.stopRecording` method as well.

# Getting In Touch

## Mailing List

This project is maintained by the same developers as the [ADAM
project](https://www.github.com/bigdatagenomics/adam). As such, [the ADAM mailing
list](https://groups.google.com/forum/#!forum/adam-developers) is a good
way to sync up with other people who use the bdg-utils code, including the core developers.
You can subscribe by sending an email to `adam-developers+subscribe@googlegroups.com` or
just post using the [web forum page](https://groups.google.com/forum/#!forum/adam-developers).

## IRC Channel

A lot of the developers are hanging on the [#adamdev](http://webchat.freenode.net/?channels=adamdev)
freenode.net channel. Come join us and ask questions.

# License

bdg-utils is released under an [Apache 2.0 license](LICENSE.txt).
