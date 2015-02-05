bdg-utils
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
+------------------------------------------------------------+-------------+-------------+---------+-----------+--------+-----------+
|                           Metric                           | Worker Time | Driver Time |  Count  |   Mean    |  Min   |    Max    |
+------------------------------------------------------------+-------------+-------------+---------+-----------+--------+-----------+
| └─ Base Quality Recalibration                              |           - |   1.41 secs |       1 | 1.41 secs |      - |         - |
|     ├─ map at DecadentRead.scala:50                        |           - |           - |       1 |         - |      - |         - |
|     │   └─ function call                                   |   9.14 secs |           - |  185804 |  49.18 µs |      0 | 1.72 secs |
|     ├─ filter at BaseQualityRecalibration.scala:71         |           - |           - |       1 |         - |      - |         - |
|     │   └─ function call                                   |    95.09 ms |           - |   92902 |   1.02 µs |      0 |    782 µs |
|     ├─ flatMap at BaseQualityRecalibration.scala:71        |           - |           - |       1 |         - |      - |         - |
|     │   └─ function call                                   |  20.18 secs |           - |   90385 | 223.29 µs |  73 µs | 1.38 secs |
|     ├─ map at BaseQualityRecalibration.scala:82            |           - |           - |       1 |         - |      - |         - |
|     │   └─ function call                                   |   3.91 secs |           - | 8446035 |    463 ns |      0 | 1.35 secs |
|     ├─ aggregate at BaseQualityRecalibration.scala:83      |           - |           - |       1 |         - |      - |         - |
|     │   └─ function call                                   |  17.23 secs |           - | 8446036 |   2.04 µs |      0 | 1.23 secs |
|     │       └─ Observation Accumulator: seq                |  14.25 secs |           - | 8446035 |   1.69 µs |      0 | 1.23 secs |
|     └─ map at BaseQualityRecalibration.scala:95            |           - |           - |       1 |         - |      - |         - |
|         └─ function call                                   |  31.98 secs |           - |   92902 | 344.23 µs | 195 µs | 1.26 secs |
|             └─ Recalibrate Read                            |   31.9 secs |           - |   92902 | 343.41 µs | 194 µs | 1.26 secs |
|                 └─ Compute Quality Score                   |  29.93 secs |           - |   92902 | 322.15 µs | 181 µs | 1.26 secs |
| └─ Save File In ADAM Format                                |           - |    83.61 ms |       1 |  83.61 ms |      - |         - |
|     ├─ map at ADAMRDDFunctions.scala:80                    |           - |           - |       1 |         - |      - |         - |
|     │   └─ function call                                   |    41.84 ms |           - |   92902 |    450 ns |      0 |     51 µs |
|     └─ saveAsNewAPIHadoopFile at ADAMRDDFunctions.scala:82 |           - |           - |       1 |         - |      - |         - |
|         └─ Write ADAM Record                               |    2.4 secs |           - |   92902 |  25.86 µs |      0 |   9.01 ms |
+------------------------------------------------------------+-------------+-------------+---------+-----------+--------+-----------+

Spark Operations
+-----------------------------------------------------+--------------+--------------+----------+
|                      Operation                      | Is Blocking? |   Duration   | Stage ID |
+-----------------------------------------------------+--------------+--------------+----------+
| map at DecadentRead.scala:50                        | false        |            - | -        |
| filter at BaseQualityRecalibration.scala:71         | false        |            - | -        |
| flatMap at BaseQualityRecalibration.scala:71        | false        |            - | -        |
| map at BaseQualityRecalibration.scala:82            | false        |            - | -        |
| aggregate at BaseQualityRecalibration.scala:83      | true         | 1 min 0 secs | 2        |
| map at BaseQualityRecalibration.scala:95            | false        |            - | -        |
| map at ADAMRDDFunctions.scala:80                    | false        |            - | -        |
| saveAsNewAPIHadoopFile at ADAMRDDFunctions.scala:82 | true         |   47.09 secs | 3        |
+-----------------------------------------------------+--------------+--------------+----------+
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
Metrics.print(writer, None)
writer.close()
```

This will result in output like the following:

```
Timings
+-------------------------------+-------------+-------------+-------+---------+------+--------+
|            Metric             | Worker Time | Driver Time | Count |  Mean   | Min  |  Max   |
+-------------------------------+-------------+-------------+-------+---------+------+--------+
| └─ map at Ins.scala:30        |           - |           - |     1 |       - |    - |      - |
|     └─ function call          |      642 µs |           - |    10 | 64.2 µs | 6 µs | 550 µs |
| └─ keyBy at Ins.scala:30      |           - |           - |     1 |       - |    - |      - |
|     └─ function call          |      140 µs |           - |    10 |   14 µs | 5 µs |  64 µs |
| └─ groupByKey at Ins.scala:30 |           - |           - |     1 |       - |    - |      - |
| └─ collect at Ins.scala:30    |           - |           - |     1 |       - |    - |      - |
+-------------------------------+-------------+-------------+-------+---------+------+--------+

Spark Operations
+----------------------------+--------------+----------+----------+
|         Operation          | Is Blocking? | Duration | Stage ID |
+----------------------------+--------------+----------+----------+
| map at Ins.scala:30        | false        |        - | -        |
| keyBy at Ins.scala:30      | true         |    91 ms | 1        |
| groupByKey at Ins.scala:30 | false        |        - | -        |
| collect at Ins.scala:30    | true         |    40 ms | 0        |
+----------------------------+--------------+----------+----------+
```

The first table contains each Spark operation, as well as timings for each of the functions that the Spark
operations use. The "Worker Time" column is the total time spent executing a particular function in the Spark Workers,
and the "Count" column is the number of times that the function was called.

The second table contains more details about the Spark operations. The "Is Blocking?" column specifies whether a
particular operation was blocking (it must complete before subsequent operations on the same RDD can proceed). For
blocking operations the "Duration" column contains the duration of the Spark stage that corresponds to this operation.
That is, this is *the duration of this operation plus all of the preceding operations on the same RDD, up until the
previous blocking operation*.

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
+-----------------------------------------------+-------------+-------------+-------+----------+-------+---------+
|                    Metric                     | Worker Time | Driver Time | Count |   Mean   |  Min  |   Max   |
+-----------------------------------------------+-------------+-------------+-------+----------+-------+---------+
| └─ Driver Function Top Level                  |           - |     8.93 ms |     1 |  8.93 ms |     - |       - |
|     └─ Driver Function Nested                 |           - |     7.01 ms |     1 |  7.01 ms |     - |       - |
|         ├─ map at Ins.scala:50                |           - |           - |     1 |        - |     - |       - |
|         │   └─ function call                  |     1.75 ms |           - |    10 | 175.3 µs | 27 µs | 1.36 ms |
|         │       └─ Worker Function Top Level  |      916 µs |           - |    10 |  91.6 µs | 15 µs |  717 µs |
|         │           └─ Worker Function Nested |       77 µs |           - |    10 |   7.7 µs |  4 µs |   38 µs |
|         └─ collect at Ins.scala:56            |           - |           - |     1 |        - |     - |       - |
+-----------------------------------------------+-------------+-------------+-------+----------+-------+---------+
```

Note that there are two columns in the output to represent the total time spent on a particular function call:
"Driver Time" and "Worker Time". A particular function call will be in one or the other.
Driver Time is the total time spent on the function call in the Spark Driver (in other words, outside of any RDD
operations). Worker Time is the total time spent in a Spark Worker (in other words, within an RDD operation).

Note that the Driver Time does not include the time taken to execute Spark operations. This is because, in Spark,
operations are performed lazily: that is, for a particular RDD most operations take hardly any time at all, and a few
take a long time. Therefore it would be misleading to include the time taken to execute Spark operations in the Driver
time.

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
