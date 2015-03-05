/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.utils.parquet.rdd

import org.apache.avro.Schema
import org.apache.avro.specific.SpecificRecord
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.MetricsContext._
import org.apache.spark.{ Logging, SparkConf, SparkContext }
import org.bdgenomics.utils.misc.HadoopUtil
import org.bdgenomics.utils.instrumentation.Metrics
import parquet.avro.{ AvroParquetInputFormat, AvroReadSupport }
import parquet.filter.UnboundRecordFilter
import parquet.hadoop.ParquetInputFormat
import parquet.hadoop.util.ContextUtil
import scala.reflect.ClassTag

object BDGParquetContext {
  implicit def scToBDGPContext(sc: SparkContext): BDGParquetContext = new BDGParquetContext(sc)

  // Add generic RDD methods for parquet RDDs
  implicit def rddToParquetRDD[T](rdd: RDD[T])(implicit ev1: T => SpecificRecord, ev2: Manifest[T]): BDGParquetRDDFunctions[T] = new BDGParquetRDDFunctions(rdd)
}

class BDGParquetContext(val sc: SparkContext) extends Serializable with Logging {

  /**
   * This method will create a new RDD from a parquet file.
   * @param filePath The path to the input data
   * @param predicate An optional pushdown predicate to use when reading the data
   * @param projection An option projection schema to use when reading the data
   * @tparam T The type of records to return
   * @return An RDD with records of the specified type
   */
  private[rdd] def parquetLoad[T, U <: UnboundRecordFilter](filePath: String, predicate: Option[Class[U]] = None, projection: Option[Schema] = None)(implicit ev1: T => SpecificRecord, ev2: Manifest[T]): RDD[T] = {
    //make sure a type was specified
    //not using require as to make the message clearer
    if (manifest[T] == manifest[scala.Nothing])
      throw new IllegalArgumentException("Type inference failed; when loading please specify a specific type. " +
        "e.g.:\nval reads: RDD[AlignmentRecord] = ...\nbut not\nval reads = ...\nwithout a return type")

    log.info("Reading the Parquet file at %s to create RDD".format(filePath))
    val job = HadoopUtil.newJob(sc)
    ParquetInputFormat.setReadSupportClass(job, classOf[AvroReadSupport[T]])

    if (predicate.isDefined) {
      log.info("Using the specified push-down predicate")
      ParquetInputFormat.setUnboundRecordFilter(job, predicate.get)
    }

    if (projection.isDefined) {
      log.info("Using the specified projection schema")
      AvroParquetInputFormat.setRequestedProjection(job, projection.get)
    }

    val records = sc.newAPIHadoopFile(
      filePath,
      classOf[ParquetInputFormat[T]],
      classOf[Void],
      manifest[T].runtimeClass.asInstanceOf[Class[T]],
      ContextUtil.getConfiguration(job))

    val instrumented = if (Metrics.isRecording) records.instrument() else records
    val mapped = instrumented.map(p => p._2)

    if (predicate.isDefined) {
      // Strip the nulls that the predicate returns
      mapped.filter(p => p != null.asInstanceOf[T])
    } else {
      mapped
    }
  }
}
