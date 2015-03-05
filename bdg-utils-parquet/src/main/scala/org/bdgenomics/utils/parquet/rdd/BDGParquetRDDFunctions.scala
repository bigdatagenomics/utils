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

import java.util.logging.Level
import org.apache.avro.specific.SpecificRecord
import org.apache.spark.Logging
import org.apache.spark.rdd.{ InstrumentedOutputFormat, RDD }
import org.apache.spark.rdd.MetricsContext._
import org.bdgenomics.utils.misc.HadoopUtil
import org.bdgenomics.utils.parquet.Timers._
import org.bdgenomics.utils.parquet.util.ParquetLogger
import parquet.avro.AvroParquetOutputFormat
import parquet.hadoop.ParquetOutputFormat
import parquet.hadoop.metadata.CompressionCodecName
import parquet.hadoop.util.ContextUtil
import org.apache.avro.generic.IndexedRecord
import org.apache.hadoop.mapreduce.{ OutputFormat => NewOutputFormat, _ }

trait ParquetRDDArgs {
  var blockSize: Int
  var pageSize: Int
  var compressionCodec: CompressionCodecName
  var disableDictionaryEncoding: Boolean
}

trait SaveArgs extends ParquetRDDArgs {
  var outputPath: String
}

class BDGParquetRDDFunctions[T <% SpecificRecord: Manifest](rdd: RDD[T]) extends Serializable with Logging {

  def saveAsParquet(args: SaveArgs): Unit = {
    saveAsParquet(
      args.outputPath,
      args.blockSize,
      args.pageSize,
      args.compressionCodec,
      args.disableDictionaryEncoding)
  }

  def saveAsParquet(filePath: String,
                    blockSize: Int = 128 * 1024 * 1024,
                    pageSize: Int = 1 * 1024 * 1024,
                    compressCodec: CompressionCodecName = CompressionCodecName.GZIP,
                    disableDictionaryEncoding: Boolean = false): Unit = SaveAsParquet.time {
    val job = HadoopUtil.newJob(rdd.context)
    ParquetLogger.hadoopLoggerLevel(Level.SEVERE)
    ParquetOutputFormat.setCompression(job, compressCodec)
    ParquetOutputFormat.setEnableDictionary(job, !disableDictionaryEncoding)
    ParquetOutputFormat.setBlockSize(job, blockSize)
    ParquetOutputFormat.setPageSize(job, pageSize)
    AvroParquetOutputFormat.setSchema(job, manifest[T].runtimeClass.asInstanceOf[Class[T]].newInstance().getSchema)
    // Add the Void Key
    val recordToSave = rdd.map(p => (null, p))
    // Save the values to the Parquet file
    recordToSave.saveAsNewAPIHadoopFile(filePath,
      classOf[java.lang.Void], manifest[T].runtimeClass.asInstanceOf[Class[T]], classOf[InstrumentedAvroParquetOutputFormat],
      ContextUtil.getConfiguration(job))
  }
}

class InstrumentedAvroParquetOutputFormat extends InstrumentedOutputFormat[Void, IndexedRecord] {
  override def outputFormatClass(): Class[_ <: NewOutputFormat[Void, IndexedRecord]] = classOf[AvroParquetOutputFormat]
  override def timerName(): String = WriteRecord.timerName
}
