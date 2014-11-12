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
package org.bdgenomics.utils.parquet {

  import org.apache.avro.Schema
  import org.apache.avro.generic.IndexedRecord
  import org.apache.spark.rdd.RDD
  import org.apache.spark.{ Partition, SparkContext, TaskContext }
  import org.bdgenomics.utils.parquet.io.{ ByteAccess, FileLocator }
  import org.bdgenomics.utils.parquet.rdd._
  import parquet.avro.{ AvroSchemaConverter, UsableAvroRecordMaterializer }
  import parquet.filter.UnboundRecordFilter
  import parquet.schema.MessageType

  import scala.reflect._

  class AvroParquetRDD[T <: IndexedRecord: ClassTag](@transient sc: SparkContext,
                                                     private val filter: UnboundRecordFilter,
                                                     private val parquetFile: FileLocator,
                                                     @transient private val requestedSchema: Option[Schema])
      extends RDD[T](sc, Nil) {

    assert(requestedSchema != null, "Use \"None\" instead of null for no schema.")

    def convertAvroSchema(schema: Option[Schema], fileMessageType: MessageType): MessageType =
      schema match {
        case None    => fileMessageType
        case Some(s) => new AvroSchemaConverter().convert(s)
      }

    def io(): ByteAccess = parquetFile.bytes

    override protected def getPartitions: Array[Partition] = {

      val fileMetadata = ParquetCommon.readFileMetadata(io())
      val footer = new Footer(fileMetadata)
      val fileMessageType = ParquetCommon.parseMessageType(fileMetadata)
      val fileSchema = new ParquetSchemaType(fileMessageType)
      val requestedMessage = convertAvroSchema(requestedSchema, fileMessageType)
      val requested = new ParquetSchemaType(requestedMessage)

      footer.rowGroups.zipWithIndex.map {
        case (rg, i) => new ParquetPartition(parquetFile, i, rg, requested, fileSchema)
      }.toArray
    }

    override def compute(split: Partition, context: TaskContext): Iterator[T] = {
      val reqSchema = classTag[T].runtimeClass.newInstance().asInstanceOf[T].getSchema
      val byteAccess = io()
      val parquetPartition = split.asInstanceOf[ParquetPartition]
      def requestedMessageType = parquetPartition.requestedSchema.convertToParquet()

      val avroRecordMaterializer = new UsableAvroRecordMaterializer[T](requestedMessageType, reqSchema)

      parquetPartition.materializeRecords(byteAccess, avroRecordMaterializer, filter)
    }
  }
}

package parquet.avro {

  import org.apache.avro.Schema
  import parquet.io.api.{ GroupConverter, RecordMaterializer }
  import parquet.schema.MessageType

  /**
   * Once again, Parquet has put AvroRecordMaterializer and import org.apache.avro.Schemad place it in the parquet.avro package directly, so that we have access to the appropriate
   * methods.
   */
  class UsableAvroRecordMaterializer[T <: org.apache.avro.generic.IndexedRecord](root: AvroIndexedRecordConverter[T]) extends RecordMaterializer[T] {
    def this(requestedSchema: MessageType, avroSchema: Schema) =
      this(new AvroIndexedRecordConverter[T](requestedSchema, avroSchema))

    def getCurrentRecord: T = root.getCurrentRecord
    def getRootConverter: GroupConverter = root
  }
}
