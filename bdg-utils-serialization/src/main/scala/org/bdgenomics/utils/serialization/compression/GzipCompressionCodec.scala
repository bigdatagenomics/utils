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
package org.bdgenomics.utils.serialization.compression

/**
 * Created by alexsalazar on 3/6/15.
 * Class was copied from maropu's pull request:
 * https://github.com/maropu/spark/commit/2b8c62503c7b41d814d67c7034084c52856388e4
 */

import java.io.{ InputStream, OutputStream }
import java.util.zip.{ GZIPInputStream, GZIPOutputStream }
import org.apache.spark.SparkConf
import org.apache.spark.io.CompressionCodec

class GzipCompressionCodec(conf: SparkConf) extends CompressionCodec {

  override def compressedOutputStream(s: OutputStream): OutputStream = {
    val blockSize = conf.getInt("bdg.gzip.block.size", 32768)
    new GZIPOutputStream(s, blockSize)
  }

  override def compressedInputStream(s: InputStream): InputStream = new GZIPInputStream(s)
}

