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
package org.bdgenomics.utils.minhash

import org.apache.spark.rdd.RDD
import scala.math.min

object MinHashRDD extends Serializable {

  /**
   * Generates a per-item signature using the hash of a single dataset entry
   * the object. This method is kept private as it is only for internal use.
   *
   * @param hash The hash of the shingle we are processing.
   * @param hashStore A table containing random values.
   * @returns The signature generated from the item.
   */
  private def applyHash(hash: Int,
                        hashStore: HashStore): Array[Int] = {
    val hashArray = new Array[Int](hashStore.size)

    // loop over the hashes and attempt an update
    hashStore.indices.foreach(i => {
      hashArray(i) = min(hashArray(i), hashStore(i) ^ hash)
    })

    hashArray
  }

  /**
   * Reduction function for two hash arrays. Takes the minimum of all elements.
   *
   * @param a1 First array to merge.
   * @param a2 Second array to merge.
   * @return Returns the elementwise minimum of both arrays.
   */
  private def mergeHashes(a1: Array[Int], a2: Array[Int]): Array[Int] = {
    assert(a1.length == a2.length)
    a1.indices.foreach(i => {
      a1(i) = min(a1(i), a2(i))
    })
    a1
  }

  /**
   * Given an RDD, generates a min hash signature for that RDD.
   *
   * @tparam T Type of RDD elements.
   * @param rdd RDD to generate signature for.
   * @param signatureLength Length of signature to use.
   * @param seed Seed to use for generating random hashes.
   * @param hashFn A function to map RDD elements to hashes.
   * @return Returns a min-hash signature for the aggregated RDD.
   */
  def generateSignature[T](rdd: RDD[T],
                           signatureLength: Int,
                           seed: Long,
                           hashFn: T => Int): MinHashSignature = {
    generateSignature(rdd, HashStore(signatureLength, seed), hashFn)
  }

  private def generateSignature[T](rdd: RDD[T],
                                   hashStore: HashStore,
                                   hashFn: T => Int): MinHashSignature = {
    MinHashSignature(rdd.map(v => applyHash(hashFn(v), hashStore))
      .reduce(mergeHashes))
  }
}
