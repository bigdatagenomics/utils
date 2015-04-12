/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.utils.minhash

import scala.math.min

trait MinHashable {

  /**
   * Provides the hashes for all of the shingles in an object we are minhashing.
   *
   * @return Returns an array containing a hash per shingle.
   */
  def provideHashes(): Array[Int]

  /**
   * Updates the minhash signature of an object with the hash of a shingle from
   * the object. This method is kept private as it is only for internal use.
   *
   * @param hash The hash of the shingle we are processing.
   * @param hashStore A table containing random values.
   * @param hashArray The signature we are updating.
   */
  private def applyHash(hash: Int,
                        hashStore: HashStore,
                        hashArray: Array[Int]) = {
    // loop over the hashes and attempt an update
    (0 until hashStore.size).foreach(i => {
      hashArray(i) = min(hashArray(i), hashStore(i) ^ hash)
    })
  }

  /**
   * Computes the minhash signature for an object.
   *
   * @param hashStore The randomized hash store to use when computing the signature.
   * @return Returns the signature of this object.
   */
  final private[minhash] def minHash(hashStore: HashStore): MinHashSignature = {

    // get hashes
    val hashes = provideHashes()

    // initialize by creating an array and setting everything to Inf
    val hashArray = new Array[Int](hashStore.size)

    (0 until hashStore.size).foreach(i => hashArray(i) = Int.MaxValue)

    // loop over hashes and apply minhashing algorithm
    hashes.foreach(hash => applyHash(hash, hashStore, hashArray))

    // return the hash array
    MinHashSignature(hashArray)
  }
}
