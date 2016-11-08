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

import scala.util.Random

private[minhash] object HashStore {

  /**
   * Generates a hash store that is a given length with a given seed.
   *
   * @param length The desired length of the hash store.
   * @param seed A seed for the random number generator.
   * @return Returns a randomized store of the requested length.
   */
  def apply(length: Int, seed: Long): HashStore = {
    val rgen = new Random(seed)

    // generate random values
    new HashStore((0 until length).map(i => rgen.nextInt()).toArray)
  }
}

private[minhash] class HashStore private (hashes: Array[Int]) extends Serializable {

  /**
   * Gets a hash from an index location.
   *
   * @param idx Index of the hash to get.
   * @return Int Returns the integer hash value.
   */
  def apply(idx: Int): Int = hashes(idx)

  /**
   * @return Returns the number of hashes in the store.
   */
  def size: Int = hashes.length

  /**
   * @regurn Returns the indices for all hash elements.
   */
  def indices: scala.collection.immutable.Range = hashes.indices
}
