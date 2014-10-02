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

private[minhash] object MinHashSignature {

  /**
   * For two signatures, finds the key for the first bucket that both signatures
   * hash to, if one exists.
   *
   * @note This method does not check to ensure that the band size divides
   *       roundly into the size of the signature; we delegate this check
   *       to the function caller.
   *
   * @param sig1 First signature to compare.
   * @param sig2 Second signature to compare.
   * @param bandSize The size of bands to use.
   * @return Returns an option containing the first overlapping bucket key, if
   *         one exists.
   */
  def firstBucket(sig1: MinHashSignature,
                  sig2: MinHashSignature,
                  bandSize: Int): Option[MinHashBucketKey] = {
    // get and zip buckets
    val keys = sig1.bucket(bandSize).zip(sig2.bucket(bandSize))

    // find first bucket where the two are equal, and take the band
    keys.find(p => p._1.equals(p._2))
      .map(p => p._1)
  }
}

private[minhash] case class MinHashSignature(hashArray: Array[Int]) {

  /**
   * Splits this signature into multiple bands, which can then be used to
   * drive locality sensitive approximate checking.
   *
   * @note This method does not check to ensure that the band size divides
   *       roundly into the size of the signature; we delegate this check
   *       to the function caller.
   *
   * @param The number of signature rows to use per band.
   * @return Returns this signature sliced into multiple band keys.
   */
  def bucket(bandSize: Int): Iterable[MinHashBucketKey] = {
    // split into groups
    val groups = hashArray.grouped(bandSize)

    // build and return keys
    var band = -1

    groups.map(keys => {
      band += 1
      MinHashBucketKey(band, keys)
    }).toIterable
  }

  /**
   * Computes the estimated Jaccard similarity of two objects that have MinHash
   * signatures.
   *
   * @note This signature must be the same length as the signature we are
   *       comparing to. We do not perform this check; we delegate this check
   *       to the function caller.
   *
   * @param Another signature to similarity against.
   * @return Returns the estimated Jaccard similarity of two signatures, which
   *         is defined as the number of elements in the signature that agree
   *         over the total length of the signature.
   */
  def similarity(other: MinHashSignature): Double = {
    var overlap = 0

    // loop over elements - increment overlap count if signatures match
    (0 until hashArray.length).foreach(i => {
      if (hashArray(i) == other.hashArray(i)) {
        overlap += 1
      }
    })

    // similarity is the number of matching elements over the signature length
    overlap.toDouble / hashArray.length.toDouble
  }
}
