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

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

/**
 * This object presents several methods for determining approximate pair-wise
 * Jaccard similarity through the use of MinHash signatures. A description of
 * this algorithm can be found in chapter 3 of:
 *
 * Rajaraman, Anand, and Jeffrey David Ullman. Mining of massive datasets.
 * Cambridge University Press, 2011.
 *
 * This chapter may be freely (and legally) downloaded from:
 *
 * http://infolab.stanford.edu/~ullman/mmds/ch3.pdf
 */
object MinHash extends Serializable {

  /**
   * Implements an exact pair-wise MinHash similarity check. Exact refers to
   * "all-pairs", not "similarity"; MinHash signature comparison approximates
   * Jaccard similarity, and this method _exactly_ compares all pairs of inputs,
   * as opposed to locality sensitive hashing (LSH) based approximations.
   *
   * @note This operation may be expensive, as it performs a cartesian
   *       product of all elements in the input RDD.
   *
   * @tparam T This function will operate on RDDs containing any type T that
   *         extends the MinHashable trait.
   *
   * @param rdd The RDD of data points to compute similarity on.
   * @param signatureLength The length of MinHash signature to use.
   * @param randomSeed An optional seed for random number generation.
   * @return Returns an RDD containing all pairs of elements, with their
   *          similarity, as a tuple of (similarity, (elem1, elem2)).
   */
  def exactMinHash[T <: MinHashable](rdd: RDD[T],
                                     signatureLength: Int,
                                     randomSeed: Option[Long] = None): RDD[(Double, (T, T))] = {
    // generate signatures
    val signedRdd = generateSignatures(rdd, signatureLength, randomSeed)

    // cartesian this rdd by itself
    val allPairsRdd = signedRdd.cartesian(signedRdd)

    // compute estimated jaccard similarity and return
    allPairsRdd.map(p => {
      val ((sig1, item1), (sig2, item2)) = p

      (sig1.similarity(sig2), (item1, item2))
    })
  }

  /**
   * Implements an approximate pair-wise MinHash similarity check. Approximate
   * refers to "all-pairs", not "similarity"; MinHash signature comparison
   * approximates Jaccard similarity. This method uses a locality sensitive
   * hashing (LSH) based approach to reduce the number of comparisons required.
   *
   * We use the LSH technique described in section 3.4.1 of the Ullman text.
   * This technique creates _b_ bands which divide the hashing space. For a
   * MinHash signature with length _l_, we require b * r = l, where _r_ is
   * the number of rows in each band. For given _b_ and _r_, we expect to
   * compare all elements with similarity greater than (1/b)^(1/r).
   *
   * @throws IllegalArgumentException Throws an illegal argument exception if
   *                                  the number of bands does not divide
   *                                  evenly into the signature length.
   *
   * @tparam T This function will operate on RDDs containing any type T that
   *         extends the MinHashable trait.
   *
   * @param rdd The RDD of data points to compute similarity on.
   * @param signatureLength The length of MinHash signature to use.
   * @param bands The number of bands to use for LSHing.
   * @param randomSeed An optional seed for random number generation.
   * @return Returns an RDD containing all pairs of elements, with their
   *          similarity, as a tuple of (similarity, (elem1, elem2)).
   */
  def approximateMinHash[T <: MinHashable](rdd: RDD[T],
                                           signatureLength: Int,
                                           bands: Int,
                                           randomSeed: Option[Long] = None): RDD[(Double, (T, T))] = {
    // generate signatures
    val signedRdd = generateSignatures(rdd, signatureLength, randomSeed)

    // get band length
    if (signatureLength % bands != 0) {
      throw new IllegalArgumentException("Signature length must divide roundly by the band count.")
    }
    val bandLength = signatureLength / bands

    // replicate all keys into buckets and group by key
    val bucketGroups = signedRdd.flatMap(kv => {
      val (signature, item) = kv

      // split signature into buckets
      val buckets = signature.bucket(bandLength)

      // copy key per bucket
      buckets.map(bucket => (bucket, (signature, item)))
    }).groupByKey()

    // per bucket, take inner product and compute similarity
    bucketGroups.flatMap(kv => {
      val (bucket, pairs) = kv

      // convert pairs to an array
      val pairArray = pairs.toArray

      // create list to append to
      var l = List[(Double, (T, T))]()

      // loop over contents to take inner product
      (0 until pairArray.length).foreach(i => {
        ((i + 1) until pairArray.length).foreach(j => {
          val (sig1, item1) = pairArray(i)
          val (sig2, item2) = pairArray(j)

          // is this the first bucket these elements overlap in? if not, don't
          // process to limit the number of dupes emitted
          if (MinHashSignature.firstBucket(sig1, sig2, bandLength).exists(_.equals(bucket))) {
            // compute similarity and add to list
            l = (sig1.similarity(sig2), (item1, item2)) :: l
          }
        })
      })

      // return list
      l
    })
  }

  /**
   * @param randomSeed An optional random seed.
   * @return Unpacks the random seed if one is provided, else returns the
   *         current Linux timestamp.
   */
  private def getSeed(randomSeed: Option[Long]): Long = randomSeed match {
    case Some(i) => i
    case _       => System.currentTimeMillis()
  }

  /**
   * Generates the signatures for an RDD of input elements.
   *
   * @param rdd The RDD to generate signatures for.
   * @param signatureLength The desired signature length.
   * @param randomSeed An optional seed for randomization.
   * @return Returns an RDD of signature/item pairs.
   */
  private def generateSignatures[T <: MinHashable](rdd: RDD[T],
                                                   signatureLength: Int,
                                                   randomSeed: Option[Long]): RDD[(MinHashSignature, T)] = {
    // generate random hash store
    val hashStore = HashStore(signatureLength, getSeed(randomSeed))

    // key each item by its signature
    rdd.keyBy(_.minHash(hashStore))
  }
}
