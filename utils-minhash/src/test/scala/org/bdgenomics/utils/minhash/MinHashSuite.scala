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

import org.bdgenomics.utils.misc.SparkFunSuite
import scala.math.abs
import scala.util.Random

case class MinHashableRead(sequence: String, start: Long, kmerLen: Int) extends MinHashable {
  def provideHashes(): Array[Int] = {
    sequence.sliding(kmerLen)
      .map(_.hashCode)
      .toArray
  }
}

class MinHashSuite extends SparkFunSuite {

  def randomString(seed: Int, len: Int): String = {
    val r = new Random(seed)

    (0 until len).map(i => r.nextInt(4))
      .map(i => i match {
        case 0 => "A"
        case 1 => "C"
        case 2 => "G"
        case _ => "T"
      }).reduceLeft(_ + _)
  }

  val kmerLength = 15

  def expectedSimilarity(difference: Long): Double = {
    def kmersPerSequence(length: Int): Int = {
      if (length > 0) {
        length - kmerLength + 1
      } else {
        0
      }
    }
    val similarSequence = 1000 - difference.toInt * 100
    val distinctSequencePerRead = difference.toInt * 100
    kmersPerSequence(similarSequence).toDouble / (2.0 * kmersPerSequence(distinctSequencePerRead) +
      kmersPerSequence(similarSequence))
  }

  val baseString = randomString(0, 2000)

  def fpCompare(a: Double, b: Double, epsilon: Double = 1e-6): Boolean = {
    abs(a - b) < epsilon
  }

  sparkTest("compute exact overlap for ten 1000 bp reads") {
    var read = -1
    val reads = sc.parallelize(baseString
      .sliding(1000, 100)
      .toSeq
      .map(s => {
        read += 1
        MinHashableRead(s, read, kmerLength)
      }))

    val exact = MinHash.exactMinHash(reads, 500, Option(1L))
      .collect()

    assert(exact.length === 121)
    exact.foreach(kv => {
      val (similarity, (r1, r2)) = kv
      val expected = expectedSimilarity(abs(r1.start - r2.start))

      assert(fpCompare(similarity, expected, 0.1))
    })
  }

  sparkTest("compute approximate overlap for ten 1000 bp reads across different band sizes") {
    var read = -1
    val reads = sc.parallelize(baseString
      .sliding(1000, 100)
      .toSeq
      .map(s => {
        read += 1
        MinHashableRead(s, read, kmerLength)
      }))

    // compare against the exact approach - build a map for lookup
    val exact = MinHash.exactMinHash(reads, 500, Option(1L))
      .collect()
      .map(kv => {
        val (similarity, (r1, r2)) = kv

        ((r1.start, r2.start), similarity)
      }).toMap
    var lastLength = exact.size

    Seq(100, 50, 25, 20, 10).foreach(bands => {
      val approx = MinHash.approximateMinHash(reads, 500, bands, Option(1L))
        .collect()

      val newLength = approx.length
      assert(newLength <= lastLength)
      lastLength = newLength

      approx.foreach(kv => {
        val (similarity, (r1, r2)) = kv
        val expected = expectedSimilarity(abs(r1.start - r2.start))

        assert(fpCompare(similarity, expected, 0.1))
        assert(fpCompare(similarity, exact((r1.start, r2.start))))
      })

      // check that we don't have any dupe keys
      assert(approx.map(kv => kv._2).distinct.length === approx.length)
    })
  }

  sparkTest("should throw exception if we pick an illegal band count") {
    var read = -1
    val reads = sc.parallelize(baseString
      .sliding(1000, 100)
      .toSeq
      .map(s => {
        read += 1
        MinHashableRead(s, read, kmerLength)
      }))

    intercept[IllegalArgumentException] {
      MinHash.approximateMinHash(reads, 500, 13, Option(1L))
    }
  }
}
