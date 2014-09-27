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

import org.scalatest.FunSuite
import scala.math.abs

class MinHashSignatureSuite extends FunSuite {

  def fpEquals(a: Double, b: Double, tol: Double = 1e-6): Boolean = {
    abs(a - b) < tol
  }

  test("compute the similarity of signatures") {
    assert(fpEquals(MinHashSignature(Array(0, 1, 2, 3))
      .similarity(MinHashSignature(Array(0, 1, 2, 3))), 1.0))
    assert(fpEquals(MinHashSignature(Array(0, 1, 2, 3))
      .similarity(MinHashSignature(Array(0, 1, 2, 5))), 0.75))
    assert(fpEquals(MinHashSignature(Array(0, 1, 2, 3))
      .similarity(MinHashSignature(Array(1, 1, 2, 0))), 0.5))
    assert(fpEquals(MinHashSignature(Array(0, 1, 2, 3))
      .similarity(MinHashSignature(Array(1, 0, 0, 3))), 0.25))
    assert(fpEquals(MinHashSignature(Array(0, 1, 2, 3))
      .similarity(MinHashSignature(Array(3, 2, 1, 0))), 0.0))
  }

  test("compute buckets for a simple signature") {
    val buckets = MinHashSignature(Array(0, 1, 2, 3)).bucket(2)

    assert(buckets.head === MinHashBucketKey(0, Array(0, 1)))
    assert(buckets.last === MinHashBucketKey(1, Array(2, 3)))
  }
}
