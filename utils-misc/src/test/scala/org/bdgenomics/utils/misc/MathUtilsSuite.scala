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
package org.bdgenomics.utils.misc

import scala.math.log

class MathUtilsSuite extends SparkFunSuite {

  test("check equality with floating point comparison") {
    assert(MathUtils.fpEquals(0.0, 0.0))
    assert(MathUtils.fpEquals(1e-6, 1e-7))
    assert(MathUtils.fpEquals(1e-6, 0.0))
  }

  test("check inequality with floating point comparison") {
    assert(!MathUtils.fpEquals(1.0, 0.0))
    assert(!MathUtils.fpEquals(1e-6, 1e6))
    assert(!MathUtils.fpEquals(1e-5, 0.0))
  }

  test("compute factorials") {
    assert(MathUtils.factorial(5) === 120)
    assert(MathUtils.factorial(1) === 1)
    assert(MathUtils.factorial(0) === 1)
  }

  test("test array aggregation in isolation") {
    val a1 = Array(0.0, 1.0, 2.0)
    val a2 = Array(1.0, -1.0, -3.0)

    val a3 = MathUtils.aggregateArray(a1, a2)

    assert(a1 == a3)
    assert(MathUtils.fpEquals(a1(0), 1.0))
    assert(MathUtils.fpEquals(a1(1), 0.0))
    assert(MathUtils.fpEquals(a1(2), -1.0))
    assert(MathUtils.fpEquals(a2(0), 1.0))
    assert(MathUtils.fpEquals(a2(1), -1.0))
    assert(MathUtils.fpEquals(a2(2), -3.0))
    assert(MathUtils.fpEquals(a3(0), 1.0))
    assert(MathUtils.fpEquals(a3(1), 0.0))
    assert(MathUtils.fpEquals(a3(2), -1.0))
  }

  sparkTest("use array aggregator with an rdd") {
    val seq = (0 until 20).map(i => {
      val a = Array.fill(20) { 0.0 }
      a(i) = 1.0
      a
    }).toSeq
    val rdd = sc.parallelize(seq)

    val accumulate = rdd.aggregate(Array.fill(20) { 0.0 })(MathUtils.aggregateArray,
      MathUtils.aggregateArray)
    (0 until 20).foreach(i => assert(MathUtils.fpEquals(accumulate(i), 1.0)))

    assert(rdd.count() === 20)
    val collectedRdd = rdd.collect()
    (0 until 20).foreach(i => {
      (0 until 20).foreach(j => {
        if (i != j) {
          assert(MathUtils.fpEquals(collectedRdd(i)(j), 0.0))
        } else {
          assert(MathUtils.fpEquals(collectedRdd(i)(j), 1.0))
        }
      })
    })
  }

  test("safe wrapped log should work") {
    assert(MathUtils.fpEquals(MathUtils.safeLog(2.0), log(2.0)))
    assert(MathUtils.fpEquals(MathUtils.safeLog(1.0), 0.0))
    assert(MathUtils.fpEquals(MathUtils.safeLog(0.5), log(0.5)))
    assert(MathUtils.fpEquals(MathUtils.safeLog(0.0, 0.0), 0.0))
  }

  test("softmax an array") {
    val a = Array(3.0, 2.0, 5.0, 0.0)

    assert(MathUtils.fpEquals(a.sum, 10.0))
    MathUtils.softmax(a)
    assert(MathUtils.fpEquals(a.sum, 1.0))
    assert(MathUtils.fpEquals(a(0), 0.3))
    assert(MathUtils.fpEquals(a(1), 0.2))
    assert(MathUtils.fpEquals(a(2), 0.5))
    assert(MathUtils.fpEquals(a(3), 0.0))
  }

  test("multiply an array by a scalar") {
    val a = Array(0.0, 1.0, -2.0)

    MathUtils.scalarArrayMultiply(2.0, a)

    assert(a.length === 3)
    assert(MathUtils.fpEquals(a(0), 0.0))
    assert(MathUtils.fpEquals(a(1), 2.0))
    assert(MathUtils.fpEquals(a(2), -4.0))
  }
}
