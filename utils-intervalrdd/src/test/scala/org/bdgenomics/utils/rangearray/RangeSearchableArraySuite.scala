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
package org.bdgenomics.utils.rangearray

import org.bdgenomics.utils.intervalrdd.Region
import org.bdgenomics.utils.misc.SparkFunSuite

class RangeSearchableArraySuite extends SparkFunSuite {

  test("succeeds in searching empty RangeSearchableArray") {
    val array = RangeSearchableArray(Array[(Region, Long)]())

    // retrieve a value wholly for nonexistant key
    val wholly = array.get(Region(11L, 12L))
    assert(wholly.size === 0)

  }

  test("build a RangeSearchableArray with a single item and retrieve data") {
    val array = RangeSearchableArray(Array((Region(10L, 15L), 1)))

    assert(array.length === 1)
    assert(array.midpoint === 1)

    // retrieve a value wholly inside the first key
    val wholly = array.get(Region(11L, 12L))
    assert(wholly.size === 1)
    assert(wholly.head === (Region(10, 15), 1))

    // retrieve a value that envelops the first key
    val envelops = array.get(Region(5L, 20L))
    assert(envelops.size === 1)
    assert(envelops.head === (Region(10, 15), 1))

    // retrieve a value overlapping the start of the first key
    val start = array.get(Region(7L, 11L))
    assert(start.size === 1)
    assert(start.head === (Region(10, 15), 1))

    // retrieve a value overlapping the end of the first key
    val end = array.get(Region(14L, 16L))
    assert(end.size === 1)
    assert(end.head === (Region(10, 15), 1))

    // retrieve a value before the first key
    val before = array.get(Region(2L, 5L))
    assert(before.isEmpty)

    // retrieve a value after the first key
    val after = array.get(Region(22L, 75L))
    assert(after.isEmpty)
  }

  sparkTest("build a RangeSearchableArray out of multiple datapoints and retrieve data") {
    val rdd = sc.parallelize(Seq((Region(10L, 15L), 1),
      (Region(9L, 12L), 0),
      (Region(100L, 150L), 4),
      (Region(80L, 95L), 2),
      (Region(80L, 110L), 3)))

    val array = RangeSearchableArray(rdd)
    assert(array.length === 5)
    assert(array.midpoint === 4)
    (0 until array.length).foreach(idx => {
      assert(array.array(idx)._2 === idx)
    })

    // retrieve a value overlapping the first two keys
    val firstTwo = array.get(Region(10L, 12L)).map(_._2).toSet
    assert(firstTwo.size === 2)
    assert(firstTwo(0))
    assert(firstTwo(1))

    // retrieve a value overlapping the last three keys
    val lastThree = array.get(Region(90L, 105L)).map(_._2).toSet
    assert(lastThree.size === 3)
    assert(lastThree(2))
    assert(lastThree(3))
    assert(lastThree(4))

    // retrieve a value overlapping just the last key
    val last = array.get(Region(130L, 140L))
    assert(last.size === 1)
    assert(last.head === (Region(100L, 150L), 4))

    // retrieve a value before the first key
    val before = array.get(Region(2L, 5L))
    assert(before.isEmpty)

    // retrieve a value between the second and third keys
    val between = array.get(Region(21L, 22L))
    assert(between.isEmpty)

    // retrieve a value after the last key
    val after = array.get(Region(500L, 675L))
    assert(after.isEmpty)
  }

  sparkTest("verify RangeSearchableArray fetches all valid ranges") {
    val longRegion = Region(2L, 200L)
    val rdd = sc.parallelize(Seq((longRegion, 0),
      (Region(1L, 10L), 1),
      (Region(18L, 24L), 2),
      (Region(28L, 32L), 3),
      (Region(32L, 40L), 4),
      (Region(40L, 60L), 5)))

    val array = RangeSearchableArray(rdd)

    // retrieve a value overlapping just the last key
    val query = array.get(Region(20L, 30L)).toArray

    assert(query.length == 3)
    assert(query.contains((longRegion, 0)))

  }

  test("inserts new elements into RangeSearchableArray") {
    val array = RangeSearchableArray(Array((Region(10L, 15L), 1),
      (Region(19L, 24L), 0),
      (Region(100L, 150L), 4),
      (Region(80L, 95L), 2),
      (Region(80L, 110L), 3)))

    val newData = Iterator((Region(21L, 22L), 5))

    val newForest = array.insert(newData)

    // retrieve a value after insert
    val after = newForest.get(Region(20L, 30L))
    assert(after.size === 2)
  }

}
