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

package org.bdgenomics.utils.intervalrdd
import org.bdgenomics.utils.rangearray._
import org.scalatest.FunSuite

case class Region(start: Long, end: Long) extends Interval[Region] {

  def compareTo(other: Region): Int = {
    if (this.start != other.start) {
      this.start.compareTo(other.start)
    } else {
      this.end.compareTo(other.end)
    }
  }

  def overlaps(other: Region): Boolean = end > other.start && start < other.end

}

class IntervalPartitionSuite extends FunSuite {

  val region1: Region = Region(0L, 99L)
  val region2: Region = Region(100L, 199L)
  val region3: Region = Region(0, 199)
  val region4: Region = Region(150L, 300L)
  val region5: Region = Region(350L, 700L)

  val read1 = 2L
  val read2 = 500L
  val read3 = 3L
  val read4 = 700L
  val read5 = 900L
  val read6 = 250L

  def createEmptyPartition: IntervalPartition[Region, Long] = {
    new IntervalPartition[Region, Long](RangeSearchableArray(Array[(Region, Long)]()))
  }

  test("create new partition") {

    var partition: IntervalPartition[Region, Long] = createEmptyPartition
    assert(partition != null)

  }

  test("create partition from iterator") {

    val iter = Iterable((region1, read1), (region2, read1))
    val partition = IntervalPartition(iter)
    assert(partition != null)
  }

  test("get values from iterator-created partition") {

    val iter = Iterable((region1, read1), (region1, read3), (region2, read2), (region2, read4))
    val partition = IntervalPartition(iter)

    var results: List[Long] = partition.get(region1).toList.map(_._2)
    results = results ++ partition.get(region2).toList.map(_._2)

    assert(results.contains(read1))
    assert(results.contains(read2))
    assert(results.contains(read3))
    assert(results.contains(read4))

  }

  test("put some for iterator of intervals and key-values") {

    val partition: IntervalPartition[Region, Long] = createEmptyPartition

    var newPartition = partition.multiput(Iterator((region1, read1), (region1, read3)))
    newPartition = newPartition.multiput(Iterator((region2, read2), (region2, read4)))

    // assert values are in the new partition
    var results: List[Long] = newPartition.get(region1).toList.map(_._2)
    results = results ++ newPartition.get(region2).toList.map(_._2)

    assert(results.contains(read1))
    assert(results.contains(read2))
    assert(results.contains(read3))
    assert(results.contains(read4))

  }

  test("get some for iterator of intervals") {

    val read1 = (1L, 2L)
    val read2 = (1L, 500L)
    val read3 = (2L, 6L)
    val read4 = (2L, 500L)
    val iter = Iterable((region1, read1), (region2, read2), (region1, read3), (region2, read4))
    val partition = IntervalPartition(iter)
    val results = partition.get(region1).toList
    assert(results.contains((region1, read1)))

  }

  test("selectively getting intervals") {

    var partition: IntervalPartition[Region, Long] = createEmptyPartition

    var newPartition = partition.multiput(Iterator((region1, read1), (region1, read3)))
    newPartition = newPartition.multiput(Iterator((region2, read2), (region2, read4)))

    // assert values are in the new partition
    val results: List[Long] = newPartition.get(region1).toList.map(_._2)

    assert(results.contains(read1))
    assert(results.contains(read3))

  }

  test("putting differing number of reads into different regions") {

    var partition: IntervalPartition[Region, Long] = createEmptyPartition

    var newPartition = partition.multiput(Iterator((region1, read1), (region1, read3)))
    newPartition = newPartition.multiput(Iterator((region2, read2), (region2, read4), (region2, read5)))

    // assert values are in the new partition
    var results: List[Long] = newPartition.get(region1).toList.map(_._2)
    results = results ++ newPartition.get(region2).toList.map(_._2)

    assert(results.contains(read1))
    assert(results.contains(read2))
    assert(results.contains(read3))
    assert(results.contains(read4))
    assert(results.contains(read5))

  }

  test("putting then getting a region that overlaps 3 regions") {

    var partition: IntervalPartition[Region, Long] = createEmptyPartition

    var newPartition = partition.multiput(Iterator((region1, read1), (region1, read3)))
    newPartition = newPartition.multiput(Iterator((region2, read2), (region2, read4)))
    newPartition = newPartition.put(region4, read5)
    newPartition = newPartition.put(region5, read6)

    val overlapReg: Region = Region(0L, 200L)

    val results = newPartition.get(overlapReg).toList
    assert(results.size == 5)

  }

  test("applying a predicate") {

    var partition: IntervalPartition[Region, Long] = createEmptyPartition

    var newPartition = partition.multiput(Iterator((region1, read1), (region1, read3)))
    newPartition = newPartition.multiput(Iterator((region2, read2), (region2, read4)))

    val filtPart = newPartition.filter(v => v._2 < 300L)
    val overlapReg: Region = Region(0L, 300L)

    val results = filtPart.get(overlapReg).toList
    assert(results.size == 2)

  }

  test("applying a map") {

    var partition: IntervalPartition[Region, Long] = createEmptyPartition

    var newPartition = partition.multiput(Iterator((region1, read1), (region1, read3)))
    newPartition = newPartition.multiput(Iterator((region2, read2), (region2, read4)))

    val filtPart = newPartition.mapValues(elem => elem + 300L)

    val results = filtPart.filter(v => v._2 < 400L).get
    assert(results.size == 2)

  }

}
