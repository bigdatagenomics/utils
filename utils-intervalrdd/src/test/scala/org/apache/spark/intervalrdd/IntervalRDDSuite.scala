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

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.bdgenomics.utils.misc.SparkFunSuite

class IntervalRDDSuite extends SparkFunSuite {

  // create regions
  val region1: Region = Region(0L, 99L)
  val region2: Region = Region(100L, 199L)
  val region3: Region = Region(200L, 299L)

  //creating data
  val rec1 = "data1"
  val rec2 = "data2"
  val rec3 = "data3"
  val rec4 = "data4"
  val rec5 = "data5"
  val rec6 = "data6"

  sparkTest("create IntervalRDD from RDD of datatype string") {

    val intArr = Array((region1, rec1), (region2, rec2), (region3, rec3))
    val intArrRDD: RDD[(Region, String)] = sc.parallelize(intArr).partitionBy(new HashPartitioner(10))

    val intRDD: IntervalRDD[Region, String] = IntervalRDD(intArrRDD)
    val results = intRDD.get(region3)

    assert(results.head == (region3, rec3))
    assert(results.size == 1)

  }

  sparkTest("merge new values into existing IntervalRDD") {

    var intArr = Array((region1, rec1), (region2, rec2), (region3, rec3))
    val intArrRDD: RDD[(Region, String)] = sc.parallelize(intArr)
    val testRDD: IntervalRDD[Region, String] = IntervalRDD(intArrRDD)

    intArr = Array((region2, rec4), (region3, rec5))

    val newRDD: IntervalRDD[Region, String] = testRDD.multiput(intArr)
    val results = newRDD.get(region3)

    assert(results.size == 2)
  }

  sparkTest("call put multiple times on data with same interval") {

    val region: Region = new Region(0L, 99L)

    val intArr = Array((region, rec1), (region, rec2), (region, rec3))
    val intArrRDD: RDD[(Region, String)] = sc.parallelize(intArr)

    val testRDD: IntervalRDD[Region, String] = IntervalRDD(intArrRDD)

    //constructing RDDs to put in
    val onePutInput = Array((region, rec4), (region, rec5))
    val twoPutInput = Array((region, rec6))

    // Call Put twice
    val onePutRDD: IntervalRDD[Region, String] = testRDD.multiput(onePutInput)
    val twoPutRDD: IntervalRDD[Region, String] = onePutRDD.multiput(twoPutInput)

    val resultsOrig = testRDD.get(region)
    val resultsOne = onePutRDD.get(region)
    val resultsTwo = twoPutRDD.get(region)

    assert(resultsOrig.size == 3) //size of results for the one region we queried
    assert(resultsOne.size == 5) //size after adding two records
    assert(resultsTwo.size == 6) //size after adding another record

  }

  sparkTest("test filterByInterval") {

    val outRegion: Region = Region(200L, 299L)
    val intArr = Array((region1, rec1), (region1, rec2), (outRegion, rec3))
    val intArrRDD: RDD[(Region, String)] = sc.parallelize(intArr)

    val testRDD: IntervalRDD[Region, String] = IntervalRDD(intArrRDD)

    val r: Region = new Region(0L, 50L)
    val newRDD = testRDD.filterByInterval(r)
    assert(newRDD.count == 2)
    assert(testRDD.count == 3)
  }

  sparkTest("verify no data is lost in conversion from rdd to intervalrdd") {
    val region = new Region(0L, 20000L)
    val rdd: RDD[Int] = sc.parallelize(Array.range(0, 100))
    val alignmentRDD: RDD[(Region, Int)] = rdd.map(v => (Region(v, v + 2), v))
    val intRDD: IntervalRDD[Region, Int] = IntervalRDD(alignmentRDD)

    assert(intRDD.count == rdd.count)

  }

  sparkTest("count data where chromosome partitions overlap") {
    val arr = Array((region1, rec1), (region2, rec2), (region3, rec3))
    val arrRDD: RDD[(Region, String)] = sc.parallelize(arr)
    val intRDD: IntervalRDD[Region, String] = IntervalRDD(arrRDD)

    assert(arrRDD.count == intRDD.count)
  }

  sparkTest("get data where chromosome partitions overlap") {
    val arr = Array((region1, rec1), (region2, rec2), (region3, rec3))
    val arrRDD: RDD[(Region, String)] = sc.parallelize(arr)

    val testRDD: IntervalRDD[Region, String] = IntervalRDD(arrRDD)

    val results1 = testRDD.get(region1)
    assert(results1.size == 1)

    val results2 = testRDD.get(region2)
    assert(results2.size == 1)

    val results3 = testRDD.get(region3)
    assert(results3.size == 1)

  }

  sparkTest("apply predicate to the RDD") {

    val overlapReg: Region = new Region(0L, 300L)

    val arr = Array((region1, rec1), (region2, rec2), (region3, rec3))
    val arrRDD: RDD[(Region, String)] = sc.parallelize(arr)

    val testRDD: IntervalRDD[Region, String] = IntervalRDD(arrRDD)
    val filteredRDD = testRDD.filter(elem => elem._2 == "data1")
    val results = filteredRDD.get(overlapReg)
    assert(results.size == 1)

  }

  sparkTest("testing collect") {

    val arr = Array((region1, rec1), (region2, rec2), (region3, rec3))
    val arrRDD: RDD[(Region, String)] = sc.parallelize(arr)

    val testRDD: IntervalRDD[Region, String] = IntervalRDD(arrRDD)
    val collected: Array[(Region, String)] = testRDD.collect()
    assert(collected.length == 3)

  }

  sparkTest("test before and after toRDD sizes are the same") {

    val arr = Array((region1, rec1), (region2, rec2), (region3, rec3))
    val arrRDD: RDD[(Region, String)] = sc.parallelize(arr)

    val testRDD: IntervalRDD[Region, String] = IntervalRDD(arrRDD)
    val backToRDD: RDD[(Region, String)] = testRDD.toRDD
    assert(backToRDD.collect.length == 3)

  }

  sparkTest("testing mapValues") {

    val arr = Array((region1, rec1), (region2, rec2), (region3, rec3))
    val arrRDD: RDD[(Region, String)] = sc.parallelize(arr)
    val overlapReg: Region = new Region(10L, 20L)

    val testRDD: IntervalRDD[Region, String] = IntervalRDD(arrRDD)
    val mapRDD: IntervalRDD[Region, String] = testRDD.mapValues(elem => elem + "_mapped")
    val results = mapRDD.get(overlapReg)
    assert(results(0) == (region1, "data1_mapped"))
  }

  sparkTest("Attempt to access data from non existing key") {

    val interval = new Region(1020L, 2000L)

    val rdd: RDD[Int] = sc.parallelize(Array.range(0, 1000))

    val alignmentRDD: RDD[(Region, Int)] = rdd.map(v => (Region(v.toLong, v + 10), v))

    val intRDD: IntervalRDD[Region, Int] = IntervalRDD(alignmentRDD)
    val results = intRDD.filterByInterval(interval)

    assert(results.count == 0)
  }

  sparkTest("Delete values from rdd") {
    val interval = new Region(90L, 100L)

    val rdd: RDD[Int] = sc.parallelize(Array.range(0, 100).filter(_ % 2 == 0))

    val alignmentRDD: RDD[(Region, Int)] = rdd.map(v => (Region(v.toLong, v + 1), v))

    var intRDD: IntervalRDD[Region, Int] = IntervalRDD(alignmentRDD)
    intRDD = intRDD.filter(r => r._1.start < 90)

    assert(intRDD.count == 45)
  }

}
