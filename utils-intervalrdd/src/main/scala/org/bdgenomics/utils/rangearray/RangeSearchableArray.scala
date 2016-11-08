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

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import org.apache.spark.rdd.RDD
import scala.annotation.tailrec
import scala.reflect.ClassTag

/**
 * Companion object for building a RangeSearchableArray from an RDD.
 */
object RangeSearchableArray extends Serializable {

  /**
   * Sorts the RDD and collects it to build RangeSearchableArray, a sorted array that searches
   * over ranges. This is used for a left side of the broadcast region join in ADAM.
   *
   * @param rdd RDD to build a RangeSearchableArray from.
   * @return The RangeSearchableArray built from this RDD.
   */
  def apply[K <: Interval[K]: ClassTag, T: ClassTag](rdd: RDD[(K, T)]): RangeSearchableArray[K, T] = {
    val sortedArray =
      rdd.sortByKey()
        .collect

    new RangeSearchableArray(sortedArray, sorted = true)
  }
}

/**
 * Originally, a RangeSearchableArray was a collection of trees.
 * Alas, we have no trees anymore.
 * I blame global warming.
 *
 * @param arr An array of values for the left side of the join. We require
 *   this array to be sorted.
 */
case class RangeSearchableArray[K <: Interval[K], T: ClassTag](arr: Array[(K, T)], sorted: Boolean = false) {

  // ensure that array is sorted
  val array =
    if (sorted) arr
    else arr.sortBy(_._1)

  def length = array.length
  def midpoint = pow2ceil()

  @tailrec private def pow2ceil(i: Int = 1): Int = {
    if (2 * i >= length) {
      i
    } else {
      pow2ceil(2 * i)
    }
  }

  @tailrec private def binarySearch(rr: K,
                                    idx: Int = 0,
                                    step: Int = midpoint): Option[Int] = {
    if (array.length == 0) {
      None
    } else if (rr.overlaps(array(idx)._1)) {
      Some(idx)
    } else if (step == 0) {
      None
    } else {
      val stepIdx = idx + step
      val nextIdx = if (stepIdx >= length ||
        (!rr.overlaps(array(stepIdx)._1) &&
          rr.compareTo(array(stepIdx)._1) < 0)) {
        idx
      } else {
        stepIdx
      }
      binarySearch(rr, nextIdx, step / 2)
    }
  }

  @tailrec private def expand(rr: K,
                              idx: Int,
                              step: Int,
                              list: List[(K, T)] = List.empty): List[(K, T)] = {
    if (idx < 0 ||
      idx >= length ||
      !rr.overlaps(array(idx)._1)) {
      list
    } else {
      expand(rr, idx + step, step, array(idx) :: list)
    }
  }

  /**
   * Insert Iterator of (K,V) items into existing RangeSearchableArray
   * @param kvs (K,V) tuples to insert into RangeSearchableArray
   * @return new RangeSearchableArray with inserted values
   */
  def insert(kvs: Iterator[(K, T)], sorted: Boolean = false): RangeSearchableArray[K, T] = {

    // sort kvs if not yet sorted
    val sortedKvs =
      if (sorted) kvs.toArray
      else kvs.toArray.sortBy(_._1)

    // initialize empty array to merge two arrays into
    val allSorted: Array[(K, T)] = new Array[(K, T)](array.length + sortedKvs.length)
    var i = 0
    var j = 0

    // merge two sorted arrays
    Array.range(0, allSorted.length).foreach(k => {
      allSorted(k) = (
        if (i >= length) {
          j = j + 1
          sortedKvs(j - 1)
        } else if (j >= sortedKvs.length) {
          i += 1
          array(i - 1)
        } else if (array(i)._1.compareTo(sortedKvs(j)._1) < 0) {
          i += 1
          array(i - 1)
        } else {
          j = j + 1
          sortedKvs(j - 1)
        })
    })

    RangeSearchableArray(allSorted)
  }

  /**
   * Filters items in RangeSearchableArray based on predicate on (K,V) tuples
   * @param pred predicate to filter elements by
   * @return new RangeSearchableArray with filtered elements
   */
  def filter(pred: ((K, T)) => Boolean): RangeSearchableArray[K, T] = {
    RangeSearchableArray(array.filter(r => pred(r._1, r._2)))
  }

  /**
   * Maps values from T to T2
   * @param f Function mapping T to T2
   * @tparam T2 new type to map values to
   * @return new RangeSearchableArray with mapped values
   */
  def mapValues[T2: ClassTag](f: T => T2): RangeSearchableArray[K, T2] = {
    RangeSearchableArray(array.map(r => (r._1, f(r._2))))
  }

  /**
   * Filters elements in array by overlapping Interval
   * @param rr Interval to filter by
   * @return Iterable of elements filtered by Interval rr
   */
  def get(rr: K): Iterable[(K, T)] = {

    val optIdx = binarySearch(rr)

    optIdx.toIterable
      .flatMap(idx => {
        expand(rr, idx, -1) ::: expand(rr, idx + 1, 1)
      })
  }
}

class RangeSearchableArraySerializer[K <: Interval[K]: ClassTag, T: ClassTag, TS <: Serializer[T], KS <: Serializer[K]](
    private val kSerializer: KS,
    private val tSerializer: TS) extends Serializer[RangeSearchableArray[K, T]] {

  def tTag: ClassTag[T] = implicitly[ClassTag[T]]
  def kTag: ClassTag[K] = implicitly[ClassTag[K]]

  def write(kryo: Kryo, output: Output, obj: RangeSearchableArray[K, T]) {

    // we will use the array length to allocate an array on read
    output.writeInt(obj.length)

    // loop and write elements
    (0 until obj.length).foreach(idx => {
      kSerializer.write(kryo, output, obj.array(idx)._1)
      tSerializer.write(kryo, output, obj.array(idx)._2)
    })
  }

  def read(kryo: Kryo, input: Input, klazz: Class[RangeSearchableArray[K, T]]): RangeSearchableArray[K, T] = {

    // read the array size and allocate
    val length = input.readInt()
    val array = new Array[(K, T)](length)

    // loop and read
    (0 until length).foreach(idx => {
      array(idx) = (kSerializer.read(kryo, input, kTag.runtimeClass.asInstanceOf[Class[K]]),
        tSerializer.read(kryo, input, tTag.runtimeClass.asInstanceOf[Class[T]]))
    })

    RangeSearchableArray[K, T](array)
  }
}
