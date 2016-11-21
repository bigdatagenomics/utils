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
import org.apache.spark.AccumulatorParam
import org.apache.spark.rdd.RDD
import scala.annotation.tailrec
import scala.math.max
import scala.reflect.ClassTag

/**
 * Companion object for building a RangeSearchableArray from an RDD.
 */
object RangeSearchableArray extends Serializable {

  private class MaxAccumulatorParam extends AccumulatorParam[Long] {

    def zero(initialValue: Long): Long = {
      initialValue
    }

    def addInPlace(r1: Long, r2: Long): Long = {
      max(r1, r2)
    }
  }

  /**
   * Sorts the RDD and collects it to build RangeSearchableArray, a sorted array that searches
   * over ranges. This is used for a left side of the broadcast region join in ADAM.
   *
   * @param rdd RDD to build a RangeSearchableArray from.
   * @return The RangeSearchableArray built from this RDD.
   */
  def apply[K <: Interval[K]: ClassTag, T: ClassTag](rdd: RDD[(K, T)]): RangeSearchableArray[K, T] = {

    val accum = {
      implicit val accumParam = new MaxAccumulatorParam

      rdd.context.accumulator(0L)
    }

    val sortedArray =
      rdd.map(i => {
        // we do this to get the width of the widest interval
        accum += i._1.width

        i
      }).sortByKey()
        .collect

    new RangeSearchableArray(sortedArray, accum.value, sorted = true)
  }
}

/**
 * Originally, a RangeSearchableArray was a collection of trees.
 * Alas, we have no trees anymore.
 * I blame global warming.
 *
 * @param arr An array of values for the left side of the join.
 * @param maxIntervalWidth The maximum width across all intervals in this array.
 * @param sorted True if arr is sorted. If false, we sort arr.
 */
class RangeSearchableArray[K <: Interval[K], T: ClassTag](
    arr: Array[(K, T)],
    private[rangearray] val maxIntervalWidth: Long,
    sorted: Boolean = false) extends Serializable {

  // ensure that array is sorted
  private[rangearray] val array =
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
    } else if (rr.covers(array(idx)._1)) {
      Some(idx)
    } else if (step == 0) {
      None
    } else {
      val stepIdx = idx + step
      val nextIdx = if (stepIdx >= length ||
        (!rr.covers(array(stepIdx)._1) &&
          rr.compareTo(array(stepIdx)._1) < 0)) {
        idx
      } else {
        stepIdx
      }
      binarySearch(rr, nextIdx, step / 2)
    }
  }

  @tailrec private def expandForward(rr: K,
                                     idx: Int,
                                     list: List[(K, T)] = List.empty): List[(K, T)] = {
    if (idx >= length) {
      list
    } else {
      val arrayElem = array(idx)

      if (!rr.covers(arrayElem._1) && rr.compareTo(arrayElem._1) > 0) {
        list
      } else {
        val hits = if (rr.overlaps(arrayElem._1)) {
          arrayElem :: list
        } else {
          list
        }

        expandForward(rr, idx + 1, hits)
      }
    }
  }

  @tailrec private def expandBackward(rr: K,
                                      idx: Int,
                                      list: List[(K, T)] = List.empty): List[(K, T)] = {
    if (idx < 0) {
      list
    } else {
      val arrayElem = array(idx)

      // get the distance between the current and query regions
      val distance = arrayElem._1.distance(rr)

      // are we out of range?
      if (distance.fold(true)(d => d > maxIntervalWidth)) {
        list
      } else {
        val hits = if (rr.overlaps(arrayElem._1)) {
          arrayElem :: list
        } else {
          list
        }

        expandBackward(rr, idx - 1, hits)
      }
    }
  }

  /**
   * Insert an Iterator of (K,V) items into existing RangeSearchableArray.
   *
   * @param kvs (K,V) tuples to insert into RangeSearchableArray
   * @return new RangeSearchableArray with inserted values
   */
  def insert(kvs: Iterator[(K, T)], sorted: Boolean = false): RangeSearchableArray[K, T] = {

    // sort kvs if not yet sorted
    val sortedKvs =
      if (sorted) kvs.toArray
      else kvs.toArray.sortBy(_._1)
    val maxWidth = sortedKvs.map(_._1.width).fold(0L)(max(_, _))

    val allSorted = merge(sortedKvs, new Array[(K, T)](array.length + sortedKvs.length))
    new RangeSearchableArray(allSorted, max(maxIntervalWidth, maxWidth), sorted = true)
  }

  /**
   * Merges the sorted array from this class with a new sorted array into a new array.
   *
   * @param arr Sorted array to merge into this sorted array
   * @param allSorted Array to merge sorted arrays into
   * @param k index of current position in allSorted array
   * @param idx1 index of current position in this array
   * @param idx2 index of current position in new array arr
   * @return new sorted array with merged components from new array arr and base array
   */
  @tailrec private def merge(arr: Array[(K, T)],
                             allSorted: Array[(K, T)],
                             k: Int = 0,
                             idx1: Int = 0,
                             idx2: Int = 0): Array[(K, T)] = {

    // if both arrays are out of bounds, return
    if (idx1 >= length && idx2 >= arr.length) {
      allSorted
      // if array 1 is out of bounds, return element from array 2
    } else if (idx1 >= length) {
      allSorted(k) = arr(idx2)
      merge(arr, allSorted, k + 1, idx1, idx2 + 1)
      // if array 2 is out of bounds, return element from array 1
    } else if (idx2 >= arr.length) {
      allSorted(k) = array(idx1)
      merge(arr, allSorted, k + 1, idx1 + 1, idx2)
      // if array 1 has element before array 2, add element from array 1
    } else if (array(idx1)._1.compareTo(arr(idx2)._1) < 0) {
      allSorted(k) = array(idx1)
      merge(arr, allSorted, k + 1, idx1 + 1, idx2)
      // if array 2 has element before array 1, add element from array 2
    } else {
      allSorted(k) = arr(idx2)
      merge(arr, allSorted, k + 1, idx1, idx2 + 1)
    }
  }

  /**
   * Filters items in RangeSearchableArray based on predicate on (K,V) tuples.
   *
   * @param pred predicate to filter elements by
   * @return new RangeSearchableArray with filtered elements
   */
  def filter(pred: ((K, T)) => Boolean): RangeSearchableArray[K, T] = {
    new RangeSearchableArray(array.filter(r => pred(r._1, r._2)), maxIntervalWidth)
  }

  /**
   * Maps values from T to T2.
   *
   * @param f Function mapping T to T2
   * @tparam T2 new type to map values to
   * @return new RangeSearchableArray with mapped values
   */
  def mapValues[T2: ClassTag](f: T => T2): RangeSearchableArray[K, T2] = {
    new RangeSearchableArray(array.map(r => (r._1, f(r._2))), maxIntervalWidth)
  }

  /**
   * Filters elements in this array by an overlapping Interval.
   *
   * @param rr Interval to filter by
   * @return Iterable of elements filtered by Interval rr
   */
  def get(rr: K): Iterable[(K, T)] = {

    // this function searches for all overlapping keys in a sorted array which
    // is keyed by intervals. we can prove that this function is correct.
    //
    // we require that:
    // - intervals exist in a coordinate space where a single interval is
    //   contiguous and has:
    //   - a width, which is always defined
    //   - a distance function, which is defined for all other intervals that
    //     are in the same coordinate plane
    //   - overlaps and covers functions, where:
    //      - covers is true if two intervals are in the same coordinate plane
    //        and have overlapping start/end ranges
    //      - overlaps is true if two intervals overlap, for whatever definition
    //        of overlap is given in that coordinate space. the only invariant
    //        here is that if overlaps returns true, covers must return true
    // - intervals have a sorted order where:
    //   - intervals that are on the same coordinate plane sort by start
    //     position on that coordinate plane
    //   - intervals that are in the same coordinate space but different planes
    //     have a defined order that sort coordinate spaces
    // 
    // as arguments, we take:
    // - a query region
    // - a sorted array (again, sorted first by coordinate plane, and then by
    //   start position on a given coordinate plane)
    // - the width of the largest interval
    //
    // our algorithm runs two phases:
    // 1. we run binary search to find a key that overlaps the query region
    // 2. if we find a key that overlaps the query region, we "expand" from this
    //    key to identify the range of all keys that overlap the query sequence
    //
    // we take as a given that binary search works. the only nuance of our
    // approach is that we use the covers function instead of the overlaps
    // function to determine when we have a query hit.
    // 
    // once we have gotten an index of a covering key/value pair in the sorted
    // array from the binary search algorithm, we need to expand this index to
    // find all overlapping key/value pairs. we need to expand the array both
    // forward and backward. these two expansions have separate approaches:
    // 1. expanding forward: we walk forward (increment the array index) until
    //    we hit a region that sorts after the query region, and that does not
    //    cover the query region. since the array is sorted by start position,
    //    once we hit an array element that sorts after the query region, that
    //    implies that:
    //      * the start position of the array element is greater than the start
    //        position of the query region, or
    //      * the array element is on a different coordinate plane
    //
    //    if the array element is on a different coordinate plane, by definition
    //    all intervals that are later in the array will also be on a different
    //    coordinate plane from the query region, and intervals on different
    //    coordinate planes cannot overlap.
    //
    //    if the array element is on the same coordinate plane as the query
    //    region, the sort order implies that the two intervals will overlap
    //    iff the end position of the query region is greater than/equal to the
    //    start position of the array element, and the start position of the
    //    array elements increases monotonically moving forward in the array.
    //    thus, once we see an array element that is sorted after the query
    //    interval and that does not cover the query interval, we know that
    //    we will never see another element between the current array index
    //    and the end of the array that is on the same coordinate plane as
    //    the query interval and that has a start position less than/equal to
    //    the end position, and thus we do not need to walk further.
    // 2. expanding backwards: here, our sort invariant helps us, but only
    //    slightly. we backtrack in the array until we find an interval whose
    //    distance to the current interval is greater than the largest interval
    //    in the dataset, or undefined. if the distance is undefined, that means
    //    we have tracked back to a coordinate plane that sorts before our
    //    coordinate plane. if the distance is defined and greater than the
    //    longest interval length, this implies that the end of the array
    //    element is more than the maximum interval length away from the start
    //    of the query region, and thus, there cannot be any intervals earlier
    //    in the array that overlap the query region.

    val optIdx = binarySearch(rr)

    optIdx.toIterable
      .flatMap(idx => {
        expandBackward(rr, idx) ::: expandForward(rr, idx + 1)
      })
  }

  /**
   * Collects and returns all elements in this array.
   *
   * @return array containing all elements
   */
  def collect(): Array[(K, T)] = array
}

class RangeSearchableArraySerializer[K <: Interval[K]: ClassTag, T: ClassTag, TS <: Serializer[T], KS <: Serializer[K]]
    extends Serializer[RangeSearchableArray[K, T]] {

  private def tTag: ClassTag[T] = implicitly[ClassTag[T]]
  private def kTag: ClassTag[K] = implicitly[ClassTag[K]]

  def write(kryo: Kryo, output: Output, obj: RangeSearchableArray[K, T]) {

    val kSerializer = kryo
      .getSerializer(kTag.runtimeClass.asInstanceOf[Class[K]])
      .asInstanceOf[Serializer[K]]
    val tSerializer = kryo
      .getSerializer(tTag.runtimeClass.asInstanceOf[Class[T]])
      .asInstanceOf[Serializer[T]]

    // write the max width
    output.writeLong(obj.maxIntervalWidth)

    // we will use the array length to allocate an array on read
    output.writeInt(obj.length)

    // loop and write elements
    (0 until obj.length).foreach(idx => {
      kSerializer.write(kryo, output, obj.array(idx)._1)
      tSerializer.write(kryo, output, obj.array(idx)._2)
    })
  }

  def read(kryo: Kryo, input: Input, klazz: Class[RangeSearchableArray[K, T]]): RangeSearchableArray[K, T] = {

    val kSerializer = kryo
      .getSerializer(kTag.runtimeClass.asInstanceOf[Class[K]])
      .asInstanceOf[Serializer[K]]
    val tSerializer = kryo
      .getSerializer(tTag.runtimeClass.asInstanceOf[Class[T]])
      .asInstanceOf[Serializer[T]]

    // read the max width
    val maxWidth = input.readInt()

    // read the array size and allocate
    val length = input.readInt()
    val array = new Array[(K, T)](length)

    // loop and read
    (0 until length).foreach(idx => {
      array(idx) = (kSerializer.read(kryo, input, kTag.runtimeClass.asInstanceOf[Class[K]]),
        tSerializer.read(kryo, input, tTag.runtimeClass.asInstanceOf[Class[T]]))
    })

    new RangeSearchableArray[K, T](array, maxWidth)
  }
}
