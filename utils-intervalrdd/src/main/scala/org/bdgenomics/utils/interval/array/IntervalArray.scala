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
package org.bdgenomics.utils.interval.array

import com.esotericsoftware.kryo.io.{ Input, Output }
import com.esotericsoftware.kryo.{ Kryo, Serializer }
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import scala.annotation.tailrec
import scala.math.max
import scala.reflect.ClassTag

/**
 * Companion object for building an IntervalArray from an RDD.
 */
object IntervalArray extends Serializable {

  private class MaxLongAccumulator extends AccumulatorV2[Long, Long] {
    private var _max = 0L

    override def isZero: Boolean = _max == 0L

    override def copy(): AccumulatorV2[Long, Long] = {
      val newAcc = new MaxLongAccumulator
      newAcc._max = _max
      newAcc
    }

    override def reset(): Unit = {
      _max = 0L
    }

    override def add(v: Long): Unit = {
      _max = max(_max, v)
    }

    override def merge(other: AccumulatorV2[Long, Long]): Unit = other match {
      case o: MaxLongAccumulator =>
        _max = max(_max, o._max)
      case _ =>
        throw new UnsupportedOperationException(
          s"Cannot merge ${this.getClass.getName} with ${other.getClass.getName}")
    }

    override def value: Long = _max
  }

  /**
   * Sorts the RDD and collects it to build an IntervalArray, a sorted array that searches
   * over intervals. This is used for a left side of the broadcast region join in ADAM.
   *
   * @param rdd RDD to build a IntervalArray from.
   * @return The IntervalArray built from this RDD.
   */
  def apply[K <: Interval[K]: ClassTag, T: ClassTag](
    rdd: RDD[(K, T)],
    buildFn: (Array[(K, T)], Long) => IntervalArray[K, T]): IntervalArray[K, T] = {

    val accum = new MaxLongAccumulator
    rdd.context.register(accum, "max")

    val sortedArray =
      rdd.map(i => {
        // we do this to get the width of the widest interval
        accum.add(i._1.width)

        i
      }).sortByKey()
        .collect

    buildFn(sortedArray, accum.value)
  }

  /**
   * Sorts the RDD and collects it to build an IntervalArray, a sorted array that searches
   * over intervals. This is used for a left side of the broadcast region join in ADAM.
   *
   * @param rdd RDD to build a IntervalArray from.
   * @return The IntervalArray built from this RDD.
   */
  def apply[K <: Interval[K]: ClassTag, T: ClassTag](
    rdd: RDD[(K, T)]): IntervalArray[K, T] = {

    apply(rdd, apply(_, _, sorted = true))
  }

  def apply[K <: Interval[K]: ClassTag, T: ClassTag](
    array: Array[(K, T)],
    maxIntervalWidth: Long,
    sorted: Boolean = true): IntervalArray[K, T] = {
    val sortedArray = if (sorted) {
      array
    } else {
      array.sortBy(_._1)
    }
    new ConcreteIntervalArray(sortedArray, maxIntervalWidth)
  }
}

/**
 * Originally, an IntervalArray was a collection of trees.
 * Alas, we have no trees anymore.
 * I blame global warming.
 *
 * WARNING: CONTAINS A VARIABLE THAT IS NOT THREAD SAFE
 * TO SAFELY USE THIS VARIABLE IN A SPARK CONTEXT, CREATE A SHALLOW COPY OF
 * THE INTERVAL ARRAY.
 *
 * @see optLastIndex
 *
 * @param arr An array of values for the left side of the join.
 * @param maxIntervalWidth The maximum width across all intervals in this array.
 * @param sorted True if arr is sorted. If false, we sort arr.
 */
trait IntervalArray[K <: Interval[K], T] extends Serializable {
  val array: Array[(K, T)]
  val maxIntervalWidth: Long
  // maintains the last index returned from the most recent binarySearch result.
  // !!!
  // WARNING: NOT THREAD SAFE
  // TO SAFELY USE THIS VARIABLE IN A SPARK CONTEXT, CREATE A SHALLOW COPY OF
  // THE INTERVAL ARRAY.
  // !!!
  private var optLastIndex: Option[Int] = None

  def length = array.length
  def midpoint = pow2ceil()

  protected def replace(arr: Array[(K, T)], maxWidth: Long): IntervalArray[K, T]

  /**
   * @return Makes a copy of the interval array. The underlying array is shallow
   *   copied, but the thread unsafe optLastIndex variable is not shared.
   */
  def duplicate(): IntervalArray[K, T]

  @tailrec private def pow2ceil(i: Int = 1): Int = {
    if (2 * i >= length) {
      i
    } else {
      pow2ceil(2 * i)
    }
  }

  /**
   * Recursively finds an index in array that overlaps with the query interval,
   * if exists.
   * @param rr The interval to query for.
   * @param idx The current index to search.
   * @param step The step size of the current search in array.
   * @return If exists, an option containing the index of an overlapping
   *         interval in array. If not exists, returns None.
   */
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

  /**
   * Recursively finds an index in array that overlaps with the query interval,
   * or if none exists returns the closest non-overlapping index.
   *
   * @see binarySearch
   *
   * @param rr The interval to query for.
   * @param idx The current index to search.
   * @param step The step size of the current search in array.
   * @return The index of array that overlaps or is closest to query the
   *         interval.
   */
  @tailrec private def binaryNearestSearch(rr: K,
                                           idx: Int = 0,
                                           step: Int = midpoint): Int = {

    if (array.length == 0 || rr.covers(array(idx)._1) || step == 0) {
      idx
    } else {
      val stepIdx = idx + step
      val nextIdx = if (stepIdx >= length ||
        (!rr.covers(array(stepIdx)._1) &&
          rr.compareTo(array(stepIdx)._1) < 0)) {
        idx
      } else {
        stepIdx
      }
      binaryNearestSearch(rr, nextIdx, step / 2)
    }
  }

  /**
   * Recursively expands the list containing all intervals in array that overlap
   * with the query interval. Expands the list forward from the given index.
   *
   * @see expandBackward
   *
   * @param rr The query interval.
   * @param idx The index in array to begin expanding from.
   * @param list The current list of tuples from array that overlap with the
   *        query interval.
   * @return The list of tuples expanded forward from array that overlap with
   *         the query interval.
   */
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

  /**
   * Recursively expands the list containing all intervals in array that overlap
   * with the query interval. Expands the list backward from the given index.
   *
   * @see expandForward
   *
   * @param rr The query interval.
   * @param idx The index in array to begin expanding from.
   * @param list The current list of tuples from array that overlap with the
   *        query interval.
   * @return The list of tuples expanded backward from array that overlap with
   *         the query interval.
   */
  @tailrec private def expandBackward(rr: K,
                                      idx: Int,
                                      list: List[(K, T)] = List.empty): List[(K, T)] = {
    if (idx < 0) {
      list
    } else {
      val arrayElem = array(idx)

      // get the distance between the current and query intervals
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
   * Insert an Iterator of items into existing IntervalArray.
   *
   * @param kvs The tuples to insert into IntervalArray.
   * @return new IntervalArray with inserted values.
   */
  def insert(kvs: Iterator[(K, T)], sorted: Boolean = false): IntervalArray[K, T] = {

    // sort kvs if not yet sorted
    val sortedKvs =
      if (sorted) kvs.toArray
      else kvs.toArray.sortBy(_._1)
    val maxWidth = sortedKvs.map(_._1.width).fold(0L)(max(_, _))

    val allSorted = merge(sortedKvs, new Array[(K, T)](array.length + sortedKvs.length))
    replace(allSorted, max(maxIntervalWidth, maxWidth))
  }

  /**
   * Merges the sorted array from this class with a new sorted array into a
   * new array.
   *
   * @param arr A sorted array to merge into this sorted array.
   * @param allSorted An array to merge sorted arrays into.
   * @param k The index of current position in allSorted array.
   * @param idx1 The index of current position in this array.
   * @param idx2 The index of current position in new array arr.
   * @return A new sorted array with merged components from new array arr and
   *         base array.
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
   * Filters items in an IntervalArray based on a given predicate.
   *
   * @param pred The predicate to filter elements by.
   * @return A new IntervalArray with filtered elements.
   */
  def filter(pred: ((K, T)) => Boolean): IntervalArray[K, T] = {
    replace(array.filter(r => pred(r._1, r._2)), maxIntervalWidth)
  }

  /**
   * Maps values from T to T2.
   *
   * @param f Function mapping T to T2.
   * @tparam T2 The new type to map values to.
   * @return new IntervalArray with mapped values.
   */
  def mapValues[T2: ClassTag](f: T => T2): IntervalArray[K, T2] = {
    ConcreteIntervalArray(array.map(r => (r._1, f(r._2))), maxIntervalWidth)
  }

  /**
   * Gets all overlapping intervals in array for a given query interval.
   *
   * @param rr The interval to filter by.
   * @return Iterable of elements filtered by the query interval containing
   *         overlapping elements in array. If none exist, returns an empty
   *         Iterable.
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
    // - a query interval
    // - a sorted array (again, sorted first by coordinate plane, and then by
    //   start position on a given coordinate plane)
    // - the width of the largest interval
    //
    // our algorithm runs two phases:
    // 1. we run binary search to find a key that overlaps the query interval
    // 2. if we find a key that overlaps the query interval, we "expand" from this
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
    //    we hit a interval that sorts after the query interval, and that does not
    //    cover the query interval. since the array is sorted by start position,
    //    once we hit an array element that sorts after the query interval, that
    //    implies that:
    //      * the start position of the array element is greater than the start
    //        position of the query interval, or
    //      * the array element is on a different coordinate plane
    //
    //    if the array element is on a different coordinate plane, by definition
    //    all intervals that are later in the array will also be on a different
    //    coordinate plane from the query interval, and intervals on different
    //    coordinate planes cannot overlap.
    //
    //    if the array element is on the same coordinate plane as the query
    //    interval, the sort order implies that the two intervals will overlap
    //    iff the end position of the query interval is greater than/equal to the
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
    //    of the query interval, and thus, there cannot be any intervals earlier
    //    in the array that overlap the query interval.

    optLastIndex = optLastIndex.filter(array(_)._1.covers(rr)).orElse(binarySearch(rr))

    optLastIndex.toIterable
      .flatMap(idx => {
        expandBackward(rr, idx) ::: expandForward(rr, idx + 1)
      })
  }

  /**
   * Gets all overlapping intervals in array for a given K, however if no
   * overlapping intervals are found, uses the element that is the closest
   * to the query.
   *
   * @param rr The interval to filter by.
   * @return An Iterable of elements filtered by the query interval containing
   *         overlapping or the closest element in array.
   */
  def get(rr: K, requireOverlap: Boolean = true): Iterable[(K, T)] = {
    if (requireOverlap) {
      get(rr)
    } else {
      val lastIndex = binaryNearestSearch(rr)

      expandBackward(rr, lastIndex - 1) :::
        (array(lastIndex) :: expandForward(rr, lastIndex + 1))
    }
  }

  /**
   * Collects and returns all elements in this array.
   *
   * @return An array containing all elements.
   */
  def collect(): Array[(K, T)] = array
}

class ConcreteIntervalArraySerializer[K <: Interval[K]: ClassTag, T: ClassTag](kryo: Kryo)
    extends IntervalArraySerializer[K, T, ConcreteIntervalArray[K, T]] {

  protected val kSerializer = kryo
    .getSerializer(kTag.runtimeClass.asInstanceOf[Class[K]])
    .asInstanceOf[Serializer[K]]
  protected val tSerializer = kryo
    .getSerializer(tTag.runtimeClass.asInstanceOf[Class[T]])
    .asInstanceOf[Serializer[T]]

  protected def builder(
    arr: Array[(K, T)],
    maxIntervalWidth: Long): ConcreteIntervalArray[K, T] = {
    ConcreteIntervalArray[K, T](arr, maxIntervalWidth)
  }
}

case class ConcreteIntervalArray[K <: Interval[K], T: ClassTag](
    array: Array[(K, T)],
    maxIntervalWidth: Long) extends IntervalArray[K, T] {

  def duplicate(): IntervalArray[K, T] = {
    new ConcreteIntervalArray(array, maxIntervalWidth)
  }

  protected def replace(arr: Array[(K, T)],
                        maxWidth: Long): IntervalArray[K, T] = {
    new ConcreteIntervalArray(arr, maxWidth)
  }
}

abstract class IntervalArraySerializer[K <: Interval[K]: ClassTag, T: ClassTag, A <: IntervalArray[K, T]]
    extends Serializer[A] {

  protected def tTag: ClassTag[T] = implicitly[ClassTag[T]]
  protected def kTag: ClassTag[K] = implicitly[ClassTag[K]]

  protected val kSerializer: Serializer[K]
  protected val tSerializer: Serializer[T]

  def write(kryo: Kryo, output: Output, obj: A) {

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

  protected def builder(arr: Array[(K, T)], maxIntervalWidth: Long): A

  def read(kryo: Kryo,
           input: Input,
           klazz: Class[A]): A = {

    // read the max width
    val maxWidth = input.readLong()

    // read the array size and allocate
    val length = input.readInt()

    val array = (0 until length).map(idx => {
      (kSerializer.read(kryo, input, kTag.runtimeClass.asInstanceOf[Class[K]]),
        tSerializer.read(kryo, input, tTag.runtimeClass.asInstanceOf[Class[T]]))
    }).toArray

    builder(array, maxWidth)
  }
}
