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

package org.bdgenomics.utils.intervaltree

import java.io.Serializable
import scala.reflect.ClassTag

protected class Node[K <: Interval, T: ClassTag](interval: K) extends Serializable {

  /* left and right children of this node */
  var leftChild: Node[K, T] = null
  var rightChild: Node[K, T] = null

  /* maximum end that is seen in this node. used for search */
  var subtreeMax: Long = interval.end

  /* returns interval for this node */
  def getInterval: K = interval

  /* stores data values for this node */
  var data: Array[T] = Array[T]()

  /* alternative constructor of node from data */
  def this(interval: K, data: Array[T]) = {
    this(interval)
    multiput(data)
  }

  /* gets the number of data values in this node */
  def getSize(): Long = {
    data.length
  }

  /* clones node */
  override def clone: Node[K, T] = {
    val n: Node[K, T] = new Node(interval)
    n.data = data
    n
  }

  /* resets left and right child to null */
  def clearChildren() = {
    leftChild = null
    rightChild = null
  }

  /**
   * Puts multiple elements in data array
   *
   * @param rs: Array of elements to place in node
   */
  def multiput(rs: Array[T]): Unit = {
    val newData = rs
    data ++= newData
  }

  /**
   * Puts multiple elements in data array
   *
   * @param rs: Iterator of elements to place in node
   */
  def multiput(rs: Iterator[T]): Unit = {
    multiput(rs.toArray)
  }

  /**
   * Puts element in data array
   *
   * @param r: Element to place in node
   */
  def put(r: T) = {
    multiput(Array(r))
  }

  /**
   * Gets all elements in node
   * @return Iterator of (key, value) elements in node
   */
  def get(): Iterator[(K, T)] = data.map(r => (interval, r)).toIterator

  /**
   * checks whether this node is greater than other Interval K
   * @return Boolean whether this interval > other
   */
  def greaterThan(other: K): Boolean = {
    interval.start > other.start
  }
  /**
   * checks whether this node equals other Interval K
   * @return Boolean whether this interval == other
   */
  def equals(other: K): Boolean = {
    (interval.start == other.start && interval.end == other.end)
  }

  /**
   * checks whether this node is less than other Interval K
   * @return Boolean whether this interval < other
   */
  def lessThan(other: K): Boolean = {
    interval.start < other.start
  }

  /**
   * checks whether this node overlaps Interval K
   * @return Boolean whether this interval overlaps other
   */
  def overlaps(other: K): Boolean = {
    interval.start < other.end && interval.end > other.start
  }
}

object Node {

  /**
   *
   * @param interval interval of node
   * @param data data to place in node
   * @tparam K Interval type
   * @tparam T data type
   * @return new Node[K, V] constructed from data
   */
  def apply[K <: Interval, T: ClassTag](interval: K, data: Array[T]) = new Node(interval, data)
}
