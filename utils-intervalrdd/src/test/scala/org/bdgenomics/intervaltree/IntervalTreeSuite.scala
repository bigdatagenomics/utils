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

import org.scalatest.FunSuite

case class Region(start: Long, end: Long) extends Interval

class IntervalTreeSuite extends FunSuite {

  test("insert regions to intervaltree") {

    val tree = new IntervalTree[Region, Long]()

    val id = 1L
    for (start <- 1L to 6L) {
      val end = start + 500L
      val region = Region(start, end)
      tree.insert(region, start)
    }
    assert(tree.size == 6)

  }

  test("get all data from tree") {

    val tree = new IntervalTree[Region, Long]()

    val id = 1L
    for (start <- 1L to 6L) {
      val end = start + 500L

      val region = Region(start, end)
      tree.insert(region, start)
    }
    assert(tree.get.size == 6)

  }

  test("insert different regions into same node, tests search") {

    val tree = new IntervalTree[Region, Long]()

    val start = 0L
    val end = 1000L
    val region = Region(start, end)

    for (i <- 1L to 6L) {
      val value: Long = i
      tree.insert(region, value)
    }
    val x = tree.search(region)
    val searchAll: List[(Region, Long)] = tree.search(region).toList
    assert(searchAll.size == 6)

  }

  test("insert in bulk with same interval") {

    val tree = new IntervalTree[Region, Long]()
    val region = Region(0, 1000)
    val r: Iterator[Long] = Iterator(2L, 3L, 4L)
    tree.insert(region, r)

    assert(tree.size == 3)
    assert(tree.search(region).size == 3)

  }

  test("rebalancing tree") {

    val tree = new IntervalTree[Region, Long]()

    for (i <- 1L to 50L) {
      val partition: Long = i
      val region = Region(i + 7L, i + 1000L)
      tree.insert(region, partition)
    }

    assert(tree.rightDepth - tree.leftDepth <= 16)
    assert(tree.size == 50)

  }

  test("clone tree") {

    val tree = new IntervalTree[Region, Long]()

    for (i <- 1L to 50L) {
      val partition: Long = i
      val region = Region(i + 7L, i + 1000L)
      tree.insert(region, partition)
    }

    val newTree = tree.snapshot()
    assert(tree.size == newTree.size)

  }

  test("merge 2 trees with nonoverlapping intervals") {

    var totalRecs = 0
    val tree1 = new IntervalTree[Region, Long]()

    for (i <- 1L to 10L) {
      val partition: Long = i
      val region = Region(i, i + 1000L)
      tree1.insert(region, partition)
      totalRecs += 1
    }

    val tree2 = new IntervalTree[Region, Long]()

    for (i <- 11L to 20L) {
      val partition: Long = i
      val region = Region(i, i + 1000L)
      tree2.insert(region, partition)
      totalRecs += 1
    }

    val newTree = tree1.merge(tree2)
    assert(newTree.size == totalRecs)

  }

  test("search empty tree") {

    val tree = new IntervalTree[Region, Long]()

    // create interval to search
    val start = 0L
    val end = 1000L
    val region = Region(start, end)

    val ids: List[Long] = List(1L, 3L, 5L)
    tree.search(region)

  }

  test("assert items inserted using insertInterval and insertNode result in the same count") {

    val tree1 = new IntervalTree[Region, Long]()
    val tree2 = new IntervalTree[Region, Long]()

    val start = 0L
    val end = 1000L
    val region = Region(start, end)

    //all the data should go into just one node for regular insert
    //but we should be left with 6 nodes for insertNode
    for (i <- 1L to 6L) {
      tree1.insert(region, i)
      val newNode = new Node[Region, Long](region)
      newNode.put(i)
      tree2.insertNode(newNode)
    }

    assert(tree1.size == tree2.size)
    assert(tree1.size == 6)

  }

  test("test consecutive puts into tree") {

    val tree1 = new IntervalTree[Region, Long]()
    val tree2 = new IntervalTree[Region, Long]()

    //regions is smaller and smaller subsets
    val reg1 = Region(0L, 1000L)
    val reg2 = Region(100L, 900L)
    val reg3 = Region(300L, 700L)

    tree1.insert(reg1, 1L)
    val node1 = new Node[Region, Long](reg1)
    node1.put(1L)
    tree2.insertNode(node1)

    tree1.insert(reg1, 2L)
    val node2 = new Node[Region, Long](reg2)
    node2.put(2L)
    tree2.insertNode(node2)

    tree1.insert(reg1, 3L)
    val node3 = new Node[Region, Long](reg3)
    node3.put(3L)
    tree2.insertNode(node3)

    assert(tree1.size == tree2.size)
    assert(tree2.size == 3)

  }

  test("general tree filter") {

    val tree = new IntervalTree[Region, Long]()

    val id = 1L
    for (start <- 1L to 6L) {
      val end = start + 500L
      val region = Region(start, end)
      tree.insert(region, start)
    }
    val filteredTree = tree.filter((k, v) => v < 3)
    assert(filteredTree.size == 2)

  }

  test("general tree map") {

    //simulates putting block sizes
    val tree = new IntervalTree[Region, (Region, Long)]()

    val id = 1L
    for (start <- 1L to 3L) {
      val end = start + 500L
      val region = Region(start, end)
      tree.insert(region, (region, start))
    }
    val filtTree = tree.mapValues(elem => elem._2 + 3L)
    assert(filtTree.size == 3)
    assert(filtTree.get()(0)._2 == 5)

  }

  test("duplicate objects") {

    val tree = new IntervalTree[Region, Long]()

    val id = 1L
    val region = Region(0L, 10L)
    tree.insert(region, 4L)
    tree.insert(region, 2L)

    val filtTree = tree.search(region)
    assert(filtTree.size == 2)

  }

  test("delete node") {

    val tree = new IntervalTree[Region, Long]()

    val id = 1L
    val region1 = Region(0L, 10L)
    val region2 = Region(20L, 30L)

    tree.insert(region1, 4L)
    tree.insert(region2, 2L)

    assert(tree.size == 2)
    val deleted = tree.filter((k, v) => k.start == 0L)
    assert(deleted.size == 1)
  }

}
