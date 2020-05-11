package at.ac.ait.clustering

import org.scalatest.flatspec.AnyFlatSpec
import scala.collection.Seq

class MultipleInputClusteringTestSpec extends AnyFlatSpec {

  def computeClusters(inputs: Iterable[Iterable[Int]]) = {
    val results = MultipleInputClustering.getClustersMutable(inputs.toIterator).toSet
    (inputs.flatten.toSet, results.map(_.id).toSet, results.map(_.cluster).toSet)
  }

  "Disjoint input sets" should "yield one cluster for each set" in {
    val inputs = Set(Set(1), Set(2), Set(3))
    val (inputIds, resultIds, resultClusterIds) = computeClusters(inputs)
    assert(resultIds.size == inputIds.size)
    assert(resultClusterIds.size == inputIds.size)
  }

  "Two intersecting input sets" should "be joined into one cluster" in {
    val inputs = Set(Set(1,2), Set(2,3))
    val (inputIds, resultIds, resultClusterIds) = computeClusters(inputs)
    assert(resultIds.size == inputIds.size)
    assert(resultClusterIds.size == 1)
  }

  "Three intersecting input sets" should "be joined into one cluster" in {
    val inputs = Set(Set(1,2), Set(2,3), Set(3,4))
    val (inputIds, resultIds, resultClusterIds) = computeClusters(inputs)
    assert(resultIds.size == inputIds.size)
    assert(resultClusterIds.size == 1)
  }

  "Five input sets with one intersection" should "yield four clusters" in {
    val inputs = Set(Set(1), Set(2), Set(3), Set(4), Set(4, 5))
    val (inputIds, resultIds, resultClusterIds) = computeClusters(inputs)
    assert(resultIds.size == inputIds.size) // All input Ids are clustered
    assert(resultClusterIds.size == 4)
  }

  "Merging 123, 456, 789 and 34" should "result in two clusters of size 6 and 3" in {
    val input_sets = Set(Set(1, 2, 3), Set(4, 5, 6), Set(7, 8, 9), Set(3, 4))
    // Clustering:
    val results = MultipleInputClustering.getClustersImmutable(input_sets.toIterator).toSet
    val clusters = results
      // group by cluster identifier
      .groupBy(_.cluster)
      // map from Map[cluster -> List[Result(id,cluster)]] to Map[cluster -> List[id]]
      .mapValues(_.map(_.id))
    val clusterSizes = clusters
      .mapValues(_.size).values.toList
    assert(clusterSizes.contains(6))
    assert(clusterSizes.contains(3))
  }

  "Input set sequences" should "also work" in {
    val inputs = Seq(Seq(1,2), Seq(2,3), Seq(3,4))
    val (inputIds, resultIds, resultClusterIds) = computeClusters(inputs)
    assert(resultIds.size == inputIds.size)
    assert(resultClusterIds.size == 1)
  }

  "Both algorithms" should "produce identical results and the mutable should be faster" in {
    // Timer
    def time[R](msg: String = "Elapsed")(block: => R): (Long, R) = {
      val t0 = System.currentTimeMillis()
      val result = block
      val t1 = System.currentTimeMillis()
      if (t1 - t0 > 10000) println(msg + " time: " + (t1 - t0) / 1000 + "s")
      else println(msg + " time: " + (t1 - t0) + "ms")
      (t1 - t0, result)
    }
    // Data generation
    def generateTestData(numSets: Int, numAddrs: Int, maxAddrs: Int) = {
      val r = scala.util.Random
      val moreSets = (1 to numSets) //
        .map(_ => 1 to (2 + r.nextInt(maxAddrs - 2))) // number of elements
        .map(x => x.map(_ => r.nextInt(numAddrs)).toSet).toSet // map to addrs
      moreSets
    }

    val TESTSIZE = 1E4.toInt
    val data = generateTestData(TESTSIZE, TESTSIZE, 5)

    val (t_mutable, result_mutable) = time("mutable") {
      val result_iterator = MultipleInputClustering.getClustersMutable(data.toIterator)
      val clusters = result_iterator.toList
        .groupBy(_.cluster)
        .mapValues(_.map(_.id).sorted)
      clusters.values
    }
    val (t_immutable, result_immutable) = time("immutable") {
      val result_iterator = MultipleInputClustering.getClustersImmutable(data.toIterator)
      val clusters = result_iterator.toList
        .groupBy(_.cluster)
        .mapValues(_.map(_.id).sorted)
      clusters.values
    }
    val (s1, s2) = (result_immutable.toSet, result_mutable.toSet)
    assert((s1 diff s2.toSet).size == 0) // no difference between results
    assert((s2 diff s1.toSet).size == 0) // no difference between results
    assert(t_mutable < t_immutable) // mutable is faster
  }
}
