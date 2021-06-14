package info.graphsense.clustering

import scala.annotation.tailrec
import scala.collection.mutable

// If common is imported, this object should be used to cluster addresses.
// There are two implementations, one uses mutable DS and the other does not.
// Most likely, the mutable implementation is faster, though this has to be
// tested in production first.
// Usage:
//   val input_sets = Set(Set(1,2),Set(2,3), Set(4,5))
//   val result = Clustering.getClusters(input_sets.toIterator) OR
//   val result = Clustering.getClustersMutable(input_sets.toIterator)
//   result.foreach(println) =>
//        Result(1,1)
//        Result(2,1)
//        Result(3,1)
//        Result(4,4)
//        Result(5,4)
case object MultipleInputClustering {

  def getClustersImmutable[A](txInputs: Iterator[Iterable[A]]): Iterator[Result[A]] = {
    val mapper = UnionFindImmutable[A](Map.empty)
    val am = doGrouping(mapper, txInputs)
    am.collect
  }

  def getClustersMutable[A](txInputs: Iterator[Iterable[A]]): Iterator[Result[A]] = {
    val mapper = UnionFindMutable[A](mutable.Map.empty)
    val am = doGrouping(mapper, txInputs)
    am.collect
  }

  // Naming is maybe a little suboptimal, as the return value is not really
  // an iterator over the clusters (in the example above: [1,2,3],[4,5])
  // but an iterator over the representatives of each node, i.e.
  // something similar to [1->1, 2->1, 3->1, 4->4, 5->4]

  @tailrec
  private def doGrouping[A](am: UnionFind[A], inputs: Iterator[Iterable[A]]): UnionFind[A] = {
    if (inputs.hasNext) {
      val addresses = inputs.next()
      doGrouping(am.union(addresses), inputs)
    } else {
      am
    }
  }
}

// For each node fed into the algorithm, one of these is returned.
//   id:      Address of node
//   cluster: Address of representative of this node
case class Result[A](id: A, cluster: A) {
  override def toString(): String = s"$id -> $cluster"
}

// For each element in UF-DS an instance of Representative is stored that
// refers to the root of the cluster
//   address: The representative of the element. If the "owner" has the same address,
//            he's the root of the cluster
//   height:  Union by rank/size is used to always add the smaller to the larger;
//            this prevents degeneration to linear (instead of logarithmic) lists
private[clustering] case class Representative[A](address: A, height: Byte) {
  def apply(exclusive: Boolean) = {
    if (exclusive) Representative[A](this.address, (this.height + 1).toByte)
    else this
  }
}

trait UnionFind[A] {
  def find(address: A): Representative[A]
  def union(addresses: Iterable[A]): UnionFind[A]
  def collect: Iterator[Result[A]]
}

private[clustering] case class UnionFindImmutable[A](entries: Map[A, Representative[A]])
    extends UnionFind[A] {

  def find(address: A): Representative[A] =
    if (entries.contains(address)) {
      val entry = entries(address)
      if (entry.address == address) entry // if root of cluster is found
      else find(entry.address) // look for root
    } else Representative[A](address, 0) // if not yet in DS, create new cluster with this element

  def union(addresses: Iterable[A]): UnionFindImmutable[A] = {
    val representatives = addresses.map(find)
    val (highestRepresentative, exclusive) =
      representatives.tail.foldLeft((representatives.head, true)) { (b, a) =>
        if (b._1.height > a.height) b
        else if (b._1.height == a.height) (b._1, false)
        else (a, true)
      }
    val setRepresentative = highestRepresentative(exclusive)
    val newEntries = representatives.map(r => (r.address, setRepresentative))
    UnionFindImmutable(entries ++ newEntries)
  }

  def collect = {
    for (a <- entries.keysIterator)
      yield Result(a, find(a).address)
  }
}

private[clustering] case class UnionFindMutable[A](entries: mutable.Map[A, Representative[A]])
    extends UnionFind[A] {

  def find(address: A): Representative[A] =
    if (entries.contains(address)) {
      val entry = entries(address)
      if (entry.address == address) {
        entry // if root of cluster is found
      } else {
        val root = find(entry.address) // look for root
        entries.put(address, root)
        root
      }
    } else Representative[A](address, 0) // if not yet in DS, create new cluster with this element

  def union(addresses: Iterable[A]): UnionFindMutable[A] = {
    val representatives = addresses.map(find)
    val (highestRepresentative, exclusive) =
      representatives.tail.foldLeft((representatives.head, true)) { (b, a) =>
        if (b._1.height > a.height) b
        else if (b._1.height == a.height) (b._1, false)
        else (a, true)
      }

    val height =
      if (exclusive) highestRepresentative.height
      else (highestRepresentative.height + 1).toByte
    val representative = Representative[A](highestRepresentative.address, height)
    representatives.foreach(r => { entries.put(r.address, representative) })
    this
  }

  def collect = {
    for (a <- entries.keysIterator)
      yield Result(a, find(a).address)
  }
}
