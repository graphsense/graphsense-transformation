package at.ac.ait.clustering

case class AddressMapping(entries: Map[Long, MappingInfo]) {

  def apply(address: Long): Representative =
    if (entries.contains(address))
      entries(address) match {
        case MappingInfo(_, Some(a)) => apply(a)
        case MappingInfo(h, None) => Representative(address, h)
      }
    else Representative(address, 0)

  def group(addresses: Set[Long]): AddressMapping = {
    val representatives = addresses.map(apply)
    val (highestRepresentative, exclusive) =
      representatives.tail.foldLeft((representatives.head, true)) { (b,a) =>
        if (b._1.height > a.height) b
        else if (b._1.height == a.height) (b._1, false)
        else (a, true)
      }
    val newEntries =
      for (r <- representatives if r != highestRepresentative)
      yield {
        val a = apply(r.address)
        (a.address, MappingInfo(a.height, Some(highestRepresentative.address)))
      }
    val height =
      if (exclusive) highestRepresentative.height
      else (highestRepresentative.height + 1).toByte
    val entryForRepresentative = (highestRepresentative.address, MappingInfo(height, None))
    AddressMapping(entries ++ newEntries + entryForRepresentative)
  }

  def collect: Iterator[ClusteringResult] = {
    for (a <- entries.keysIterator)
    yield ClusteringResult(a, apply(a).address)
  }
}

case class MappingInfo(height: Byte, next: Option[Long])
case class Representative(address: Long, height: Byte)
