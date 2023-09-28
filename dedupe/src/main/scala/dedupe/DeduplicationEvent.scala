package dedupe

trait DeduplicationEvent {

}

object DeduplicationEvent {

  case class DeduplicationStarted(collisionClassCount: Int, level: Int) extends DeduplicationEvent {
    override def toString = "Deduplication level " + level + " started with " + collisionClassCount + " collision classes."
  }
  case class SplittingChunkTooBig(chunkSize: Long, maxChunkSize: Long, chunkIndex: Int, chunksCount: Int) extends DeduplicationEvent {
    override def toString = s"Splitting chunk ($chunkIndex ouf of $chunksCount) too big: " + chunkSize + " > " + maxChunkSize
  }
  case class ChunkStoreCreated(index: Int, size: Int, recursionLevel: Int) extends DeduplicationEvent {
    override def toString = s"Chunk store $index of $size created at recursion level $recursionLevel."
  }
  case class ChunkStoreDeleted(chunkIndex: Int, size: Int, recursionLevel: Int) extends DeduplicationEvent {
    override def toString = s"Chunk $chunkIndex of $size deleted at recursion level $recursionLevel."
  }
}
