package dedupe

trait DeduplicationEvent

object DeduplicationEvent {

  case class DeduplicationStarted(collisionClassCount: Int, level: Int) extends DeduplicationEvent {
    override def toString = "Deduplication level " + level + " started with " + collisionClassCount + " collision classes."
  }
  case class DeduplicationFinished(level: Int) extends DeduplicationEvent {
    override def toString = "Deduplication level " + level + " finished."
  }
  case class SplittingChunkTooBig(chunkSize: Long, maxChunkSize: Long, chunkIndex: Int, chunksCount: Int, level: Int) extends DeduplicationEvent {
    override def toString = s"Splitting chunk ($chunkIndex ouf of $chunksCount, recursion level $level) too big: " + chunkSize + " > " + maxChunkSize
  }
  case class ChunkStoreCreated(index: Int, size: Int, level: Int) extends DeduplicationEvent {
    override def toString = s"Chunk store $index of $size created at recursion level $level."
  }
  case class ChunkStoreDeleted(chunkIndex: Int, size: Int, level: Int) extends DeduplicationEvent {
    override def toString = s"Chunk $chunkIndex of $size deleted at recursion level $level."
  }

  def noDebug(f: DeduplicationEvent => Any)(evt: DeduplicationEvent) = evt match {
    case _: ChunkStoreCreated | _: ChunkStoreDeleted =>
    case evt => f(evt)
  }

}
