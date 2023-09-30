package dedupe

import dedupe.DeduplicationEvent._

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.hashing.MurmurHash3

class RecursiveChungusDedupe[T](
  datasetByteSize: Long,
  maxChunkBytes: Long,
  chunkStoreFactory: Int => ChunkStore[T],
  deduplicationEventListener: DeduplicationEventListener,
  collisionTableEstimator: CollisionTableEstimator = new BrainThinkingCollisionTableEstimator,
  private val recursionLevel: Int = 0,
  private val chunkStoreCounter: AtomicInteger = new AtomicInteger()
)(implicit hasher: DedupeHash[T], comparator: DedupeEquals[T], mixer: DedupeMerge[T]) extends Iterable[T] with AutoCloseable {

  assert(recursionLevel < 16)

  require(datasetByteSize > 0, "datasetByteSize must be positive")

  private val collisionTableSize = collisionTableEstimator.estimateColisionTableSize(datasetByteSize, maxChunkBytes)
  deduplicationEventListener.onDeduplicationEvent(DeduplicationStarted(collisionTableSize, recursionLevel))

  private val allocatedChunkStores = ListBuffer.empty[ChunkStore[T]]
  private val topLevelCollectionChunkStoreFactory = if (recursionLevel > 0) chunkStoreFactory else { index: Int =>
    val store = chunkStoreFactory(index)
    allocatedChunkStores += store
    store
  }

  private val chunks = Array.ofDim[ChunkStore[T]](collisionTableSize)
  private def safeStore(index: Int): ChunkStore[T] = {
    val store = chunks(index)
    if (store == null) {
      val newStore = topLevelCollectionChunkStoreFactory(chunkStoreCounter.getAndIncrement())
      deduplicationEventListener.onDeduplicationEvent(ChunkStoreCreated(index, chunks.size, recursionLevel))
      chunks(index) = newStore
      newStore
    } else store
  }

  def add(record: T): Unit = {
    def supplementalHash(hash: Int) = {
      var h = hash
      h ^= (h >>> 20) ^ (h >>> 12)
      h ^ (h >>> 7) ^ (h >>> 4)
    }
    val salt = if (recursionLevel == 0) MurmurHash3.productSeed else 31 * recursionLevel
    val collisionClass = supplementalHash(hasher.calculateHash(record, salt)) & (collisionTableSize - 1)
    safeStore(collisionClass).add(record)
  }

  private class ClassDedupeIterator(collisionClassIterator: Iterator[T]) extends Iterator[T] {
    class WrappedT(val data: T) {
      override def hashCode(): Int = hasher.calculateHash(data, MurmurHash3.productSeed)
      override def equals(that: Any) = comparator.areDuplicate(data, that.asInstanceOf[WrappedT].data)
    }

    private val seen = mutable.HashMap[WrappedT, WrappedT]()
    collisionClassIterator foreach { suspect =>
      val wrapped = new WrappedT(suspect)
      seen.get(wrapped) match {
        case Some(existing) =>
          val rewrapped = new WrappedT(mixer.merge(existing.data, suspect))
          seen.put(rewrapped, rewrapped)
        case None =>
          seen.put(wrapped, wrapped)
      }
    }
    private val mergedIterator = seen.valuesIterator.map(_.data)

    override def hasNext: Boolean = mergedIterator.hasNext
    override def next(): T = mergedIterator.next()
  }

  private class DepletionAwareIterator(depletionAction: () => Any)(iterator: Iterator[T]) extends Iterator[T] {
    override def hasNext: Boolean = {
      if (iterator.hasNext) true
      else {
        depletionAction()
        false
      }
    }
    override def next(): T = iterator.next
  }

  override def iterator: Iterator[T] = new DepletionAwareIterator(() => deduplicationEventListener.onDeduplicationEvent(DeduplicationFinished(recursionLevel)))(
    chunks
      .iterator
      .zipWithIndex
      .flatMap { case (chunk, chunkIndex) =>
        if (chunk == null) Iterator.empty
        else {
          val it = if (chunk.memoryImprintBytes > maxChunkBytes) {
            deduplicationEventListener.onDeduplicationEvent(SplittingChunkTooBig(chunk.memoryImprintBytes, maxChunkBytes, chunkIndex, chunks.size, recursionLevel))
            val subchungus = new RecursiveChungusDedupe[T](
              chunk.memoryImprintBytes,
              maxChunkBytes,
              topLevelCollectionChunkStoreFactory,
              deduplicationEventListener,
              collisionTableEstimator,
              recursionLevel + 1,
              chunkStoreCounter
            )
            chunk foreach subchungus.add
            subchungus.iterator
          } else {
            new ClassDedupeIterator(chunk.iterator)
          }
          def deleteChunk(): Unit = {
            chunk.free()
            chunks(chunkIndex) = null
            deduplicationEventListener.onDeduplicationEvent(ChunkStoreDeleted(chunkIndex, chunks.size, recursionLevel))
          }
          new DepletionAwareIterator(deleteChunk)(it)
        }
      }
  )

  override def close(): Unit = {
    val firstError = new AtomicReference[Throwable]()
    allocatedChunkStores foreach { store =>
      try {
        store.free()
      } catch {
        case t: Throwable => firstError.compareAndSet(null, t)
      }
    }
    Option(firstError.get) foreach (t => throw t)
  }

}
