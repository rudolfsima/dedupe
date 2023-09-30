package dedupe

trait CollisionTableEstimator {

  def estimateColisionTableSize(inputDataByteSize: Long, maxSortableChunkByteSize: Long): Int

}
