package dedupe

class BrainThinkingCollisionTableEstimator extends CollisionTableEstimator {

  override def estimateColisionTableSize(inputDataByteSize: Long, maxSortableChunkByteSize: Long) = {
    val baseEstimate = inputDataByteSize / maxSortableChunkByteSize
    val estimateWithMargin = (baseEstimate * 1.3).toInt
    val smallestHighPowerOf2 = 1 << (32 - Integer.numberOfLeadingZeros(estimateWithMargin - 1))
    2 max smallestHighPowerOf2
  }

}
