package dedupe

import org.scalatest.funsuite.AnyFunSuite
import scala.util.Random

import scala.language.postfixOps

class DefaultCollisionTableEstimatorTest extends AnyFunSuite {

  private object Estimator extends BrainThinkingCollisionTableEstimator

  test("reasonable size") {
    val estimate = Estimator.estimateColisionTableSize(20 GiB, 150 MiB)
    assert(estimate == 256)
  }

  test("extreme demands") {
    val estimate = Estimator.estimateColisionTableSize(1 TiB, 5 MiB)
    assert(estimate == 524288)
  }

  test("small data") {
    val estimate = Estimator.estimateColisionTableSize(450 MiB, 500 MiB)
    assert(estimate == 2)
  }

  test("tiny data") {
    val estimate = Estimator.estimateColisionTableSize(128, 500 MiB)
    assert(estimate == 2)
  }

  test("256 buckets bounds test") {
    testWithinBounds(25 KiB, 150)
  }

  test("8 buckets bounds test") {
    testWithinBounds(25 KiB, 5 KiB)
  }

  private def testWithinBounds(inputSize: Long, maxSortableSize: Long) = {
    val estimate = Estimator.estimateColisionTableSize(inputSize, maxSortableSize)
    val longest = simulateLongest(inputSize.toInt, estimate)
    val overhead = 1.0 - inputSize / (estimate * maxSortableSize).toDouble
    Console.err.println(s"inputSize=$inputSize, maxSortableSize=$maxSortableSize, estimate=$estimate, longest=$longest, overhead=$overhead")
    assert(longest <= maxSortableSize)
  }


  def simulateLongest(inputSize: Int, bucketCount: Int) = {
    val a = Array.ofDim[Int](bucketCount)
    for (_ <- 1 to inputSize) {
      val i = Random.nextInt(bucketCount)
      a.update(i, a(i) + 1)
    }
    a.toSeq.max
  }


  private implicit class ByteSize(factor: Int) {
    def KiB = 1024L * factor
    def MiB = 1024L * KiB
    def GiB = 1024L * MiB
    def TiB = 1024L * GiB
  }

}
