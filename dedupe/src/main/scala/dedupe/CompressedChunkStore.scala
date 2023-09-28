package dedupe

import org.apache.commons.io.IOUtils

import java.io.{BufferedInputStream, BufferedOutputStream, InputStream, OutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

abstract class CompressedChunkStore[T: DedupeCodec]() extends ChunkStore[T] {

  protected def createOutputStream(): OutputStream
  protected def createInputStream(): InputStream

  private val compressedOut = new GZIPOutputStream(new BufferedOutputStream(createOutputStream()))
  private var compressedIn: GZIPInputStream = null

  private var totalBytesWritten = 0L
  private val codec = implicitly[DedupeCodec[T]]

  private val headerBuf = Array.ofDim[Byte](2)
  private var recordBuf = IOUtils.byteArray()

  override def add(record: T): Unit = {
    val recBytes = codec.encode(record)
    val recSize = recBytes.size
    headerBuf(0) = (recSize >> 8).toByte
    headerBuf(1) = recSize.toByte
    compressedOut.write(headerBuf)
    compressedOut.write(recBytes)
    totalBytesWritten += recSize
  }

  override def memoryImprintBytes: Long = totalBytesWritten

  override def iterator: Iterator[T] = {
    closeOpenStreams()
    compressedIn = new GZIPInputStream(new BufferedInputStream(createInputStream()))
    Iterator
      .continually {
        val len = IOUtils.read(compressedIn, headerBuf)
        if (len == 0) None
        else {
          val recordLength = (headerBuf(0) << 8) + (headerBuf(1) & 0xFF)
          if (recordBuf.length < recordLength) recordBuf = IOUtils.byteArray(recordLength * 2)
          IOUtils.readFully(compressedIn, recordBuf, 0, recordLength)
          Some(codec.decode(recordBuf, 0, recordLength))
        }
      }
      .takeWhile(_.isDefined)
      .map(_.get)
  }

  private def closeOpenStreams(): Unit = {
    IOUtils.closeQuietly(compressedOut)
    IOUtils.closeQuietly(compressedIn)
  }

  override def free(): Unit = {
    closeOpenStreams()
  }

}
