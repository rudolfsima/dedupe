package dedupe

import org.apache.commons.io.output.ByteArrayOutputStream

import java.io.{InputStream, OutputStream}


class MemoryChunkStore[T: DedupeCodec]() extends CompressedChunkStore[T] {
  private lazy val buffer = new ByteArrayOutputStream()

  override protected def createOutputStream(): OutputStream = buffer
  override protected def createInputStream(): InputStream = buffer.toInputStream

}
