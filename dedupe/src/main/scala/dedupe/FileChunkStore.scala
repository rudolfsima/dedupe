package dedupe

import java.io.{FileInputStream, FileOutputStream, InputStream, OutputStream}
import java.nio.file.{Files, Path}

class FileChunkStore[T: DedupeCodec](file: Path) extends CompressedChunkStore[T] {

  override protected def createOutputStream(): OutputStream = new FileOutputStream(file.toFile)
  override protected def createInputStream(): InputStream = new FileInputStream(file.toFile)

  override def free(): Unit = {
    super.free()
    Files.deleteIfExists(file)
  }

}
