package dedupe

import org.openjdk.jmh.annotations.{Benchmark, Scope, Setup, State}
import zio.json._
import zio.stream.{ZPipeline, ZSink, ZStream}
import zio.{Chunk, Task, Unsafe, ZIO}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.util.Random
import scala.util.hashing.MurmurHash3

@State(Scope.Benchmark)
class DeduplicationBenchmark {

  case class PartialKeyA(field0: String, field1: String, field2: String)
  case class PartialKeyB(field0: String, field1: String, field2: String)
  case class MergeSubject(wordCount: Int, sentence: String)

  case class InputRecord(pka: PartialKeyA, pkb: PartialKeyB, mergeSubject: MergeSubject)
  implicit val partialKeyACodec: JsonCodec[PartialKeyA] = DeriveJsonCodec.gen[PartialKeyA]
  implicit val partialKeyBCodec: JsonCodec[PartialKeyB] = DeriveJsonCodec.gen[PartialKeyB]
  implicit val mergeSubjectCodec: JsonCodec[MergeSubject] = DeriveJsonCodec.gen[MergeSubject]
  implicit val inputRecordCodec: JsonCodec[InputRecord] = DeriveJsonCodec.gen[InputRecord]

  implicit val inputRecordDedupeCodec = new DedupeCodec[InputRecord] {
    override def encode(record: InputRecord): Array[Byte] = record.toJson.getBytes(StandardCharsets.UTF_8)
    override def decode(bytes: Array[Byte], offset: Int, length: Int): InputRecord = new String(bytes.slice(offset, offset + length)).fromJson[InputRecord].right.get
  }

  implicit val inputRecordDedupeTrinitiy = new DedupeTrinity[InputRecord] {
    override def calculateHash(record: InputRecord, salt: Int): Int = {
      MurmurHash3.mixLast(
        MurmurHash3.productHash(record.pka, salt),
        MurmurHash3.productHash(record.pkb, salt)
      )
    }
    override def merge(a: InputRecord, b: InputRecord): InputRecord = {
      assert(areDuplicate(a, b))
      InputRecord(a.pka, a.pkb, MergeSubject(a.mergeSubject.wordCount + b.mergeSubject.wordCount, a.mergeSubject.sentence + " " + b.mergeSubject.sentence))
    }
    override def areDuplicate(a: InputRecord, b: InputRecord): Boolean = {
      a.pka == b.pka && a.pkb == b.pkb
    }
  }

  var tempFile: Path = _

  @Setup
  def prepareData = unsafely {
    tempFile = Files.createTempFile("dedupe-benchmark-input-", ".ndjson")
    ZStream
      .repeatZIO {
        ZIO.succeed {
          val word = notSoRandom(15)
          val wordCount = 1
          val pka = PartialKeyA(notSoRandom(60), notSoRandom(40), notSoRandom(50))
          val pkb = PartialKeyB(notSoRandom(20), notSoRandom(30), notSoRandom(40))
          InputRecord(pka, pkb, MergeSubject(wordCount, word))
        }
      }
      .take(10000000) // 10 million records
      .map(rec => (rec.toJson + "\n").getBytes(StandardCharsets.UTF_8))
      .flatMap(arr => ZStream.fromChunk(Chunk.fromArray(arr)))
      .run(ZSink.fromFile(tempFile.toFile()))
      .map(_ => ())
  }

  @Benchmark
  def memoryChunkStoreBenchmark() = genericBenchmark(_ => new MemoryChunkStore[InputRecord]())

  @Benchmark
  def fileChunkStoreBenchmark() = {
    val baseDir = Files.createTempDirectory("dedupe-benchmark-chunkstore-")
    genericBenchmark(index => new FileChunkStore[InputRecord](baseDir.resolve(index.toString)))
  }

  private def genericBenchmark(chunkStoreFactory: Int => ChunkStore[InputRecord]) = unsafely {
    def createDedupe() = {
      new RecursiveChungusDedupe[InputRecord](
        Files.size(tempFile),
        80 * 1024 * 1024, // 80 MiB
        chunkStoreFactory,
        _ => ()
      )
    }

    val chungusZio = ZStream.fromFile(tempFile.toFile())
      .via(ZPipeline.utf8Decode >>> ZPipeline.splitLines)
      .map(_.fromJson[InputRecord].right.get)
      .runFold(createDedupe()) { (dedupe, rec) =>
        dedupe.add(rec)
        dedupe
      }

    // Write the result to disk just to simulate a real-world use case.
    ZStream
      .fromZIO(chungusZio)
      .flatMap(dedupe => ZStream.fromIterable(dedupe))
      .map(rec => (rec.toJson + "\n").getBytes(StandardCharsets.UTF_8))
      .flatMap(arr => ZStream.fromChunk(Chunk.fromArray(arr)))
      .run(ZSink.fromFile(Files.createTempFile("dedupe-benchmark-output-", ".ndjson").toFile))
      .map(_ => ())
  }

  private def notSoRandom(length: Int): String = {
    val c = (Random.nextInt(10) + 65).toChar
    Seq.fill(length)(c).mkString
  }

  private def unsafely(task: Task[Unit]): Unit = {
    Unsafe.unsafe {
      implicit unsafe =>
        zio.Runtime.default.unsafe.run(
          task.catchAll { err =>
            err.printStackTrace()
            ZIO.fail(err)
          }
        )
    }
  }

}
