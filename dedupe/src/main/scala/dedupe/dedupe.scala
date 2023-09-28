package object dedupe {

  trait DedupeCodec[T] {
    def encode(record: T): Array[Byte]
    def decode(bytes: Array[Byte], offset: Int, length: Int): T
  }

  trait DedupeHash[T] {
    def calculateHash(record: T, salt: Int): Int
  }
  trait DedupeEquals[T] {
    def areDuplicate(a: T, b: T): Boolean
  }
  trait DedupeMerge[T] {
    def merge(a: T, b: T): T
  }

  trait DedupeTrinity[T] extends DedupeHash[T] with DedupeEquals[T] with DedupeMerge[T]

}
