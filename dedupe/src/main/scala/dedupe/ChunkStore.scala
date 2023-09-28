package dedupe

trait ChunkStore[T] extends Iterable[T] {

  def add(record: T): Unit

  /** An estimate of the total uncompressed memory size of all added records.
   *
   * In order to implement this method efficiently, it is good enough to count the number
   * of bytes after serialization of each record (before any potential compression).
   * Another way would be to use something like the java-sizeof library.
   * */
  def memoryImprintBytes: Long

  /** Free all resources associated with this store including the removal of any files persisted, if applicable. */
  def free(): Unit

}
