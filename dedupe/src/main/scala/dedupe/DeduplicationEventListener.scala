package dedupe

trait DeduplicationEventListener {

  def onDeduplicationEvent(event: DeduplicationEvent): Unit

}
