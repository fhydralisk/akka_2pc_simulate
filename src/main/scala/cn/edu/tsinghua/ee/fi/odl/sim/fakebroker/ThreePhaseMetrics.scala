package cn.edu.tsinghua.ee.fi.odl.sim.fakebroker


trait ThreePhaseMetrics {
  def phaseTimestamp: Int => Long
  def phaseToTimestamp: Map[Int, Long]
}


private[fakebroker] trait ThreePhaseMetricsTesting {
  def testPoint(phase: Int)
}

private class ThreePhaseMetricsImpl extends ThreePhaseMetrics with ThreePhaseMetricsTesting {
  val metricsContainer = collection.mutable.HashMap[Int, Long]()
  def testPoint(phase: Int) {
    metricsContainer += phase -> System.nanoTime
  }
  
  def phaseTimestamp = metricsContainer.get(_).get
  
  def phaseToTimestamp = metricsContainer.toMap
}


object CommitPhase {
  /*
   * CommitPhase(phase)
   * phase:
   * 1 - can-commit - frontend -> backend
   * 2 - can-commit-received - backend -> frontend
   * 3 - pre-commit - frontend -> backend
   * 4 - pre-commit-received - backend -> frontend
   * 5 - commited - frontend
   */
  def apply(phase: Int) = new CommitPhase(phase)
  case class CommitPhase(phase: Int)
  val CAN_COMMIT = 1
  val CAN_COMMIT_REPLIED = 2
  val PRE_COMMIT = 3
  val PRE_COMMIT_REPLIED = 4
  val COMMITED = 5
}