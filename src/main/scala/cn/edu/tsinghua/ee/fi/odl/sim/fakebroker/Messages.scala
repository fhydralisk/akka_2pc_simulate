package cn.edu.tsinghua.ee.fi.odl.sim.fakebroker

object CommitMessages {
  final case class CanCommitMessage(txn: Transaction)
  final case class CommitMessage(txn: Transaction)
  final case class AbortMessage()
  
  final case class CanCommitAck()
  final case class CanCommitNack()
  final case class CommitAck()
}