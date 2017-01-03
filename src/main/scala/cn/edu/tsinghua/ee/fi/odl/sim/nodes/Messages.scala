package cn.edu.tsinghua.ee.fi.odl.sim.nodes

trait Transaction

object CommitMessages {
  final case class CanCommitMessage(txn: Transaction)
  final case class CommitMessage()
  final case class AbortMessage()
  
  final case class CanCommitAck()
  final case class CanCommitNack()
  final case class CommitAck()
}