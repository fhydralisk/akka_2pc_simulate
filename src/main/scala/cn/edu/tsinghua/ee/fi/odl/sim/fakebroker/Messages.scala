package cn.edu.tsinghua.ee.fi.odl.sim.fakebroker

object CommitMessages {
  final case class CanCommitMessage(txn: Transaction)
  final case class CommitMessage(txn: Transaction)
  final case class AbortMessage(txn: Transaction)
  
  final case class CanCommitAck(txnId: Int)
  final case class CanCommitNack(txnId: Int)
  final case class CommitAck(txnId: Int)
  final case class CommitNack(txnId: Int)
  
  // for Forward Cohort Proxy
  final case class ForwardCanCommit(txn: Transaction)
}
