package cn.edu.tsinghua.ee.fi.odl.sim.util

import com.typesafe.config.Config

object ShardManagerMessages {
  case class GetShardFactory()
  case class GetShardFactoryReply(config: Config)
  case class Deploy(name: String)
  case class DeployReply(success: Boolean)
  case class Destroy(name: String)
  case class DestroyReply(success: Boolean)
}

object TransactionMessages {
  case class GetTransactionId()
  case class GetTransactionIdReply(transId: Int)
}

object FrontendMessages {
  case class GetDataBroker()
  case class GetDataBrokerReply(brokerConfig: Config, cohortProxyConfig: Config, shardDeployment: Config)
  case class DoSubmit(submitConfig: Config)
  case class SubmitMetrics() //TODO: Parameters here
}

object MetricsMessages {
  trait MetricsResult
  
  case class ReadyMetrics()
  case class ReadyMetricsReply(ready: Boolean)
  case class FinishMetrics()
  case class FinishMetricsReply(result: MetricsResult)
  case class MetricsElement(transId: Int, timestamp: Long, process: CommitPhase.CommitPhase)
  
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
    case class CommitPhase(phase: Int)
    val CAN_COMMIT = 1
    val CAN_COMMIT_REPLIED = 2
    val PRE_COMMIT = 3
    val PRE_COMMIT_REPLIED = 4
    val COMMITED = 5
  }
}
