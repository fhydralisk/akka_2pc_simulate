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
  import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.CommitPhase
  
  case class ReadyMetrics()
  case class ReadyMetricsReply(ready: Boolean)
  case class FinishMetrics()
  case class FinishMetricsReply(result: MetricsResult)
  case class MetricsElement(transId: Int, shard: String, timestamp: Long, process: CommitPhase.CommitPhase)
}
