package cn.edu.tsinghua.ee.fi.odl.sim.nodes

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