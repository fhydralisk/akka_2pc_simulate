package cn.edu.tsinghua.ee.fi.odl.sim.nodes

import akka.actor.{Actor, ActorRef, ActorSelection, ActorLogging, Props}
import akka.cluster.Cluster
import akka.cluster.pubsub.{ DistributedPubSub, DistributedPubSubMediator }
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.{Config, ConfigFactory, ConfigValue}
import concurrent.duration._
import cn.edu.tsinghua.ee.fi.odl.sim.util.TransactionMessages._
import cn.edu.tsinghua.ee.fi.odl.sim.util.ShardManagerMessages._
import cn.edu.tsinghua.ee.fi.odl.sim.util.FrontendMessages._


object LeaderConfiguration {
  val leaderConfig = ConfigFactory.load("leader.conf")
  val shardConfig = leaderConfig.getConfig("shard")
  val shardDeployConfig = shardConfig.getConfig("shard-deploy")
  val shardFactoryConfig = shardConfig.getConfig("shard-factory")
  val brokerConfig = leaderConfig.getConfig("data-broker")
  val cohortProxyConfig = leaderConfig.getConfig("cohort-proxy")
}

object Leader {
  def props : Props = Props(new Leader)
}

/**
 * A Leader Actor has the following responsibility:
 * 
 */
class Leader extends Actor with ActorLogging {
  var shardConfigDispatcher : Option[ActorRef] = None
  var frontendConfigDispatcher : Option[ActorRef] = None
  var transactionIdDispatcher: Option[ActorRef] = None
  var metricsActor: Option[ActorRef] = None
  
  override def preStart = {
    shardConfigDispatcher = Some(context.actorOf(ShardConfigDispatcher.props))
    transactionIdDispatcher = Some(context.actorOf(TransactionIDDispatcher.props))
    frontendConfigDispatcher = Some(context.actorOf(FrontendConfigDispatcher.props))
    metricsActor = Some(context.actorOf(MetricsActor.props(new TwoPhaseRecounter)))
  }
  
  def receive = {
    // TODO: forward can-commit to CanCommitProxy
    case msg : GetShardFactory =>
      shardConfigDispatcher foreach { 
        _ forward msg
      }
    case msg : GetTransactionId =>
      transactionIdDispatcher foreach {
        _ forward msg
      }
    case msg : GetDataBroker =>
      frontendConfigDispatcher foreach {
        _ forward msg
    }
  }
}


object ShardConfigDispatcher {
  object ShardDeployTick
  
  def props : Props = Props(new ShardConfigDispatcher)
}


class ShardConfigDispatcher extends Actor with ActorLogging {
  import LeaderConfiguration._
  import ShardConfigDispatcher._
  
  val cluster = Cluster(context.system)
  
  import scala.collection.JavaConversions._
  
  import concurrent.ExecutionContext.Implicits.global
  
  def shardDeployTimeout = Timeout(2 seconds)
  def shardDeployTickTimeout = 2 seconds
  
  // record the deployment of shards in nodes
  val deployedShardsOfRoles = collection.mutable.HashMap[String, Set[String]]()
  
  // role -> shard(s)
  lazy val shardsOfRoles = collection.immutable.HashMap[String, Set[String]](
      shardDeployConfig.root().toArray map { case (k, v) => (k, v.unwrapped().asInstanceOf[java.util.List[String]].toSet) }: _*
      )
      
  val deployTickTask = context.system.scheduler.schedule(5 seconds, shardDeployTickTimeout, self, ShardDeployTick)
  
  def receive = {
    case GetShardFactory() =>
      // GetShardFactory is sent by an uninitialized shardmanager that might just be restarted. clear the deploy state of it.
      getRolesOfActor(sender) foreach { deployedShardsOfRoles -= _ }
      sender ! GetShardFactoryReply(shardFactoryConfig)
      
    case DeployReply(success) =>
      // it shall be taken care of in reply, not here.
      
    case ShardDeployTick => doShardDeploy
      
  }
  
  override def postStop = {
    deployTickTask.cancel()
  }
  
  def doShardDeploy = {
    
    // Roles deployed but not sure about all shard of it has been deployed
    val rolesDeployed = shardsOfRoles filterKeys { deployedShardsOfRoles.contains }
    
    val rolesUndeployed = shardsOfRoles filterNot { case (k, _) => deployedShardsOfRoles contains k}
    val rolesDeployedButShard = rolesDeployed map { case (k, v) => (k, v diff deployedShardsOfRoles(k)) }
    
    val shardsShallDeployed = rolesUndeployed ++ rolesDeployedButShard
    
    shardsShallDeployed foreach { case (role, shards) =>
      val roleShardManagers = getShardManagerPathOfRole(role) map { context.system.actorSelection }
      roleShardManagers foreach { roleActorSelection =>
        shards foreach { shard =>
          (roleActorSelection ? Deploy(shard))(shardDeployTimeout, self) map {
            case DeployReply(suc)  =>
              log.info(s"Deploy of shard: $shard in role $role succeed")
              deployedShardsOfRoles += (role -> (Set(shard) ++ deployedShardsOfRoles.getOrElse(role, Set())))
              
            case m @ _ =>
              log.debug(s"Unknown deploy reply $m")
              
          } recover {
            case e @ _ =>
              log.debug(s"Deploy of shard: $shard in role $role failed with $e")
          }
        }
      }
    }
    
  }
  
  def getRolesOfActor(actor: ActorRef) = cluster.state.members.filter { _.address.equals(actor.path.address) } flatMap { _.roles }
  
  def getShardsOfActor(actor: ActorRef) = {
    val roles = getRolesOfActor(actor)
    (shardsOfRoles filterKeys { roles.contains } values() toSet) flatten
  }
  
  // FIXME: what if more than one node have same role? currently it is not allowed
  def getShardManagerPathOfRole(role: String) = 
    cluster.state.members.filter { e => 
      e.hasRole(role) && e.status == akka.cluster.MemberStatus.Up 
    } map { _.address.toString + "/user/shardmanager" } headOption
  
}


object FrontendConfigDispatcher {
  def props: Props = Props(new FrontendConfigDispatcher)
}


class FrontendConfigDispatcher extends Actor with ActorLogging {
  import LeaderConfiguration._
  def receive = {
    case GetDataBroker() =>
      sender ! GetDataBrokerReply(brokerConfig, cohortProxyConfig, shardDeployConfig)
  }
}


object TransactionIDDispatcher {
  def props: Props = Props(new TransactionIDDispatcher)
}


class TransactionIDDispatcher extends Actor with ActorLogging {
  
  var transactionIdNext = 1
  
  def receive = {
    case GetTransactionId() =>
      sender ! GetTransactionIdReply(transactionIdNext)
      transactionIdNext += 1
  }
}


object CanCommitProxy {
  def props: Props = Props(new CanCommitProxy)
}


class CanCommitProxy extends Actor with ActorLogging {
  // TODO: Implement this proxy which can forward can-commit message to its destination
  override def receive = {
    case _ =>
  }
}

