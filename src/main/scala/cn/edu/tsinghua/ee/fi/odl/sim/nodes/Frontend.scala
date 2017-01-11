package cn.edu.tsinghua.ee.fi.odl.sim.nodes


import concurrent.{Promise, Future}
import concurrent.duration._
import akka.actor.{Actor, ActorRef, ActorSelection, ActorLogging, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.Config
import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.{DataBroker, FakeBroker, CohortProxyFactory}


object Frontend {
  object GetSettingsTick
  
  def props: Props = Props(new Frontend)
}


class Frontend extends EndActor with ActorLogging {
  import Frontend._
  import NodeGetter.LeaderGetter._

  import cn.edu.tsinghua.ee.fi.odl.sim.util.FrontendMessages._
  import akka.cluster.pubsub.DistributedPubSubMediator.{Subscribe, SubscribeAck}
  
  import context.dispatcher
  
  val dataBrokerPromise = Promise[DataBroker]
  
  // role -> shard
  var shardDeployment: Option[Map[String, Set[String]]] = None
  val getSettingsTickTimeout = 2 seconds
  val getTransactionIdTimeout = 2 seconds
  
  val getSettingsTickTask = context.system.scheduler.schedule(2 seconds, getSettingsTickTimeout, self, GetSettingsTick)
  
  subscribe("dosubmit")
  
  def receive = uninitialized
  
  implicit val akkaSystem = context.system
  
  /*
   * Implicit conversion from shard name to actor selection
   */
  implicit def shardGetter: String => ActorSelection = NodeGetter.ShardGetter.shardGetter(shardDeployment)
  
  private def uninitialized : Actor.Receive = {
    case GetSettingsTick => tryGetSettings
    case GetDataBrokerReply(brokerConfig, cohortProxyConfig, shardDeployment) =>
      constructShardMap(shardDeployment)
      dataBrokerPromise success new FakeBroker(getNewTransactionId, new CohortProxyFactory(cohortProxyConfig), brokerConfig)
      log.info("frontend has successfully created fake broker")
      becomeInitialized
    case SubscribeAck(Subscribe(topic, None, `self`)) =>
      log.info(s"Frontend has successfully subscribed topic $topic")
    case m @ _ =>
      log.warning(s"unhandled message $m")
  }
  
  private def initialized : Actor.Receive = {
    case GetSettingsTick =>
      // ignore, maybe shut it up
    case _ : GetDataBrokerReply =>
      // duplicated reply, ignore 
    case DoSubmit(config) =>
      doSubmitTest(config)
    case m @ _ =>
      log.warning(s"unhandled message $m")
  }
  
  def tryGetSettings  {
    leaderActorPath foreach {
      context.actorSelection(_) ! GetDataBroker()
    }
  }
  
  def becomeInitialized {
    getSettingsTickTask.cancel()
    context.become(initialized)
  }
  
  def constructShardMap(shardDeploymentConfig: Config) {
    import collection.JavaConversions._
    shardDeployment = Some(collection.immutable.HashMap[String, Set[String]](
    shardDeploymentConfig.root().toArray map { case (k, v) => (k, v.unwrapped().asInstanceOf[java.util.List[String]].toSet) }: _*
    ))
    
    log.info("frontend has successfully constructed shard map")
  }
  
  def getNewTransactionId = {
    import cn.edu.tsinghua.ee.fi.odl.sim.util.TransactionMessages._
    implicit val timeout = Timeout(getTransactionIdTimeout)
    leaderActorPath.headOption map {
      context.actorSelection(_) ? GetTransactionId() transform ({
        case GetTransactionIdReply(transId) =>
          transId
      }, f => f)
    } getOrElse (Future.failed(new NullPointerException))
  }
  
  def doSubmitTest(config: Config) {
    import cn.edu.tsinghua.ee.fi.odl.sim.util.MetricsMessages._
    log.info(s"start submit test with config: $config")
    val broker = dataBrokerPromise.future.value.get.get
    
    val submits = config.getInt("nm-of-submits")
    val shards = config.getInt("nm-of-shards")
    val shardPrefix = config.getString("shard-prefix")
    
    for (x <- 1 to submits) {
      val trans = broker.newTransaction
      
      trans flatMap { t =>
        (1 to shards) foreach (n => t.put(shardPrefix + n, ""))
        log.debug(s"submitting txn-${t.transId}")
        t.submit() map { t.transId -> _ }
      } map { case (transId, metrics) =>
        log.info(s"Submit of transaction: $transId Succeed")
        metrics.phaseToTimestamp foreach { case (phase, timestamp) => publish("metrics", MetricsElement(transId, timestamp, phase)) }
      } recover { case e @ _ =>
        println(s"Submit Failed, expection: $e")
      }
    }
  }
  
  override def postStop {
    getSettingsTickTask.cancel()
  }
  
}
