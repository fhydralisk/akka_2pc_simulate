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

//TODO: metrics here
class Frontend extends EndActor with ActorLogging {
  import Frontend._
  import cn.edu.tsinghua.ee.fi.odl.sim.util.FrontendMessages._
  import akka.cluster.pubsub.DistributedPubSubMediator.{Subscribe, SubscribeAck}
  
  import concurrent.ExecutionContext.Implicits.global
  
  val dataBrokerPromise = Promise[DataBroker]
  
  // role -> shard
  var shardDeployment: Option[Map[String, Set[String]]] = None
  val getSettingsTickTimeout = 2 seconds
  val getTransactionIdTimeout = 2 seconds
  
  val getSettingsTickTask = context.system.scheduler.schedule(2 seconds, getSettingsTickTimeout, self, GetSettingsTick)
  
  subscribe("dosubmit")
  
  def receive = uninitialized
  
  /*
   * Implicit conversion from shard name to actor selection
   */
  implicit def shardGetter: String => ActorSelection = s => {
    // akka.tcp://system@address:port/user/shardmanager/$s
    val role = shardDeployment flatMap { m =>
       (m filter { _._2.contains(s) } headOption) map { _._1 } 
    }

    (role flatMap { roleAddresses(_) headOption } ) map { addr => 
      val actorPath = addr + s"/user/shardmanager/$s"
      context.actorSelection(actorPath)
    } getOrElse { 
      // this throw cause ask failed immediately
      throw new IllegalArgumentException 
    }
  }
  
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
      // TODO: do submit here and reply metrics?
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
    val trans = broker.newTransaction
    trans flatMap { t =>
      t.put("shard1", "")
      t.put("shard2", "")
      t.submit() map { t.transId -> _ }
    } map { case (transId, metrics) =>
      println("Submit OK")
      metrics.phaseToTimestamp foreach { case (phase, timestamp) => publish("metrics", MetricsElement(transId, timestamp, phase)) }
    } recover { case e @ _ =>
      println(s"Submit Failed, expection: $e")
    }
  }
  
  override def postStop {
    getSettingsTickTask.cancel()
  }
  
}
