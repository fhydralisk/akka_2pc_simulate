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
  import cn.edu.tsinghua.ee.fi.odl.sim.util.FrontendMessages._
  
  import concurrent.ExecutionContext.Implicits.global
  
  val dataBrokerPromise = Promise[DataBroker]
  
  // role -> shard
  var shardDeployment: Option[Map[String, Set[String]]] = None
  val getSettingsTickTimeout = 2 seconds
  val getTransactionIdTimeout = 2 seconds
  
  val getSettingsTickTask = context.system.scheduler.schedule(2 seconds, getSettingsTickTimeout, self, GetSettingsTick)
  
  def receive = uninitialized
  
  /*
   * Implicit conversion from shard name to actor selection
   */
  implicit def shardGetter: String => ActorSelection = s => {
    // akka.tcp://system@address:port/user/shardmanager/$s
    val role = shardDeployment flatMap { m =>
       (m filter { _._2.contains(s) } headOption) map { _._1 } 
    }
    
    (role map { roleAddresses(_) headOption } ) map { addr => 
      context.actorSelection(addr + s"/user/shardmanager/$s") 
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
      becomeInitialized
    case _ =>
  }
  
  private def initialized : Actor.Receive = {
    case GetSettingsTick =>
      // ignore, maybe shut it up
    case _ : GetDataBrokerReply =>
      // duplicated reply, ignore 
    case DoSubmit(config) =>
      // TODO: do submit here and reply metrics?
      val broker = dataBrokerPromise.future.value.get.get
      val trans = broker.newTransaction
      trans map { t =>
        t.put("shard1", "")
        t.put("shard2", "")
        t.submit()
      } map { _ =>
        println("Submit OK")
      } recover { case _ =>
        println("Submit Failed")
      }
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
  
  override def postStop {
    getSettingsTickTask.cancel()
  }
  
}
