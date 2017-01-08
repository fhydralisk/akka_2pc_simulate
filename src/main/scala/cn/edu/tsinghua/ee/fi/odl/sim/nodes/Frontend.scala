package cn.edu.tsinghua.ee.fi.odl.sim.nodes


import concurrent.{Promise, Future}
import concurrent.duration._
import akka.actor.{Actor, ActorRef, ActorLogging, Props}
import akka.pattern.ask
import akka.util.Timeout
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
  val getSettingsTickTimeout = 2 seconds
  val getTransactionIdTimeout = 2 seconds
  
  val getSettingsTickTask = context.system.scheduler.schedule(2 seconds, getSettingsTickTimeout, self, GetSettingsTick)
  
  def receive = uninitialized
  private def uninitialized : Actor.Receive = {
    case GetSettingsTick => tryGetSettings
    case GetDataBrokerReply(brokerConfig, cohortProxyConfig) =>
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
  
  override def postStop {
    getSettingsTickTask.cancel()
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
}
