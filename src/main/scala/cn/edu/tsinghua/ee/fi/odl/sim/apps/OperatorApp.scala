package cn.edu.tsinghua.ee.fi.odl.sim.apps

import com.typesafe.config.ConfigFactory
import akka.cluster.pubsub.{DistributedPubSub, DistributedPubSubMediator}
import concurrent.duration._
import akka.event.Logging

object OperatorApp {
  import cn.edu.tsinghua.ee.fi.odl.sim.util.FrontendMessages._
  
  def main(args: Array[String]) {
    val operatorConfig = ConfigFactory.parseResources("operator.conf").withFallback(ConfigFactory.parseString("akka.cluster.roles=[\"operator\"]"))
    val system = AkkaSystem.createSystem(Some(operatorConfig))
    
    val log = Logging(system, this.getClass)
    
    val mediator = DistributedPubSub(system).mediator
    
    import concurrent.ExecutionContext.Implicits.global
    val submitConfig  = operatorConfig.getConfig("submit")
    system.scheduler.scheduleOnce(20 seconds) {
      log.info("informing frontends to start commit test")
      mediator ! DistributedPubSubMediator.Publish("dosubmit", DoSubmit(submitConfig))
    }

  }
}