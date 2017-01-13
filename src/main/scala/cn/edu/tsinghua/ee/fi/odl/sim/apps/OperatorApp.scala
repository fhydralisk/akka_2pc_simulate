package cn.edu.tsinghua.ee.fi.odl.sim.apps

import com.typesafe.config.ConfigFactory
import akka.cluster.Cluster
import concurrent.duration._
import akka.event.Logging
import cn.edu.tsinghua.ee.fi.odl.sim.nodes.Operator


object OperatorApp {
  import cn.edu.tsinghua.ee.fi.odl.sim.util.FrontendMessages._
  
  def main(args: Array[String]) {
    val operatorConfig = ConfigFactory.parseFile(new java.io.File("config/operator.conf"))
                                      .withFallback(ConfigFactory.parseResources("operator.conf"))
                                      .withFallback(ConfigFactory.parseString("akka.cluster.roles=[\"operator\"]"))
      
    val system = AkkaSystem.createSystem(Some(operatorConfig))
    val config = { 
      if (args.size == 1)
        operatorConfig.getConfig(args(0))
      else
        operatorConfig
    } getConfig("testing")
      
    
    Cluster(system).registerOnMemberUp {
      system.actorOf(Operator.props(config))
    }
  }
}