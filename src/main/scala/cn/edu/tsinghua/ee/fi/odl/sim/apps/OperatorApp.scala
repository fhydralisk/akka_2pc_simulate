package cn.edu.tsinghua.ee.fi.odl.sim.apps

import com.typesafe.config.ConfigFactory

class OperatorApp {
  def main(args: Array[String]) {
    val operatorConfig = ConfigFactory.parseString("akka.cluster.roles=[\"operator\"]")
    val system = AkkaSystem.createSystem(Some(operatorConfig))
    
    

  }
}