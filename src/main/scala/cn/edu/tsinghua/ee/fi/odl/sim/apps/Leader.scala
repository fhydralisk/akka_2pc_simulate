package cn.edu.tsinghua.ee.fi.odl.sim.apps


import com.typesafe.config.ConfigFactory
import cn.edu.tsinghua.ee.fi.odl.sim.nodes.Leader

/* TODO:
 * Construct Leader Actor
 */
object LeaderApp {
  def main(args: Array[String]) {
    
    val leaderConfig = ConfigFactory.load("leader.conf").withFallback(ConfigFactory.parseString("akka.cluster.roles=[\"leader\"]"))
    val system = AkkaSystem.createSystem(Some(leaderConfig))
    
    system.actorOf(Leader.props, name="leader")
  }

}