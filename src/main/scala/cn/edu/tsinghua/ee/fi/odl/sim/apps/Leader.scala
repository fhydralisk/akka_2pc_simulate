package cn.edu.tsinghua.ee.fi.odl.sim.apps


import com.typesafe.config.ConfigFactory
import cn.edu.tsinghua.ee.fi.odl.sim.nodes.Leader

/* TODO:
 * Construct Leader Actor
 */
object LeaderApp {
  def main(args: Array[String]) {
    
    val leaderConfig = ConfigFactory.parseFile(new java.io.File("config/leader.conf"))
                                    .withFallback(ConfigFactory.parseResources("leader.conf"))
    val system = AkkaSystem.createSystem(Some(leaderConfig))
    
    system.actorOf(Leader.props, name="leader")
  }

}