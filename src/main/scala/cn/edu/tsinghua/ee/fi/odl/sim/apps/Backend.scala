package cn.edu.tsinghua.ee.fi.odl.sim.apps


import com.typesafe.config.ConfigFactory
import akka.cluster.Cluster
import cn.edu.tsinghua.ee.fi.odl.sim.nodes.ShardManager

/* TODO:
 * Construct ShardMananger Actor when Cluster is up
 */
object BackendApp {
  def main(args: Array[String]) {
    
    val argsToRoles = if (args.isEmpty) "" else (("\"" + args.head + "\"") /: args.tail) { (s, e) => s + ", \"" + e + "\"" }
    val backendConfig = ConfigFactory.load("backend.conf").withFallback(ConfigFactory.parseString(s"akka.cluster.roles=[$argsToRoles]"))
    val system = AkkaSystem.createSystem(Some(backendConfig))
    val cluster = Cluster(system)
    cluster registerOnMemberUp {
      system.actorOf(ShardManager.props, name="shardmanager")
    }
  }
}