package cn.edu.tsinghua.ee.fi.odl.sim.nodes
import akka.actor.{ActorSystem, ActorSelection, ActorPath}
import akka.cluster.Cluster
object NodeGetter {
  
  def roleAddresses(role: String)(implicit system: ActorSystem) =
    Cluster(system).state.members.filter { n => n.hasRole(role) && n.status == akka.cluster.MemberStatus.Up } map { _.address.toString }

  
  object ShardGetter {
    def shardGetter(shardDeployment: Option[Map[String, Set[String]]])(implicit system: ActorSystem): String => ActorSelection = { s=>
      val role = shardDeployment flatMap { m =>
         (m filter { _._2.contains(s) } headOption) map { _._1 } 
      }
  
      (role flatMap { roleAddresses(_) headOption } ) map { addr => 
        val actorPath = addr + s"/user/shardmanager/$s"
        system.actorSelection(actorPath)
      } getOrElse { 
        // this throw cause ask failed immediately
        throw new IllegalArgumentException 
      }
    }
  }
  
  object LeaderGetter {
    def leaderAddress(implicit system: ActorSystem) = roleAddresses("leader") 
    
    def leaderActorOfAddress(address: String): String = address + "/user/leader"
  
    def leaderActorOfAddress(address: ActorPath): String = leaderActorOfAddress(address.toString())
    
    def leaderActorPath(implicit system: ActorSystem) = leaderAddress map { leaderActorOfAddress }
  }
}
