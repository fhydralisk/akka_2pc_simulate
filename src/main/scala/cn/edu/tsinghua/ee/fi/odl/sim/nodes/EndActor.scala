package cn.edu.tsinghua.ee.fi.odl.sim.nodes


import akka.actor.Actor
import akka.cluster.Cluster


abstract class EndActor extends Actor {
  
  lazy val cluster = Cluster(context.system)
  
  protected def leaderAddress = 
    cluster.state.members.filter { n => n.hasRole("leader") && n.status == akka.cluster.MemberStatus.Up } map { _.address.toString }
  
  protected def leaderActorOfAddress(address: String) = address + "/user/leader"
  
  protected def leaderActorPath = leaderAddress map { leaderActorOfAddress }
  
}