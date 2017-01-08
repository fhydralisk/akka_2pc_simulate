package cn.edu.tsinghua.ee.fi.odl.sim.apps


import akka.actor.ActorSystem
import akka.cluster.Cluster
import com.typesafe.config.{ Config, ConfigFactory }

object AkkaSystem {
  val systemName = "OdlSimulationSystem"
  def createSystem(specialConfig : Option[Config] = None) = {
    val config = specialConfig map { ConfigFactory.load() withFallback } getOrElse(ConfigFactory.load())
    ActorSystem(systemName, config)
  }
  
  def getMemberAddressesOfRole(role: String)(implicit system: ActorSystem) = {
    val cluster = Cluster(system)
    cluster.state.members.filter { _.hasRole(role) } map { _.address }
  }
}