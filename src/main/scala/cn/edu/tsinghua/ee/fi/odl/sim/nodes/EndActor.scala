package cn.edu.tsinghua.ee.fi.odl.sim.nodes


import akka.actor.Actor
import akka.cluster.Cluster
import akka.cluster.pubsub.DistributedPubSub
import akka.cluster.pubsub.DistributedPubSubMediator.{Subscribe, Publish}


abstract class EndActor extends Actor {
  
  implicit val system = context.system
  
  lazy val cluster = Cluster(context.system)
  
  val mediator = DistributedPubSub(context.system).mediator
  
  protected def subscribe(topic: String) {
    mediator ! Subscribe(topic, self)
  }
  
  protected def publish(topic: String, msg: Any) {
    mediator ! Publish(topic, msg)
  }
  
}