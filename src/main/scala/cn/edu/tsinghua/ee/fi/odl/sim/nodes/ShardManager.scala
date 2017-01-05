package cn.edu.tsinghua.ee.fi.odl.sim.nodes

import akka.actor.{ActorRef, Props, Actor, ActorLogging, PoisonPill}
import concurrent.{Future, Promise}
import collection.mutable.HashMap


class ShardManager extends Actor with ActorLogging {
  
  import ShardManagerMessages._
  
  val shardFactoryPromise = Promise[ShardFactory]()
  val shards = HashMap[String, ActorRef]()
  
  def receive = uninitialized
  
  // Startup state, that is, uninitialized
  def uninitialized : Actor.Receive = {
    case GetShardFactoryReply(config) =>
      shardFactoryPromise success new ShardFactory(config)
      context.become(initialized)
    case msg @ _ =>
      log.debug(s"unhandled message of $msg")
  }
  
  def initialized : Actor.Receive = {
    case GetShardFactoryReply(config) =>
      // duplicated message, ignore
    case Deploy(shardName) =>
      val result = (shards filterKeys { _ == shardName } headOption) map {_ => false} getOrElse { 
        getShardFactory map { f => 
          shards += shardName -> f.newShard(shardName, context)
          true 
          } getOrElse false 
        }
      sender ! DeployReply(result)
      
    case Destroy(shardName) => 
      val result = (shards filterKeys { _ == shardName } headOption) map { e =>
        e._2 ! PoisonPill
        shards -= e._1
        true
      } getOrElse false
      sender ! DestroyReply(result)
  }
  
  def getShardFactory() : Option[ShardFactory] = {
    None
  }
}