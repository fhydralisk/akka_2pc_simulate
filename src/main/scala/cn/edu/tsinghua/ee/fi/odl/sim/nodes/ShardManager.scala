package cn.edu.tsinghua.ee.fi.odl.sim.nodes

import akka.actor.{ActorRef, Props, Actor, ActorLogging, PoisonPill}
import akka.cluster.Cluster
import concurrent.{Future, Promise}
import concurrent.duration._
import util.{Success, Failure}
import collection.mutable.HashMap
import cn.edu.tsinghua.ee.fi.odl.sim.util.ShardManagerMessages


object ShardManager { 
  object GetFactoryTick
  def props: Props = Props(new ShardManager)
}


class ShardManager extends EndActor with ActorLogging {
  
  import ShardManagerMessages._
  import ShardManager._
  
  import context.dispatcher
  
  private val shardFactoryPromise = Promise[ShardFactory]()
  val shards = HashMap[String, ActorRef]()
  
  val getFactoryTickTimeout = 2 seconds
  
  private val getFactoryTask = context.system.scheduler.schedule(2 seconds, getFactoryTickTimeout, self, GetFactoryTick)
  
  def receive = uninitialized
  
  // Startup state, that is, uninitialized
  private def uninitialized : Actor.Receive = {
    case GetFactoryTick => tryGetFactory
      
    case GetShardFactoryReply(config) =>
      shardFactoryPromise success new ShardFactory(config)
      log.debug(s"shard factory constructed, config: $config")
      becomeInitialized
      
    case msg @ _ =>
      log.debug(s"unhandled message of $msg")
  }
  
  private def initialized : Actor.Receive = {
    case GetShardFactoryReply(config) =>
      // duplicated message, ignore
    case Deploy(shardName) =>
      // If there's already a shard with the given name, reply false. else reply true
      
      val result = (shards filterKeys { _ == shardName } headOption) map {_ => false} getOrElse { 
        getShardFactory map { f => 
          log.debug(s"Deploying shard $shardName")
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
  
  private def becomeInitialized {
    context.become(initialized)
    getFactoryTask.cancel()
  }
  
  private def tryGetFactory {
    //Send GetShardFactory Message to "Leader"
    NodeGetter.LeaderGetter.leaderActorPath foreach {
      context.actorSelection(_) ! GetShardFactory()
    }
  }
  
  // FIXME: This is really ugly
  private def getShardFactory() : Option[ShardFactory] = {
    val sfFuture = shardFactoryPromise.future
    sfFuture.isCompleted match {
      case true =>
        sfFuture.value.get match {
          case Success(sf) =>
            Some(sf)
          case _ =>
            None
        }
      case false =>
        None
    }
  }
  
  override def postStop {
    getFactoryTask.cancel()
  }
}
