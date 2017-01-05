package cn.edu.tsinghua.ee.fi.odl.sim.nodes


import akka.actor.{Actor, ActorLogging, Props}
import com.typesafe.config.Config
import cn.edu.tsinghua.ee.fi.odl.sim.util.{SimplifiedQueue, ActorContext}
import cn.edu.tsinghua.ee.fi.odl.sim.util.{QueueWrapper, TreeSetWrapper}
import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.Transaction

class ShardFactory(config: Config, actorContext: ActorContext) {
  def newShard(name: String) = {
    val canCommitQueue: SimplifiedQueue[Transaction] = config.getString("can-commit-queue-type") match {
      case "priorityQueue" =>
        new TreeSetWrapper[Transaction]()
      case _ =>
        new QueueWrapper[Transaction]()
    }
    
    actorContext.actorSystem.actorOf(Shard.props(canCommitQueue), name=name)
  }
}

object Shard {
  def props(canCommitQueue: SimplifiedQueue[Transaction]): Props = Props(new Shard(canCommitQueue))
}

class Shard(canCommitQueue: SimplifiedQueue[Transaction]) extends Actor with ActorLogging {
  //TODO: AnyRef -> Transaction
  import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.CommitMessages._
  def receive = {
    case CanCommitMessage(txn) =>
      
    case CommitMessage(txn) =>
      sender() ! CommitAck()
    case unknown @ _ => 
      log.warning("Message Not Found: " + unknown.toString())
  }
}