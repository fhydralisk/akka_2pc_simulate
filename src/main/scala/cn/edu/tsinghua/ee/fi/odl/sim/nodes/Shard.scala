package cn.edu.tsinghua.ee.fi.odl.sim.nodes


import akka.actor.{Actor, ActorLogging, Props}
import com.typesafe.config.Config
import cn.edu.tsinghua.ee.fi.odl.sim.util.{SimplifiedQueue, ActorContext}
import cn.edu.tsinghua.ee.fi.odl.sim.util.{QueueWrapper, TreeSetWrapper}
import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.Transaction


trait Shard

class ShardFactory(config: Config, actorContext: ActorContext) {
  def newShard(name: String) = {
    val canCommitQueue: SimplifiedQueue[Transaction] = config.getString("can-commit-queue-type") match {
      case "priorityQueue" =>
        new TreeSetWrapper[Transaction]()
      case _ =>
        new QueueWrapper[Transaction]()
    }
    
    val shardProps: Props = config.getString("shard-type") match {
      case "normalShard" =>
        NormalShard.props(canCommitQueue)
      case "dealDeadlockShard" =>
        DealDeadlockShard.props(canCommitQueue)
    }
    
    actorContext.actorSystem.actorOf(shardProps, name=name)
  }
}

object NormalShard {
  def props(canCommitQueue: SimplifiedQueue[Transaction]): Props = Props(new NormalShard(canCommitQueue))
}

object DealDeadlockShard {
  def props(canCommitQueue: SimplifiedQueue[Transaction]): Props = Props(new DealDeadlockShard(canCommitQueue))
}

abstract class AbstractShard(canCommitQueue: SimplifiedQueue[Transaction]) extends Shard with Actor with ActorLogging {
  //TODO: AnyRef -> Transaction
  import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.CommitMessages._
  def receive = {      
    case CommitMessage(txn) =>
      sender() ! CommitAck()
    case unknown @ _ => 
      log.warning("Message Not Found: " + unknown.toString())
  }
}

class NormalShard(canCommitQueue: SimplifiedQueue[Transaction]) extends AbstractShard(canCommitQueue) {
  import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.CommitMessages._
  override def receive = {
    case CanCommitMessage(txn) =>
    case _ =>
      super.receive
  }
}

class DealDeadlockShard(canCommitQueue: SimplifiedQueue[Transaction]) extends AbstractShard(canCommitQueue) {
  import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.CommitMessages._
  override def receive = {
    case CanCommitMessage(txn) =>
    case _ =>
      super.receive
  }
}
