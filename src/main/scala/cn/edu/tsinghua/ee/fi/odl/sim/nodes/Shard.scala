package cn.edu.tsinghua.ee.fi.odl.sim.nodes


import akka.actor.{Actor, ActorLogging, Props, ActorContext}
import com.typesafe.config.Config
import cn.edu.tsinghua.ee.fi.odl.sim.util.{SimplifiedQueue}
import cn.edu.tsinghua.ee.fi.odl.sim.util.{QueueWrapper, TreeSetWrapper}
import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.Transaction


trait Shard extends Actor

class ShardFactory(config: Config) {
  def newShard(name: String, actorContext: ActorContext) = {
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
    
    actorContext.actorOf(shardProps, name=name)
  }
}

object NormalShard {
  def props(canCommitQueue: SimplifiedQueue[Transaction]): Props = Props(new NormalShard(canCommitQueue))
}

object DealDeadlockShard {
  def props(canCommitQueue: SimplifiedQueue[Transaction]): Props = Props(new DealDeadlockShard(canCommitQueue))
}

abstract class AbstractShard(canCommitQueue: SimplifiedQueue[Transaction]) extends Shard with ActorLogging {
  import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.CommitMessages._
  
  var processingTransaction: Option[Transaction] = None
  
  def receive = processCanCommit orElse processCommit
  
  def processCommit: Actor.Receive = {      
    case CommitMessage(txn) =>
      sender() ! CommitAck()
    case unknown @ _ => 
      log.warning("Message Not Found: " + unknown.toString())
  }
  
  def processCanCommit: Actor.Receive
}

class NormalShard(canCommitQueue: SimplifiedQueue[Transaction]) extends AbstractShard(canCommitQueue) {
  import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.CommitMessages._
  def processCanCommit = {
    case CanCommitMessage(txn) =>
  }
}

class DealDeadlockShard(canCommitQueue: SimplifiedQueue[Transaction]) extends AbstractShard(canCommitQueue) {
  import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.CommitMessages._
  def processCanCommit = {
    case CanCommitMessage(txn) =>
  }
}
