package cn.edu.tsinghua.ee.fi.odl.sim.nodes


import akka.actor.{Actor, ActorRef, ActorLogging, Props, ActorContext}
import com.typesafe.config.Config
import cn.edu.tsinghua.ee.fi.odl.sim.util.{SimplifiedQueue}
import cn.edu.tsinghua.ee.fi.odl.sim.util.{QueueWrapper, TreeSetWrapper}
import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.Transaction
import cn.edu.tsinghua.ee.fi.odl.sim.fakedatatree._


trait Shard extends Actor

class ShardFactory(config: Config) {
  def newShard(name: String, actorContext: ActorContext) = {
    implicit def orderingMap: Ordering[Tuple2[Transaction, ActorRef]] = Ordering.by(_._1)
    val canCommitQueue: SimplifiedQueue[Tuple2[Transaction, ActorRef]] = config.getString("can-commit-queue-type") match {
      case "priorityQueue" =>
        new TreeSetWrapper[Tuple2[Transaction, ActorRef]]()
      case _ =>
        new QueueWrapper[Tuple2[Transaction, ActorRef]]()
    }
    
    val dataTree = new FakeDataTree()
    
    val shardProps: Props = config.getString("shard-type") match {
      case "normalShard" =>
        NormalShard.props(canCommitQueue, dataTree)
      case "dealDeadlockShard" =>
        DealDeadlockShard.props(canCommitQueue, dataTree)
    }
    
    actorContext.actorOf(shardProps, name=name)
  }
}

object NormalShard {
  def props(canCommitQueue: SimplifiedQueue[Tuple2[Transaction, ActorRef]], dataTree: DataTree): Props = Props(new NormalShard(canCommitQueue, dataTree))
}

object DealDeadlockShard {
  def props(canCommitQueue: SimplifiedQueue[Tuple2[Transaction, ActorRef]], dataTree: DataTree): Props = Props(new DealDeadlockShard(canCommitQueue, dataTree))
}

abstract class AbstractShard(canCommitQueue: SimplifiedQueue[Tuple2[Transaction, ActorRef]], dataTree: DataTree) extends Shard with ActorLogging {
  import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.CommitMessages._
  
  protected var processingTransaction: Option[Transaction] = None
  
  def receive = processCanCommit orElse processCommit
  
  protected def processCommit: Actor.Receive = {      
    case CommitMessage(txn) =>
      doCommit(txn)
    case unknown @ _ => 
      log.warning("Message Not Found: " + unknown.toString())
  }
  
  protected def doCommit(txn: Transaction) {
    /*
     * Commits the transaction.
     */
    processingTransaction.filter { _.transId == txn.transId } map { _ => 
      dataTree.applyModification(txn.modification)
      sender() ! CommitAck(txn.transId)
    } orElse {
      sender() ! CommitNack(txn.transId)
      None
    }
    processingTransaction = None
  }
  
  protected def maybeCommitOrQueue(txn: Transaction, senderOfTxn: ActorRef): Option[Boolean] = {
    /*
     * Checks if there's a proceeding transaction. If
     * No, validates the transaction and return the result of it;
     * Yes, queues the transaction and return None
     */
    (processingTransaction orElse canCommitQueue.peek()) map { _ =>
      queueCanCommit(txn, sender)
      None
    } orElse {
      Some(Some(dataTree.validate(txn.modification)))
    } get
  }
  
  protected def queueCanCommit(txn: Transaction, senderOfTxn: ActorRef) {
    /*
     * Queues the transaction.
     */
    canCommitQueue.offer(txn -> senderOfTxn)
  }
  
  protected def replyCanCommit(txn: Transaction, senderOfTxn: ActorRef)(result: Boolean) {
    /*
     * Replies the message of can-commit 
     */
    result match {
      case true =>
        senderOfTxn ! CanCommitAck(txn.transId)
      case false =>
        senderOfTxn ! CanCommitNack(txn.transId)
    }
  }
  
  protected def processCanCommit: Actor.Receive
}

class NormalShard(canCommitQueue: SimplifiedQueue[Tuple2[Transaction, ActorRef]], dataTree: DataTree) extends AbstractShard(canCommitQueue, dataTree) {
  import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.CommitMessages._
  
  protected def processCanCommit = {
    case CanCommitMessage(txn) =>
      maybeCommitOrQueue(txn, sender) foreach { replyCanCommit(txn, sender) }
  }
}

class DealDeadlockShard(canCommitQueue: SimplifiedQueue[Tuple2[Transaction, ActorRef]], dataTree: DataTree) extends AbstractShard(canCommitQueue, dataTree) {
  import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.CommitMessages._
  
  protected def processCanCommit = {
    case CanCommitMessage(txn) =>
  }
}
