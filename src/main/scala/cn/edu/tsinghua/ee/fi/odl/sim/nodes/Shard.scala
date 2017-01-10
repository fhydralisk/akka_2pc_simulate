package cn.edu.tsinghua.ee.fi.odl.sim.nodes


import akka.actor.{Actor, ActorRef, ActorLogging, Props, ActorContext, Cancellable}
import com.typesafe.config.Config
import cn.edu.tsinghua.ee.fi.odl.sim.util.{SimplifiedQueue}
import cn.edu.tsinghua.ee.fi.odl.sim.util.{QueueWrapper, TreeSetWrapper}
import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.Transaction
import cn.edu.tsinghua.ee.fi.odl.sim.fakedatatree._
import concurrent.duration._
import cn.edu.tsinghua.ee.fi.odl.sim.fakedatatree.FakeDataTree
import cn.edu.tsinghua.ee.fi.odl.sim.fakedatatree.DataTree


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
  def props(canCommitQueue: SimplifiedQueue[Tuple2[Transaction, ActorRef]], dataTree: DataTree): Props = 
    Props(new NormalShard(canCommitQueue, dataTree))
}


object DealDeadlockShard {
  def props(canCommitQueue: SimplifiedQueue[Tuple2[Transaction, ActorRef]], dataTree: DataTree): Props = 
    Props(new DealDeadlockShard(canCommitQueue, dataTree))
}


abstract class AbstractShard(
    canCommitQueue: SimplifiedQueue[Tuple2[Transaction, ActorRef]], 
    dataTree: DataTree
    ) extends Shard with ActorLogging {
  
  import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.CommitMessages._
  
  protected var processingTransaction: Option[Tuple2[Transaction, Cancellable]] = None
  
  def receive = processCanCommit orElse processAbort orElse processCommit
  
  // TODO: make this into settings
  def canCommitTimeout = 2 seconds
  
  protected def processCommit: Actor.Receive = {      
    case CommitMessage(txn) =>
      doCommit(txn, sender)
    case unknown @ _ => 
      log.warning("Message Not Found: " + unknown.toString())
  }
  
  protected def processAbort: Actor.Receive = {
    case AbortMessage(txn) =>
      processingTransaction.filter { _._1.transId == txn.transId } match {
        case None =>
          // Not current, abort
          log.debug("ignoring abort message due to not processing this transaction")
        case Some(_) =>
          finishCanCommit
          finishCommit
      }
  }
  
  protected def processCanCommit: Actor.Receive
  
  protected def doCanCommit(txn: Transaction, senderOfTxn: ActorRef): Boolean = {
    // Timeout here
    import concurrent.ExecutionContext.Implicits.global
    val abortWhenTimeoutTask = context.system.scheduler.scheduleOnce(canCommitTimeout, self, AbortMessage(txn))
    
    val valid = validateTransaction(txn)
    replyCanCommit(txn, senderOfTxn)(valid)
    if (valid) processingTransaction = Some(txn -> abortWhenTimeoutTask)
    log.debug(s"can-commit replied: $valid")
    valid
  }
  
  protected def finishCanCommit {
    processingTransaction foreach { _._2.cancel() }
  }
  
  /*
   * Commits the transaction.
   */
  protected def doCommit(txn: Transaction, senderOfTxn: ActorRef) {
    finishCanCommit
    
    senderOfTxn ! (processingTransaction.filter { _._1.transId == txn.transId } match {
      case Some(_) =>
        dataTree.applyModification(txn.modification)
        log.debug(s"commit replied")
        CommitAck(txn.transId)
      case None => 
        CommitNack(txn.transId)
    })
    
    finishCommit
  }
  
  protected def finishCommit {
    processingTransaction = None
    maybeProcessNextCanCommit
  }
  
  /*
   * Checks if there's a proceeding transaction. If
   * No, validates the transaction and returns the result of it;
   * Yes, queues the transaction and returns None
   */
  protected def maybeCommitOrQueue(txn: Transaction, senderOfTxn: ActorRef) { 
    (processingTransaction orElse canCommitQueue.peek()) match {
      case Some(_) => queueCanCommit(txn, senderOfTxn)
      case None => doCanCommit(txn, senderOfTxn)
    }
  }
    
  protected def maybeProcessNextCanCommit {    
    assert(processingTransaction == None)
    while ( !(canCommitQueue.poll map { (doCanCommit _) tupled } getOrElse { true }) ) {}
  }
  
  protected def validateTransaction(txn: Transaction) = dataTree.validate(txn.modification)
  
  /*
   * Queues the transaction.
   */
  protected def queueCanCommit(txn: Transaction, senderOfTxn: ActorRef) {
    canCommitQueue.offer(txn -> senderOfTxn)
  }
  
  /*
   * Replies the message of can-commit 
   */
  protected def replyCanCommit(txn: Transaction, senderOfTxn: ActorRef)(result: Boolean) {
    result match {
      case true =>
        senderOfTxn ! CanCommitAck(txn.transId)
      case false =>
        senderOfTxn ! CanCommitNack(txn.transId)
    }
  }
  
  override def postStop {
    // TODO: Shall we kill the abort timer here?
  }
  
}


class NormalShard(
    canCommitQueue: SimplifiedQueue[Tuple2[Transaction, ActorRef]], 
    dataTree: DataTree
    ) extends AbstractShard(canCommitQueue, dataTree) {
  
  import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.CommitMessages._
  
  protected def processCanCommit = {
    case CanCommitMessage(txn) =>
      maybeCommitOrQueue(txn, sender)
  }
}


// TODO: Implement this shard, which could respond to multiple can-commit message with different reply
class DealDeadlockShard(
    canCommitQueue: SimplifiedQueue[Tuple2[Transaction, ActorRef]], 
    dataTree: DataTree
    ) extends AbstractShard(canCommitQueue, dataTree) {
  
  import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.CommitMessages._
  
  protected def processCanCommit = {
    case CanCommitMessage(txn) =>
  }
}
