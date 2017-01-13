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


object CohortEntry {
  implicit val cmp: Ordering[CohortEntry] = Ordering.by(_.transaction)
}


class CohortEntry(val transaction: Transaction, val peer: ActorRef, val timeoutTask: Cancellable) {
  
}


class ShardFactory(config: Config) {
  def newShard(name: String, actorContext: ActorContext) = {
    
    val canCommitQueue: SimplifiedQueue[CohortEntry] = config.getString("can-commit-queue-type") match {
      case "priorityQueue" =>
        new TreeSetWrapper[CohortEntry]()
      case _ =>
        new QueueWrapper[CohortEntry]()
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


abstract class AbstractShard(
    canCommitQueue: SimplifiedQueue[CohortEntry], 
    dataTree: DataTree
    ) extends Shard with ActorLogging {
  
  import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.CommitMessages._
  
  protected var processingTransaction: Option[CohortEntry] = None
  
  def receive = processCanCommit orElse processAbort orElse processCommit orElse unhandled
  
  // TODO: make this into settings
  def canCommitTimeout = 5 seconds
  
  protected def processCommit: Actor.Receive = {      
    case CommitMessage(txn) =>
      doCommit(txn, sender)
  }
  
  protected def processAbort: Actor.Receive = {
    case AbortMessage(txn) =>
      processingTransaction.filter { _.transaction.transId == txn.transId } match {
        case None =>
          val queuedAbort = canCommitQueue.removeWhen { _.transaction.transId == txn.transId }
          queuedAbort map { qt => 
            log.info(s"aborting queued transaction ${qt.transaction.transId} due to timeout")
          } getOrElse {
            log.debug(s"unknown abort message received for transaction ${txn.transId}")
          }
        case Some(_) =>
          finishCanCommit
          log.info(s"aborting transaction $txn due to timeout")
          finishCommit
      }
  }
  
  protected def processCanCommit: Actor.Receive
  
  protected def unhandled : Actor.Receive = {
    case unknown @ _ => 
      log.warning("Message Not Found: " + unknown.toString())
  }
  
  protected def createCohortEntry(txn: Transaction, peer: ActorRef): CohortEntry = {
    // Timeout here
    import context.dispatcher
    val abortWhenTimeoutTask = context.system.scheduler.scheduleOnce(canCommitTimeout, self, AbortMessage(txn))
    new CohortEntry(txn, peer, abortWhenTimeoutTask)
  }
  
  protected def doCanCommit(cohortEntry: CohortEntry): Boolean = {    
    val valid = validateTransaction(cohortEntry.transaction)
    replyCanCommit(cohortEntry)(valid)
    if (valid) processingTransaction = Some(cohortEntry)
    log.debug(s"can-commit replied: $valid")
    valid
  }
  
  protected def finishCanCommit {
    processingTransaction foreach { _.timeoutTask.cancel() }
  }
  
  /*
   * Commits the transaction.
   */
  protected def doCommit(txn: Transaction, senderOfMsg: ActorRef) {
    // FIXME: doCommit shall use cohort entry instead of transaction
    senderOfMsg ! (processingTransaction match {
      case Some(ce) if ce.transaction.transId == txn.transId =>
        finishCanCommit
        dataTree.applyModification(txn.modification)
        log.debug(s"commit replied")
        CommitAck(txn.transId)
        finishCommit
      case _ => 
        CommitNack(txn.transId)
    })
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
  protected def maybeCommitOrQueue(cohortEntry: CohortEntry) { 
    (processingTransaction orElse canCommitQueue.peek()) match {
      case Some(_) => queueCanCommit(cohortEntry)
      case None => doCanCommit(cohortEntry)
    }
  }
    
  protected def maybeProcessNextCanCommit {    
    assert(processingTransaction == None)
    while ( !(canCommitQueue.poll map { doCanCommit _ } getOrElse { true }) ) {}
  }
  
  protected def validateTransaction(txn: Transaction) = dataTree.validate(txn.modification)
  
  /*
   * Queues the transaction.
   */
  protected def queueCanCommit(cohortEntry: CohortEntry) {
    canCommitQueue.offer(cohortEntry)
  }
  
  /*
   * Replies the message of can-commit 
   */
  protected def replyCanCommit(cohortEntry: CohortEntry)(result: Boolean) {
    result match {
      case true =>
        cohortEntry.peer ! CanCommitAck(cohortEntry.transaction.transId)
      case false =>
        cohortEntry.peer ! CanCommitNack(cohortEntry.transaction.transId)
    }
  }
  
  override def postStop {
    // TODO: Shall we kill the abort timer here?
  }
  
}


object NormalShard {
  def props(canCommitQueue: SimplifiedQueue[CohortEntry], dataTree: DataTree): Props = 
    Props(new NormalShard(canCommitQueue, dataTree))
}


class NormalShard(
    canCommitQueue: SimplifiedQueue[CohortEntry], 
    dataTree: DataTree
    ) extends AbstractShard(canCommitQueue, dataTree) {
  
  import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.CommitMessages._
  
  protected def processCanCommit = {
    case CanCommitMessage(txn) =>
      maybeCommitOrQueue(createCohortEntry(txn, sender))
  }
}


object DealDeadlockShard {
  object DoRealCanCommit
  
  def props(canCommitQueue: SimplifiedQueue[CohortEntry], dataTree: DataTree): Props = 
    Props(new DealDeadlockShard(canCommitQueue, dataTree))
}


/*
 *  DealDeadlockShard:
 *  Shard that ensures the sequence of transaction, which eventually resolves the deadlock.
 *  IMPORTANT: Sorted Queue needed
 */
class DealDeadlockShard(
    canCommitQueue: SimplifiedQueue[CohortEntry], 
    dataTree: DataTree
    ) extends NormalShard(canCommitQueue, dataTree) {
  
  import cn.edu.tsinghua.ee.fi.odl.sim.fakebroker.CommitMessages._
  import DealDeadlockShard._
  import context.dispatcher
  
  
  val delayFirstCanCommit = 300 millis
  override def receive = processRealCanCommit orElse super.receive 
  
  def processRealCanCommit : Actor.Receive = {
    case DoRealCanCommit =>
      maybeProcessNextCanCommit
  }
  
  override protected def maybeCommitOrQueue(cohortEntry: CohortEntry) { 
    (processingTransaction orElse canCommitQueue.peek()) match {
      case Some(_) => 
      case None => context.system.scheduler.scheduleOnce(delayFirstCanCommit, self, DoRealCanCommit)
    }
    queueCanCommit(cohortEntry)
  }
}
