package cn.edu.tsinghua.ee.fi.odl.sim.fakebroker


import concurrent.{Future, Promise}
import concurrent.duration._
import com.typesafe.config.Config
import akka.pattern.ask
import akka.util.Timeout
import akka.actor.ActorSelection
import collection.immutable.TreeMap
import Transaction.SubmitResult


trait CohortProxy {
  def submit(): Future[SubmitResult]
}


class CohortProxyFactory(config: Config)(implicit shardGetter: String => ActorSelection) {
  def getCohortProxy(txn: Transaction) : Option[CohortProxy] = {
    config.getString("cohort-proxy-type") match {
      case "SyncCohortProxy" =>
        Some(new SyncCohortProxy(txn))
      case "ForwardCohortProxy" =>
        Some(new ForwardCohortProxy(txn))
      case "ConcurrentCohortProxy" =>
        Some(new ConcurrentCohortProxy(txn))
      case _ =>
        None
    }
  }
}


abstract class AbstractCohortProxy(implicit val shardGetter: String => ActorSelection) extends CohortProxy {
  
  implicit val timeout : Timeout = 2 seconds
  protected val txn: Transaction
  
  protected val metrics: ThreePhaseMetricsTesting = new ThreePhaseMetricsImpl
  
  def submit() = {
    // TODO: Check shard is available here
    val txns = txn.asInstanceOf[TransactionProxy]
    val commitResultPromise = Promise[SubmitResult]()
    
    if (txns.subTransactions.size == 0)
      commitResultPromise success null
    else {
      metrics.testPoint(CommitPhase.CAN_COMMIT)
      
      doCanCommit(txns, commitResultPromise)
    }
    commitResultPromise.future
  }
  
  protected def doCanCommit(txns: TransactionProxy, commitResultPromise: Promise[SubmitResult])
  
  protected def doPreCommit(txns: TransactionProxy, commitResultPromise: Promise[SubmitResult]) = {
    import CommitMessages._
    import concurrent.ExecutionContext.Implicits.global
    
    metrics.testPoint(CommitPhase.PRE_COMMIT)
    val replies = txns.subTransactions map {
      //d => d._1 ? CommitMessage(d._2)
      // If path cannot be translate to ActorSelection, It shall failed here and enter abort process
      d => ask(d._1, CommitMessage(d._2))
    }
    
    Future.sequence(replies).transform(
        _ => {metrics.testPoint(CommitPhase.COMMITED); commitResultPromise success metrics.asInstanceOf[ThreePhaseMetrics]}, 
        f => { commitResultPromise failure Exceptions.CommitTimeoutException; f }
        )
  }  
  
  protected def invokeCanCommit(shard: ActorSelection, txn: Transaction) = {
    import CommitMessages._
    import concurrent.ExecutionContext.Implicits.global
    
    println(s"asking shard $shard can-commit")
    
    (shard ? CanCommitMessage(txn)).transform({
      case _: CanCommitNack => 
        throw Exceptions.CanCommitFailedException
      case s @ _ =>
        s
    }, f => f);
    // TODO: Abort shall be taken care here
  }
}


class SyncCohortProxy(override val txn: Transaction)(implicit shardGetter: String => ActorSelection) 
  extends AbstractCohortProxy {
  
  protected def doCanCommit(txns: TransactionProxy, commitResultPromise: Promise[SubmitResult]) {
    import concurrent.ExecutionContext.Implicits.global
    
    val sortedTxns = TreeMap(txns.subTransactions.toArray: _*).iterator
    
    var queue = Future[Any]{}
    for ( d <- sortedTxns ) queue = queue flatMap { _ => invokeCanCommit(d._1, d._2) }
    queue map { _ => doPreCommit(txns, commitResultPromise) } recover { 
      case exp @ _ => commitResultPromise failure exp 
    }
  }
}


class ForwardCohortProxy(override val txn: Transaction)(implicit shardGetter: String => ActorSelection) 
  extends AbstractCohortProxy {
  //TODO: Implement this
  protected def doCanCommit(txns: TransactionProxy, commitResultPromise: Promise[SubmitResult]) {
    
  }
}


class ConcurrentCohortProxy(override val txn: Transaction)(implicit shardGetter: String => ActorSelection) 
  extends AbstractCohortProxy {
  
  protected def doCanCommit(txns: TransactionProxy, commitResultPromise: Promise[SubmitResult]) = {
    import concurrent.ExecutionContext.Implicits.global
    import CommitMessages._
    
    val replies = txns.subTransactions map {
      s => invokeCanCommit(s._1, s._2)
    }
    
    Future.sequence(replies).transform(
        _ => doPreCommit(txns, commitResultPromise), 
        f => { commitResultPromise.failure(f); f }
        )
  }
}
