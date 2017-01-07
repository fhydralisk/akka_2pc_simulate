package cn.edu.tsinghua.ee.fi.odl.sim.fakebroker


import concurrent.{Future, Promise}
import concurrent.duration._
import com.typesafe.config.Config
import akka.pattern.ask
import akka.util.Timeout
import akka.actor.ActorRef
import collection.immutable.TreeMap


trait CohortProxy {
  def submit(): Future[Null]
}

class CohortProxyFactory(config: Config) {
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

abstract class AbstractCohortProxy extends CohortProxy {
  
  implicit val timeout : Timeout = 2 seconds
  val txn: Transaction
  
  def submit() = {
    val txns = txn.asInstanceOf[TransactionProxy]
    val commitResultPromise = Promise[Null]()
    
    if (txns.subTransactions.size == 0)
      commitResultPromise success null
    else
      doCanCommit(txns, commitResultPromise)
    
    commitResultPromise.future
  }
  
  def doCanCommit(txns: TransactionProxy, commitResultPromise: Promise[Null])
  
  def doPreCommit(txns: TransactionProxy, commitResultPromise: Promise[Null]) = {
    import CommitMessages._
    import concurrent.ExecutionContext.Implicits.global
    val replies = txns.subTransactions map {
      d => d._1 ? CommitMessage(d._2)
    }
    
    Future.sequence(replies).transform(
        _ => commitResultPromise success null, 
        f => { commitResultPromise failure Exceptions.CommitTimeoutException; f }
        )
  }  
  
  def invokeCanCommit(shard: ActorRef, txn: Transaction) = {
    import CommitMessages._
    import concurrent.ExecutionContext.Implicits.global
    
    (shard ? CanCommitMessage(txn)).transform({
      case _: CanCommitNack => 
        throw Exceptions.CanCommitFailedException
      case s @ _ =>
        s
    }, f => f);
  }
}

class SyncCohortProxy(override val txn: Transaction) extends AbstractCohortProxy {
  
  def doCanCommit(txns: TransactionProxy, commitResultPromise: Promise[Null]) {
    import concurrent.ExecutionContext.Implicits.global
    
    val sortedTxns = TreeMap(txns.subTransactions.toArray: _*).iterator
    
    var queue = Future[Any]{}
    for ( d <- sortedTxns ) queue = queue flatMap { _ => invokeCanCommit(d._1, d._2) }
    queue map { _ => doPreCommit(txns, commitResultPromise) } recover { 
      case exp @ _ => commitResultPromise failure exp 
    }
  }
}

class ForwardCohortProxy(override val txn: Transaction) extends AbstractCohortProxy {
  //TODO: Implement this
  def doCanCommit(txns: TransactionProxy, commitResultPromise: Promise[Null]) {
    
  }
}

class ConcurrentCohortProxy(override val txn: Transaction) extends AbstractCohortProxy {
  
  def doCanCommit(txns: TransactionProxy, commitResultPromise: Promise[Null]) = {
    import concurrent.ExecutionContext.Implicits.global
    import CommitMessages._
    
    val replies = txns.subTransactions map {
      invokeCanCommit _ tupled
    }
    
    Future.sequence(replies).transform(
        _ => doPreCommit(txns, commitResultPromise), 
        f => { commitResultPromise.failure(f); f }
        )
  }
}