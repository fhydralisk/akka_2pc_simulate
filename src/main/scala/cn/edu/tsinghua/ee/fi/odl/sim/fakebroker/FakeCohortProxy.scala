package cn.edu.tsinghua.ee.fi.odl.sim.fakebroker


import concurrent.{Future, Promise}
import concurrent.duration._
import com.typesafe.config.Config
import akka.pattern.ask
import akka.util.Timeout
import akka.event.Logging
import akka.actor.{ActorSystem, ActorSelection}
import collection.immutable.TreeMap
import Transaction.SubmitResult
import CommitMessages._


trait CohortProxy {
  def submit(): Future[SubmitResult]
}


class CohortProxyFactory(config: Config)(implicit shardGetter: String => ActorSelection, akkaSystem: ActorSystem) {
  def getCohortProxy(txn: Transaction) : Option[CohortProxy] = {
    config.getString("cohort-proxy-type") match {
      case "SyncCohortProxy" =>
        Some(new SyncCohortProxy(txn))
      case "ForwardCohortProxy" =>
        Some(new ForwardCohortProxy(txn, config.getString("ForwardCohortProxy.proxy-postfix")))
      case "ConcurrentCohortProxy" =>
        Some(new ConcurrentCohortProxy(txn))
      case _ =>
        None
    }
  }
}


abstract class AbstractCohortProxy(implicit val shardGetter: String => ActorSelection, akkaSystem: ActorSystem) extends CohortProxy {
  
  // TODO: Make this timeout into settings
  implicit val timeout : Timeout = 5 seconds
  protected val txn: Transaction
  
  protected val metrics: ThreePhaseMetricsTesting = new ThreePhaseMetricsImpl
  
  val log = Logging(akkaSystem, this.getClass)
  
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
  
  protected def doPreCommit(txns: TransactionProxy, commitResultPromise: Promise[SubmitResult]) {
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
  
  import concurrent.ExecutionContext.Implicits.global
  
  protected def invokeCanCommit(shard: ActorSelection, txn: Transaction) = {
    
    log.debug(s"asking shard $shard can-commit")
    
    (shard ? CanCommitMessage(txn)).transform({
      case _: CanCommitNack => 
        throw Exceptions.CanCommitFailedException
      case s @ _ =>
        s
    }, f => f)
  }
  
  protected def invokeAbort(shard: ActorSelection, txn: Transaction) = {
    log.debug(s"aborting shard $shard")
    
    shard ! AbortMessage(txn)
    
  }
}


class SyncCohortProxy(override val txn: Transaction)(implicit shardGetter: String => ActorSelection, akkaSystem: ActorSystem) 
  extends AbstractCohortProxy {
  
  protected def doCanCommit(txns: TransactionProxy, commitResultPromise: Promise[SubmitResult]) {
    import concurrent.ExecutionContext.Implicits.global
    
    val sortedTxns = TreeMap(txns.subTransactions.toArray: _*).iterator
    
    var queue = Future[Any]{}
    for ( d <- sortedTxns ) queue = queue flatMap { _ => invokeCanCommit(d._1, d._2) }
    queue map { _ => doPreCommit(txns, commitResultPromise) } recover { 
      case exp @ _ => 
        commitResultPromise failure exp
        sortedTxns foreach { t => invokeAbort(t._1, t._2) }
    }
  }
}


class ConcurrentCohortProxy(override val txn: Transaction)(implicit shardGetter: String => ActorSelection, akkaSystem: ActorSystem) 
  extends AbstractCohortProxy {
  
  protected def doCanCommit(txns: TransactionProxy, commitResultPromise: Promise[SubmitResult]) = {
    import concurrent.ExecutionContext.Implicits.global
    
    val replies = txns.subTransactions map {
      s => invokeCanCommit(s._1, s._2)
    }
    
    Future.sequence(replies).transform(
        _ => doPreCommit(txns, commitResultPromise), 
        f => { commitResultPromise.failure(f); f }
        )
  }
}


class ForwardCohortProxy(override val txn: Transaction, proxyPostfix: String)(implicit shardGetter: String => ActorSelection, akkaSystem: ActorSystem) 
  extends AbstractCohortProxy {
    
  import akka.cluster.Cluster
  
  val cluster = Cluster(akkaSystem)
  
  def addressesOfProxy = 
    cluster.state.members.filter { m => m.hasRole("proxy") && m.status == akka.cluster.MemberStatus.Up } map { _.address }
  
    
  //TODO: Move this to node getter
  def actorOfProxy = addressesOfProxy.headOption map { addr => akkaSystem.actorSelection(s"$addr/user/$proxyPostfix") }
  
  protected def doCanCommit(txns: TransactionProxy, commitResultPromise: Promise[SubmitResult]) {
    import concurrent.ExecutionContext.Implicits.global
    actorOfProxy map { p =>
      (p ? ForwardCanCommit(txns)) map { 
        case CanCommitAck(_) => doPreCommit(txns, commitResultPromise) 
        case CanCommitNack(_) => throw Exceptions.CanCommitFailedException 
        } recover { case e @ _ => commitResultPromise failure e }
    } getOrElse {
      commitResultPromise failure Exceptions.NoProxyException
    }
  }
}

import akka.actor.ActorRef

class ForwardCohortProxyBackend(override val txn: Transaction)(implicit shardGetter: String => ActorSelection, akkaSystem: ActorSystem) 
  extends ConcurrentCohortProxy(txn) {
  
  override def doPreCommit(txns: TransactionProxy, commitResultPromise: Promise[SubmitResult]) {
    commitResultPromise success null
  }
}
