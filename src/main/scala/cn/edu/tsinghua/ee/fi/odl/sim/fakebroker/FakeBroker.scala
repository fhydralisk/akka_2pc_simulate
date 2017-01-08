package cn.edu.tsinghua.ee.fi.odl.sim.fakebroker

import akka.actor.ActorRef
import com.typesafe.config.Config

import concurrent.Future


class FakeBroker(txnIdGetter: => Future[Int], cohortProxyFactory: CohortProxyFactory, settings: Config) 
  extends DataBroker with TransactionFactory {
  
  override def newTransaction() = {
    import concurrent.ExecutionContext.Implicits.global 
    val newTransactionId = txnIdGetter
    newTransactionId.transform[Transaction](new TransactionProxy(_, this), f => f)
  }
  
  def submit(txn: Transaction) = {
    val cohortProxy = cohortProxyFactory.getCohortProxy(txn)
    cohortProxy map { _.submit() } getOrElse Future.failed(new NullPointerException)
  }
}
