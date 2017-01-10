package cn.edu.tsinghua.ee.fi.odl.sim.fakebroker

import akka.actor.{ActorSystem, ActorRef}
import akka.event.Logging
import com.typesafe.config.Config

import concurrent.Future


class FakeBroker(txnIdGetter: => Future[Int], cohortProxyFactory: CohortProxyFactory, settings: Config)(implicit akkaSystem: ActorSystem)
  extends DataBroker with TransactionFactory {
  
  val log = Logging(akkaSystem, this.getClass)
  
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
