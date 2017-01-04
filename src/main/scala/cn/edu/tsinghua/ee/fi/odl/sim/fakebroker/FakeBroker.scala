package cn.edu.tsinghua.ee.fi.odl.sim.fakebroker

import akka.actor.ActorRef

import concurrent.Future


trait TransactionIdGetter {
  def getNewTransactionId : Future[Int]
}

class FakeBroker(txnIdGetter: TransactionIdGetter, cohortProxyFactory: CohortProxyFactory) 
  extends DataBroker with TransactionFactory {
  
  override def newTransaction() = {
    import concurrent.ExecutionContext.Implicits.global 
    val newTransactionId = txnIdGetter.getNewTransactionId
    newTransactionId.transform[Transaction](s => new TransactionProxy(s, this), f => f)
  }
  
  override def submit(txn: Transaction) = {
    val cohortProxy = cohortProxyFactory.getCohortProxy(txn)
    cohortProxy foreach { _.submit() }
  }
}