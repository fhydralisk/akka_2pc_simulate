package cn.edu.tsinghua.ee.fi.odl.sim.fakebroker

import collection.mutable.HashMap
import akka.actor.ActorRef
import concurrent.Future


trait Transaction {
  val transId : Int
  def submit() : Future[Nothing]
  def put(dest: ActorRef) : Unit //Fake put, ignore the parameter
}

trait DataBroker {
  def newTransaction : Future[Transaction]
}

trait TransactionFactory {
  def summit(txn: Transaction): Future[Nothing]
}

object Transaction {
  implicit object TransactionOrdering extends Ordering[Transaction] {
    def compare(thisTrans: Transaction, thatTrans: Transaction) = {
      thisTrans.transId - thatTrans.transId
    }
  }
}

class TransactionProxy(val transId: Int, delegate: TransactionFactory) extends Transaction {
  val subTransactions = new HashMap[ActorRef, Transaction]
  
  def put(dest: ActorRef) {
    subTransactions += dest -> new WriteTransaction(transId)
  }
  
  def summit() = {
    delegate.summit(this)
  }
}

class WriteTransaction(val transId: Int) extends Transaction {
  
}