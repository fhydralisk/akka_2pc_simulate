package cn.edu.tsinghua.ee.fi.odl.sim.fakebroker

import collection.mutable.HashMap
import akka.actor.ActorRef
import concurrent.Future
import cn.edu.tsinghua.ee.fi.odl.sim.fakedatatree.Modification

trait Transaction {
  val transId : Int
  def submit() : Future[Null]
  def put(dest: ActorRef) : Unit //Fake put, ignore the parameter
  def modification : Modification
}

trait DataBroker {
  def newTransaction : Future[Transaction]
}

trait TransactionFactory {
  def submit(txn: Transaction): Future[Null]
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
  
  def submit() = {
    delegate.submit(this)
  }
  
  def modification = null
}

class WriteTransaction(val transId: Int) extends Transaction {
  
  def submit() = {
    Future.failed[Null](new NoSuchMethodException())
  }
  
  def put(dest: ActorRef) {
    
  }
  
  def modification = null
}

