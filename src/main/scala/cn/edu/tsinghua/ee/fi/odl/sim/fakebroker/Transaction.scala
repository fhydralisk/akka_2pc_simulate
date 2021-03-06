package cn.edu.tsinghua.ee.fi.odl.sim.fakebroker

import collection.mutable.HashMap
import concurrent.Future
import cn.edu.tsinghua.ee.fi.odl.sim.fakedatatree.Modification


trait Transaction {
  
  val transId : Int
  def submit() : Future[Transaction.SubmitResult]
  def put(path: String, value: String) : Unit //Fake put, ignore the parameter
  def modification : Modification
}

trait DataBroker {
  def newTransaction : Future[Transaction]
}

trait TransactionFactory {
  def submit(txn: Transaction): Future[Transaction.SubmitResult]
}

object Transaction {
  type SubmitResult = ThreePhaseMetrics
  implicit object TransactionOrdering extends Ordering[Transaction] {
    def compare(thisTrans: Transaction, thatTrans: Transaction) = {
      thisTrans.transId - thatTrans.transId
    }
  }
}

case class TransactionProxy(val transId: Int, @transient delegate: TransactionFactory) extends Transaction with Serializable {
  val subTransactions = new HashMap[String, Transaction]
  
  def put(dest: String, value: String) {
    subTransactions += dest -> new WriteTransaction(transId)
  }
  
  def submit() = {
    delegate.submit(this)
  }
  
  def modification = null
  
  // Serializable
}

case class WriteTransaction(val transId: Int) extends Transaction {
  
  def submit() = {
    Future.failed[Null](new NoSuchMethodException())
  }
  
  def put(dest: String, value: String) {
    
  }
  
  def modification = null
}

