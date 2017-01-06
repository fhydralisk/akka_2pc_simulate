package cn.edu.tsinghua.ee.fi.odl.sim.fakedatatree

trait Modification
trait Snapshot

trait DataTree {
  // validate modification tree
  def validate(modification: Modification) : Boolean
  
  // take snapshot
  def takeSnapshot() : Snapshot
  
  // apply modification tree
  def applyModification(modification: Modification)
}

class FakeDataTree extends DataTree {
  
  def validate(modification: Modification) = {
    true
  }
  
  def takeSnapshot() = {
    null
  }
  
  def applyModification(modification: Modification) {
    
  }
}
