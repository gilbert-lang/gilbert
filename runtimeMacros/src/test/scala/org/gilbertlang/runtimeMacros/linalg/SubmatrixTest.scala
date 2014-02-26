package org.gilbertlang.runtimeMacros.linalg

import org.scalatest.Assertions
import org.junit.Test
import breeze.linalg.*

class SubmatrixTest extends Assertions {
  
  @Test def testSubmatrixRowBroadcasting(){
    val partition = Partition(-1, 0,0,10,10,0,0,10,10)
    val expected = Subvector[Double](10,0,0,10)
    
    for(idx <- 0 until expected.length-1){
      expected.update(idx, idx-1)
    }
    
    expected.update(expected.length-1, expected.length-1)
    
    val submatrix = Submatrix[Double](partition)
    for(idx <- 0 until submatrix.rows){
      submatrix.update(idx,idx,idx)
    }
    
    for(idx <- 0 until submatrix.rows-1)
      submatrix.update(idx,idx+1,-1)
      
    val result = breeze.linalg.sum(submatrix(*, ::))
    
    expectResult(expected)(result)
  }
}