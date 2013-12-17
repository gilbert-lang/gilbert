/**
 * gilbert - Distributed Linear Algebra on Sparse Matrices
 * Copyright (C) 2013  Sebastian Schelter, Till Rohrmann
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gilbertlang.language.definition

import Types.Type

abstract class AbstractBuiltinSymbols {

  protected var symbols = collection.mutable.HashMap[String,SymbolEntry]()

  def builtinSymbols: Set[String] = {
    symbols.keys.toSet
  }
  
  def getType(symbol: String) = {
    symbols.get(symbol) match {
      case Some(entry) => Some(entry.symbolType)
      case _ => None
    }
  }
  
  def isSymbol(symbol: String) = {
    symbols.contains(symbol)
  }

  def apply(symbol: String): Option[Type] = {
    getType(symbol)
  }
  
  protected def Symbol(symbol:String, symbolType: Type) = SymbolEntry(symbol,symbolType)
  
  protected case class SymbolEntry(symbol: String, symbolType : Type){
    if (!symbols.contains(symbol)) {
      symbols(symbol) = this
    }
  }
}