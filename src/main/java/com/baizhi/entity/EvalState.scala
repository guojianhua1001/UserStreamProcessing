package com.baizhi.entity
class EvalState extends Serializable {

  var historyData: HistoryData = _
  var dayTime: String = _

}

object EvalState{
  def apply(): EvalState = new EvalState()
}
