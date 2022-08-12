package com.startdataengineering

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import com.startdataengineering.model.ServerLog

/**
 * TODO: extend the KeyedProcessFunction --> review
 * 
    The open method is executed first.
    Then for each input the processElement method gets executed.
    The onTimer method is a call back method that gets executed when a timer runs out, we will see how and where we set the timer in a following section.


 */
class FraudDetection extends KeyedProcessFunction[String, String, String]{

  private var loginState: ValueState[java.lang.Boolean] = _
  private var prevLoginCountry: ValueState[java.lang.String] = _
  private var timerState: ValueState[java.lang.Long] = _

  /**
   * Initialization method for the function
   */
  @throws[Exception]
  override def open(parameters: Configuration): Unit = {
    // boolean flag used to denote if the account(denoted by accountId) is already logged in.
    val loginDescriptor = new ValueStateDescriptor("login-flag", Types.BOOLEAN)
    loginState = getRuntimeContext.getState(loginDescriptor)

    // used to keep track of the originating country of the most recent login for an account.
    val prevCountryDescriptor = new ValueStateDescriptor("prev-country", Types.STRING)
    prevLoginCountry = getRuntimeContext.getState(prevCountryDescriptor)

    // used to keep track of the timer
    val timerStateDescriptor = new ValueStateDescriptor("timer-state", Types.LONG)
    timerState = getRuntimeContext.getState(timerStateDescriptor)

  }

  
  @throws[Exception]
  override def processElement(
                               value: String,
                               ctx: KeyedProcessFunction[String, String, String]#Context,
                               out: Collector[String]): Unit = {
    //  This method defines how we process each individual element in the input data stream

    /*
    Uses the input string to get a server log event.
    Checks if the user is logged in, using the local state loginState.
    If the current event is a login type and the current country is different from the previous login country.
     Create an output event that contains details of the event id and account id which triggered the alert, the previous login country and the current login country. Goto step 5.
    Else If the event is a login type, then set the login state as true and previous login country to the country the login event is originating from. These states will be used for the next time this account’s event is checked. Also set a timer for 5 min from the time this login event was created, asynchronously after the 5 min the onTimer method will be triggered for that accountId. The processing of this event is done. Goto step 5.
    If the event is a logout type clear all the three states and stop the timer. The processing of this event is done.
    */
    val logEvent = ServerLog.fromString(value)
    val loggedIn = loginState.value
    val prevCountry = prevLoginCountry.value 
    if ((loggedIn != null) && (prevCountry != null)){
      if ((loggedIn == true) && (logEvent.eventType == "login")) {
        // Issue alert if user is logged in and country is incorrect
        if (prevCountry != logEvent.locationCountry) {
          val alert: String = f"Alert eventID: ${logEvent.eventId}%s, " +
            f"violatingAccountId: ${logEvent.accountId}%d, prevCountry: ${prevCountry}%s, " +
            f"currentCountry: ${logEvent.locationCountry}%s"
          out.collect(alert)
        }
      }
    } // User not logged in, make new login
    else if (logEvent.eventType == "login"){
      loginState.update(true)
      prevLoginCountry.update(logEvent.locationCountry)

      // 5 * 60 * 1000L -> 5 min, time is expected in Long format
      val timer = logEvent.eventTimeStamp + (5 * 60 * 1000L)
      ctx.timerService.registerProcessingTimeTimer(timer)
      timerState.update(timer)
    }
    // Clear states and stop timer
    if (logEvent.eventType == "log-out") {
      loginState.clear()
      prevLoginCountry.clear()

      val timer = timerState.value()
      if (timer != null){
        ctx.timerService.deleteProcessingTimeTimer(timer)
      }
      timerState.clear()
    }

  }

  @throws[Exception]
  override def onTimer(timestamp: Long,
                       ctx: KeyedProcessFunction[String, String, String]#OnTimerContext,
                       out: Collector[String]): Unit = {
    timerState.clear()
    loginState.clear()
    prevLoginCountry.clear()
  }
}