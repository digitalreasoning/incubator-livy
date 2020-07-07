/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.livy.utils

import org.apache.livy.{Logging, Utils}

/**
 * Provide a class to control a Spark application using spark-submit.
 *
 * @param process The spark-submit process launched the Spark application.
 */
class SparkProcApp(
                    process: LineBufferedProcess,
                    listener: Option[SparkAppListener])
  extends SparkApp with Logging {

  private var state = SparkApp.State.STARTING

  override def kill(): Unit = {
    if (process.isAlive) {
      process.destroy()
      waitThread.join()
    }
  }

  override def log(): IndexedSeq[String] =
    ("stdout: " +: process.inputLines) ++ ("\nstderr: " +: process.errorLines)

  private def changeState(newState: SparkApp.State.Value) = {
    if (state != newState) {
      listener.foreach(_.stateChanged(state, newState))
      state = newState
    }
  }

  private val waitThread = Utils.startDaemonThread(s"SparProcApp_$this") {
    changeState(SparkApp.State.RUNNING)
    process.waitFor() match {
      case 0 =>
        val exitCodeOption = procExitCode()
        if (exitCodeOption.isDefined) {
          val exitCode = exitCodeOption.get
          if (exitCode != 0) {
            changeState(SparkApp.State.FAILED)
            info(s"Parsed $exitCode from the output.")
            error(s"spark-submit exited with code $exitCode")
          } else {
            changeState(SparkApp.State.FINISHED)
          }
        } else {
          changeState(SparkApp.State.FINISHED)
        }
      case exitCode =>
        changeState(SparkApp.State.FAILED)
        error(s"spark-submit exited with code $exitCode")
    }
  }

  private def procExitCode(): Option[Int] = {
    val EXIT_CODE_LOG_PREFIX = "Exit code:"
    val processStdOutIt = process.inputLines
    val exitCodeLog = processStdOutIt.find(it => it.contains(EXIT_CODE_LOG_PREFIX))
    if (exitCodeLog.isDefined) {
      val exitCodeLogSplit = exitCodeLog.get.split(EXIT_CODE_LOG_PREFIX)
      if (exitCodeLogSplit.size > 1) {
        val errorCode = exitCodeLogSplit(1).trim
        return toNumber(errorCode)
      }
    }
    Option.empty
  }

  private def toNumber(string: String): Option[Int] = {
    try {
      Option.apply(string.toInt)
    } catch {
      case _: Exception => Option.empty
    }
  }
}
