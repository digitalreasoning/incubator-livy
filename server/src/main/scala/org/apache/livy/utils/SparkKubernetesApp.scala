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

import org.apache.livy.LivyConf

/**
 * Provide a class to control a Spark application using spark-submit to Kubernetes cluster.
 *
 * @param process  The spark-submit process launched the Spark application.
 * @param listener A listener to Spark application
 * @param livyConf Livy configuration used to launch Spark application
 */
class SparkKubernetesApp(
    process: LineBufferedProcess,
    listener: Option[SparkAppListener],
    livyConf: LivyConf)
  extends SparkProcApp(process, listener) {

  override def kill(): Unit = {
    if (process.isAlive) {

      // Access k8s driver pod name and namespace from logs before destroying the process
      val driverPodName = findDriverPodName().getOrElse("")
      val namespace = findNameSpace().getOrElse("")

      process.destroy()
      waitThread.join()

      // Kill the k8s driver pod
      if (namespace.nonEmpty && driverPodName.nonEmpty) {
        val processBuilder = new SparkProcessBuilder(livyConf)
        processBuilder.master(livyConf.sparkMaster())
        processBuilder.conf("spark.kubernetes.appKillPodDeletionGracePeriod", "0")
        processBuilder.start(Option.empty,
          Traversable("--kill", s"$namespace:$driverPodName"),
          killOrStatusRequest = true)
      }
    }
  }

  /**
   * Scans [[process.inputLines]] for an entry that contains a substring represented
   * by "itemLogPrefix" and returns the item of interest by removing the "itemLogPrefix"
   * substring from the entry.
   *
   * @param itemLogPrefix the prefix of log item to find
   */
  private def findItemFromLineBuffer(itemLogPrefix: String): Option[String] = {
    val processStdOutIt = process.inputLines
    val itemLog = processStdOutIt.find(it => it.contains(itemLogPrefix))
    if (itemLog.isDefined) {
      val itemLogSplit = itemLog.get.split(itemLogPrefix)
      if (itemLogSplit.size > 1) {
        val item = itemLogSplit(1).trim
        return Option.apply(item)
      }
    }
    Option.empty
  }

  override def changeState(newState: SparkApp.State.Value): Unit = {
    var updatedNewState = newState
    if(updatedNewState == SparkApp.State.FINISHED){
      updatedNewState = kubernetesAppState()
    }
    if (state != updatedNewState) {
      listener.foreach(_.stateChanged(state, updatedNewState))
      state = updatedNewState
    }
  }

  private def findDriverPodName(): Option[String] = {
    findItemFromLineBuffer("pod name:")
  }

  private def findNameSpace(): Option[String] = {
    findItemFromLineBuffer("namespace:")
  }

  private def kubernetesAppState(): SparkApp.State.Value = {

    def findTerminationReason(): Option[String] = {
      val terminationReason = findItemFromLineBuffer("termination reason:")
      if (terminationReason.isDefined) {
        return terminationReason
      }
      Option.empty
    }

    def findProcExitCode(): Option[Int] = {
      val exitCode = findItemFromLineBuffer("exit code:")
      if (exitCode.isDefined) {
        return toNumber(exitCode.get)
      }
      Option.empty
    }

    def toNumber(string: String): Option[Int] = {
      try {
        Option.apply(string.toInt)
      } catch {
        case _: Exception => Option.empty
      }
    }

    val exitCodeOption = findProcExitCode()
    if (exitCodeOption.isDefined) {
      val exitCode = exitCodeOption.get
      if (exitCode != 0) {
        info(s"Parsed exit code: $exitCode from the output.")
        error(s"spark-submit exited with code $exitCode")
        SparkApp.State.FAILED
      } else {
        val terminationReason = findTerminationReason()
        if (terminationReason.isDefined && terminationReason.get != "Completed") {
          info(s"Parsed termination reason: ${terminationReason.get} from the output.")
          error(s"spark-submit exited with code $exitCode but termination reason wasn't completed.")
          SparkApp.State.FAILED
        } else {
          SparkApp.State.FINISHED
        }
      }
    } else {
      SparkApp.State.FINISHED
    }
  }
}
