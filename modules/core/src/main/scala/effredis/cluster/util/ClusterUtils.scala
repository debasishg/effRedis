/*
 * Copyright 2020 Debasish Ghosh
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package effredis.cluster
package util

import scala.collection.immutable.BitSet
import scala.concurrent.duration._

import cats.effect._
import cats.implicits._
import cats.data.{ EitherNec, NonEmptyList }

import io.chrisdavenport.cormorant._
import io.chrisdavenport.cormorant.generic.semiauto._
import io.chrisdavenport.cormorant.parser._
import io.chrisdavenport.cormorant.implicits._

object ClusterUtils {
  case class TopologyString(
      nodeId: String,
      uri: String,
      nodeFlags: String,
      replicaUpstreamNodeId: String,
      pingTimestamp: Long,
      pongTimestamp: Long,
      configEpoch: Long,
      linkState: String,
      slots: Option[String]
  )

  implicit val lr: LabelledRead[TopologyString]  = deriveLabelledRead
  implicit val lw: LabelledWrite[TopologyString] = deriveLabelledWrite

  def fromRedisServer(
      csv: String
  ): EitherNec[String, NonEmptyList[TopologyString]] =
    parseComplete(csv, parser = parsers.SSVParser)
      .leftWiden[Error]
      .flatMap(_.readLabelled[TopologyString].sequence)
      .toValidatedNec
      .toEither
      .leftMap(_.map(_.toString))
      .map(l => NonEmptyList.fromList(l).get)

  def parseNodeFlags(nodeFlagString: String): Set[NodeFlag] =
    nodeFlagString.split(",").map(NodeFlag.withName).toSet

  def parseSlotString(slotString: String): BitSet = {
    val parts = slotString.split("-")
    if (parts.size == 1) BitSet(slotString.toInt)
    else BitSet((parts(0).toInt to parts(1).toInt).toList: _*)
  }

  def repeatAtFixedRate[F[+_]: Concurrent](period: Duration, task: F[Unit])(implicit timer: Timer[F]): F[Unit] =
    timer.clock.monotonic(MILLISECONDS).flatMap { start =>
      task *> timer.clock.monotonic(MILLISECONDS).flatMap { finish =>
        val nextDelay = period.toMillis - (finish - start)
        timer.sleep(nextDelay.millis) *> repeatAtFixedRate(period, task)
      }
    }
}

/*
07c37dfeb235213a872192d90877d0cd55635b91 127.0.0.1:30004@31004 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1426238317239 4 connected
67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002@31002 master - 0 1426238316232 2 connected 5461-10922
292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003@31003 master - 0 1426238318243 3 connected 10923-16383
6ec23923021cf3ffec47632106199cb7f496ce01 127.0.0.1:30005@31005 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected
824fe116063bc5fcf9f4ffd895bc17aee7731ac3 127.0.0.1:30006@31006 slave 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238317741 6 connected
e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:30001@31001 myself,master - 0 0 1 connected 0-5460
 */
