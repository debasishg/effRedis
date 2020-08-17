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

import cats.effect._
import io.chrisdavenport.cormorant._
import io.chrisdavenport.cormorant.implicits._

import effredis.RedisClient

import effredis.cluster.util.ClusterUtils._

trait TestClusterNodesScenarios {
  implicit def cs: ContextShift[IO]

  def parseClusterNodes(client: RedisClient[IO]): IO[Unit] = {
    val ts1 = TopologyString(
      "07c37dfeb235213a872192d90877d0cd55635b91",
      "127.0.0.1:30004@31004",
      "slave",
      "e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca",
      0L,
      1426238317239L,
      4L,
      "connected",
      None
    )
    val ts2 = TopologyString(
      "67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1",
      "127.0.0.1:30002@31002",
      "master",
      "-",
      0L,
      1426238316232L,
      2L,
      "connected",
      Some("5461-10922")
    )
    val ts  = List(ts1, ts2)
    val csv = ts.writeComplete.print(Printer.default)

    fromRedisServer(csv) match {
      case Right(v) => v.toList.foreach(println)
      case Left(e)  => println(e)
    }
    for {
      _ <- client.flushdb
    } yield ()
  }
}
