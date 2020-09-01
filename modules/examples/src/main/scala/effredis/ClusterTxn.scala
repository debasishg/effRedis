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

package effredis
package cluster

import util.ClusterUtils
import java.net.URI
import scala.concurrent.duration._

import cats.data.NonEmptyList
import cats.effect._
import log4cats._
import RedisClient._

object ClusterTxn extends LoggerIOApp {

  val nKeys = 1000
  def program: IO[Unit] =
    RedisClusterClient.make[IO, TRANSACT.type](NonEmptyList.one(new URI("http://localhost:7000"))).flatMap { cl =>
      for {
        // optionally the cluster topology can be refreshed to reflect the latest partitions
        // this step schedules that job at a pre-configured interval
        _ <- ClusterUtils.repeatAtFixedRate(4.seconds, cl.topologyCache.expire).start
        _ <- RedisClientPool.poolResource[IO, TRANSACT.type](TRANSACT).use { pool =>
              implicit val p = pool
              for {
                _ <- cl.set("k1", "v1")
                _ <- cl.set("k2", 100)
                _ <- cl.incrby("k2", 12)
                _ <- cl.get("k1")
                _ <- cl.get("k2")
                _ <- cl.lpop("k1")
              } yield ()
            }
      } yield ()
    }

  override def run(args: List[String]): IO[ExitCode] = {
    program.unsafeRunSync()
    IO(ExitCode.Success)
  }
}
