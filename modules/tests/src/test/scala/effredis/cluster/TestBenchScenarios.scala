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
import cats.syntax.all._
import effredis.Log.NoOp._
import effredis.RedisClientPool
import effredis.RedisClient._

import effredis.EffRedisFunSuite._
import cats.effect.Temporal

trait TestBenchScenarios {
  implicit def cs: ContextShift[IO]
  implicit def tr: Temporal[IO]

  def clusterCommandsB(cmd: RedisClusterClient[IO, SINGLE.type]): IO[Unit] = {
    import cmd._
    RedisClientPool.poolResource[IO, SINGLE.type](SINGLE).use { pool =>
      implicit val p = pool
      for {
        x <- set("ley-2", "bar")
        _ <- IO(assert(getBoolean(x)))
        x <- get("ley-2")
        _ <- IO(assert(getResp(x).get == "bar"))
        x <- lpush("mley-1", "lval-1", "lval-2", "lval-3")
        _ <- IO(assert(getResp(x).get == 3))
        x <- llen("mley-1")
        _ <- IO(assert(getResp(x).get == 3))
      } yield ()
    }
  }

  def clusterBench(cmd: RedisClusterClient[IO, SINGLE.type]): IO[Unit] = {
    val nKeys = 1000
    RedisClientPool.poolResource[IO, SINGLE.type](SINGLE).use { pool =>
      implicit val p = pool
      for {
        x <- (0 to nKeys).map(i => cmd.set(s"ley-$i", s"debasish ghosh $i")).toList.sequence
        _ <- IO(println(x.size))
        _ <- IO(assert(x.map(getBoolean(_)).forall(_ == true)))
      } yield ()
    }
  }
}
