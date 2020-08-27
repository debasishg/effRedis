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

import effredis.EffRedisFunSuite._
import effredis.Log.NoOp._

trait TestClusterScenarios {
  implicit def cs: ContextShift[IO]
  implicit def tr: Timer[IO]

  def clusterCommands(cmd: RedisClusterClient[IO]): IO[Unit] = {
    import cmd._
    RedisClientPool.poolResource[IO].use { pool =>
      implicit val p = pool
      for {
        x <- set("key-2", "bar")
        _ <- IO(assert(getBoolean(x)))
        x <- get("key-2")
        _ <- IO(assert(getResp(x).get == "bar"))
        x <- lpush("lkey-1", "lval-1", "lval-2", "lval-3")
        _ <- IO(assert(getResp(x).get == 3))
        x <- llen("lkey-1")
        _ <- IO(assert(getResp(x).get == 3))
      } yield ()
    }
  }

  def clusterListsLPush(cmd: RedisClusterClient[IO]): IO[Unit] = {
    import cmd._
    RedisClientPool.poolResource[IO].use { pool =>
      implicit val p = pool
      for {
        x <- lpush("list-1", "foo")
        _ <- IO(assert(getResp(x).get == 1))
        x <- lpush("list-1", "bar")
        _ <- IO(assert(getResp(x).get == 2))

        x <- set("anshin-1", "debasish")
        _ <- IO(assert(getBoolean(x)))
        x <- lpush("anshin-1", "bar")
        _ <- IO(assert(getResp(x).get.toString.contains("Operation against a key holding the wrong kind of value")))

        // lpush with variadic arguments
        x <- lpush("list-2", "foo", "bar", "baz")
        _ <- IO(assert(getResp(x).get == 3))
        x <- lpush("list-2", "bag", "fog")
        _ <- IO(assert(getResp(x).get == 5))
        x <- lpush("list-2", "bag", "fog")
        _ <- IO(assert(getResp(x).get == 7))

        // lpushx
        x <- lpush("list-3", "foo")
        _ <- IO(assert(getResp(x).get == 1))
        x <- lpushx("list-3", "bar")
        _ <- IO(assert(getResp(x).get == 2))

        x <- set("anshin-2", "debasish")
        _ <- IO(assert(getBoolean(x)))
        x <- lpushx("anshin-2", "bar")
        _ <- IO(assert(getResp(x).get.toString.contains("Operation against a key holding the wrong kind of value")))

      } yield ()
    }
  }

}
