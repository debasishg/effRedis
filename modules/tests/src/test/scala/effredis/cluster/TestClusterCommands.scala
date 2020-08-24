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

trait TestClusterCommands {
  implicit def cs: ContextShift[IO]

  def clusterCommands(cmd: RedisClusterClient[IO]): IO[Unit] = {
    import cmd._
    for {
      x <- set("key-1", "foo")
      _ <- IO(assert(getBoolean(x)))
      x <- get("key-1")
      _ <- IO(assert(getResp(x).get == "foo"))
    } yield ()
  }
}
