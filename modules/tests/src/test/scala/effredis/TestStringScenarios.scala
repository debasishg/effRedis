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

import scala.concurrent.duration._

import java.util.concurrent.TimeUnit

import cats.effect._
import EffRedisFunSuite._
import algebra.StringApi.NX

trait TestStringScenarios {
  implicit def cs: ContextShift[IO]

  def stringsGetAndSet(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      x <- set("key-1", "value-1")
      _ <- IO(assert(getBoolean(x)))

      x <- set("amit-1", "mor", NX, 6.seconds)
      _ <- IO(assert(getBoolean(x)))

      x <- get("amit-1")
      _ <- IO(assert(getResp(x).get == "mor"))

      x <- del("amit-1")
      _ <- IO(assert(getResp(x).get == 1L))

      x <- del("key-1")
      _ <- IO(assert(getResp(x).get == 1L))
    } yield ()
  }

  def stringsGetAndSetIfExistsOrNot(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      x <- get("amit-1")
      _ <- IO(assert(getResp(x) == None))

      _ <- set("amit-1", "mor", NX, 2.seconds)
      _ <- IO(TimeUnit.SECONDS.sleep(3))

      x <- get("amit-1")
      _ <- IO(assert(getResp(x) == None))
    } yield ()
  }

}
