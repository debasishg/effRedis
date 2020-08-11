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

import cats.effect._
import EffRedisFunSuite._

trait TestBaseScenarios {
  implicit def cs: ContextShift[IO]

  def baseMisc(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      _ <- set("anshin-1", "debasish")
      _ <- set("anshin-2", "maulindu")
      x <- keys("anshin*")
      _ <- IO(assert(getResp(x).get == List(Some("anshin-2"), Some("anshin-1"))))

      // fetch keys with spaces
      _ <- set("key 1", "debasish")
      _ <- set("key 2", "maulindu")
      x <- keys("key*")
      _ <- IO(assert(getResp(x).get == List(Some("key 2"), Some("key 1"))))

      // randomkey
      _ <- set("anshin-1", "debasish")
      _ <- set("anshin-2", "maulindu")
      x <- randomkey
      _ <- IO(assert(getResp(x).isDefined))

      // rename
      _ <- set("anshin-1", "debasish")
      _ <- set("anshin-2", "maulindu")
      x <- rename("anshin-2", "anshin-2-new")
      _ <- IO(assert(getBoolean(x)))
      x <- rename("anshin-2", "anshin-2-new")
      _ <- IO(assert(getResp(x).get.toString.contains("ERR no such key")))

      // renamenx
      _ <- set("newkey-1", "debasish")
      _ <- set("newkey-2", "maulindu")
      x <- renamenx("newkey-2", "newkey-2-new")
      _ <- IO(assert(getBoolean(x)))
      x <- renamenx("newkey-1", "newkey-2-new")
      _ <- IO(assert(!getBoolean(x)))

      // time
      x <- time
      _ <- IO(assert {
            getResp(x) match {
              case Some(s: List[_]) => s.size == 2
              case _                => false
            }
          })
      x <- time
      _ <- IO(assert {
            getResp(x) match {
              case Some(Some(timestamp) :: Some(elapsedtime) :: Nil) =>
                (timestamp.toString.toLong * 1000000L) > elapsedtime.toString.toLong
              case _ => false
            }
          })
    } yield ()
  }
}
