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

trait TestSetScenarios {
  implicit def cs: ContextShift[IO]

  def setsAdd(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      x <- sadd("set-1", "foo")
      _ <- IO(assert(getResp(x).get == 1))
      x <- sadd("set-1", "bar")
      _ <- IO(assert(getResp(x).get == 1))
      x <- sadd("set-1", "foo")
      _ <- IO(assert(getResp(x).get == 0))

      x <- lpush("list-1", "foo")
      _ <- IO(assert(getResp(x).get == 1))
      x <- sadd("list-1", "foo")
      _ <- IO(assert(getResp(x).get.toString.contains("Operation against a key holding the wrong kind of value")))
    } yield ()
  }

  def setsAddVariadic(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      x <- sadd("set-1", "foo", "bar", "baz")
      _ <- IO(assert(getResp(x).get == 3))
      x <- sadd("set-1", "foo", "bar", "faz")
      _ <- IO(assert(getResp(x).get == 1))
      x <- sadd("set-1", "bar")
      _ <- IO(assert(getResp(x).get == 0))
    } yield ()
  }

  def setsRem(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      _ <- sadd("set-1", "foo")
      _ <- sadd("set-1", "bar")
      x <- srem("set-1", "bar")
      _ <- IO(assert(getResp(x).get == 1))
      x <- srem("set-1", "foo")
      _ <- IO(assert(getResp(x).get == 1))
      x <- srem("set-1", "baz")
      _ <- IO(assert(getResp(x).get == 0))

      x <- lpush("list-1", "foo")
      _ <- IO(assert(getResp(x).get == 1))
      x <- srem("list-1", "foo")
      _ <- IO(assert(getResp(x).get.toString.contains("Operation against a key holding the wrong kind of value")))
    } yield ()
  }

  def setsRemVariadic(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      x <- sadd("set-1", "foo", "bar", "baz", "faz")
      _ <- IO(assert(getResp(x).get == 4))
      x <- srem("set-1", "foo", "bar")
      _ <- IO(assert(getResp(x).get == 2))
      x <- srem("set-1", "foo")
      _ <- IO(assert(getResp(x).get == 0))
      x <- srem("set-1", "baz", "bar")
      _ <- IO(assert(getResp(x).get == 1))
    } yield ()
  }

  def setsPop(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      _ <- sadd("set-1", "foo")
      _ <- sadd("set-1", "bar")
      _ <- sadd("set-1", "baz")
      x <- spop("set-1")
      _ <- IO {
            val elem = getResp(x).get
            assert(elem == "foo" || elem == "bar" || elem == "baz")
          }
    } yield ()
  }

  def setsPopWithCount(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      _ <- sadd("set-1", "one")
      _ <- sadd("set-1", "two")
      _ <- sadd("set-1", "three")
      _ <- sadd("set-1", "four")
      _ <- sadd("set-1", "five")
      _ <- sadd("set-1", "six")
      _ <- sadd("set-1", "seven")
      _ <- sadd("set-1", "eight")

      x <- spop("set-1", 2)
      _ <- IO {
            getResp(x).get match {
              case s: Set[_] => assert(s.size == 2)
              case _         => false
            }
          }

      // if supplied count > size, then whole set is returned
      x <- spop("set-1", 24)
      _ <- IO {
            getResp(x).get match {
              case s: Set[_] => assert(s.size == 6)
              case _         => false
            }
          }

      // if empty, returned set is empty
      x <- spop("set-1", 5)
      _ <- IO {
            getResp(x).get match {
              case s: Set[_] => assert(s.size == 0)
              case _         => false
            }
          }
    } yield ()
  }
}
