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

  final def setsAdd(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
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

  final def setsAddVariadic(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
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

  final def setsRem(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
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

  final def setsRemVariadic(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
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

  final def setsPop(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
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

  final def setsPopWithCount(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
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

  final def setsMove(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    for {
      _ <- sadd("set-1", "foo")
      _ <- sadd("set-1", "bar")
      _ <- sadd("set-1", "baz")

      _ <- sadd("set-2", "1")
      _ <- sadd("set-2", "2")

      x <- smove("set-1", "set-2", "baz")
      _ <- IO(assert(getBoolean(x)))
      x <- sadd("set-2", "baz")
      _ <- IO(assert(getResp(x).get == 0))
      x <- sadd("set-1", "baz")
      _ <- IO(assert(getResp(x).get == 1))

      // should return 0 if the element does not exist in source set
      _ <- flushdb
      _ <- sadd("set-1", "foo")
      _ <- sadd("set-1", "bar")
      _ <- sadd("set-1", "baz")
      x <- smove("set-1", "set-2", "bat")
      _ <- IO(assert(!getBoolean(x)))
      x <- smove("set-3", "set-2", "bat")
      _ <- IO(assert(!getBoolean(x)))

      // should give error if the source or destination key is not a set
      _ <- flushdb
      _ <- lpush("list-1", "foo")
      _ <- lpush("list-1", "bar")
      _ <- lpush("list-1", "baz")
      x <- sadd("set-1", "foo")
      _ <- IO(assert(getResp(x).get == 1))
      x <- smove("list-1", "set-1", "bat")
      _ <- IO(assert(getResp(x).get.toString.contains("Operation against a key holding the wrong kind of value")))
    } yield ()
  }

  final def setsCard(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    for {
      _ <- sadd("set-1", "foo")
      _ <- sadd("set-1", "bar")
      _ <- sadd("set-1", "baz")
      x <- scard("set-1")
      _ <- IO(assert(getResp(x).get == 3))

      // should return 0 if key does not exist
      _ <- flushdb
      x <- scard("set-1")
      _ <- IO(assert(getResp(x).get == 0))
    } yield ()
  }

  final def setsIsMember(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    for {
      _ <- sadd("set-1", "foo")
      _ <- sadd("set-1", "bar")
      _ <- sadd("set-1", "baz")
      x <- sismember("set-1", "foo")
      _ <- IO(assert(getBoolean(x)))

      // should return false for no membership
      _ <- sadd("set-1", "foo")
      _ <- sadd("set-1", "bar")
      _ <- sadd("set-1", "baz")
      x <- sismember("set-1", "fo")
      _ <- IO(assert(!getBoolean(x)))

      // should return false if key does not exist
      x <- sismember("set-1", "fo")
      _ <- IO(assert(!getBoolean(x)))
    } yield ()
  }

  final def setsInter(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    for {
      _ <- sadd("set-1", "foo")
      _ <- sadd("set-1", "bar")
      _ <- sadd("set-1", "baz")

      _ <- sadd("set-2", "foo")
      _ <- sadd("set-2", "bat")
      _ <- sadd("set-2", "baz")

      _ <- sadd("set-3", "for")
      _ <- sadd("set-3", "bat")
      _ <- sadd("set-3", "bay")

      x <- sinter("set-1", "set-2")
      _ <- IO(assert(getResp(x).get == Set("foo", "baz")))
      x <- sinter("set-1", "set-3")
      _ <- IO(assert(getResp(x).get == Set.empty))

      // should return empty set for non-existing key
      _ <- flushdb
      _ <- sadd("set-1", "foo")
      _ <- sadd("set-1", "bar")
      _ <- sadd("set-1", "baz")
      x <- sinter("set-1", "set-4")
      _ <- IO(assert(getResp(x).get == Set.empty))

    } yield ()
  }

  final def setsInterstore(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    for {
      // should store intersection
      _ <- flushdb
      _ <- sadd("set-1", "foo")
      _ <- sadd("set-1", "bar")
      _ <- sadd("set-1", "baz")

      _ <- sadd("set-2", "foo")
      _ <- sadd("set-2", "bat")
      _ <- sadd("set-2", "baz")

      _ <- sadd("set-3", "for")
      _ <- sadd("set-3", "bat")
      _ <- sadd("set-3", "bay")

      x <- sinterstore("set-r", "set-1", "set-2")
      _ <- IO(assert(getResp(x).get == 2))
      x <- scard("set-r")
      _ <- IO(assert(getResp(x).get == 2))
      x <- sinterstore("set-s", "set-1", "set-3")
      _ <- IO(assert(getResp(x).get == 0))
      x <- scard("set-s")
      _ <- IO(assert(getResp(x).get == 0))

      // should return empty set for non-existing key
      _ <- flushdb
      _ <- sadd("set-1", "foo")
      _ <- sadd("set-1", "bar")
      _ <- sadd("set-1", "baz")
      x <- sinterstore("set-r", "set-1", "set-4")
      _ <- IO(assert(getResp(x).get == 0))
      x <- scard("set-r")
      _ <- IO(assert(getResp(x).get == 0))
    } yield ()
  }

  final def setsUnion(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    for {
      _ <- sadd("set-1", "foo")
      _ <- sadd("set-1", "bar")
      _ <- sadd("set-1", "baz")

      _ <- sadd("set-2", "foo")
      _ <- sadd("set-2", "bat")
      _ <- sadd("set-2", "baz")

      _ <- sadd("set-3", "for")
      _ <- sadd("set-3", "bat")
      _ <- sadd("set-3", "bay")

      x <- sunion("set-1", "set-2")
      _ <- IO(assert(getResp(x).get == Set("foo", "bar", "baz", "bat")))
      x <- sunion("set-1", "set-3")
      _ <- IO(
            assert(getResp(x).get == Set("foo", "bar", "baz", "for", "bat", "bay"))
          )

      // should return empty set for non-existing key
      _ <- flushdb
      _ <- sadd("set-1", "foo")
      _ <- sadd("set-1", "bar")
      _ <- sadd("set-1", "baz")
      x <- sunion("set-1", "set-2")
      _ <- IO(assert(getResp(x).get == Set("foo", "bar", "baz")))
    } yield ()
  }

  final def setsUnionstore(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    for {
      _ <- sadd("set-1", "foo")
      _ <- sadd("set-1", "bar")
      _ <- sadd("set-1", "baz")

      _ <- sadd("set-2", "foo")
      _ <- sadd("set-2", "bat")
      _ <- sadd("set-2", "baz")

      _ <- sadd("set-3", "for")
      _ <- sadd("set-3", "bat")
      _ <- sadd("set-3", "bay")

      x <- sunionstore("set-r", "set-1", "set-2")
      _ <- IO(assert(getResp(x).get == 4))
      x <- scard("set-r")
      _ <- IO(assert(getResp(x).get == 4))
      x <- sunionstore("set-s", "set-1", "set-3")
      _ <- IO(assert(getResp(x).get == 6))
      x <- scard("set-s")
      _ <- IO(assert(getResp(x).get == 6))

      // should treat non-existing keys as empty sets
      _ <- flushdb
      _ <- sadd("set-1", "foo")
      _ <- sadd("set-1", "bar")
      _ <- sadd("set-1", "baz")
      x <- sunionstore("set-r", "set-1", "set-4")
      _ <- IO(assert(getResp(x).get == 3))
      x <- scard("set-r")
      _ <- IO(assert(getResp(x).get == 3))
    } yield ()
  }

  final def setsDiff(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    for {
      _ <- sadd("set-1", "foo")
      _ <- sadd("set-1", "bar")
      _ <- sadd("set-1", "baz")

      _ <- sadd("set-2", "foo")
      _ <- sadd("set-2", "bat")
      _ <- sadd("set-2", "baz")

      _ <- sadd("set-3", "for")
      _ <- sadd("set-3", "bat")
      _ <- sadd("set-3", "bay")

      x <- sdiff("set-1", "set-2", "set-3")
      _ <- IO(assert(getResp(x).get == Set("bar")))

      // should treat non-existing keys as empty sets
      _ <- flushdb
      _ <- sadd("set-1", "foo")
      _ <- sadd("set-1", "bar")
      _ <- sadd("set-1", "baz")
      x <- sdiff("set-1", "set-2")
      _ <- IO(assert(getResp(x).get == Set("foo", "bar", "baz")))
    } yield ()
  }

  final def setsMember(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    for {
      _ <- sadd("set-1", "foo")
      _ <- sadd("set-1", "bar")
      _ <- sadd("set-1", "baz")
      x <- smembers("set-1")
      _ <- IO(assert(getResp(x).get == Set("foo", "bar", "baz")))

      // should return None for an empty set
      _ <- flushdb
      x <- smembers("set-1")
      _ <- IO(assert(getResp(x).get == Set()))

      _ <- flushdb
      _ <- sadd("set-1", "foo")
      _ <- sadd("set-1", "bar")
      _ <- sadd("set-1", "baz")
      x <- srandmember("set-1")
      _ <- IO {
            getResp(x).get match {
              case "foo" => assert(true)
              case "bar" => assert(true)
              case "baz" => assert(true)
              case _     => assert(false)
            }
          }

      // should return None for a non-existing key") {
      _ <- flushdb
      x <- srandmember("set-1")
      _ <- IO(assert(getResp(x) == None))
    } yield ()
  }

  final def setsRandomMemberWithCount(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
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

      x <- srandmember("set-1", 2)
      _ <- IO {
            getResp(x).get match {
              case s: Set[_] => assert(s.size == 2)
              case _         => false
            }
          }

      // returned elements should be unique
      x <- srandmember("set-1", 4)
      _ <- IO {
            getResp(x).get match {
              case s: Set[_] => s.size == 4
            }
          }

      // returned elements may have duplicates
      x <- srandmember("set-1", -4)
      _ <- IO {
            getResp(x).get match {
              case s: Set[_] => s.size <= 4
            }
          }

      // if supplied count > size, then whole set is returned
      x <- srandmember("set-1", 24)
      _ <- IO {
            getResp(x).get match {
              case s: Set[_] => s.size == 8
            }
          }

    } yield ()
  }
}
