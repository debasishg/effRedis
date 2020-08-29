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
import cats.implicits._
import EffRedisFunSuite._

trait TestBaseScenarios {
  implicit def cs: ContextShift[IO]

  final def baseMisc1(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {

    import cmd._
    for {
      _ <- set("anshin-1", "debasish")
      _ <- set("anshin-2", "maulindu")
      x <- keys("anshin*")
      _ <- IO {
            getResp(x).get match {
              case vs: List[_] => vs.size == 2 && vs.contains(Some("anshin-2")) && vs.contains(Some("anshin-1"))
              case _           => false
            }
          }

      // fetch keys with spaces
      _ <- set("key 1", "debasish")
      _ <- set("key 2", "maulindu")
      x <- keys("key*")
      _ <- IO(assert {
            getResp(x).get match {
              case keys: List[_] => keys.size == 2
              case _             => false
            }
          })

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

      // dbsize
      _ <- flushdb
      _ <- set("anshin-1", "debasish")
      _ <- set("anshin-2", "maulindu")
      x <- dbsize
      _ <- IO(assert(getResp(x).get == 2))

    } yield ()
  }

  final def baseMisc2(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {

    import cmd._
    for {
      // exists
      _ <- set("anshin-1", "debasish")
      _ <- set("anshin-2", "maulindu")
      x <- exists("anshin-2")
      _ <- IO(assert(getBoolean(x)))
      x <- exists("anshin-1")
      _ <- IO(assert(getBoolean(x)))
      x <- exists("anshin-3")
      _ <- IO(assert(!getBoolean(x)))

      // del
      x <- del("anshin-2", "anshin-1")
      _ <- IO(assert(getResp(x).get == 2))
      x <- del("anshin-2", "anshin-1")
      _ <- IO(assert(getResp(x).get == 0))

      // getType
      _ <- set("anshin-2", "maulindu")
      x <- getType("anshin-2")
      _ <- IO(assert(getResp(x).get == "string"))

      // expire
      _ <- set("anshin-1", "debasish")
      _ <- set("anshin-2", "maulindu")
      x <- expire("anshin-2", 1000)
      _ <- IO(assert(getBoolean(x)))
      x <- ttl("anshin-2")
      _ <- IO(assert(getResp(x).get == 1000))
      x <- expire("anshin-3", 1000)
      _ <- IO(assert(!getBoolean(x)))
    } yield ()
  }

  final def baseMisc3(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    val timeout          = 2.second
    val timeoutTimestamp = System.currentTimeMillis().millis + timeout

    val testData: Map[String, String] = Map(
      "key1" -> "value1",
      "key2" -> "value2",
      "key3" -> "value3",
      "key4" -> "value4",
      "key5" -> "value5"
    )

    val testKeys: List[String] = testData.keys.toList

    import cmd._
    for {

      _ <- IO(testData.map { case (k, v) => set(k, v) }.toList.sequence).flatten

      // We set expiry dates on some keys for $timeout
      x <- expire(testKeys(0), timeout.length.toInt)
      _ <- IO(assert(getBoolean(x)))
      x <- pexpire(testKeys(1), timeout.toMillis.toInt)
      _ <- IO(assert(getBoolean(x)))
      x <- expireat(testKeys(2), timeoutTimestamp.toSeconds)
      _ <- IO(assert(getBoolean(x)))
      x <- pexpireat(testKeys(3), timeoutTimestamp.toMillis)
      _ <- IO(assert(getBoolean(x)))

      // It returns some ttl for the keys
      x <- ttl(testKeys(0))
      _ <- IO(assert(getLong(x).get > 0L))
      x <- pttl(testKeys(0))
      _ <- IO(assert(getLong(x).get > 0L))

      x <- ttl(testKeys(1))
      _ <- IO(assert(getLong(x).get > 0L))
      x <- pttl(testKeys(1))
      _ <- IO(assert(getLong(x).get > 0L))

      x <- ttl(testKeys(2))
      _ <- IO(assert(getLong(x).get > 0L))
      x <- pttl(testKeys(2))
      _ <- IO(assert(getLong(x).get > 0L))

      x <- ttl(testKeys(3))
      _ <- IO(assert(getLong(x).get > 0L))
      x <- pttl(testKeys(3))
      _ <- IO(assert(getLong(x).get > 0L))

      // After ttl, keys should be long gone
      _ <- IO(TimeUnit.MILLISECONDS.sleep(timeout.toMillis + 1))
      x <- keys()
      _ <- IO {
            getResp(x).get match {
              case vs: List[_] =>
                !vs.contains(Some(testKeys(0))) &&
                  !vs.contains(Some(testKeys(1))) &&
                  !vs.contains(Some(testKeys(2)))
              case _ => false
            }
          }
    } yield ()
  }

  final def baseMisc4(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {

    import cmd._
    for {
      _ <- set("key-2", "maulindu")
      x <- expire("key-2", 1000)
      _ <- IO(assert(getBoolean(x)))
      x <- ttl("key-2")
      _ <- IO(assert(getResp(x).get == 1000))
      x <- persist("key-2")
      _ <- IO(assert(getBoolean(x)))
      x <- ttl("key-2")
      _ <- IO(assert(getResp(x).get == -1))
      x <- persist("key-3")
      _ <- IO(assert(!getBoolean(x)))

      _ <- hset("hash-1", "description", "one")
      _ <- hset("hash-1", "order", "100")
      _ <- hset("hash-2", "description", "two")
      _ <- hset("hash-2", "order", "25")
      _ <- hset("hash-3", "description", "three")
      _ <- hset("hash-3", "order", "50")
      _ <- sadd("alltest", 1)
      _ <- sadd("alltest", 2)
      _ <- sadd("alltest", 3)
      x <- sort("alltest")
      _ <- IO(assert(getResp(x).get == List(Some("1"), Some("2"), Some("3"))))
      x <- sort("alltest", Some((0, 1)))
      _ <- IO(assert(getResp(x).getOrElse(Nil) == List(Some("1"))))
      x <- sort("alltest", None, true)
      _ <- IO(assert(getResp(x).getOrElse(Nil) == List(Some("3"), Some("2"), Some("1"))))
      x <- sort("alltest", None, false, false, Some("hash-*->order"))
      _ <- IO(assert(getResp(x).getOrElse(Nil) == List(Some("2"), Some("3"), Some("1"))))
      x <- sort("alltest", None, false, false, None, List("hash-*->description"))
      _ <- IO(assert(getResp(x).getOrElse(Nil) == List(Some("one"), Some("two"), Some("three"))))
      x <- sort("alltest", None, false, false, None, List("hash-*->description", "hash-*->order"))
      _ <- IO(
            assert(
              getResp(x)
                .getOrElse(Nil) == List(Some("one"), Some("100"), Some("two"), Some("25"), Some("three"), Some("50"))
            )
          )
    } yield ()
  }

  import codecs._
  final def baseMisc5(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {

    import cmd._
    for {
      _ <- sadd("alltest", 10)
      _ <- sadd("alltest", 30)
      _ <- sadd("alltest", 3)
      _ <- sadd("alltest", 1)

      // default serialization : return String
      x <- sortNStore("alltest", storeAt = "skey")
      _ <- IO(assert(getResp(x).getOrElse(-1) == 4))
      x <- lrange("skey", 0, 10)
      _ <- IO(assert(getResp(x).get == List(Some("1"), Some("3"), Some("10"), Some("30"))))

      // Long serialization : return Long
      x <- {
        implicit val parseLong = Parse[Long](new String(_).toLong)
        sortNStore[Long]("alltest", storeAt = "skey")
      }
      _ <- IO(assert(getResp(x).getOrElse(-1) == 4))
      x <- {
        implicit val parseLong = Parse[Long](new String(_).toLong)
        lrange[Long]("skey", 0, 10)
      }
      _ <- IO(assert(getResp(x).get == List(Some(1), Some(3), Some(10), Some(30))))
    } yield ()
  }
}
