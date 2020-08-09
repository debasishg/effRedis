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
import algebra.StringApi.{ NX, XX }

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

      x <- get("amit-1")
      _ <- IO(assert(getResp(x) == None))

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

      // first trying to set with 'xx' should fail since there is not key present
      x <- set("amit-1", "mor", XX, 2.seconds)
      _ <- IO(assert(!getBoolean(x)))
      x <- get("amit-1")
      _ <- IO(assert(getResp(x) == None))

      // second, we set if there is no key and we should succeed
      x <- set("amit-1", "mor", NX, 2.seconds)
      _ <- IO(assert(getBoolean(x)))
      x <- get("amit-1")
      _ <- IO(assert(getResp(x).get == "mor"))

      // third, since the key is now present (if second succeeded), this would succeed too
      x <- set("amit-1", "mor", keepTTL = true)
      _ <- IO(assert(getBoolean(x)))
      x <- ttl("amit-1")
      _ <- IO(assert(getResp(x).get == 2L))
    } yield ()
  }

  def stringsGetSet(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      x <- set("anshin-1", "debasish")
      _ <- IO(assert(getBoolean(x)))

      x <- get("anshin-1")
      _ <- IO(assert(getResp(x).get == "debasish"))

      x <- getset("anshin-1", "maulindu")
      _ <- IO(assert(getResp(x).get == "debasish"))

      x <- get("anshin-1")
      _ <- IO(assert(getResp(x).get == "maulindu"))
    } yield ()
  }

  def stringsSetNxEx(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    val key   = "setex-1"
    val value = "value"
    for {

      x <- set("anshin-1", "debasish")
      _ <- IO(assert(getBoolean(x)))
      x <- setnx("anshin-1", "maulindu")
      _ <- IO(assert(!getBoolean(x)))
      x <- setnx("anshin-2", "maulindu")
      _ <- IO(assert(getBoolean(x)))

      x <- setex(key, 1, value)
      _ <- IO(assert(getBoolean(x)))
      x <- get(key)
      _ <- IO(assert(getResp(x).get == value))

      _ <- IO(TimeUnit.SECONDS.sleep(2))
      x <- get(key)
      _ <- IO(assert(getResp(x) == None))
    } yield ()
  }

  def stringsIncr(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      x <- set("anshin-1", 10)
      _ <- IO(assert(getBoolean(x)))
      x <- incr("anshin-1")
      _ <- IO(assert(getResp(x).get == 11))

      x <- set("anshin-1", "10")
      _ <- IO(assert(getBoolean(x)))
      x <- incr("anshin-1")
      _ <- IO(assert(getResp(x).get == 11))

      x <- set("anshin-1", "void")
      _ <- IO(assert(getBoolean(x)))
      x <- incr("anshin-1")
      _ <- IO(assert(getResp(x).get.toString.contains("ERR")))

      x <- set("anshin-2", 10)
      _ <- IO(assert(getBoolean(x)))
      x <- incrby("anshin-2", 5)
      _ <- IO(assert(getResp(x).get == 15))

      x <- set("anshin-3", 10.5f)
      _ <- IO(assert(getBoolean(x)))
      x <- incrbyfloat("anshin-3", 0.7f)
      _ <- IO(assert(getResp(x).get == 11.2f))

      x <- set("anshin-3", "void")
      _ <- IO(assert(getBoolean(x)))
      x <- incrbyfloat("anshin-3", 0.7f)
      _ <- IO(assert(getResp(x).get.toString.contains("ERR")))
    } yield ()
  }

  def stringsDecr(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      x <- set("anshin-1", 10)
      _ <- IO(assert(getBoolean(x)))
      x <- decr("anshin-1")
      _ <- IO(assert(getResp(x).get == 9))

      x <- set("anshin-1", "10")
      _ <- IO(assert(getBoolean(x)))
      x <- decr("anshin-1")
      _ <- IO(assert(getResp(x).get == 9))

      x <- set("anshin-1", "void")
      _ <- IO(assert(getBoolean(x)))
      x <- decr("anshin-1")
      _ <- IO(assert(getResp(x).get.toString.contains("ERR")))

      x <- set("anshin-2", 10)
      _ <- IO(assert(getBoolean(x)))
      x <- decrby("anshin-2", 5)
      _ <- IO(assert(getResp(x).get == 5))
    } yield ()
  }

  def stringsMget(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      x <- set("anshin-1", "debasish")
      _ <- IO(assert(getBoolean(x)))
      x <- set("anshin-2", "maulindu")
      _ <- IO(assert(getBoolean(x)))
      x <- set("anshin-3", "nilanjan")
      _ <- IO(assert(getBoolean(x)))
      x <- mget("anshin-1", "anshin-2", "anshin-3")
      _ <- IO(assert(getResp(x).get == List(Some("debasish"), Some("maulindu"), Some("nilanjan"))))
      x <- mget("anshin-1", "anshin-2", "anshin-4")
      _ <- IO(assert(getResp(x).get == List(Some("debasish"), Some("maulindu"), None)))
    } yield ()
  }

  def stringsMset(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      // should set all keys irrespective of whether they exist
      x <- mset(("anshin-1", "debasish"), ("anshin-2", "maulindu"), ("anshin-3", "nilanjan"))
      _ <- IO(assert(getBoolean(x)))

      // should set all keys only if none of them exist
      x <- msetnx(("anshin-4", "debasish"), ("anshin-5", "maulindu"), ("anshin-6", "nilanjan"))
      _ <- IO(assert(getBoolean(x)))
      x <- msetnx(("anshin-7", "debasish"), ("anshin-8", "maulindu"), ("anshin-6", "nilanjan"))
      _ <- IO(assert(!getBoolean(x)))
      x <- msetnx(("anshin-4", "debasish"), ("anshin-5", "maulindu"), ("anshin-6", "nilanjan"))
      _ <- IO(assert(!getBoolean(x)))
    } yield ()
  }

  def stringsWithSpacesInKeys(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      x <- set("anshin software", "debasish ghosh")
      _ <- IO(assert(getBoolean(x)))
      x <- get("anshin software")
      _ <- IO(assert(getResp(x).get == "debasish ghosh"))

      x <- set("test key with spaces", "I am a value with spaces")
      _ <- IO(assert(getBoolean(x)))
      x <- get("test key with spaces")
      _ <- IO(assert(getResp(x).get == "I am a value with spaces"))

      x <- set("anshin-x", "debasish\nghosh\nfather")
      _ <- IO(assert(getBoolean(x)))
      x <- get("anshin-x")
      _ <- IO(assert(getResp(x).get == "debasish\nghosh\nfather"))
    } yield ()
  }

  def stringsGetSetRange(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      x <- set("key1", "hello world")
      _ <- IO(assert(getBoolean(x)))
      x <- setrange("key1", 6, "redis")
      _ <- IO(assert(getResp(x).get == 11))
      x <- get("key1")
      _ <- IO(assert(getResp(x).get == "hello redis"))

      x <- setrange("key2", 6, "redis")
      _ <- IO(assert(getResp(x).get == 11))
      x <- get("key2")
      _ <- IO(assert(getResp(x).get.toString.trim == "redis"))
      x <- get("key2")
      _ <- IO(assert(getResp(x).get.toString.length == 11))
    } yield ()
  }
}
