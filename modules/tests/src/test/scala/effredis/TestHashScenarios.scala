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

trait TestHashScenarios {
  implicit def cs: ContextShift[IO]

  final def hashHSet1(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    for {
      // should set and get fields
      x <- hset("hash1", "field1", "val")
      _ <- IO(assert(getResp(x).get == 1))
      x <- hget("hash1", "field1")
      _ <- IO(assert(getResp(x).get == "val"))
      x <- hset("hash2", Map("field1" -> "val1", "field2" -> "val2"))
      _ <- IO(assert(getResp(x).get == 2))

      // should return true if field did not exist and was inserted
      x <- hdel("hash1", "field1")
      _ <- IO(assert(getResp(x).get == 1))
      x <- hset("hash1", "field1", "val")
      _ <- IO(assert(getResp(x).get == 1))

      // should return false if field existed before and was overwritten
      _ <- hset("hash1", "field1", "val")
      x <- hset("hash1", "field1", "val")
      _ <- IO(assert(getResp(x).get == 0))

      // should set and get maps
      x <- hmset("hash2", Map("field1" -> "val1", "field2" -> "val2"))
      _ <- IO(assert(getBoolean(x)))
      x <- hmget("hash2", "field1")
      _ <- IO(assert(getResp(x).get == Map("field1" -> Some("val1"))))
      x <- hmget("hash2", "field1", "field2")
      _ <- IO(assert(getResp(x).get == Map("field1" -> Some("val1"), "field2" -> Some("val2"))))
      x <- hmget("hash2", "field1", "field2", "field3")
      _ <- IO(assert(getResp(x).get == Map("field1" -> Some("val1"), "field2" -> Some("val2"), "field3" -> None)))

      // should increment map values
      _ <- hincrby("hash3", "field1", 1)
      x <- hget("hash3", "field1")
      _ <- IO(assert(getResp(x).get == "1"))

      // should check existence
      _ <- hset("hash4", "field1", "val")
      x <- hexists("hash4", "field1")
      _ <- IO(assert(getBoolean(x)))
      x <- hexists("hash4", "field2")
      _ <- IO(assert(!getBoolean(x)))

    } yield ()
  }

  final def hashHSet2(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    for {
      // should delete fields
      _ <- hset("hash5", "field1", "val")
      x <- hexists("hash5", "field1")
      _ <- IO(assert(getBoolean(x)))
      x <- hdel("hash5", "field1")
      _ <- IO(assert(getResp(x).get == 1))
      x <- hexists("hash5", "field1")
      _ <- IO(assert(!getBoolean(x)))
      _ <- hmset("hash5", Map("field1" -> "val1", "field2" -> "val2"))
      x <- hdel("hash5", "field1", "field2")
      _ <- IO(assert(getResp(x).get == 2))

      // should return the length of the fields
      _ <- hmset("hash6", Map("field1" -> "val1", "field2" -> "val2"))
      x <- hlen("hash6")
      _ <- IO(assert(getResp(x).get == 2))

      // should return the aggregates
      _ <- hmset("hash7", Map("field1" -> "val1", "field2" -> "val2"))
      x <- hkeys("hash7")
      _ <- IO(assert(getResp(x).get == List(Some("field1"), Some("field2"))))
      x <- hvals("hash7")
      _ <- IO(assert(getResp(x).get == List(Some("val1"), Some("val2"))))

      // should increment map values by floats
      _ <- hset("hash1", "field1", 10.50f)
      x <- hincrbyfloat("hash1", "field1", 0.1f)
      _ <- IO(assert(getResp(x).get == 10.6f))
      _ <- hset("hash1", "field1", 5.0e3f)
      x <- hincrbyfloat("hash1", "field1", 2.0e2f)
      _ <- IO(assert(getResp(x).get == 5200f))
      _ <- hset("hash1", "field1", "abc")
      x <- hincrbyfloat("hash1", "field1", 2.0e2f)
      _ <- IO(assert(getResp(x).get.toString.contains("hash value is not a float")))

      // should delete multiple keys if present on a hash
      _ <- hset("hash100", "key1", 10.20f)
      _ <- hset("hash100", "key2", 10.30f)
      _ <- hset("hash100", "key3", 10.40f)
      _ <- hset("hash100", "key4", 10.50f)
      _ <- hset("hash100", "key5", 10.60f)
      x <- hkeys("hash100")
      _ <- IO(assert(getResp(x).get == List(Some("key1"), Some("key2"), Some("key3"), Some("key4"), Some("key5"))))
      x <- hdel("hash100", "key1", "key2", "key3", "key4", "key5")
      _ <- IO(assert(getResp(x).get == 5))
      x <- hkeys("hash100")
      _ <- IO(assert(getResp(x).get == List()))
    } yield ()
  }

  final def hashHGetAll(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    for {
      // should behave symmetrically with hmset
      _ <- hmset("hash1", Map("field1" -> "val1", "field2" -> "val2"))
      x <- hmset("hash2", Map())
      _ <- IO(assert(getResp(x).get.toString.contains("wrong number of arguments for 'hmset' command")))
      x <- hget("hash1", "field1")
      _ <- IO(assert(getResp(x).get == "val1"))
      x <- hgetall("hash1")
      _ <- IO(assert(getResp(x).get == Map("field1" -> "val1", "field2" -> "val2")))
      x <- hgetall("NonExistentKey")
      _ <- IO(assert(getResp(x) == None))
    } yield ()
  }
}
