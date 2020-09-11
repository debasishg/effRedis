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

trait TestHyperLogLogScenarios {
  implicit def cs: ContextShift[IO]

  final def hllPfAdd(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    for {
      // should return one for changed estimated cardinality") {
      x <- pfadd("hll-updated-cardinality", "value1")
      _ <- IO(assert(getResp(x).get == 1))

      // should return zero for unchanged estimated cardinality
      _ <- flushdb
      _ <- pfadd("hll-nonupdated-cardinality", "value1")
      x <- pfadd("hll-nonupdated-cardinality", "value1")
      _ <- IO(assert(getResp(x).get == 0))

      // should return one for variadic values and estimated cardinality changes
      _ <- flushdb
      _ <- pfadd("hll-variadic-cardinality", "value1")
      x <- pfadd("hll-variadic-cardinality", "value1", "value2")
      _ <- IO(assert(getResp(x).get == 1))
    } yield ()
  }

  final def hllPfCount(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    for {
      // should return zero for an empty
      x <- pfcount("hll-empty")
      _ <- IO(assert(getResp(x).get == 0))

      // should return estimated cardinality
      _ <- flushdb
      _ <- pfadd("hll-card", "value1")
      x <- pfcount("hll-card")
      _ <- IO(assert(getResp(x).get == 1))

      // should return estimated cardinality of unioned keys
      _ <- flushdb
      _ <- pfadd("hll-union-1", "value1")
      _ <- pfadd("hll-union-2", "value2")
      x <- pfcount("hll-union-1", "hll-union-2")
      _ <- IO(assert(getResp(x).get == 2))
    } yield ()
  }

  final def hllPfMerge(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    for {
      // should merge existing entries
      _ <- pfadd("hll-merge-source-1", "value1")
      _ <- pfadd("hll-merge-source-2", "value2")
      _ <- pfmerge("hell-merge-destination", "hll-merge-source-1", "hll-merge-source-2")
      x <- pfcount("hell-merge-destination")
      _ <- IO(assert(getResp(x).get == 2))
    } yield ()
  }
}