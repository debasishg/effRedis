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
import RedisClient.DESC

import EffRedisFunSuite._
import Containers.{ NX, Score, ValueScorePair, XX }

trait TestSortedSetScenarios {
  implicit def cs: ContextShift[IO]

  private def add(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] =
    for {
      x <- cmd.zadd("hackers", 1965, "yukihiro matsumoto")
      _ <- IO(assert(getResp(x).get == 1))
      x <- cmd.zadd(
            "hackers",
            1953d,
            "richard stallman",
            (1916d, "claude shannon"),
            (1969d, "linus torvalds"),
            (1940d, "alan kay"),
            (1912d, "alan turing")
          )
      _ <- IO(assert(getResp(x).get == 5))
    } yield ()

  final def sortedSetsZrangeByLex(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    for {
      // should return the elements between min and max
      x <- zadd("hackers-joker", 0d, "a", (0d, "b"), (0d, "c"), (0d, "d"))
      _ <- IO(assert(getResp(x).get == 4))
      x <- zrangebylex("hackers-joker", "[a", "[b", None)
      _ <- IO(assert(getResp(x).get == List(Some("a"), Some("b"))))

      // should return the elements between min and max with offset and count
      x <- zrangebylex("hackers-joker", "[a", "[c", Some((0, 1)))
      _ <- IO(assert(getResp(x).get == List(Some("a"))))
    } yield ()
  }

  final def sortedSetsZAdd(client: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import client._
    for {
      // should add based on proper sorted set semantics
      _ <- add(client)
      x <- zadd("hackers", 1912, "alan turing")
      _ <- IO(assert(getResp(x).get == 0))
      x <- zcard("hackers")
      _ <- IO(assert(getResp(x).get == 6))
    } yield ()
  }

  final def sortedSetsZAddWithOptions(client: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import client._
    for {
      // should add based on proper sorted set semantics
      _ <- add(client)
      x <- zadd("hackers", NX, true, 1922, "alan turing")
      _ <- IO(assert(getResp(x).get == 0))
      x <- zadd("hackers", XX, true, 1922, "alan turing")
      _ <- IO(assert(getResp(x).get == 1))
      x <- zcard("hackers")
      _ <- IO(assert(getResp(x).get == 6))
    } yield ()
  }

  final def sortedSetsZRem(client: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import client._
    for {
      // should remove
      _ <- add(client)
      x <- zrem("hackers", "alan turing")
      _ <- IO(assert(getResp(x).get == 1))
      x <- zrem("hackers", "alan kay", "linus torvalds")
      _ <- IO(assert(getResp(x).get == 2))
      x <- zrem("hackers", "alan kay", "linus torvalds")
      _ <- IO(assert(getResp(x).get == 0))
    } yield ()
  }

  final def sortedSetsZRange(client: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import client._
    for {
      _ <- add(client)
      x <- zrange("hackers")
      _ <- IO(assert(getRespListSize(x).get == 6))
      x <- zrange("NonExisting")
      _ <- IO(assert(getResp(x).get == List.empty))
      x <- zrangeWithScore("hackers")
      _ <- IO(assert(getRespListSize(x).get == 6))
      x <- zrangeWithScore("NonExisting")
      _ <- IO(assert(getResp(x).get == List.empty))
    } yield ()
  }

  final def sortedSetsZRank(client: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import client._
    for {
      _ <- add(client)
      x <- zrank("hackers", "yukihiro matsumoto")
      _ <- IO(assert(getResp(x).get == 4))
      x <- zrank("hackers", "yukihiro matsumoto", reverse = true)
      _ <- IO(assert(getResp(x).get == 1))
    } yield ()
  }

  final def sortedSetsZRemRange(client: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import client._
    for {
      _ <- add(client)
      x <- zremrangebyrank("hackers", 0, 2)
      _ <- IO(assert(getResp(x).get == 3))
      _ <- flushdb
      _ <- add(client)
      x <- zremrangebyscore("hackers", 1912, 1940)
      _ <- IO(assert(getResp(x).get == 3))
      x <- zremrangebyscore("hackers", 0, 3)
      _ <- IO(assert(getResp(x).get == 0))
    } yield ()
  }

  final def sortedSetsZUnion(client: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import client._
    for {
      _ <- zadd("hackers 1", 1965, "yukihiro matsumoto")
      _ <- zadd("hackers 1", 1953, "richard stallman")
      _ <- zadd("hackers 2", 1916, "claude shannon")
      _ <- zadd("hackers 2", 1969, "linus torvalds")
      _ <- zadd("hackers 3", 1940, "alan kay")
      _ <- zadd("hackers 4", 1912, "alan turing")

      // union with weight = 1
      x <- zunionstore("hackers", List("hackers 1", "hackers 2", "hackers 3", "hackers 4"))
      _ <- IO(assert(getResp(x).get == 6))
      x <- zcard("hackers")
      _ <- IO(assert(getResp(x).get == 6))

      x <- zrangeWithScore("hackers")
      _ <- IO(assert(getRespList[(String, Score)](x).get.map(_._2.value) == List(1912, 1916, 1940, 1953, 1965, 1969)))

      // union with modified weights
      x <- zunionstoreWeighted(
            "hackers weighted",
            Map("hackers 1" -> 1.0, "hackers 2" -> 2.0, "hackers 3" -> 3.0, "hackers 4" -> 4.0)
          )
      _ <- IO(assert(getResp(x).get == 6))
      x <- zrangeWithScore(
            "hackers weighted"
          )
      _ <- IO(
            assert(
              getRespList[(String, Score)](x).get.map(_._2.value.toInt) == List(1953, 1965, 3832, 3938, 5820, 7648)
            )
          )
    } yield ()
  }

  final def sortedSetsZInter(client: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import client._
    for {
      _ <- zadd("hackers", 1912, "alan turing")
      _ <- zadd("hackers", 1916, "claude shannon")
      _ <- zadd("hackers", 1927, "john mccarthy")
      _ <- zadd("hackers", 1940, "alan kay")
      _ <- zadd("hackers", 1953, "richard stallman")
      _ <- zadd("hackers", 1954, "larry wall")
      _ <- zadd("hackers", 1956, "guido van rossum")
      _ <- zadd("hackers", 1965, "paul graham")
      _ <- zadd("hackers", 1965, "yukihiro matsumoto")
      _ <- zadd("hackers", 1969, "linus torvalds")

      _ <- zadd("baby boomers", 1948, "phillip bobbit")
      _ <- zadd("baby boomers", 1953, "richard stallman")
      _ <- zadd("baby boomers", 1954, "cass sunstein")
      _ <- zadd("baby boomers", 1954, "larry wall")
      _ <- zadd("baby boomers", 1956, "guido van rossum")
      _ <- zadd("baby boomers", 1961, "lawrence lessig")
      _ <- zadd("baby boomers", 1965, "paul graham")
      _ <- zadd("baby boomers", 1965, "yukihiro matsumoto")

      // intersection with weight = 1
      x <- zinterstore("baby boomer hackers", List("hackers", "baby boomers"))
      _ <- IO(assert(getResp(x).get == 5))
      x <- zcard("baby boomer hackers")
      _ <- IO(assert(getResp(x).get == 5))

      x <- zrange("baby boomer hackers")
      _ <- IO(
            assert(
              getResp(x).get == List(
                    Some("richard stallman"),
                    Some("larry wall"),
                    Some("guido van rossum"),
                    Some("paul graham"),
                    Some("yukihiro matsumoto")
                  )
            )
          )

      // intersection with modified weights
      x <- zinterstoreWeighted("baby boomer hackers weighted", Map("hackers" -> 0.5, "baby boomers" -> 0.5))
      _ <- IO(assert(getResp(x).get == 5))
      x <- zrangeWithScore("baby boomer hackers weighted")
      _ <- IO(assert(getRespList[(String, Score)](x).get.map(_._2.value.toInt) == List(1953, 1954, 1956, 1965, 1965)))
    } yield ()
  }

  final def sortedSetsZCount(client: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import client._
    for {
      _ <- add(client)
      x <- zcount("hackers", 1912, 1920)
      _ <- IO(assert(getResp(x).get == 2))
    } yield ()
  }

  final def sortedSetsZLexCount(client: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import client._
    for {
      _ <- zadd("myzset", 0d, "a", (0d, "b"), (0d, "c"), (0d, "d"), (0d, "e"))
      _ <- zadd("myzset", 0d, "f", (0d, "g"))
      x <- zlexcount("myzset", "-", "+")
      _ <- IO(assert(getResp(x).get == 7))
      x <- zlexcount("myzset", "[b", "[f")
      _ <- IO(assert(getResp(x).get == 5))
    } yield ()
  }

  final def sortedSetsZRemRangeByLex(client: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import client._
    for {
      x <- zadd("myzset", 0d, "aaaa", (0d, "b"), (0d, "c"), (0d, "d"), (0d, "e"))
      _ <- IO(assert(getResp(x).get == 5))
      x <- zadd("myzset", 0d, "foo", (0d, "zap"), (0d, "zip"), (0d, "ALPHA"), (0d, "alpha"))
      _ <- IO(assert(getResp(x).get == 5))
      x <- zremrangebylex("myzset", "[alpha", "[omega")
      _ <- IO(assert(getResp(x).get == 6))
      x <- zrange("myzset", 0, -1)
      _ <- IO(assert(getRespList(x).get == List(Some("ALPHA"), Some("aaaa"), Some("zap"), Some("zip"))))
    } yield ()
  }

  final def sortedSetsZRangeByScore(client: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import client._
    for {
      // should return the elements between min and max") {
      _ <- add(client)
      x <- zrangebyscore("hackers", 1940, true, 1969, true, None)
      _ <- IO(
            assert(
              getResp(x).get == List(
                    Some("alan kay"),
                    Some("richard stallman"),
                    Some("yukihiro matsumoto"),
                    Some("linus torvalds")
                  )
            )
          )

      x <- zrangebyscore("hackers", 1940, true, 1969, true, None, DESC)
      _ <- IO(
            assert(
              getResp(x).get == List(
                    Some("linus torvalds"),
                    Some("yukihiro matsumoto"),
                    Some("richard stallman"),
                    Some("alan kay")
                  )
            )
          )

      _ <- flushdb

      // should return the elements between min and max and allow offset and limit
      _ <- add(client)
      x <- zrangebyscore("hackers", 1940, true, 1969, true, Some((0, 2)))
      _ <- IO(assert(getResp(x).get == List(Some("alan kay"), Some("richard stallman"))))

      x <- zrangebyscore("hackers", 1940, true, 1969, true, Some((0, 2)), DESC)
      _ <- IO(assert(getResp(x).get == List(Some("linus torvalds"), Some("yukihiro matsumoto"))))

      x <- zrangebyscore("hackers", 1940, true, 1969, true, Some((3, 1)))
      _ <- IO(assert(getResp(x).get == List(Some("linus torvalds"))))

      x <- zrangebyscore("hackers", 1940, true, 1969, true, Some((3, 1)), DESC)
      _ <- IO(assert(getResp(x).get == List(Some("alan kay"))))

      x <- zrangebyscore("hackers", 1940, false, 1969, true, Some((0, 2)))
      _ <- IO(assert(getResp(x).get == List(Some("richard stallman"), Some("yukihiro matsumoto"))))

      x <- zrangebyscore("hackers", 1940, true, 1969, false, Some((0, 2)), DESC)
      _ <- IO(assert(getResp(x).get == List(Some("yukihiro matsumoto"), Some("richard stallman"))))
    } yield ()
  }

  final def sortedSetsZRangeByScoreWithScore(client: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import client._
    for {
      // should return the elements between min and max"
      _ <- add(client)
      x <- zrangebyscoreWithScore("hackers", 1940, true, 1969, true, None)
      _ <- IO(
            assert(
              getResp(x).get == List(
                    ("alan kay", Score(1940.0)),
                    ("richard stallman", Score(1953.0)),
                    ("yukihiro matsumoto", Score(1965.0)),
                    ("linus torvalds", Score(1969.0))
                  )
            )
          )

      x <- zrangebyscoreWithScore("hackers", 1940, true, 1969, true, None, DESC)
      _ <- IO(
            assert(
              getResp(x).get == List(
                    ("linus torvalds", Score(1969.0)),
                    ("yukihiro matsumoto", Score(1965.0)),
                    ("richard stallman", Score(1953.0)),
                    ("alan kay", Score(1940.0))
                  )
            )
          )

      x <- zrangebyscoreWithScore("hackers", 1940, true, 1969, true, Some((3, 1)))
      _ <- IO(assert(getResp(x).get == List(("linus torvalds", Score(1969.0)))))

      x <- zrangebyscoreWithScore("hackers", 1940, true, 1969, true, Some((3, 1)), DESC)
      _ <- IO(assert(getResp(x).get == List(("alan kay", Score(1940.0)))))
    } yield ()
  }

  final def sortedSetsZPopMin(client: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import client._
    for {
      // should return the elements between min and max") {
      _ <- add(client)
      x <- zpopmin("hackers")
      _ <- IO(assert(getRespListSize(x).get == 1))
      _ <- IO {
            val resp = getRespList[ValueScorePair[String]](x).get
            assert(resp.head.value == "alan turing")
            assert(resp.head.score == Score(1912.0))
          }
      x <- zpopmin("hackers", 2)
      _ <- IO(assert(getRespListSize(x).get == 2))
      _ <- IO {
            val resp = getRespList[ValueScorePair[String]](x).get
            assert(resp.map(_.value).toSet == Set("claude shannon", "alan kay"))
            assert(resp.map(_.score).toSet == Set(Score(1916.0), Score(1940.0)))
          }
      x <- zpopmin("NonExistent")
      _ <- IO(assert(getRespListSize(x).get == 0))
    } yield ()
  }

  final def sortedSetsZPopMax(client: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import client._
    for {
      // should return the elements between min and max") {
      _ <- add(client)
      x <- zpopmax("hackers")
      _ <- IO(assert(getRespListSize(x).get == 1))
      _ <- IO {
            val resp = getRespList[ValueScorePair[String]](x).get
            assert(resp.head.value == "linus torvalds")
            assert(resp.head.score == Score(1969.0))
          }
      x <- zpopmax("hackers", 2)
      _ <- IO(assert(getRespListSize(x).get == 2))
      _ <- IO {
            val resp = getRespList[ValueScorePair[String]](x).get
            assert(resp.map(_.value).toSet == Set("yukihiro matsumoto", "richard stallman"))
            assert(resp.map(_.score).toSet == Set(Score(1965.0), Score(1953.0)))
          }
      x <- zpopmax("NonExistent")
      _ <- IO(assert(getRespListSize(x).get == 0))
    } yield ()
  }

  final def sortedSetsBZPopMin(
      cmds: (RedisClient[IO, RedisClient.SINGLE.type], RedisClient[IO, RedisClient.SINGLE.type])
  ): IO[Unit] = {
    val cmd1 = cmds._1
    val cmd2 = cmds._2
    val r1 = for {
      x <- cmd1.bzpopmin(3, "zset1", "zset2")
    } yield x

    val r2 = for {
      z <- cmd2.zcard("zset1")
      _ <- IO(assert(getResp(z).get == 0))
      _ <- cmd2.zadd("zset1", 0d, "a", (1d, "b"), (2d, "c"))
    } yield ()

    // start r1 and r2 in fibers and then
    // then join : r1 blocks but then gets
    // the value as soon as r2 ends
    val f = for {
      a <- r1.start
      b <- r2.start
      c <- a.join
      _ <- b.join
    } yield c
    f.map(r => assert(getResp(r) == Some(("zset1", ValueScorePair(Score(0d), "a")))))
  }

  final def sortedSetsBZPopMinWithTimeout(
      cmds: (RedisClient[IO, RedisClient.SINGLE.type], RedisClient[IO, RedisClient.SINGLE.type])
  ): IO[Unit] = {
    val cmd1 = cmds._1
    val cmd2 = cmds._2
    val r1 = for {
      x <- cmd1.bzpopmin[String, String](1, "zset1", "zset2")
    } yield x

    val r2 = for {
      z <- cmd2.zcard("zset1")
      _ <- IO(assert(getResp(z).get == 0))
      _ <- cmd2.zadd("zset3", 0d, "a", (1d, "b"), (2d, "c"))
    } yield ()

    // start r1 and r2 in fibers and then
    // then join : r1 blocks but then gets
    // the value as soon as r2 ends
    val f = for {
      a <- r1.start
      b <- r2.start
      c <- a.join
      _ <- b.join
    } yield c
    // f.map(r => println(getResp(r)))
    f.map(r => assert(getResp(r) == None))
  }

  final def sortedSetsBZPopMax(
      cmds: (RedisClient[IO, RedisClient.SINGLE.type], RedisClient[IO, RedisClient.SINGLE.type])
  ): IO[Unit] = {
    val cmd1 = cmds._1
    val cmd2 = cmds._2
    val r1 = for {
      x <- cmd1.bzpopmax(3, "zset1", "zset2")
    } yield x

    val r2 = for {
      z <- cmd2.zcard("zset1")
      _ <- IO(assert(getResp(z).get == 0))
      _ <- cmd2.zadd("zset1", 0d, "a", (1d, "b"), (2d, "c"))
    } yield ()

    // start r1 and r2 in fibers and then
    // then join : r1 blocks but then gets
    // the value as soon as r2 ends
    val f = for {
      a <- r1.start
      b <- r2.start
      c <- a.join
      _ <- b.join
    } yield c
    f.map(r => assert(getResp(r) == Some(("zset1", ValueScorePair(Score(2d), "c")))))
  }

  final def sortedSetsBZPopMaxWithTimeout(
      cmds: (RedisClient[IO, RedisClient.SINGLE.type], RedisClient[IO, RedisClient.SINGLE.type])
  ): IO[Unit] = {
    val cmd1 = cmds._1
    val cmd2 = cmds._2
    val r1 = for {
      x <- cmd1.bzpopmax[String, String](1, "zset1", "zset2")
    } yield x

    val r2 = for {
      z <- cmd2.zcard("zset1")
      _ <- IO(assert(getResp(z).get == 0))
      _ <- cmd2.zadd("zset3", 0d, "a", (1d, "b"), (2d, "c"))
    } yield ()

    // start r1 and r2 in fibers and then
    // then join : r1 blocks but then gets
    // the value as soon as r2 ends
    val f = for {
      a <- r1.start
      b <- r2.start
      c <- a.join
      _ <- b.join
    } yield c
    // f.map(r => println(getResp(r)))
    f.map(r => assert(getResp(r) == None))
  }

}
