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

trait TestListScenarios {
  implicit def cs: ContextShift[IO]

  def listsLPush(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      x <- lpush("list-1", "foo")
      _ <- IO(assert(getResp(x).get == 1))
      x <- lpush("list-1", "bar")
      _ <- IO(assert(getResp(x).get == 2))

      x <- set("anshin-1", "debasish")
      _ <- IO(assert(getBoolean(x)))
      x <- lpush("anshin-1", "bar")
      _ <- IO(assert(getResp(x).get.toString.contains("Operation against a key holding the wrong kind of value")))

      // lpush with variadic arguments
      x <- lpush("list-2", "foo", "bar", "baz")
      _ <- IO(assert(getResp(x).get == 3))
      x <- lpush("list-2", "bag", "fog")
      _ <- IO(assert(getResp(x).get == 5))
      x <- lpush("list-2", "bag", "fog")
      _ <- IO(assert(getResp(x).get == 7))

      // lpushx
      x <- lpush("list-3", "foo")
      _ <- IO(assert(getResp(x).get == 1))
      x <- lpushx("list-3", "bar")
      _ <- IO(assert(getResp(x).get == 2))

      x <- set("anshin-2", "debasish")
      _ <- IO(assert(getBoolean(x)))
      x <- lpushx("anshin-2", "bar")
      _ <- IO(assert(getResp(x).get.toString.contains("Operation against a key holding the wrong kind of value")))

    } yield ()
  }

  def listsRPush(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      x <- rpush("list-1", "foo")
      _ <- IO(assert(getResp(x).get == 1))
      x <- rpush("list-1", "bar")
      _ <- IO(assert(getResp(x).get == 2))

      x <- set("anshin-1", "debasish")
      _ <- IO(assert(getBoolean(x)))
      x <- rpush("anshin-1", "bar")
      _ <- IO(assert(getResp(x).get.toString.contains("Operation against a key holding the wrong kind of value")))

      // rpush with variadic arguments
      x <- rpush("list-2", "foo", "bar", "baz")
      _ <- IO(assert(getResp(x).get == 3))
      x <- rpush("list-2", "bag", "fog")
      _ <- IO(assert(getResp(x).get == 5))
      x <- rpush("list-2", "bag", "fog")
      _ <- IO(assert(getResp(x).get == 7))

      // rpushx
      x <- rpush("list-3", "foo")
      _ <- IO(assert(getResp(x).get == 1))
      x <- rpushx("list-3", "bar")
      _ <- IO(assert(getResp(x).get == 2))

      x <- set("anshin-2", "debasish")
      _ <- IO(assert(getBoolean(x)))
      x <- rpushx("anshin-2", "bar")
      _ <- IO(assert(getResp(x).get.toString.contains("Operation against a key holding the wrong kind of value")))

    } yield ()
  }

  def listsLlen(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      _ <- lpush("list-1", "foo")
      _ <- lpush("list-1", "bar")
      x <- llen("list-1")
      _ <- IO(assert(getResp(x).get == 2))
      x <- llen("list-2")
      _ <- IO(assert(getResp(x).get == 0))

      x <- set("anshin-2", "debasish")
      _ <- IO(assert(getBoolean(x)))
      x <- llen("anshin-2")
      _ <- IO(assert(getResp(x).get.toString.contains("Operation against a key holding the wrong kind of value")))
    } yield ()
  }

  def listsLrange(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      _ <- lpush("list-1", "6")
      _ <- lpush("list-1", "5")
      _ <- lpush("list-1", "4")
      _ <- lpush("list-1", "3")
      _ <- lpush("list-1", "2")
      _ <- lpush("list-1", "1")
      x <- llen("list-1")
      _ <- IO(assert(getResp(x).get == 6))
      x <- lrange("list-1", 0, 4)
      _ <- IO(assert(getResp(x).get == List(Some("1"), Some("2"), Some("3"), Some("4"), Some("5"))))

      // should return empty list if start > end
      _ <- lpush("list-2", "3")
      _ <- lpush("list-2", "2")
      _ <- lpush("list-2", "1")
      x <- lrange("list-2", 2, 0)
      _ <- IO(assert(getResp(x).get == List()))

      // should treat as end of list if end is over the actual end of list
      _ <- lpush("list-3", "3")
      _ <- lpush("list-3", "2")
      _ <- lpush("list-3", "1")
      x <- lrange("list-3", 0, 7)
      _ <- IO(assert(getResp(x).get == List(Some("1"), Some("2"), Some("3"))))
    } yield ()
  }

  def listsLtrim(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      _ <- lpush("list-1", "6")
      _ <- lpush("list-1", "5")
      _ <- lpush("list-1", "4")
      _ <- lpush("list-1", "3")
      _ <- lpush("list-1", "2")
      _ <- lpush("list-1", "1")
      x <- ltrim("list-1", 0, 3)
      _ <- IO(assert(getBoolean(x)))
      x <- llen("list-1")
      _ <- IO(assert(getResp(x).get == 4))

      // should should return empty list for start > end
      _ <- lpush("list-1", "6")
      _ <- lpush("list-1", "5")
      _ <- lpush("list-1", "4")
      _ <- ltrim("list-1", 6, 3)
      x <- llen("list-1")
      _ <- IO(assert(getResp(x).get == 0))

      // should treat as end of list if end is over the actual end of list
      _ <- lpush("list-1", "6")
      _ <- lpush("list-1", "5")
      _ <- lpush("list-1", "4")
      _ <- ltrim("list-1", 0, 12)
      x <- llen("list-1")
      _ <- IO(assert(getResp(x).get == 3))
    } yield ()
  }

  def listsLIndex(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      _ <- lpush("list-1", "6")
      _ <- lpush("list-1", "5")
      _ <- lpush("list-1", "4")
      _ <- lpush("list-1", "3")
      _ <- lpush("list-1", "2")
      _ <- lpush("list-1", "1")
      x <- lindex("list-1", 2)
      _ <- IO(assert(getResp(x).get == "3"))
      x <- lindex("list-1", 3)
      _ <- IO(assert(getResp(x).get == "4"))
      x <- lindex("list-1", -1)
      _ <- IO(assert(getResp(x).get == "6"))

      _ <- set("anshin-1", "debasish")
      x <- lindex("anshin-1", 0)
      _ <- IO(assert(getResp(x).get.toString.contains("Operation against a key holding the wrong kind of value")))

      // should return empty string for an index out of range
      _ <- lpush("list-2", "6")
      _ <- lpush("list-2", "5")
      _ <- lpush("list-2", "4")
      x <- lindex("list-2", 8)
      _ <- IO(assert(getResp(x) == None))
    } yield ()
  }

  def listsLSet(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      _ <- lpush("list-1", "6")
      _ <- lpush("list-1", "5")
      _ <- lpush("list-1", "4")
      _ <- lpush("list-1", "3")
      _ <- lpush("list-1", "2")
      _ <- lpush("list-1", "1")
      x <- lset("list-1", 2, "30")
      _ <- IO(assert(getBoolean(x)))
      x <- lindex("list-1", 2)
      _ <- IO(assert(getResp(x).get == "30"))

      _ <- lpush("list-2", "6")
      _ <- lpush("list-2", "5")
      _ <- lpush("list-2", "4")
      x <- lset("list-2", 12, "30")
      _ <- IO(assert(getResp(x).get.toString.contains("index out of range")))
    } yield ()
  }

  def listsLRem(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      // should remove count elements matching value from beginning
      _ <- lpush("list-1", "6")
      _ <- lpush("list-1", "hello")
      _ <- lpush("list-1", "4")
      _ <- lpush("list-1", "hello")
      _ <- lpush("list-1", "hello")
      _ <- lpush("list-1", "hello")
      x <- lrem("list-1", 2, "hello")
      _ <- IO(assert(getResp(x).get == 2))
      x <- llen("list-1")
      _ <- IO(assert(getResp(x).get == 4))

      // should remove all elements matching value from beginning
      _ <- lpush("list-2", "6")
      _ <- lpush("list-2", "hello")
      _ <- lpush("list-2", "4")
      _ <- lpush("list-2", "hello")
      _ <- lpush("list-2", "hello")
      _ <- lpush("list-2", "hello")
      x <- lrem("list-2", 0, "hello")
      _ <- IO(assert(getResp(x).get == 4))
      x <- llen("list-2")
      _ <- IO(assert(getResp(x).get == 2))

      // should remove count elements matching value from end
      _ <- lpush("list-3", "6")
      _ <- lpush("list-3", "hello")
      _ <- lpush("list-3", "4")
      _ <- lpush("list-3", "hello")
      _ <- lpush("list-3", "hello")
      _ <- lpush("list-3", "hello")
      x <- lrem("list-3", -2, "hello")
      _ <- IO(assert(getResp(x).get == 2))
      x <- llen("list-3")
      _ <- IO(assert(getResp(x).get == 4))
      x <- lindex("list-3", -2)
      _ <- IO(assert(getResp(x).get == "4"))

    } yield ()
  }

  def listsLPop(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      _ <- lpush("list-1", "6")
      _ <- lpush("list-1", "5")
      _ <- lpush("list-1", "4")
      _ <- lpush("list-1", "3")
      _ <- lpush("list-1", "2")
      _ <- lpush("list-1", "1")
      x <- lpop("list-1")
      _ <- IO(assert(getResp(x).get == "1"))
      x <- lpop("list-1")
      _ <- IO(assert(getResp(x).get == "2"))
      x <- lpop("list-1")
      _ <- IO(assert(getResp(x).get == "3"))
      x <- llen("list-1")
      _ <- IO(assert(getResp(x).get == 3))
      x <- lpop("list-2")
      _ <- IO(assert(getResp(x) == None))
    } yield ()
  }

  def listsRPop(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      _ <- lpush("list-1", "6")
      _ <- lpush("list-1", "5")
      _ <- lpush("list-1", "4")
      _ <- lpush("list-1", "3")
      _ <- lpush("list-1", "2")
      _ <- lpush("list-1", "1")
      x <- rpop("list-1")
      _ <- IO(assert(getResp(x).get == "6"))
      x <- rpop("list-1")
      _ <- IO(assert(getResp(x).get == "5"))
      x <- rpop("list-1")
      _ <- IO(assert(getResp(x).get == "4"))
      x <- llen("list-1")
      _ <- IO(assert(getResp(x).get == 3))
      x <- rpop("list-2")
      _ <- IO(assert(getResp(x) == None))
    } yield ()
  }

  def listsRPopLPush(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      _ <- rpush("list-1", "a")
      _ <- rpush("list-1", "b")
      _ <- rpush("list-1", "c")

      _ <- rpush("list-2", "foo")
      _ <- rpush("list-2", "bar")
      x <- rpoplpush("list-1", "list-2")
      _ <- IO(assert(getResp(x).get == "c"))
      x <- lindex("list-2", 0)
      _ <- IO(assert(getResp(x).get == "c"))
      x <- llen("list-1")
      _ <- IO(assert(getResp(x).get == 2))
      x <- llen("list-2")
      _ <- IO(assert(getResp(x).get == 3))

      // should rotate the list when src and dest are the same
      _ <- rpush("list-3", "a")
      _ <- rpush("list-3", "b")
      _ <- rpush("list-3", "c")
      x <- rpoplpush("list-3", "list-3")
      _ <- IO(assert(getResp(x).get == "c"))
      x <- lindex("list-3", 0)
      _ <- IO(assert(getResp(x).get == "c"))
      x <- lindex("list-3", 2)
      _ <- IO(assert(getResp(x).get == "b"))
      x <- llen("list-3")
      _ <- IO(assert(getResp(x).get == 3))

      // should give None for non-existent key
      x <- rpoplpush("list-4", "list-5")
      _ <- IO(assert(getResp(x) == None))
      x <- rpush("list-4", "a")
      _ <- IO(assert(getResp(x).get == 1))
      x <- rpush("list-4", "b")
      _ <- IO(assert(getResp(x).get == 2))
      x <- rpoplpush("list-4", "list-5")
      _ <- IO(assert(getResp(x).get == "b"))
    } yield ()
  }

  def listsLPushPopWithNL(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      // lpush with newlines
      x <- lpush("list-1", "foo\nbar\nbaz")
      _ <- IO(assert(getResp(x).get == 1))
      x <- lpush("list-1", "bar\nfoo\nbaz")
      _ <- IO(assert(getResp(x).get == 2))
      x <- lpop("list-1")
      _ <- IO(assert(getResp(x).get == "bar\nfoo\nbaz"))
      x <- lpop("list-1")
      _ <- IO(assert(getResp(x).get == "foo\nbar\nbaz"))
    } yield ()
  }

  def listsLPushPopWithArrayBytes(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      x <- lpush("list-1", "foo\nbar\nbaz".getBytes("UTF-8"))
      _ <- IO(assert(getResp(x).get == 1))
      x <- lpop("list-1")
      _ <- IO(assert(getResp(x).get == "foo\nbar\nbaz"))
    } yield ()
  }

  def listsBRPoplPush(cmd: RedisClient[IO]): IO[Unit] = {
    import cmd._
    for {
      _ <- rpush("list-1", "a")
      _ <- rpush("list-1", "b")
      _ <- rpush("list-1", "c")

      _ <- rpush("list-2", "foo")
      _ <- rpush("list-2", "bar")
      x <- brpoplpush("list-1", "list-2", 2)
      _ <- IO(assert(getResp(x).get == "c"))
      x <- lindex("list-2", 0)
      _ <- IO(assert(getResp(x).get == "c"))
      x <- llen("list-1")
      _ <- IO(assert(getResp(x).get == 2))
      x <- llen("list-2")
      _ <- IO(assert(getResp(x).get == 3))

      // should rotate the list when src and dest are the same
      _ <- rpush("list-3", "a")
      _ <- rpush("list-3", "b")
      _ <- rpush("list-3", "c")
      x <- brpoplpush("list-3", "list-3", 2)
      _ <- IO(assert(getResp(x).get == "c"))
      x <- lindex("list-3", 0)
      _ <- IO(assert(getResp(x).get == "c"))
      x <- lindex("list-3", 2)
      _ <- IO(assert(getResp(x).get == "b"))
      x <- llen("list-3")
      _ <- IO(assert(getResp(x).get == 3))

      // should time out and give None for non-existent key
      x <- brpoplpush("test-1", "test-2", 2)
      _ <- IO(assert(getResp(x) == None))
      _ <- rpush("test-1", "a")
      _ <- rpush("test-1", "b")
      x <- brpoplpush("test-1", "test-2", 2)
      _ <- IO(assert(getResp(x).get == "b"))
    } yield ()
  }

  def listsBRPoplPushWithBlockingPop(cmds: (RedisClient[IO], RedisClient[IO])): IO[Unit] = {
    val cmd1 = cmds._1
    val cmd2 = cmds._2
    val r1 = for {
      x <- cmd1.brpoplpush("l1", "l2", 3)
      y <- cmd1.lpop("l2")
    } yield (x, y)

    val r2 = for {
      z <- cmd2.llen("l1")
      _ <- IO(assert(getResp(z).get == 0))
      _ <- cmd2.lpush("l1", "a")
    } yield ()

    r2 *> r1.flatMap { r =>
      val r1 = r._1
      val r2 = r._2
      IO(assert(getResp(r1).get == "a"))
      IO(assert(getResp(r2).get == "a"))
    }
  }

  def listsBLPop(cmds: (RedisClient[IO], RedisClient[IO])): IO[Unit] = {
    val cmd1 = cmds._1
    val cmd2 = cmds._2
    val r1 = for {
      x <- cmd1.blpop(3, "l1", "l2")
    } yield x

    val r2 = for {
      z <- cmd2.llen("l1")
      _ <- IO(assert(getResp(z).get == 0))
      _ <- cmd2.lpush("l1", "a")
    } yield ()

    // start r1 and r2 in fibers and then
    // then join : r1 blocks but then gets
    // the valie as soon as r2 ends
    val f = for {
      a <- r1.start
      b <- r2.start
      c <- a.join
      _ <- b.join
    } yield c

    f.map(r => assert(getResp(r) == Some(("l1", "a"))))
  }
}
