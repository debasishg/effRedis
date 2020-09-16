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

trait TestScriptsScenarios {
  implicit def cs: ContextShift[IO]

  final def luaEvalWithReply(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    for {
      x <- evalBulk[String]("return 'val1';", List(), List())
      _ <- IO(assert(getResp(x).get == "val1"))

      x <- evalMultiBulk[String]("return { 'val1','val2' };", List(), List())
      _ <- IO(assert(getResp(x).get == List(Some("val1"), Some("val2"))))

      x <- evalMultiBulk[String]("return { ARGV[1],ARGV[2] };", List(), List("a", "b"))
      _ <- IO(assert(getResp(x).get == List(Some("a"), Some("b"))))

      x <- evalMultiBulk[String]("return { KEYS[1],KEYS[2],ARGV[1],ARGV[2] };", List("a", "b"), List("a", "b"))
      _ <- IO(assert(getResp(x).get == List(Some("a"), Some("b"), Some("a"), Some("b"))))

      _ <- lpush("z", "a")
      _ <- lpush("z", "b")
      x <- evalMultiBulk[String]("return redis.call('lrange', KEYS[1], 0, 1);", List("z"), List())
      _ <- IO(assert(getResp(x).get == List(Some("b"), Some("a"))))

    } yield ()
  }

  final def luaEvalSha(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    val setname = "records";

    val luaCode = """
	    local res = redis.call('ZRANGEBYSCORE', KEYS[1], 0, 100, 'WITHSCORES')
	    return res
    """

    // should evalsha lua code hash and execute script when passing keys
    for {
      _ <- zadd(setname, 10, "mmd")
      _ <- zadd(setname, 22, "mmc")
      _ <- zadd(setname, 12.5, "mma")
      _ <- zadd(setname, 14, "mem")
      x <- scriptLoad(luaCode)
      y <- evalMultiSHA[String](getResp(x).map(_.toString).get, List("records"), List())
      _ <- IO(
            assert(
              getResp(y).get == List(
                    Some("mmd"),
                    Some("10"),
                    Some("mma"),
                    Some("12.5"),
                    Some("mem"),
                    Some("14"),
                    Some("mmc"),
                    Some("22")
                  )
            )
          )

    } yield ()
  }

  final def luaEvalShaInt(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._

    // should evalsha lua code hash and return the integer result
    for {
      sha <- scriptLoad("return 1;")
      x <- evalSHA(getResp(sha).map(_.toString).get, List(), List())
      _ <- IO(assert(getResp(x).get == 1))

    } yield ()
  }

  final def luaEvalShaDouble(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._

    // should evalsha lua code hash and return the integer result
    for {
      sha <- scriptLoad("return 1.5")
      x <- evalSHA(getResp(sha).map(_.toString).get, List(), List())
      _ <- IO(assert(getResp(x).get == 1))

    } yield ()
  }

  final def luaEvalMultiShaString(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    import codecs.Parse.Implicits.parseString

    // should evalsha lua code hash and return the list of strings result
    for {
      sha <- scriptLoad("return {'1', '2'}")
      x <- evalMultiSHA(getResp(sha).map(_.toString).get, List(), List())
      _ <- IO(assert(getResp(x).get == List(Some("1"), Some("2"))))

    } yield ()
  }

  final def luaEvalMultiShaInt(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    import codecs.Parse.Implicits.parseInt

    // should evalsha lua code hash and return the list of integer result
    for {
      sha <- scriptLoad("return {'1', '2'}")
      x <- evalMultiSHA(getResp(sha).map(_.toString).get, List(), List())
      _ <- IO(assert(getResp(x).get == List(Some(1), Some(2))))

    } yield ()
  }

  final def luaScriptExists(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._

    val luaCode = """
	        local res = redis.call('ZRANGEBYSCORE', KEYS[1], 0, 100, 'WITHSCORES')
	        return res
	        """
    for {
      x <- scriptLoad(luaCode)
      y <- scriptExists(getResp(x).map(_.toString).get)
      _ <- IO(assert(getResp(y).get == List(Some(1))))

    } yield ()
  }

  final def luaScriptFlush(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    val luaCode = """
	        local res = redis.call('ZRANGEBYSCORE', KEYS[1], 0, 100, 'WITHSCORES')
	        return res
	        """
    for {
      x <- scriptLoad(luaCode)
      y <- scriptFlush
      _ <- IO(assert(getBoolean(y)))
      y <- scriptExists(getResp(x).map(_.toString).get)
      _ <- IO(assert(getResp(y).get == List(Some(0))))

    } yield ()
  }

  final def luaExec(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._

    val luaCode = """
          if redis.call("EXISTS", KEYS[1]) == 1 then
            local res = {}
            local payload = redis.call("LRANGE", KEYS[1], 0, -1)
            local row = cjson.decode(payload[1])
            res[1] = row["source"]
            return #res
          else
            return -1
          end
	      """

    for {
      _ <- lpush(
            "content",
            "{\"source\": \"output1.txt\", \"col1\": \"water_pressure\", \"col2\": \"sunday\", \"col3\": \"december\"}"
          )
      _ <- lpush(
            "content",
            "{\"source\": \"output1.txt\", \"col1\": \"air_pressure\", \"col2\": \"saturday\", \"col3\": \"november\"}"
          )
      _ <- lpush(
            "content",
            "{\"source\": \"output2.txt\", \"col1\": \"air_pressure\", \"col2\": \"saturday\", \"col3\": \"november\"}"
          )
      x <- evalInt(luaCode, List("content"), List())
      _ <- IO(assert(getResp(x).get == 1))

    } yield ()
  }
}
