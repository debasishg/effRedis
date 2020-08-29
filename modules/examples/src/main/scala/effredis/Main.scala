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

import java.net.URI
import cats.effect._
import cats.implicits._
import log4cats._

object Main extends LoggerIOApp {
  override def run(args: List[String]): IO[ExitCode] =
    RedisClient.single[IO](new URI("http://localhost:6379")).use { cmd =>
      import cmd._

      // just 1 command
      println(set("k1", "v1").unsafeRunSync())

      // List of commands
      println(List(set("k1", "v1"), get("k1")).sequence.unsafeRunSync())
      println(List(set("k1", "v1"), get("k1"), set("k2", 100), incrby("k2", 12)).sequence.unsafeRunSync())

      // Use as applicative
      case class Foo(str: String, num: Long)

      val res = (set("k1", "v1"), set("k2", 100), get("k1"), incrby("k2", 12)).mapN { (_, _, k1val, k2val) =>
        (k1val, k2val) match {
          case (Value(Some(k1)), Value(Some(k2))) => Foo(k1, k2)
          case err                                => println(s"Error $err")
        }
      }
      println(res.unsafeRunSync())

      // monadic
      val result = for {

        a <- set("k1", "v1")
        b <- set("k2", "v2")
        c <- get("k1")
        _ <- flushall

      } yield (a, b, c)

      println(result.unsafeRunSync())

      // monadic with fail
      val rsult = for {

        a <- set("k1", "vnew")
        b <- set("k2", "v2")
        c <- lpop("k1")
        d <- get("k1")

      } yield List(a, b, c, d)

      println(rsult.unsafeRunSync())

      // applicative
      val rs = (
        set("k1", "vnew"),
        set("k2", "v2"),
        lpop("k1"),
        get("k1")
      ).mapN((a, b, c, d) => List(a, b, c, d))

      println(rs.unsafeRunSync())

      IO(ExitCode.Success)
    }
}
