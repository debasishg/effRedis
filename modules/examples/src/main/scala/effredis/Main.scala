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
import cats.syntax.all._
import log4cats._

object Main extends LoggerIOApp {
  override def run(args: List[String]): IO[ExitCode] =
    program.as(ExitCode.Success)

  val program: IO[Unit] =
    RedisClient.single[IO](new URI("http://localhost:6379")).use { cmd =>
      import cmd._

      // just 1 command
      val oneCommand = for {
        x <- set("k1", "v1")
        _ <- IO(println(x))
      } yield ()

      // List of commands
      val listCommands1 = for {
        x <- List(set("k1", "v1"), get("k1")).sequence
        _ <- IO(println(x))
      } yield ()

      val listCommands2 = for {
        x <- List(set("k1", "v1"), get("k1"), set("k2", 100), incrby("k2", 12)).sequence
        _ <- IO(println(x))
      } yield ()

      // Use as applicative
      case class Foo(str: String, num: Long)

      val asApplicative = (set("k1", "v1"), set("k2", 100), get("k1"), incrby("k2", 12)).mapN { (_, _, k1val, k2val) =>
        (k1val, k2val) match {
          case (Value(Some(k1)), Value(k2)) => println(Foo(k1, k2))
          case err                          => println(s"Error $err")
        }
      }

      // monadic
      val monadic = for {

        a <- set("k1", "v1")
        b <- set("k2", "v2")
        c <- get("k1")
        _ <- flushall
        _ <- IO(println(s"$a $b $c"))

      } yield ()

      // on a separate ContextShift
      val resl = for {

        a <- set("k1", "v1")
        b <- set("k2", "v2")
        c <- get("k1")
        _ <- flushall

      } yield (a, b, c)

      val onSeparateContextShift = Blocker[IO].use { blocker =>
        for {
          a <- blocker.blockOn(resl)
          _ <- IO(println(a))
        } yield ()
      }

      // monadic with fail
      val monadicWithFail = for {

        a <- set("k1", "vnew")
        b <- set("k2", "v2")
        c <- lpop("k1")
        d <- get("k1")
        _ <- IO(println(List(a, b, c, d)))

      } yield ()

      // applicative
      val applicativeWithFailure = (
        set("k1", "vnew"),
        set("k2", "v2"),
        lpop("k1"),
        get("k1")
      ).mapN((a, b, c, d) => println(List(a, b, c, d)))

      oneCommand >>
        listCommands1 >>
        listCommands2 >>
        asApplicative >>
        monadic >>
        onSeparateContextShift >>
        monadicWithFail >>
        applicativeWithFailure
    }
}
