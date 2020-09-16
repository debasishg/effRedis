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
import log4cats._

object Transaction extends LoggerIOApp {

  override def run(args: List[String]): IO[ExitCode] =
    RedisClient.transact[IO](new URI("http://localhost:6379")).use { cli =>
      normalTransaction(cli)
      discardedTransaction(cli)
      IO(ExitCode.Success)
    }

  def normalTransaction(cli: RedisClient[IO, RedisClient.TRANSACT.type]) = {
    val r1 = RedisClient.transaction(cli) {
      import cli._
      for {
        _ <- set("k1", "v1")
        _ <- set("k2", 100)
        _ <- incrby("k2", 12)
        _ <- get("k1")
        _ <- get("k2")
        _ <- lpop("k1")
      } yield ()
    }
    r1.unsafeRunSync() match {

      case Value(ls)        => ls.foreach(println)
      case TxnDiscarded(cs) => println(s"Transaction discarded $cs")
      case Error(err)       => println(s"oops! $err")
      case err              => println(err)
    }
  }

  def discardedTransaction(cli: RedisClient[IO, RedisClient.TRANSACT.type]) = {
    val r1 = RedisClient.transaction(cli) {
      import cli._
      for {
        _ <- set("k2", 100)
        _ <- incrby("k2", 12)
        _ <- discard
        _ <- get("k2")
      } yield ()
    }
    r1.unsafeRunSync() match {

      case Value(ls)        => ls.foreach(println)
      case TxnDiscarded(cs) => println(s"Transaction discarded $cs")
      case Error(err)       => println(s"oops! $err")
      case err              => println(err)
    }
  }
}
