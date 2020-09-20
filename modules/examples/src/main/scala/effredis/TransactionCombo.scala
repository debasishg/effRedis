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

object TransactionCombo extends LoggerIOApp {

  override def run(args: List[String]): IO[ExitCode] =
    RedisClient.transact[IO](new URI("http://localhost:6379")).use { cli =>
      // one normal transaction
      normalTransaction(cli)
      // another normal transaction
      normalTxn(cli)
      // discarded transaction
      discardedTransaction(cli)
      // no transaction
      val r = for {
        x <- cli.set("k3", 100)
        y <- cli.incrby("k3", 450)
        z <- cli.get[Int]("k3")(codecs.Format.default, codecs.Parse.Implicits.parseInt)
      } yield (x, y, z)
      println(r.unsafeRunSync())
      IO(ExitCode.Success)
    }

  def normalTransaction(cli: RedisClient[IO, RedisClient.TRANSACT.type]) = {
    val r1 = RedisClient.transaction(cli) {
      import cli._
      (
        set("k1", "v1"),
        set("k2", 100),
        incrby("k2", 12),
        get("k1"),
        get("k2"),
        lpop("k1")
      ).tupled
    }
    r1.unsafeRunSync() match {

      case Value(ls)            => println(ls)
      case TransactionDiscarded => println("Transaction discarded")
      case Error(err)           => println(s"oops! $err")
      case err                  => println(err)
    }
  }

  def normalTxn(cli: RedisClient[IO, RedisClient.TRANSACT.type]) = {
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

      case Value(ls)            => println(ls)
      case TransactionDiscarded => println("Transaction discarded")
      case Error(err)           => println(s"oops! $err")
      case err                  => println(err)
    }
  }

  def discardedTransaction(cli: RedisClient[IO, RedisClient.TRANSACT.type]) = {
    val r1 = RedisClient.transaction(cli) {
      import cli._
      (
        set("k2", 100),
        incrby("k2", 12),
        discard,
        get("k2")
      ).tupled
    }
    r1.unsafeRunSync() match {

      case Value(ls)            => println(ls)
      case TransactionDiscarded => println("Transaction discarded")
      case Error(err)           => println(s"oops! $err")
      case err                  => println(err)
    }
  }
}
