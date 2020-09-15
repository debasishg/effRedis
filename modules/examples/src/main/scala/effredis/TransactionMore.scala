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

object TransactionMore extends LoggerIOApp {

  // transactional part
  def program(i: Int)(c: RedisClient[IO, RedisClient.TRANSACT.type]): IO[Resp[Option[String]]] = {
    import c._
    for {
      _ <- set("k2", i)
      _ <- incrby("k2", 12)
      _ <- IO(Thread.sleep(3))
      x <- get("k2")
    } yield x
  }

  // change the value of the key being watched
  def setKey(c: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] =
    for {
      _ <- c.set("k2", 2000)
    } yield (())

  // helper function
  def getResp[T](resp: Resp[T]): IO[T] = resp match {
    case Value(v) => IO(v)
    case x        => IO(throw new Exception(s"boom $x"))
  }

  override def run(args: List[String]): IO[ExitCode] = {
    // acquire connections: we need 2 to demonstrate how watch works
    val uri = new URI("http://localhost:6379")
    val conns = for {
      single <- RedisClient.single[IO](uri)
      transact <- RedisClient.transact[IO](uri)
    } yield (single, transact)

    val r = conns.use {
      case (s, t) =>
        // transaction + some pre-processing
        // note we can use the same connection though it is in TRANSACT mode
        val tr = for {
          _ <- t.set("k2", 200)
          v <- t.get[Int]("k2")(codecs.Format.default, codecs.Parse.Implicits.parseInt)
          x <- getResp(v)
          t <- RedisClient.transaction(t)(program(x.get))
        } yield t

        // final block:
        // 1. set up the watch
        // 2. run transaction in a separate fiber
        // 3. change the key being watched
        val res = for {
          _ <- t.watch("k2")
          y <- tr.start
          _ <- setKey(s)
          x <- y.join
        } yield x

        res.flatMap {
          case Value(v) => IO(println(s"Got value $v")) // Will get Value(None) since watched key changed
          case x        => IO(println(s"Error $x"))
        }
    }
    r.unsafeRunSync()
    IO(ExitCode.Success)
  }
}
