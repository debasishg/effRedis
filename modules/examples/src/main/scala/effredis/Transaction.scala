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
  def program(c: RedisClient[IO, RedisClient.TRANSACT.type]): IO[Unit] =
    for {
      _ <- c.set("k1", "v1")
      _ <- c.set("k2", 100)
      _ <- c.incrby("k2", 12)
      _ <- c.get("k1")
      _ <- c.get("k2")
      _ <- c.lpop("k1")
    } yield ()

  override def run(args: List[String]): IO[ExitCode] =
    RedisClient.transact[IO](new URI("http://localhost:6379")).use { cli =>
      val r1 = RedisClient.transaction(cli)(program)
      r1.unsafeRunSync() match {

        case Value(ls)        => ls.foreach(println)
        case TxnDiscarded(cs) => println(s"Transaction discarded $cs")
        case Error(err)       => println(s"oops! $err")
        case err              => println(err)
      }
      IO(ExitCode.Success)
    }
}
