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

object Pipeline extends LoggerIOApp {

  // pipeline formation
  def program(c: RedisClient[IO, RedisClient.PIPE.type]): IO[Unit] =
    for {
      _ <- c.set("k1", "v1")
      _ <- c.get("k1")
      _ <- c.set("k2", 100)
      _ <- c.incrby("k2", 12)
      _ <- c.get("k2")
    } yield ()

  // another pipeline formation
  def program2(pcli: RedisClient[IO, RedisClient.PIPE.type]) =
    (
      pcli.set("k1", "v1"),
      pcli.get("k1"),
      pcli.set("k2", 100),
      pcli.incrby("k2", 12),
      pcli.get("k2")
    ).tupled

  override def run(args: List[String]): IO[ExitCode] =
    RedisClient.pipe[IO](new URI("http://localhost:6379")).use { cli =>
      import cli._

      val res = for {
        r1 <- RedisClient.pipeline(cli)(program)
        r2 <- RedisClient.pipeline(cli)(program2)
      } yield (r1, r2)

      println(res.unsafeRunSync())
      IO(ExitCode.Success)
    }
}
