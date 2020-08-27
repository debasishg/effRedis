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
package cluster

import java.net.URI
import scala.concurrent.duration._
import cats.effect._
import cats.implicits._
import log4cats._

object Cluster extends LoggerIOApp {

  val nKeys = 4000
  def program: IO[Unit] =
    RedisClusterClient.make[IO](new URI("http://localhost:7000"), 10.seconds).flatMap { cl =>
      RedisClientPool.poolResource[IO].use { pool =>
        implicit val p = pool
        for {
          _ <- (0 to nKeys)
                .map { i =>
                  // cl.set(s"ley$i", s"debasish ghosh $i") *> IO(println(s"State ${cl.pool.state.unsafeRunSync()}"))
                  cl.set(s"ley$i", s"debasish ghosh $i")
                }
                .toList
                .sequence
        } yield ()
      }
    }
  override def run(args: List[String]): IO[ExitCode] = {
    println(program.unsafeRunSync())
    IO(ExitCode.Success)
  }
}
