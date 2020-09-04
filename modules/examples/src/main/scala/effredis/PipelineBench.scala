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
import cats.syntax.all._
import log4cats._

object PipelineBench extends LoggerIOApp {

  def setupPipeline(keyPrefix: String, valPrefix: String): IO[Resp[Option[List[Any]]]] =
    RedisClient.pipe[IO](new java.net.URI("http://localhost:6379")).use { cli =>
      val nKeys = 12500
      RedisClient.pipeline(cli)(c => (0 to nKeys).map(i => c.set(s"$keyPrefix$i", s"$valPrefix $i")).toList.sequence)
    }

  override def run(args: List[String]): IO[ExitCode] = {

    val s = System.currentTimeMillis()

    val res = (0 to 7).map(i => setupPipeline(s"key-$i", s"value-$i")).toList.sequence
    res.unsafeRunSync()

    val timeElapsedSet = (System.currentTimeMillis() - s) / 1000
    println(s"Time elapsed in setting 100000 keys = $timeElapsedSet seconds")
    println(s"Rate = ${100000 / timeElapsedSet} sets per second")

    // Time elapsed in setting 100000 keys = 1 seconds
    // Rate = 100000 sets per second

    IO(ExitCode.Success)
  }
}
