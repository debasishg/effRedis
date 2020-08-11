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
import cats.implicits._
import log4cats._

object Bench extends LoggerIOApp {
  override def run(args: List[String]): IO[ExitCode] =
    RedisClient.make[IO](new java.net.URI("http://localhost:6379")).use { cmd =>
      import cmd._

      val nKeys = 1000000
      val s     = System.currentTimeMillis()
      val x = (0 to nKeys).map { i =>
        for {
          a <- set(s"key$i", s"debasish ghosh $i")
        } yield a
      }.toList.sequence
      x.unsafeRunSync()

      val timeElapsedSet = (System.currentTimeMillis() - s) / 1000
      println(s"Time elapsed in setting $nKeys keys = $timeElapsedSet seconds")
      println(s"Rate = ${nKeys / timeElapsedSet} sets per second")

      val t = System.currentTimeMillis()
      val y = (0 to nKeys).map { i =>
        for {
          a <- get(s"key$i")
        } yield a
      }.toList.sequence
      y.unsafeRunSync()

      val timeElapsed = (System.currentTimeMillis() - t) / 1000
      println(s"Time elapsed in getting $nKeys values = $timeElapsed seconds")
      println(s"Rate = ${nKeys / timeElapsed} gets per second")

      IO(ExitCode.Success)
    }
}
