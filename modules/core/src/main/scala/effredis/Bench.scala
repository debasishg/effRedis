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

object Bench extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    RedisClient.makeWithURI[IO](new java.net.URI("http://localhost:6379")).use { cmd =>
      import cmd._

      val nKeys = 100000
      val s     = System.currentTimeMillis()
      val x = (0 to nKeys).map { i =>
        for {
          a <- set(s"ley$i", s"debasish ghosh $i")
        } yield a
      }
      x.foreach(_.unsafeRunSync)
      /*
      for (i <- 0 to nKeys) {
        val r = for {
          a <- set(s"key$i", s"debasish ghosh $i")
        } yield a
        r.unsafeRunSync()
      }
       */
      val timeElapsedSet = (System.currentTimeMillis() - s) / 1000
      println(s"Time elapsed in setting $nKeys keys = $timeElapsedSet seconds")
      println(s"Rate = ${nKeys / timeElapsedSet} sets per second")

      val t = System.currentTimeMillis()
      for (i <- 0 to 10000) {
        val r = for {
          a <- get(s"key$i")
        } yield a
        r.unsafeRunSync()
      }
      val timeElapsed = System.currentTimeMillis() - t
      println(s"Time elapsed in getting 10000 values = $timeElapsed")

      IO(ExitCode.Success)
    }
}
