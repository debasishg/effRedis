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

import cats.effect._
import dev.profunktor.redis4cats.Redis
import dev.profunktor.redis4cats.effect.Log.Stdout._

object Bench4cats extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    Redis[IO].utf8("redis://localhost").use { cmd =>
      val nKeys = 100000
      val s     = System.currentTimeMillis()
      for (i <- 0 to nKeys) {
        val x = for {
          _ <- cmd.set(s"key-$i", s"debasish ghosh $i ")
        } yield ()
        x.unsafeRunSync()
      }
      val timeElapsedSet = (System.currentTimeMillis() - s) / 1000
      println(s"Time elapsed in setting $nKeys keys = $timeElapsedSet seconds")
      println(s"Rate = ${nKeys / timeElapsedSet} sets per second")
      IO(ExitCode.Success)
    }
}
