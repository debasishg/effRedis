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

object PBench extends LoggerIOApp {
  def setter(keyPrefix: String, valPrefix: String) =
    RedisClient.make[IO](new java.net.URI("http://localhost:6379")).use { cmd =>
      val nKeys = 12500
      (0 to nKeys)
        .map { i =>
          for {
            a <- cmd.set(s"$keyPrefix$i", s"$valPrefix $i")
          } yield a
        }
        .toList
        .sequence
    }

  override def run(args: List[String]): IO[ExitCode] = {

    val s = System.currentTimeMillis()

    val res = for {
      s1 <- setter("key-1", "value-1").start
      s2 <- setter("key-2", "value-2").start
      s3 <- setter("key-3", "value-3").start
      s4 <- setter("key-4", "value-4").start
      s5 <- setter("key-5", "value-5").start
      s6 <- setter("key-6", "value-6").start
      s7 <- setter("key-7", "value-7").start
      s8 <- setter("key-8", "value-8").start
      j1 <- s1.join
      j2 <- s2.join
      j3 <- s3.join
      j4 <- s4.join
      j5 <- s5.join
      j6 <- s6.join
      j7 <- s7.join
      j8 <- s8.join
    } yield List(j1, j2, j3, j4, j5, j6, j7, j8)
    res.unsafeRunSync()

    val timeElapsedSet = (System.currentTimeMillis() - s) / 1000
    println(s"Time elapsed in setting 100000 keys = $timeElapsedSet seconds")
    println(s"Rate = ${1000000 / timeElapsedSet} sets per second")

    // Time elapsed in setting 100000 keys = 2 seconds
    // Rate = 500000 sets per second

    IO(ExitCode.Success)
  }
}
