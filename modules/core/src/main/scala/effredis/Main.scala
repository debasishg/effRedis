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

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    RedisClient.make[IO](new URI("http://localhost:6379")).use { cmd =>
      import cmd._

      /*
      val result = for {

        a <- set("k1", "v1")
        b <- set("k2", "v2")
        c <- lpop("k1")

      } yield (a, b, c)
       */

      val r = (set("k1", "v1"), get("k1"), lpop("k1")).mapN((a, b, c) => List(a, b, c))
      // val r = (set("k1", "v1"), get("k1")).mapN{ (a, b) => List(a, b)}
      println(r.unsafeRunSync()) // .unsafeRunSync())

      // println(result.unsafeRunSync())
      // result.unsafeRunAsync {
      // case Left(ex)    => ex.printStackTrace
      // case Right(vals) => println(vals)
      // }
      IO(ExitCode.Success)
    }
}
