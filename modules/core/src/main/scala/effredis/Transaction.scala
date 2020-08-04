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

object Transaction extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    RedisClient.makeWithURI[IO](new URI("http://localhost:6379")).use { parent =>
      RedisClient.makePipelineClientWithURI[IO](parent).use { c =>
        import c._

        val r1 = parent.transaction(c) { _ =>
          val t1 =
            (
              set("k11", "v11"),
              set("k12", true),
              lpop("k12"),
              get("k12")
            ).mapN { case (a, b, c, d) => List(a, b, c, d) }

          val t2 =
            (
              set("k13", "v13"),
              get("k13")
            ).mapN { case (a, b) => List(a, b) }

          t1.combine(t2)
        }

        r1.unsafeRunSync() match {

          case Right(Right(ls)) => ls.foreach(println)
          case Left(err)        => println(err)
          case _                => println("oops!")

        }
        IO(ExitCode.Success)
      }
    }
}
