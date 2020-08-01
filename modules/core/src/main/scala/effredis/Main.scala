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

object Main extends IOApp {
  override def run(args: List[String]): IO[ExitCode] =
    RedisClient.makeWithURI[IO](new URI("http://localhost:6379")).use { cmd =>
      import cmd._

      val result = for {

        _ <- set("key1", "debasish ghosh")
        _ <- set("key2", 100)
        _ <- set("key3", true)
        d <- get("key2")
        p <- incrby("key2", 12)
        a <- mget("key1", "key2", "key3")
        l <- lpush("list1", "debasish", "paramita", "aarush")

      } yield (d, p, a, l)

      // println(result.unsafeRunSync())
      result.unsafeRunAsync {
        case Left(ex)    => ex.printStackTrace
        case Right(vals) => println(vals)
      }
      Thread.sleep(1000)
      IO(ExitCode.Success)
    }
}
