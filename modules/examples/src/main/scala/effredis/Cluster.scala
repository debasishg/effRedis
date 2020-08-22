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
import cats.effect._
import log4cats._

object Cluster extends LoggerIOApp {
  override def run(args: List[String]): IO[ExitCode] =
    RedisClusterClient.make(new URI("http://127.0.0.1:7000")).use { cl =>
      println(cl)

      val res = for {
        _ <- cl.set("k1", "v1")
        y <- cl.get("k1")
      } yield y
      println(res.unsafeRunSync())

      IO(ExitCode.Success)
    }
}