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
import shapeless.HNil
import cats.effect._
import log4cats._

object HPipeline extends LoggerIOApp {
  override def run(args: List[String]): IO[ExitCode] =
    RedisClient.make[IO](new URI("http://localhost:6379")).use { cli =>
      RedisClient.pipe[IO](cli).use { txnClient =>
        import txnClient._

        val cmds = { () =>
          set("k1", "v1") ::
            get("k1") ::
            set("k2", 100) ::
            incrby("k2", 12) ::
            get("k2") ::
            HNil
        }

        val r = cli.hpipeline(txnClient)(cmds)

        println(r.unsafeRunSync())
        IO(ExitCode.Success)
      }
    }
}
