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

package effredis.cluster

import java.net.URI
import scala.concurrent.duration._

import io.chrisdavenport.keypool._
import cats.effect._
import cats.implicits._
import effredis.{ Log, RedisClient }

case class RedisClientPool[F[+_]: Concurrent: ContextShift: Log: Timer]()

object RedisClientPool {
  def poolResource[F[+_]: Concurrent: ContextShift: Log: Timer]: Resource[F, KeyPool[F, URI, RedisClient[F]]] =
    KeyPoolBuilder[F, URI, RedisClient[F]](
      { uri: URI => F.debug(s"Building client for $uri") *> RedisClient.build(uri) }, { client: RedisClient[F] =>
        F.debug(s"Closing client for ${client.host}:${client.port}") *> client.close().pure[F]
      }
    ).withDefaultReuseState(Reusable.Reuse)
      .withIdleTimeAllowedInPool(Duration.Inf)
      .withMaxPerKey(Function.const(4))
      .withMaxTotal(10)
      .withOnReaperException { _: Throwable => F.unit }
      .build
}
