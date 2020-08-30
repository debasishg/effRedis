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

import util.Cached
import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._

import effredis.{ Log, RedisClient }

final case class RedisClusterClient[F[+_]: Concurrent: ContextShift: Log: Timer] private (
    // collection of initial seed uris for the cluster
    // try sequentially till one of them works
    seedURIs: NonEmptyList[URI],
    topologyCache: Cached[F, ClusterTopology]
) extends RedisClusterOps[F] {

  def conc: cats.effect.Concurrent[F]  = implicitly[Concurrent[F]]
  def ctx: cats.effect.ContextShift[F] = implicitly[ContextShift[F]]
  def l: Log[F]                        = implicitly[Log[F]]
}

object RedisClusterClient {

  def make[F[+_]: Concurrent: ContextShift: Log: Timer](
      seedURIs: NonEmptyList[URI]
  ): F[RedisClusterClient[F]] =
    RedisClient.single(seedURIs).flatMap {
      _.use { cl =>
        Cached
          .create[F, ClusterTopology](ClusterTopology.create[F](cl))
          .flatMap(cachedTopology => F.delay(new RedisClusterClient[F](seedURIs, cachedTopology)))
      }
    }
}
