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
// import java.util.concurrent._

// import scala.concurrent._
import scala.concurrent.duration._
import util.Cached
// import util.ClusterUtils

import io.chrisdavenport.keypool._
import effredis.{ Log, RedisClient }
import cats.effect._
import cats.implicits._
// import cats.effect.implicits._

final case class RedisClusterClient[F[+_]: Concurrent: ContextShift: Log: Timer] private (
    // need to make this a collection and try sequentially till
    // one of them works
    seedURI: URI,
    topologyCache: Cached[F, ClusterTopology],
    pool: KeyPool[F, URI, (RedisClient[F], F[Unit])]
) extends RedisClusterOps[F] {

  def conc: cats.effect.Concurrent[F]  = implicitly[Concurrent[F]]
  def ctx: cats.effect.ContextShift[F] = implicitly[ContextShift[F]]
  def l: Log[F]                        = implicitly[Log[F]]
}

object RedisClusterClient {

  def make[F[+_]: Concurrent: ContextShift: Log: Timer](
      seedURI: URI,
      topologyRefreshInterval: Duration = Duration.Inf
  ): F[RedisClusterClient[F]] = {
    println(topologyRefreshInterval)

    RedisClient.make(seedURI).use { cl =>
      println(s"After redis client make for $seedURI")
      RedisClientPool.poolResource[F].use { pool =>
        Cached
          .create[F, ClusterTopology](ClusterTopology.create[F](cl))
          .flatMap(cachedTopology => (new RedisClusterClient[F](seedURI, cachedTopology, pool)).pure[F])
      }
    }
  }
}
