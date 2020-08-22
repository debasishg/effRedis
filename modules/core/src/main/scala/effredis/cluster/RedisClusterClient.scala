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

import effredis.{ Log, RedisBlocker, RedisClient }
import cats.effect._
import cats.implicits._

final case class RedisClusterClient[F[+_]: Concurrent: ContextShift: Log] private (
    seedURI: URI,
    topology: List[RedisClusterNode[F]]
) extends RedisClusterOps[F]
    with BaseOps[F]
    with StringOps[F]
    with ListOps[F]
    with SetOps[F]
    with HashOps[F] {

  def conc: cats.effect.Concurrent[F]  = implicitly[Concurrent[F]]
  def ctx: cats.effect.ContextShift[F] = implicitly[ContextShift[F]]
  def l: Log[F]                        = implicitly[Log[F]]
}

object RedisClusterClient {

  private[effredis] def acquireAndRelease[F[+_]: Concurrent: ContextShift: Log](
      seedURI: URI,
      blocker: Blocker
  ): Resource[F, RedisClusterClient[F]] = {

    val acquire: F[RedisClusterClient[F]] = {
      F.info(s"Acquiring cluster client with sample seed URI $seedURI") *> {
        blocker.blockOn {
          RedisClient.make(seedURI).use(cl => ClusterTopology.create(cl).map(new RedisClusterClient(seedURI, _)))
        }
      }
    }

    val release: RedisClusterClient[F] => F[Unit] = { clusterClient =>
      F.info(s"Releasing cluster client with topology of ${clusterClient.topology.size} members") *> {
        clusterClient.topology.foreach(_.client.disconnect)
        ().pure[F]
      }
    }

    Resource.make(acquire)(release)
  }

  def make[F[+_]: ContextShift: Concurrent: Log](
      uri: URI
  ): Resource[F, RedisClusterClient[F]] =
    for {
      blocker <- RedisBlocker.make
      client <- acquireAndRelease(uri, blocker)
    } yield client
}
