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
import java.util.concurrent._

import scala.concurrent._
import util.Cached

import effredis.{ Log, RedisClient }
import cats.effect._
import cats.implicits._

final case class RedisClusterClient[F[+_]: Concurrent: ContextShift: Log: Timer] private (
    // need to make this a collection and try sequentially till
    // one of them works
    seedURI: URI,
    topology: Cached[F, ClusterTopology[F]]
) extends RedisClusterOps[F] {

  def conc: cats.effect.Concurrent[F]  = implicitly[Concurrent[F]]
  def ctx: cats.effect.ContextShift[F] = implicitly[ContextShift[F]]
  def l: Log[F]                        = implicitly[Log[F]]
}

object RedisClusterClient {
  def make[F[+_]: Concurrent: ContextShift: Log: Timer](seedURI: URI): F[RedisClusterClient[F]] = {
    val blocker = Blocker.liftExecutionContext(ExecutionContext.fromExecutor(Executors.newFixedThreadPool(1)))
    blocker.blockOn {
      RedisClient.make(seedURI).use { cl =>
        Cached
          .create[F, ClusterTopology[F]](ClusterTopology.create[F](cl))
          .flatMap { ct =>
            new RedisClusterClient[F](seedURI, ct).pure[F] <*
              ct.get.flatMap(topo => F.debug(s"RedisClusterClient created with topology $topo"))
          }
      }
    }
  }
}
