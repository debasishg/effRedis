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

import cats.implicits._
import cats.effect._

import effredis.{ Log, RedisBlocker, RedisClient }

object ClusterClient {
  private[effredis] def acquireAndRelease[F[+_]: Concurrent: ContextShift: Log](
      blocker: Blocker,
      uris: URI*
  ): Resource[F, ClusterClient[F]] = {

    val acquire: F[ClusterClient[F]] = {
      val clients = uris.map { uri =>
        new RedisClient(uri, Blocker.liftExecutorService(Executors.newFixedThreadPool(1)))
      }.toList
      blocker.blockOn((new ClusterClient[F](clients: _*)).pure[F])
    }

    val release: ClusterClient[F] => F[Unit] = { c =>
      c.clients.map(_.disconnect)
      ().pure[F]
    }

    Resource.make(acquire)(release)
  }

  def make[F[+_]: ContextShift: Concurrent: Log](
      uris: URI*
  ): Resource[F, ClusterClient[F]] =
    for {
      blocker <- RedisBlocker.make
      client <- acquireAndRelease(blocker, uris: _*)
    } yield client
}

sealed case class ClusterClient[F[+_]: Concurrent: ContextShift: Log] private (
    clients: RedisClient[F]*
)
