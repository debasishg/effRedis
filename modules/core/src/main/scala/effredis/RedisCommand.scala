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

import cats.effect._
import RedisClient._

abstract class RedisCommand[F[+_]: Concurrent: Log, M <: Mode](mode: M)
    extends Redis[F, M](mode)
    with BaseOperations[F]
    with StringOperations[F]
    with ListOperations[F]
    with HashOperations[F]
    with SetOperations[F]
    with SortedSetOperations[F]
    with NodeOperations[F]
    with TransactionOperations[F]
    with HyperLogLogOperations[F]
    with GeoOperations[F]
    with ScriptsOperations[F]
    with cluster.ClusterOperations[F]
    with AutoCloseable {

  val database: Int       = 0
  val secret: Option[Any] = None

  override def onConnect(): Unit = {
    secret.foreach(s => auth(s))
    selectDatabase()
  }

  private def selectDatabase(): Unit = {
    val _ = if (database != 0) select(database)
    ()
  }
}
