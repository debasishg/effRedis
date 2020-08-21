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

trait RedisCommand[F[+_]]
    extends Redis[F]
    with StringOperations[F]
    with BaseOperations[F]
    with ListOperations[F]
    with SetOperations[F]
    with HashOperations[F]
    with SortedSetOperations[F]
    with NodeOperations[F]
    with GeoOperations[F]
    with EvalOperations[F]
    with HyperLogLogOperations[F]
    with TransactionOperations[F]
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
