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
import scala.collection.immutable.BitSet

import io.chrisdavenport.keypool._
import cats.effect._
import cats.implicits._
import effredis.{ Log, RedisClient }
import effredis.RedisClient._

final case class RedisClusterNode(
    uri: URI,
    nodeId: String,
    connected: Boolean,
    slaveOf: String,
    lastPendingPingSentTimestamp: Long,
    lastPongReceivedTimestamp: Long,
    configEpoch: Long,
    slots: BitSet,
    nodeFlags: Set[NodeFlag]
) {
  def getSlots(): List[Int]       = slots.toList
  def hasSlot(slot: Int): Boolean = slots(slot)

  def managedClient[F[+_]: Concurrent: ContextShift: Log: Timer, M <: Mode](
      keypool: KeyPool[F, URI, (RedisClient[F, M], F[Unit])],
      uri: URI
  ): Resource[F, RedisClient[F, M]] =
    for {
      r <- keypool.take(uri)
    } yield r.value._1

  def getSlotsString(): String =
    if (slots.isEmpty) "[](0)"
    else {
      val l = slots.toList
      s"[${l.min}-${l.max}](${l.size})"
    }

  override def toString() = s"""
    uri: $uri,
    nodeId: $nodeId,
    connected: $connected,
    replicaOf: $slaveOf,
    lastPendingPingSentTimestamp: $lastPendingPingSentTimestamp,
    lastPongReceivedTimestamp: $lastPongReceivedTimestamp,
    configEpoch: $configEpoch,
    slots: ${getSlotsString()},
    nodeFlags: $nodeFlags
  """
}
