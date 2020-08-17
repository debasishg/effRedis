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

import cats.effect._
import cats.implicits._
import effredis.{ Log, RedisBlocker, RedisClient }

final private[effredis] case class RedisClusterNode[F[+_]: Concurrent: ContextShift: Log](
    val client: RedisClient[F],
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
}

object RedisClusterNode {

  private[effredis] def acquireAndRelease[F[+_]: Concurrent: ContextShift: Log](
      uri: URI,
      nodeId: String,
      connected: Boolean,
      slaveOf: String,
      lastPendingPingSentTimestamp: Long,
      lastPongReceivedTimestamp: Long,
      configEpoch: Long,
      slots: List[Int],
      nodeFlags: Set[NodeFlag],
      blocker: Blocker
  ): Resource[F, RedisClusterNode[F]] = {

    val acquire: F[RedisClusterNode[F]] = {
      F.info(s"Acquiring cluster node $nodeId") *>
        blocker.blockOn(
          (
            new RedisClusterNode(
              new RedisClient[F](uri, blocker),
              nodeId,
              connected,
              slaveOf,
              lastPendingPingSentTimestamp,
              lastPongReceivedTimestamp,
              configEpoch,
              BitSet(slots: _*),
              nodeFlags
            )
          ).pure[F]
        )
    }

    val release: RedisClusterNode[F] => F[Unit] = { c =>
      F.info(s"Releasing cluster node $nodeId") *> {
        c.client.disconnect
        ().pure[F]
      }
    }

    Resource.make(acquire)(release)
  }

  def make[F[+_]: ContextShift: Concurrent: Log](
      uri: URI,
      nodeId: String,
      connected: Boolean,
      slaveOf: String,
      lastPendingPingSentTimestamp: Long,
      lastPongReceivedTimestamp: Long,
      configEpoch: Long,
      slots: List[Int],
      nodeFlags: Set[NodeFlag]
  ): Resource[F, RedisClusterNode[F]] =
    for {
      blocker <- RedisBlocker.make
      client <- acquireAndRelease(
                 uri,
                 nodeId,
                 connected,
                 slaveOf,
                 lastPendingPingSentTimestamp,
                 lastPongReceivedTimestamp,
                 configEpoch,
                 slots,
                 nodeFlags,
                 blocker
               )
    } yield client
}
