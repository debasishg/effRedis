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

import java.util.concurrent._
import scala.concurrent.ExecutionContext
import util.ClusterUtils
import scala.collection.immutable.BitSet
import cats.effect._
import cats.implicits._
import effredis.{ Error, Log, RedisClient, Value }

object ClusterTopology {

  def create[F[+_]: Concurrent: ContextShift: Log](cl: RedisClient[F]): F[List[RedisClusterNode[F]]] = {
    def toRedisClusterNode(
        ts: ClusterUtils.TopologyString
    ): RedisClusterNode[F] = {
      import ts._
      val blocker = Blocker.liftExecutionContext(ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(1)))
      val rc      = new RedisClient[F](new java.net.URI(s"http://${ts.uri.split("@")(0)}"), blocker)

      RedisClusterNode[F](
        rc,
        nodeId,
        if (linkState == "connected") true else false,
        replicaUpstreamNodeId,
        pingTimestamp,
        pongTimestamp,
        configEpoch,
        slots.map(ClusterUtils.parseSlotString).getOrElse(BitSet.empty),
        ClusterUtils.parseNodeFlags(nodeFlags)
      )
    }

    cl.clusterNodes.flatMap {
      case Value(Some(nodeInfo)) => {
        val n = nodeInfo.split("\n").map(e => s"$e ").mkString("\n")
        ClusterUtils.fromRedisServer(
          s"nodeId uri nodeFlags replicaUpstreamNodeId pingTimestamp pongTimestamp configEpoch linkState slots\n$n"
        ) match {
          case Right(value) => value.toList.map(ts => toRedisClusterNode(ts)).pure[F]
          case Left(err) =>
            F.error(s"Error fetching topology $err") *> List.empty[RedisClusterNode[F]].pure[F]
        }
      }
      case Error(err) =>
        F.error(s"Error fetching topology $err") *> List.empty[RedisClusterNode[F]].pure[F]
      case err =>
        F.error(s"Error fetching topology $err") *> List.empty[RedisClusterNode[F]].pure[F]
    }
  }
}
