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

import cats.effect._
import cats.implicits._
import effredis.{ Log, Resp, Value }

class RedisClusterOps[F[+_]: Concurrent: ContextShift: Log] { self: RedisClusterClient[F] =>
  def onANode[R](fn: RedisClusterNode[F] => F[Resp[R]]): F[Resp[R]] =
    topology.headOption
      .map(fn)
      .getOrElse(F.raiseError(new IllegalArgumentException("No cluster node found")))

  def onAllNodes(fn: RedisClusterNode[F] => F[Resp[Boolean]]): F[Resp[Boolean]] = {
    val _ = topology.foreach(fn)
    Value(true).pure[F]
  }

  def forKey[R](key: String)(fn: RedisClusterNode[F] => F[Resp[R]]): F[Resp[R]] = {
    val slot = HashSlot.find(key)
    val node = topology.filter(_.hasSlot(slot)).headOption
    node
      .map(fn)
      .getOrElse(
        F.raiseError(
          new IllegalArgumentException(
            s"Slot not found corresponding to key $key"
          )
        )
      )
  }

  def forKeys[R](key: String, keys: String*)(fn: RedisClusterNode[F] => F[Resp[R]]): F[Resp[R]] = {
    val slots = (key :: keys.toList).map(HashSlot.find(_))
    if (slots.forall(_ == slots.head)) {
      val node = topology.filter(_.hasSlot(slots.head)).headOption
      node
        .map(fn)
        .getOrElse(
          F.raiseError(
            new IllegalArgumentException(
              s"Slot not found corresponding to keys ${(key :: keys.toList).mkString(",")}"
            )
          )
        )
    } else {
      F.raiseError(
        new IllegalArgumentException(
          s"Keys ${(key :: keys.toList).mkString(",")} do not map to the same slot"
        )
      )
    }
  }
}

class NotAllowedInClusterError(message: String) extends RuntimeException(message)
