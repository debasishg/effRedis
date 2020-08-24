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

import scala.concurrent.duration._
import cats.effect._
import cats.implicits._
import effredis.{ Error, Log, Resp, Value }
import effredis.codecs._

abstract class RedisClusterOps[F[+_]: Concurrent: ContextShift: Log: Timer] { self: RedisClusterClient[F] =>
  implicit def pool: RedisClientPool[F]

  def onANode[R](fn: RedisClusterNode[F] => F[Resp[R]]): F[Resp[R]] =
    topology.nodes.headOption
      .map(fn)
      .getOrElse(F.raiseError(new IllegalArgumentException("No cluster node found")))

  def onAllNodes(fn: RedisClusterNode[F] => F[Resp[Boolean]]): F[Resp[Boolean]] = {
    val _ = topology.nodes.foreach(fn)
    Value(true).pure[F]
  }

  def forKey[R](key: String)(fn: RedisClusterNode[F] => F[Resp[R]]): F[Resp[R]] = {
    val slot = HashSlot.find(key)
    val node = topology.nodes.filter(_.hasSlot(slot)).headOption

    F.info(s"Command mapped to slot $slot node uri ${node.get.uri}") *>
      executeOnNode(node, slot, key)(fn).flatMap {
        case r @ Value(_) => r.pure[F]
        case Error(err) =>
          F.error(s"Error from server $err - will retry") *>
              retryForMoved(err, key)(fn)
        case err => F.raiseError(new IllegalStateException(s"Unexpected response from server $err"))
      }
  }

  import effredis.algebra.StringApi._

  def cset(key: Any, value: Any, whenSet: SetBehaviour = Always, expire: Duration = null, keepTTL: Boolean = false)(
      implicit format: Format
  ): F[Resp[Boolean]] =
    forKey(key.toString) {
      _.managedClient1.use {
        _.value.set(key, value, whenSet, expire, keepTTL)
      }
    }

  def cget[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]] =
    forKey(key.toString) {
      _.managedClient1.use {
        _.value.get[A](key)
      }
    }

  def clpush[G[+_]: Concurrent: ContextShift: Log: Timer](key: Any, value: Any, values: Any*)(
      implicit format: Format
  ): F[Resp[Option[Long]]] =
    forKey(key.toString) {
      _.managedClient1.use {
        _.value.lpush(key, value, values)
      }
    }

  def executeOnNode[R](node: Option[RedisClusterNode[F]], slot: Int, key: String)(
      fn: RedisClusterNode[F] => F[Resp[R]]
  ): F[Resp[R]] =
    node
      .map(fn)
      .getOrElse(
        F.raiseError(
          new IllegalArgumentException(
            s"Redis Cluster Node $node not found corresponding to slot $slot for $key"
          )
        )
      )

  def retryForMoved[R](err: String, key: String)(fn: RedisClusterNode[F] => F[Resp[R]]): F[Resp[R]] =
    if (err.startsWith("MOVED")) {
      val parts = err.split(" ")
      val slot  = parts(1).toInt

      F.info(s"Retrying with ${parts(1)} ${parts(2)}") *> {
        if (parts.size != 3) {
          F.raiseError(
            new IllegalStateException(s"Expected error for MOVED to contain 3 parts (MOVED, slot, URI) - found $err")
          )
        } else {
          val node = topology.nodes.filter(_.hasSlot(slot)).headOption
          executeOnNode(node, slot, key)(fn)
        }
      }
    } else {
      F.raiseError(
        new IllegalStateException(
          s"Expected MOVED error but found $err"
        )
      )
    }

  def forKeys[R](key: String, keys: String*)(fn: RedisClusterNode[F] => F[Resp[R]]): F[Resp[R]] = {
    val slots = (key :: keys.toList).map(HashSlot.find(_))
    if (slots.forall(_ == slots.head)) {
      val node = topology.nodes.filter(_.hasSlot(slots.head)).headOption
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
