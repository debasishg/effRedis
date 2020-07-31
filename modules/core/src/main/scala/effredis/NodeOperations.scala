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
import algebra.NodeApi

trait NodeOperations[F[_]] extends NodeApi[F] { self: Redis =>
  implicit def blocker: Blocker
  implicit def conc: Concurrent[F]
  implicit def ctx: ContextShift[F]

  override def save: F[Boolean] =
    send("SAVE")(asBoolean)

  override def bgsave: F[Boolean] =
    send("BGSAVE")(asBoolean)

  override def lastsave: F[Option[Long]] =
    send("LASTSAVE")(asLong)

  override def shutdown: F[Boolean] =
    send("SHUTDOWN")(asBoolean)

  override def bgrewriteaof: F[Boolean] =
    send("BGREWRITEAOF")(asBoolean)

  override def info: F[Option[String]] =
    send("INFO")(asBulk)

  override def monitor: F[Boolean] =
    send("MONITOR")(asBoolean)

  override def slaveof(options: Any): F[Boolean] = options match {
    case (h: String, p: Int) =>
      send("SLAVEOF", List(h, p))(asBoolean)
    case _ => setAsMaster()
  }

  @deprecated("use slaveof", "1.2.0")
  def slaveOf(options: Any): F[Boolean] =
    slaveof(options)

  private def setAsMaster(): F[Boolean] =
    send("SLAVEOF", List("NO", "ONE"))(asBoolean)
}
