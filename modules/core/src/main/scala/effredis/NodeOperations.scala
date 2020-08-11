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

trait NodeOperations[F[+_]] extends NodeApi[F] { self: Redis[F] =>
  implicit def blocker: Blocker
  implicit def conc: Concurrent[F]
  implicit def ctx: ContextShift[F]

  override def save: F[Resp[Boolean]] =
    send("SAVE")(asBoolean)

  override def bgsave: F[Resp[Boolean]] =
    send("BGSAVE")(asBoolean)

  override def lastsave: F[Resp[Option[Long]]] =
    send("LASTSAVE")(asLong)

  override def shutdown: F[Resp[Boolean]] =
    send("SHUTDOWN")(asBoolean)

  override def bgrewriteaof: F[Resp[Boolean]] =
    send("BGREWRITEAOF")(asBoolean)

  override def info: F[Resp[Option[String]]] =
    send("INFO")(asBulk)

  override def monitor: F[Resp[Boolean]] =
    send("MONITOR")(asBoolean)

  override def slaveof(options: Any): F[Resp[Boolean]] = options match {
    case (h: String, p: Int) =>
      send("SLAVEOF", List(h, p))(asBoolean)
    case _ => setAsMaster()
  }

  @deprecated("use slaveof", "1.2.0")
  def slaveOf(options: Any): F[Resp[Boolean]] =
    slaveof(options)

  private def setAsMaster(): F[Resp[Boolean]] =
    send("SLAVEOF", List("NO", "ONE"))(asBoolean)
}
