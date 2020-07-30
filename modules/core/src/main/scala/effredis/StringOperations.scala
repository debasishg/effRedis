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

import serialization._
import algebra.StringApi
import StringApi._

import scala.concurrent.duration.Duration

// class StringOperations[F[_]: Concurrent: ContextShift](implicit blocker: Blocker) extends StringApi[F] with Redis {
trait StringOperations[F[_]] extends StringApi[F] { self: Redis =>
  implicit def blocker: Blocker
  implicit def conc: Concurrent[F]
  implicit def ctx: ContextShift[F]

  def set(
      key: Any,
      value: Any,
      whenSet: SetBehaviour = Always,
      expire: Duration = null
  )(implicit format: Format): F[Boolean] = {

    val expireCmd = if (expire != null) {
      List("PX", expire.toMillis.toString)
    } else {
      List.empty
    }
    val cmd = List(key, value) ::: expireCmd ::: whenSet.command
    send("SET", cmd)(asBoolean)
  }
}
