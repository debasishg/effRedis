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
import algebra.HyperLogLogApi

trait HyperLogLogOperations[F[+_]] extends HyperLogLogApi[F] { self: Redis[F, _] =>
  implicit def conc: Concurrent[F]
  implicit def ctx: ContextShift[F]

  override def pfadd(key: Any, value: Any, values: Any*): F[Resp[Option[Long]]] =
    send("PFADD", List(key, value) ::: values.toList)(asLong)

  override def pfcount(key: Any, keys: Any*): F[Resp[Option[Long]]] =
    send("PFCOUNT", key :: keys.toList)(asLong)

  override def pfmerge(destination: Any, sources: Any*): F[Resp[Boolean]] =
    send("PFMERGE", List(destination) ::: sources.toList)(asBoolean)
}
