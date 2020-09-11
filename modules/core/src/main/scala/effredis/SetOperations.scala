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
import algebra.SetApi
import codecs._

trait SetOperations[F[+_]] extends SetApi[F] { self: Redis[F, _] =>
  implicit def conc: Concurrent[F]
  implicit def ctx: ContextShift[F]

  override def sadd(key: Any, value: Any, values: Any*)(implicit format: Format): F[Resp[Long]] =
    send("SADD", List(key, value) ::: values.toList)(asInteger)

  override def srem(key: Any, value: Any, values: Any*)(implicit format: Format): F[Resp[Long]] =
    send("SREM", List(key, value) ::: values.toList)(asInteger)

  override def spop[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]] =
    send("SPOP", List(key))(asBulkString)

  override def spop[A](
      key: Any,
      count: Int
  )(implicit format: Format, parse: Parse[A]): F[Resp[Set[A]]] =
    send("SPOP", List(key, count))(asSet)

  override def smove(sourceKey: Any, destKey: Any, value: Any)(
      implicit format: Format
  ): F[Resp[Boolean]] =
    send("SMOVE", List(sourceKey, destKey, value))(if (asInteger == 1) true else false)

  override def scard(key: Any)(implicit format: Format): F[Resp[Long]] =
    send("SCARD", List(key))(asInteger)

  override def sismember(key: Any, value: Any)(implicit format: Format): F[Resp[Boolean]] =
    send("SISMEMBER", List(key, value))(if (asInteger == 1) true else false)

  override def sinter[A](
      key: Any,
      keys: Any*
  )(implicit format: Format, parse: Parse[A]): F[Resp[Set[A]]] =
    send("SINTER", key :: keys.toList)(asSet)

  override def sinterstore(key: Any, keys: Any*)(implicit format: Format): F[Resp[Long]] =
    send("SINTERSTORE", key :: keys.toList)(asInteger)

  override def sunion[A](
      key: Any,
      keys: Any*
  )(implicit format: Format, parse: Parse[A]): F[Resp[Set[A]]] =
    send("SUNION", key :: keys.toList)(asSet)

  override def sunionstore(key: Any, keys: Any*)(implicit format: Format): F[Resp[Long]] =
    send("SUNIONSTORE", key :: keys.toList)(asInteger)

  override def sdiff[A](
      key: Any,
      keys: Any*
  )(implicit format: Format, parse: Parse[A]): F[Resp[Set[A]]] =
    send("SDIFF", key :: keys.toList)(asSet)

  override def sdiffstore(key: Any, keys: Any*)(implicit format: Format): F[Resp[Long]] =
    send("SDIFFSTORE", key :: keys.toList)(asInteger)

  override def smembers[A](
      key: Any
  )(implicit format: Format, parse: Parse[A]): F[Resp[Set[A]]] =
    send("SMEMBERS", List(key))(asSet)

  override def srandmember[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]] =
    send("SRANDMEMBER", List(key))(asBulkString)

  override def srandmember[A](
      key: Any,
      count: Int
  )(implicit format: Format, parse: Parse[A]): F[Resp[Set[A]]] =
    send("SRANDMEMBER", List(key, count))(asSet)

  override def sscan[A](key: Any, cursor: Int, pattern: Any = "*", count: Int = 10)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[(Int, List[A])]]] =
    send(
      "SSCAN",
      key :: cursor :: ((x: List[Any]) => if (pattern == "*") x else "match" :: pattern :: x)(
            if (count == 10) Nil else List("count", count)
          )
    )(asPair)
}
