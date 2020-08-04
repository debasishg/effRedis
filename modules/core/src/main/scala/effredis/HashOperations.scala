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
import algebra.HashApi
import codecs._

trait HashOperations[F[+_]] extends HashApi[F] { self: Redis[F] =>
  implicit def blocker: Blocker
  implicit def conc: Concurrent[F]
  implicit def ctx: ContextShift[F]

  override def hset(key: Any, field: Any, value: Any)(implicit format: Format): F[RedisResponse[Boolean]] =
    send("HSET", List(key, field, value))(asBoolean)

  override def hset1(key: Any, field: Any, value: Any)(implicit format: Format): F[RedisResponse[Option[Long]]] =
    send("HSET", List(key, field, value))(asLong)

  override def hsetnx(key: Any, field: Any, value: Any)(implicit format: Format): F[RedisResponse[Boolean]] =
    send("HSETNX", List(key, field, value))(asBoolean)

  override def hget[A](key: Any, field: Any)(implicit format: Format, parse: Parse[A]): F[RedisResponse[Option[A]]] =
    send("HGET", List(key, field))(asBulk)

  override def hmset(key: Any, map: Iterable[Product2[Any, Any]])(implicit format: Format): F[RedisResponse[Boolean]] =
    send("HMSET", key :: flattenPairs(map))(asBoolean)

  override def hmget[K, V](
      key: Any,
      fields: K*
  )(implicit format: Format, parseV: Parse[V]): F[RedisResponse[Option[Map[K, V]]]] =
    send("HMGET", key :: fields.toList) {
      asList.map { values =>
        fields
          .zip(values)
          .flatMap {
            case (field, Some(value)) => Some((field, value))
            case (_, None)            => None
          }
          .toMap
      }
    }

  override def hincrby(key: Any, field: Any, value: Long)(implicit format: Format): F[RedisResponse[Option[Long]]] =
    send("HINCRBY", List(key, field, value))(asLong)

  override def hincrbyfloat(key: Any, field: Any, value: Float)(
      implicit format: Format
  ): F[RedisResponse[Option[Float]]] =
    send("HINCRBYFLOAT", List(key, field, value))(asBulk.map(_.toFloat))

  override def hexists(key: Any, field: Any)(implicit format: Format): F[RedisResponse[Boolean]] =
    send("HEXISTS", List(key, field))(asBoolean)

  override def hdel(key: Any, field: Any, fields: Any*)(implicit format: Format): F[RedisResponse[Option[Long]]] =
    send("HDEL", List(key, field) ::: fields.toList)(asLong)

  override def hlen(key: Any)(implicit format: Format): F[RedisResponse[Option[Long]]] =
    send("HLEN", List(key))(asLong)

  override def hkeys[A](key: Any)(implicit format: Format, parse: Parse[A]): F[RedisResponse[Option[List[A]]]] =
    send("HKEYS", List(key))(asList.map(_.flatten))

  override def hvals[A](key: Any)(implicit format: Format, parse: Parse[A]): F[RedisResponse[Option[List[A]]]] =
    send("HVALS", List(key))(asList.map(_.flatten))

  override def hgetall[K, V](
      key: Any
  )(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): F[RedisResponse[Option[Map[K, V]]]] =
    send("HGETALL", List(key))(asListPairs[K, V].map(_.flatten.toMap))

  override def hgetall1[K, V](
      key: Any
  )(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): F[RedisResponse[Option[Map[K, V]]]] = {
    val fa = send("HGETALL", List(key))(asListPairs[K, V].map(_.flatten.toMap))
    val ev = implicitly[Concurrent[F]]
    ev.fmap(fa) {
      case s @ Right(Right(Some(m))) if m.nonEmpty => s
      case _                                       => Right(Right(None))
    }
  }

  override def hscan[A](key: Any, cursor: Int, pattern: Any = "*", count: Int = 10)(
      implicit format: Format,
      parse: Parse[A]
  ): F[RedisResponse[Option[(Option[Int], Option[List[Option[A]]])]]] =
    send(
      "HSCAN",
      key :: cursor :: ((x: List[Any]) => if (pattern == "*") x else "match" :: pattern :: x)(
            if (count == 10) Nil else List("count", count)
          )
    )(asPair)
}
