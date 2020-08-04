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
import algebra.ListApi
import codecs._

trait ListOperations[F[+_]] extends ListApi[F] { self: Redis[F] =>
  implicit def blocker: Blocker
  implicit def conc: Concurrent[F]
  implicit def ctx: ContextShift[F]

  override def lpush(key: Any, value: Any, values: Any*)(implicit format: Format): F[RedisResponse[Option[Long]]] =
    send("LPUSH", List(key, value) ::: values.toList)(asLong)

  override def lpushx(key: Any, value: Any)(implicit format: Format): F[RedisResponse[Option[Long]]] =
    send("LPUSHX", List(key, value))(asLong)

  override def rpush(key: Any, value: Any, values: Any*)(implicit format: Format): F[RedisResponse[Option[Long]]] =
    send("RPUSH", List(key, value) ::: values.toList)(asLong)

  override def rpushx(key: Any, value: Any)(implicit format: Format): F[RedisResponse[Option[Long]]] =
    send("RPUSHX", List(key, value))(asLong)

  override def llen(key: Any)(implicit format: Format): F[RedisResponse[Option[Long]]] =
    send("LLEN", List(key))(asLong)

  override def lrange[A](key: Any, start: Int, end: Int)(
      implicit format: Format,
      parse: Parse[A]
  ): F[RedisResponse[Option[List[Option[A]]]]] =
    send("LRANGE", List(key, start, end))(asList)

  override def ltrim(key: Any, start: Int, end: Int)(implicit format: Format): F[RedisResponse[Boolean]] =
    send("LTRIM", List(key, start, end))(asBoolean)

  override def lindex[A](key: Any, index: Int)(implicit format: Format, parse: Parse[A]): F[RedisResponse[Option[A]]] =
    send("LINDEX", List(key, index))(asBulk)

  override def lset(key: Any, index: Int, value: Any)(implicit format: Format): F[RedisResponse[Boolean]] =
    send("LSET", List(key, index, value))(asBoolean)

  override def lrem(key: Any, count: Int, value: Any)(implicit format: Format): F[RedisResponse[Option[Long]]] =
    send("LREM", List(key, count, value))(asLong)

  override def lpop[A](key: Any)(implicit format: Format, parse: Parse[A]): F[RedisResponse[Option[A]]] =
    send("LPOP", List(key))(asBulk)

  override def rpop[A](key: Any)(implicit format: Format, parse: Parse[A]): F[RedisResponse[Option[A]]] =
    send("RPOP", List(key))(asBulk)

  override def rpoplpush[A](
      srcKey: Any,
      dstKey: Any
  )(implicit format: Format, parse: Parse[A]): F[RedisResponse[Option[A]]] =
    send("RPOPLPUSH", List(srcKey, dstKey))(asBulk)

  override def brpoplpush[A](srcKey: Any, dstKey: Any, timeoutInSeconds: Int)(
      implicit format: Format,
      parse: Parse[A]
  ): F[RedisResponse[Option[A]]] =
    send("BRPOPLPUSH", List(srcKey, dstKey, timeoutInSeconds))(asBulkWithTime)

  override def blpop[K, V](
      timeoutInSeconds: Int,
      key: K,
      keys: K*
  )(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): F[RedisResponse[Option[(K, V)]]] =
    send("BLPOP", key :: keys.foldRight(List[Any](timeoutInSeconds))(_ :: _))(
      asListPairs[K, V].flatMap(_.flatten.headOption)
    )

  override def brpop[K, V](
      timeoutInSeconds: Int,
      key: K,
      keys: K*
  )(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): F[RedisResponse[Option[(K, V)]]] =
    send("BRPOP", key :: keys.foldRight(List[Any](timeoutInSeconds))(_ :: _))(
      asListPairs[K, V].flatMap(_.flatten.headOption)
    )
}
