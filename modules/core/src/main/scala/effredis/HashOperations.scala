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

trait HashOperations[F[+_]] extends HashApi[F] { self: Redis[F, _] =>
  implicit def conc: Concurrent[F]
  implicit def ctx: ContextShift[F]

  override def hset(key: Any, field: Any, value: Any)(implicit format: Format): F[Resp[Long]] =
    send("HSET", List(key, field, value))(asInteger)

  override def hset(key: Any, map: Iterable[Product2[Any, Any]])(implicit format: Format): F[Resp[Long]] =
    send("HSET", key :: flattenPairs(map))(asInteger)

  override def hsetnx(key: Any, field: Any, value: Any)(implicit format: Format): F[Resp[Boolean]] =
    send("HSETNX", List(key, field, value))(if (asInteger == 1) true else false)

  override def hget[A](key: Any, field: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]] =
    send("HGET", List(key, field))(asBulkString)

  override def hmset(key: Any, map: Iterable[Product2[Any, Any]])(implicit format: Format): F[Resp[Boolean]] =
    send("HMSET", key :: flattenPairs(map))(asBoolean)

  override def hmget[K, V](
      key: Any,
      fields: K*
  )(implicit format: Format, parseV: Parse[V]): F[Resp[Map[K, Option[V]]]] =
    send("HMGET", List(key) ::: fields.toList)(fields.toList.zip(asFlatList[V]).toMap)

  override def hincrby(key: Any, field: Any, value: Long)(implicit format: Format): F[Resp[Long]] =
    send("HINCRBY", List(key, field, value))(asInteger)

  override def hincrbyfloat(key: Any, field: Any, value: Float)(
      implicit format: Format
  ): F[Resp[Option[Float]]] =
    send("HINCRBYFLOAT", List(key, field, value))(asBulkString.map(_.toFloat))

  override def hexists(key: Any, field: Any)(implicit format: Format): F[Resp[Boolean]] =
    send("HEXISTS", List(key, field))(if (asInteger == 1) true else false)

  override def hdel(key: Any, field: Any, fields: Any*)(implicit format: Format): F[Resp[Long]] =
    send("HDEL", List(key, field) ::: fields.toList)(asInteger)

  override def hlen(key: Any)(implicit format: Format): F[Resp[Long]] =
    send("HLEN", List(key))(asInteger)

  override def hkeys[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Resp[List[Option[A]]]] =
    send("HKEYS", List(key))(asFlatList)

  override def hvals[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Resp[List[Option[A]]]] =
    send("HVALS", List(key))(asFlatList)

  /**
    * returns a Some(Map) of key/value pairs when the key exists. If the key does not exist, then
    * it returns None. This is symmetrical with how hset works. You cannot set an empty Map
    * - hence hgetall should not return an empty Map if the key does not exist.
    */
  override def hgetall[K, V](
      key: Any
  )(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): F[Resp[Option[Map[K, V]]]] =
    send("HGETALL", List(key)) {
      val m = asFlatListPairs[K, V].toMap
      if (m.isEmpty) None
      else Some(m)
    }

  override def hscan[A](key: Any, cursor: Int, pattern: Any = "*", count: Int = 10)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[(Int, List[Option[A]])]]] =
    send(
      "HSCAN",
      key :: cursor :: ((x: List[Any]) => if (pattern == "*") x else "match" :: pattern :: x)(
            if (count == 10) Nil else List("count", count)
          )
    )(asPair)
}
