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
import effredis.Resp
import effredis.codecs.{ Format, Parse }

trait ListOps[F[+_]] extends RedisClusterOps[F] { self: RedisClusterClient[F] =>
  // implicit def blocker: Blocker
  implicit def conc: Concurrent[F]
  implicit def ctx: ContextShift[F]

  /**
    * add values to the head of the list stored at key (Variadic: >= 2.4)
    */
  def lpush(key: Any, value: Any, values: Any*)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString)(_.client.lpush(key, value, values))

  /**
    * add value to the head of the list stored at key (Variadic: >= 2.4)
    */
  def lpushx(key: Any, value: Any)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString)(_.client.lpushx(key, value))

  /**
    * add values to the tail of the list stored at key (Variadic: >= 2.4)
    */
  def rpush(key: Any, value: Any, values: Any*)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString)(_.client.rpush(key, value, values))

  /**
    * add value to the tail of the list stored at key (Variadic: >= 2.4)
    */
  def rpushx(key: Any, value: Any)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString)(_.client.rpushx(key, value))

  /**
    * return the length of the list stored at the specified key.
    * If the key does not exist zero is returned (the same behaviour as for empty lists).
    * If the value stored at key is not a list an error is returned.
    */
  def llen(key: Any)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString)(_.client.llen(key))

  /**
    * return the specified elements of the list stored at the specified key.
    * Start and end are zero-based indexes.
    */
  def lrange[A](key: Any, start: Int, end: Int)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[List[Option[A]]]]] =
    forKey(key.toString)(_.client.lrange(key, start, end))

  /**
    * Trim an existing list so that it will contain only the specified range of elements specified.
    */
  def ltrim(key: Any, start: Int, end: Int)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString)(_.client.ltrim(key, start, end))

  /**
    * return the especified element of the list stored at the specified key.
    * Negative indexes are supported, for example -1 is the last element, -2 the penultimate and so on.
    */
  def lindex[A](key: Any, index: Int)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]] =
    forKey(key.toString)(_.client.lindex(key, index))

  /**
    * set the list element at index with the new value. Out of range indexes will generate an error
    */
  def lset(key: Any, index: Int, value: Any)(implicit format: Format): F[Resp[Boolean]] =
    forKey(key.toString)(_.client.lset(key, index, value))

  /**
    * Remove the first count occurrences of the value element from the list.
    */
  def lrem(key: Any, count: Int, value: Any)(implicit format: Format): F[Resp[Option[Long]]] =
    forKey(key.toString)(_.client.lrem(key, count, value))

  /**
    * atomically return and remove the first (LPOP) or last (RPOP) element of the list
    */
  def lpop[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]] =
    forKey(key.toString)(_.client.lpop(key))

  /**
    * atomically return and remove the first (LPOP) or last (RPOP) element of the list
    */
  def rpop[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]] =
    forKey(key.toString)(_.client.rpop(key))

  /**
    * Remove the first count occurrences of the value element from the list.
    */
  def rpoplpush[A](srcKey: Any, dstKey: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]] =
    forKeys(srcKey.toString, dstKey.toString)(_.client.rpoplpush[A](srcKey, dstKey))

  def brpoplpush[A](srcKey: Any, dstKey: Any, timeoutInSeconds: Int)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[A]]] =
    forKeys(srcKey.toString, dstKey.toString)(_.client.brpoplpush[A](srcKey, dstKey, timeoutInSeconds))

  def blpop[K, V](timeoutInSeconds: Int, key: K, keys: K*)(
      implicit format: Format,
      parseK: Parse[K],
      parseV: Parse[V]
  ): F[Resp[Option[(K, V)]]] =
    forKeys(key.toString, keys.toList.map(_.toString): _*)(_.client.blpop[K, V](timeoutInSeconds, key, keys: _*))

  def brpop[K, V](timeoutInSeconds: Int, key: K, keys: K*)(
      implicit format: Format,
      parseK: Parse[K],
      parseV: Parse[V]
  ): F[Resp[Option[(K, V)]]] =
    forKeys(key.toString, keys.toList.map(_.toString): _*)(_.client.brpop[K, V](timeoutInSeconds, key, keys: _*))
}
