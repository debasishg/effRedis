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
package algebra

import codecs.{ Format, Parse }

trait HashApi[F[+_]] {

  def hset(key: Any, field: Any, value: Any)(implicit format: Format): F[Resp[Long]]
  def hset(key: Any, map: Iterable[Product2[Any, Any]])(implicit format: Format): F[Resp[Long]]

  def hsetnx(key: Any, field: Any, value: Any)(implicit format: Format): F[Resp[Boolean]]

  def hget[A](key: Any, field: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]]

  def hmset(key: Any, map: Iterable[Product2[Any, Any]])(implicit format: Format): F[Resp[Boolean]]

  def hmget[K, V](key: Any, fields: K*)(implicit format: Format, parseV: Parse[V]): F[Resp[Map[K, Option[V]]]]

  def hincrby(key: Any, field: Any, value: Long)(implicit format: Format): F[Resp[Long]]

  def hincrbyfloat(key: Any, field: Any, value: Float)(implicit format: Format): F[Resp[Option[Float]]]

  def hexists(key: Any, field: Any)(implicit format: Format): F[Resp[Boolean]]

  def hdel(key: Any, field: Any, fields: Any*)(implicit format: Format): F[Resp[Long]]

  def hlen(key: Any)(implicit format: Format): F[Resp[Long]]

  def hkeys[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Resp[List[Option[A]]]]

  def hvals[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Resp[List[Option[A]]]]

  def hgetall[K, V](
      key: Any
  )(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): F[Resp[Option[Map[K, V]]]]

  /**
    * Incrementally iterate hash fields and associated values (since 2.8)
    */
  def hscan[A](key: Any, cursor: Int, pattern: Any = "*", count: Int = 10)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[(Int, List[Option[A]])]]]
}
