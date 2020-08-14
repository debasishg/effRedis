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

  /**
    * Sets <code>field</code> in the hash stored at <code>key</code> to <code>value</code>.
    * If <code>key</code> does not exist, a new key holding a hash is created.
    * If field already exists in the hash, it is overwritten.
    *
    * @see [[http://redis.io/commands/hset HSET documentation]]
    * @deprecated return value semantics is inconsistent with [[effredis.HashOperations#hsetnx]] and
    *             [[effredis.HashOperations#hmset]]. Use [[effredis.HashOperations#hset1]] instead
    * @return <code>True</code> if <code>field</code> is a new field in the hash and value was set,
    *         <code>False</code> if <code>field</code> already exists in the hash and the value was updated.
    *
    */
  def hset(key: Any, field: Any, value: Any)(implicit format: Format): F[Resp[Boolean]]

  /**
    * Sets <code>field</code> in the hash stored at <code>key</code> to <code>value</code>.
    * If <code>key</code> does not exist, a new key holding a hash is created.
    * If field already exists in the hash, it is overwritten.
    *
    * @see [[http://redis.io/commands/hset HSET documentation]]
    * @return <code>Some(0)</code> if <code>field</code> is a new field in the hash and value was set,
    *         <code>Some(1)</code> if <code>field</code> already exists in the hash and the value was updated.
    */
  def hset1(key: Any, field: Any, value: Any)(implicit format: Format): F[Resp[Option[Long]]]

  /**
    * Sets <code>field</code> in the hash stored at <code>key</code> to <code>value</code>, only if field does not yet exist.
    * If key does not exist, a new key holding a hash is created.
    * If field already exists, this operation has no effect.
    *
    * @see [[http://redis.io/commands/hsetnx HSETNX documentation]]
    * @return <code>True</code> if <code>field</code> is a new field in the hash and value was set.
    *         </code>False</code> if <code>field</code> exists in the hash and no operation was performed.
    */
  def hsetnx(key: Any, field: Any, value: Any)(implicit format: Format): F[Resp[Boolean]]

  def hget[A](key: Any, field: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]]

  /**
    * Sets the specified fields to their respective values in the hash stored at key.
    * This command overwrites any existing fields in the hash.
    * If key does not exist, a new key holding a hash is created.
    *
    * @param map from fields to values
    * @see [[http://redis.io/commands/hmset HMSET documentation]]
    * @return <code>True</code> if operation completed successfully,
    *         <code>False</code> otherwise.
    */
  def hmset(key: Any, map: Iterable[Product2[Any, Any]])(implicit format: Format): F[Resp[Boolean]]

  def hmget[K, V](key: Any, fields: K*)(implicit format: Format, parseV: Parse[V]): F[Resp[Option[Map[K, V]]]]

  def hincrby(key: Any, field: Any, value: Long)(implicit format: Format): F[Resp[Option[Long]]]

  def hincrbyfloat(key: Any, field: Any, value: Float)(implicit format: Format): F[Resp[Option[Float]]]

  def hexists(key: Any, field: Any)(implicit format: Format): F[Resp[Boolean]]

  def hdel(key: Any, field: Any, fields: Any*)(implicit format: Format): F[Resp[Option[Long]]]

  def hlen(key: Any)(implicit format: Format): F[Resp[Option[Long]]]

  def hkeys[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[List[A]]]]

  def hvals[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[List[A]]]]

  @deprecated(
    "Use the more idiomatic variant hgetall1, which has the returned Map behavior more consistent. See issue https://github.com/debasishg/scala-redis/issues/122",
    "3.2"
  )
  def hgetall[K, V](
      key: Any
  )(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): F[Resp[Option[Map[K, V]]]]

  def hgetall1[K, V](
      key: Any
  )(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): F[Resp[Option[Map[K, V]]]]

  /**
    * Incrementally iterate hash fields and associated values (since 2.8)
    */
  def hscan[A](key: Any, cursor: Int, pattern: Any = "*", count: Int = 10)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[(Option[Int], Option[List[Option[A]]])]]]
}
