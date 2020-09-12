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
package contract

import codecs.{ Format, Parse }

trait ListApi[F[+_]] {

  /**
    * add values to the head of the list stored at key (Variadic: >= 2.4)
    */
  def lpush(key: Any, value: Any, values: Any*)(implicit format: Format): F[Resp[Long]]

  /**
    * add value to the head of the list stored at key (Variadic: >= 2.4)
    */
  def lpushx(key: Any, value: Any)(implicit format: Format): F[Resp[Long]]

  /**
    * add values to the tail of the list stored at key (Variadic: >= 2.4)
    */
  def rpush(key: Any, value: Any, values: Any*)(implicit format: Format): F[Resp[Long]]

  /**
    * add value to the tail of the list stored at key (Variadic: >= 2.4)
    */
  def rpushx(key: Any, value: Any)(implicit format: Format): F[Resp[Long]]

  /**
    * return the length of the list stored at the specified key.
    * If the key does not exist zero is returned (the same behaviour as for empty lists).
    * If the value stored at key is not a list an error is returned.
    */
  def llen(key: Any)(implicit format: Format): F[Resp[Long]]

  /**
    * return the specified elements of the list stored at the specified key.
    * Start and end are zero-based indexes.
    */
  def lrange[A](key: Any, start: Int, end: Int)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[List[Option[A]]]]

  /**
    * Trim an existing list so that it will contain only the specified range of elements specified.
    */
  def ltrim(key: Any, start: Int, end: Int)(implicit format: Format): F[Resp[Boolean]]

  /**
    * return the especified element of the list stored at the specified key.
    * Negative indexes are supported, for example -1 is the last element, -2 the penultimate and so on.
    */
  def lindex[A](key: Any, index: Int)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]]

  /**
    * set the list element at index with the new value. Out of range indexes will generate an error
    */
  def lset(key: Any, index: Int, value: Any)(implicit format: Format): F[Resp[Boolean]]

  /**
    * Remove the first count occurrences of the value element from the list.
    */
  def lrem(key: Any, count: Int, value: Any)(implicit format: Format): F[Resp[Long]]

  /**
    * atomically return and remove the first (LPOP) or last (RPOP) element of the list
    */
  def lpop[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]]

  /**
    * atomically return and remove the first (LPOP) or last (RPOP) element of the list
    */
  def rpop[A](key: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]]

  /**
    * Remove the first count occurrences of the value element from the list.
    */
  def rpoplpush[A](srcKey: Any, dstKey: Any)(implicit format: Format, parse: Parse[A]): F[Resp[Option[A]]]

  def brpoplpush[A](srcKey: Any, dstKey: Any, timeoutInSeconds: Int)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[A]]]

  def blpop[K, V](timeoutInSeconds: Int, key: K, keys: K*)(
      implicit format: Format,
      parseK: Parse[K],
      parseV: Parse[V]
  ): F[Resp[Option[(K, V)]]]

  def brpop[K, V](timeoutInSeconds: Int, key: K, keys: K*)(
      implicit format: Format,
      parseK: Parse[K],
      parseV: Parse[V]
  ): F[Resp[Option[(K, V)]]]

}
