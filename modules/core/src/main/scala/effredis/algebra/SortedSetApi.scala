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

import RedisClient.{ ASC, Aggregate, SUM, SortOrder }
import codecs.{ Format, Parse }
import Containers._

trait SortedSetApi[F[+_]] {

  /**
    * Add the specified members having the specified score to the sorted set stored at key. (Variadic: >= 2.4)
    */
  def zadd(key: Any, score: Double, member: Any, scoreVals: (Double, Any)*)(
      implicit format: Format
  ): F[Resp[Long]]

  def zadd(
      key: Any,
      updateOption: SortedSetUpdateOption,
      changed: Boolean,
      score: Double,
      member: Any,
      scoreVals: (Double, Any)*
  )(
      implicit format: Format
  ): F[Resp[Long]]

  def zadd(
      key: Any,
      updateOption: Option[SortedSetUpdateOption],
      changed: Boolean,
      incr: Boolean,
      score: Double,
      member: Any
  )(
      implicit format: Format
  ): F[Resp[Long]]

  /**
    * Remove the specified members from the sorted set value stored at key. (Variadic: >= 2.4)
    */
  def zrem(key: Any, member: Any, members: Any*)(implicit format: Format): F[Resp[Long]]

  def zincrby(key: Any, incr: Double, member: Any)(implicit format: Format): F[Resp[Option[Double]]]

  def zcard(key: Any)(implicit format: Format): F[Resp[Long]]

  def zscore(key: Any, element: Any)(implicit format: Format): F[Resp[Option[Score]]]

  def zrange[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[List[Option[A]]]]

  def zrangeWithScore[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[List[(A, Score)]]]

  def zrangebylex[A](key: Any, min: String, max: String, limit: Option[(Int, Int)], sortAs: SortOrder)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[List[Option[A]]]]

  def zrangebyscore[A](
      key: Any,
      min: Double = Double.NegativeInfinity,
      minInclusive: Boolean = true,
      max: Double = Double.PositiveInfinity,
      maxInclusive: Boolean = true,
      limit: Option[(Int, Int)],
      sortAs: SortOrder = ASC
  )(implicit format: Format, parse: Parse[A]): F[Resp[List[Option[A]]]]

  def zrangebyscoreWithScore[A](
      key: Any,
      min: Double = Double.NegativeInfinity,
      minInclusive: Boolean = true,
      max: Double = Double.PositiveInfinity,
      maxInclusive: Boolean = true,
      limit: Option[(Int, Int)],
      sortAs: SortOrder = ASC
  )(implicit format: Format, parse: Parse[A]): F[Resp[List[(A, Score)]]]

  def zrank(key: Any, member: Any, reverse: Boolean = false)(implicit format: Format): F[Resp[Long]]

  def zremrangebyrank(key: Any, start: Int = 0, end: Int = -1)(implicit format: Format): F[Resp[Long]]

  def zremrangebyscore(key: Any, start: Double = Double.NegativeInfinity, end: Double = Double.PositiveInfinity)(
      implicit format: Format
  ): F[Resp[Long]]

  def zremrangebylex[A](key: Any, min: String, max: String)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Long]]

  def zunionstore(dstKey: Any, keys: Iterable[Any], aggregate: Aggregate = SUM)(
      implicit format: Format
  ): F[Resp[Long]]

  def zunionstoreWeighted(dstKey: Any, kws: Iterable[Product2[Any, Double]], aggregate: Aggregate = SUM)(
      implicit format: Format
  ): F[Resp[Long]]

  def zinterstore(dstKey: Any, keys: Iterable[Any], aggregate: Aggregate = SUM)(
      implicit format: Format
  ): F[Resp[Long]]

  def zinterstoreWeighted(dstKey: Any, kws: Iterable[Product2[Any, Double]], aggregate: Aggregate = SUM)(
      implicit format: Format
  ): F[Resp[Long]]

  def zcount(
      key: Any,
      min: Double = Double.NegativeInfinity,
      max: Double = Double.PositiveInfinity,
      minInclusive: Boolean = true,
      maxInclusive: Boolean = true
  )(implicit format: Format): F[Resp[Long]]

  def zlexcount[A](key: Any, min: String, max: String)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Long]]

  /**
    * Incrementally iterate sorted sets elements and associated scores (since 2.8)
    */
  def zscan[A](key: Any, cursor: Int, pattern: Any = "*", count: Int = 10)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[(Int, List[Option[A]])]]]

  def zpopmin[A](key: Any, count: Int = 1)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[List[ValueScorePair[A]]]]

  def zpopmax[A](key: Any, count: Int = 1)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[List[ValueScorePair[A]]]]

  def bzpopmin[K, V](timeoutInSeconds: Int, key: K, keys: K*)(
      implicit format: Format,
      parseK: Parse[K],
      parseV: Parse[V]
  ): F[Resp[Option[(K, ValueScorePair[V])]]]

  def bzpopmax[K, V](timeoutInSeconds: Int, key: K, keys: K*)(
      implicit format: Format,
      parseK: Parse[K],
      parseV: Parse[V]
  ): F[Resp[Option[(K, ValueScorePair[V])]]]
}
