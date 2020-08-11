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

trait SortedSetApi[F[+_]] {

  /**
    * Add the specified members having the specified score to the sorted set stored at key. (Variadic: >= 2.4)
    */
  def zadd(key: Any, score: Double, member: Any, scoreVals: (Double, Any)*)(
      implicit format: Format
  ): F[Resp[Option[Long]]]

  /**
    * Remove the specified members from the sorted set value stored at key. (Variadic: >= 2.4)
    */
  def zrem(key: Any, member: Any, members: Any*)(implicit format: Format): F[Resp[Option[Long]]]

  def zincrby(key: Any, incr: Double, member: Any)(implicit format: Format): F[Resp[Option[Double]]]

  def zcard(key: Any)(implicit format: Format): F[Resp[Option[Long]]]

  def zscore(key: Any, element: Any)(implicit format: Format): F[Resp[Option[Double]]]

  def zrange[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[List[A]]]]

  def zrangeWithScore[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[List[(A, Double)]]]]

  def zrangebylex[A](key: Any, min: String, max: String, limit: Option[(Int, Int)])(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[List[A]]]]

  def zrangebyscore[A](
      key: Any,
      min: Double = Double.NegativeInfinity,
      minInclusive: Boolean = true,
      max: Double = Double.PositiveInfinity,
      maxInclusive: Boolean = true,
      limit: Option[(Int, Int)],
      sortAs: SortOrder = ASC
  )(implicit format: Format, parse: Parse[A]): F[Resp[Option[List[A]]]]

  def zrangebyscoreWithScore[A](
      key: Any,
      min: Double = Double.NegativeInfinity,
      minInclusive: Boolean = true,
      max: Double = Double.PositiveInfinity,
      maxInclusive: Boolean = true,
      limit: Option[(Int, Int)],
      sortAs: SortOrder = ASC
  )(implicit format: Format, parse: Parse[A]): F[Resp[Option[List[(A, Double)]]]]

  def zrank(key: Any, member: Any, reverse: Boolean = false)(implicit format: Format): F[Resp[Option[Long]]]

  def zremrangebyrank(key: Any, start: Int = 0, end: Int = -1)(implicit format: Format): F[Resp[Option[Long]]]

  def zremrangebyscore(key: Any, start: Double = Double.NegativeInfinity, end: Double = Double.PositiveInfinity)(
      implicit format: Format
  ): F[Resp[Option[Long]]]

  def zunionstore(dstKey: Any, keys: Iterable[Any], aggregate: Aggregate = SUM)(
      implicit format: Format
  ): F[Resp[Option[Long]]]

  def zunionstoreWeighted(dstKey: Any, kws: Iterable[Product2[Any, Double]], aggregate: Aggregate = SUM)(
      implicit format: Format
  ): F[Resp[Option[Long]]]

  def zinterstore(dstKey: Any, keys: Iterable[Any], aggregate: Aggregate = SUM)(
      implicit format: Format
  ): F[Resp[Option[Long]]]

  def zinterstoreWeighted(dstKey: Any, kws: Iterable[Product2[Any, Double]], aggregate: Aggregate = SUM)(
      implicit format: Format
  ): F[Resp[Option[Long]]]

  def zcount(
      key: Any,
      min: Double = Double.NegativeInfinity,
      max: Double = Double.PositiveInfinity,
      minInclusive: Boolean = true,
      maxInclusive: Boolean = true
  )(implicit format: Format): F[Resp[Option[Long]]]

  /**
    * Incrementally iterate sorted sets elements and associated scores (since 2.8)
    */
  def zscan[A](key: Any, cursor: Int, pattern: Any = "*", count: Int = 10)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[(Option[Int], Option[List[Option[A]]])]]]
}
