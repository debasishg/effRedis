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
import RedisClient._
import algebra.SortedSetApi
import codecs._

trait SortedSetOperations[F[+_]] extends SortedSetApi[F] { self: Redis[F, _] =>
  implicit def conc: Concurrent[F]
  implicit def ctx: ContextShift[F]

  override def zadd(key: Any, score: Double, member: Any, scoreVals: (Double, Any)*)(
      implicit format: Format
  ): F[Resp[Option[Long]]] =
    send("ZADD", List(key, score, member) ::: scoreVals.toList.flatMap(x => List(x._1, x._2)))(asLong)

  override def zrem(key: Any, member: Any, members: Any*)(implicit format: Format): F[Resp[Option[Long]]] =
    send("ZREM", List(key, member) ::: members.toList)(asLong)

  override def zincrby(key: Any, incr: Double, member: Any)(implicit format: Format): F[Resp[Option[Double]]] =
    send("ZINCRBY", List(key, incr, member))(asBulk(Parse.Implicits.parseDouble))

  override def zcard(key: Any)(implicit format: Format): F[Resp[Option[Long]]] =
    send("ZCARD", List(key))(asLong)

  override def zscore(key: Any, element: Any)(implicit format: Format): F[Resp[Option[Double]]] =
    send("ZSCORE", List(key, element))(asBulk(Parse.Implicits.parseDouble))

  override def zrange[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[List[A]]]] =
    send(if (sortAs == ASC) "ZRANGE" else "ZREVRANGE", List(key, start, end))(asList.map(_.flatten))

  override def zrangeWithScore[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[List[(A, Double)]]]] =
    send(if (sortAs == ASC) "ZRANGE" else "ZREVRANGE", List(key, start, end, "WITHSCORES"))(
      asListPairs(parse, Parse.Implicits.parseDouble).map(_.flatten)
    )

  override def zrangebylex[A](key: Any, min: String, max: String, limit: Option[(Int, Int)])(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[List[A]]]] =
    if (!limit.isEmpty) {
      val params = limit.toList.flatMap(l => List(key, min, max, "LIMIT", l._1, l._2))
      send("ZRANGEBYLEX", params)(asList.map(_.flatten))
    } else {
      send("ZRANGEBYLEX", List(key, min, max))(asList.map(_.flatten))
    }

  override def zrangebyscore[A](
      key: Any,
      min: Double = Double.NegativeInfinity,
      minInclusive: Boolean = true,
      max: Double = Double.PositiveInfinity,
      maxInclusive: Boolean = true,
      limit: Option[(Int, Int)],
      sortAs: SortOrder = ASC
  )(implicit format: Format, parse: Parse[A]): F[Resp[Option[List[A]]]] = {

    val (limitEntries, minParam, maxParam) =
      zrangebyScoreWithScoreInternal(min, minInclusive, max, maxInclusive, limit)

    val params = sortAs match {
      case ASC  => ("ZRANGEBYSCORE", key :: minParam :: maxParam :: limitEntries)
      case DESC => ("ZREVRANGEBYSCORE", key :: maxParam :: minParam :: limitEntries)
    }
    send(params._1, params._2)(asList.map(_.flatten))
  }

  override def zrangebyscoreWithScore[A](
      key: Any,
      min: Double = Double.NegativeInfinity,
      minInclusive: Boolean = true,
      max: Double = Double.PositiveInfinity,
      maxInclusive: Boolean = true,
      limit: Option[(Int, Int)],
      sortAs: SortOrder = ASC
  )(implicit format: Format, parse: Parse[A]): F[Resp[Option[List[(A, Double)]]]] = {

    val (limitEntries, minParam, maxParam) =
      zrangebyScoreWithScoreInternal(min, minInclusive, max, maxInclusive, limit)

    val params = sortAs match {
      case ASC  => ("ZRANGEBYSCORE", key :: minParam :: maxParam :: "WITHSCORES" :: limitEntries)
      case DESC => ("ZREVRANGEBYSCORE", key :: maxParam :: minParam :: "WITHSCORES" :: limitEntries)
    }
    send(params._1, params._2)(asListPairs(parse, Parse.Implicits.parseDouble).map(_.flatten))
  }

  private def zrangebyScoreWithScoreInternal[A](
      min: Double,
      minInclusive: Boolean,
      max: Double,
      maxInclusive: Boolean,
      limit: Option[(Int, Int)]
  ): (List[Any], String, String) = {

    val limitEntries =
      if (!limit.isEmpty) {
        "LIMIT" :: limit.toList.flatMap(l => List(l._1.toString, l._2.toString))
      } else {
        List()
      }

    val minParam = Format.formatDouble(min, minInclusive)
    val maxParam = Format.formatDouble(max, maxInclusive)
    (limitEntries, minParam, maxParam)
  }

  override def zrank(key: Any, member: Any, reverse: Boolean = false)(
      implicit format: Format
  ): F[Resp[Option[Long]]] =
    send(if (reverse) "ZREVRANK" else "ZRANK", List(key, member))(asLong)

  override def zremrangebyrank(key: Any, start: Int = 0, end: Int = -1)(
      implicit format: Format
  ): F[Resp[Option[Long]]] =
    send("ZREMRANGEBYRANK", List(key, start, end))(asLong)

  override def zremrangebyscore(
      key: Any,
      start: Double = Double.NegativeInfinity,
      end: Double = Double.PositiveInfinity
  )(implicit format: Format): F[Resp[Option[Long]]] =
    send("ZREMRANGEBYSCORE", List(key, start, end))(asLong)

  override def zunionstore(dstKey: Any, keys: Iterable[Any], aggregate: Aggregate = SUM)(
      implicit format: Format
  ): F[Resp[Option[Long]]] =
    send("ZUNIONSTORE", (Iterator(dstKey, keys.size) ++ keys.iterator ++ Iterator("AGGREGATE", aggregate)).toList)(
      asLong
    )

  override def zunionstoreWeighted(dstKey: Any, kws: Iterable[Product2[Any, Double]], aggregate: Aggregate = SUM)(
      implicit format: Format
  ): F[Resp[Option[Long]]] =
    send(
      "ZUNIONSTORE",
      (Iterator(dstKey, kws.size) ++ kws.iterator.map(_._1) ++ Iterator.single("WEIGHTS") ++ kws.iterator
            .map(_._2) ++ Iterator("AGGREGATE", aggregate)).toList
    )(asLong)

  override def zinterstore(dstKey: Any, keys: Iterable[Any], aggregate: Aggregate = SUM)(
      implicit format: Format
  ): F[Resp[Option[Long]]] =
    send("ZINTERSTORE", (Iterator(dstKey, keys.size) ++ keys.iterator ++ Iterator("AGGREGATE", aggregate)).toList)(
      asLong
    )

  override def zinterstoreWeighted(dstKey: Any, kws: Iterable[Product2[Any, Double]], aggregate: Aggregate = SUM)(
      implicit format: Format
  ): F[Resp[Option[Long]]] =
    send(
      "ZINTERSTORE",
      (Iterator(dstKey, kws.size) ++ kws.iterator.map(_._1) ++ Iterator.single("WEIGHTS") ++ kws.iterator
            .map(_._2) ++ Iterator("AGGREGATE", aggregate)).toList
    )(asLong)

  override def zcount(
      key: Any,
      min: Double = Double.NegativeInfinity,
      max: Double = Double.PositiveInfinity,
      minInclusive: Boolean = true,
      maxInclusive: Boolean = true
  )(implicit format: Format): F[Resp[Option[Long]]] =
    send("ZCOUNT", List(key, Format.formatDouble(min, minInclusive), Format.formatDouble(max, maxInclusive)))(asLong)

  override def zscan[A](key: Any, cursor: Int, pattern: Any = "*", count: Int = 10)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[(Option[Int], Option[List[Option[A]]])]]] =
    send(
      "ZSCAN",
      key :: cursor :: ((x: List[Any]) => if (pattern == "*") x else "match" :: pattern :: x)(
            if (count == 10) Nil else List("count", count)
          )
    )(asPair)

}
