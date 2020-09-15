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
import Containers.{ Score, SortedSetUpdateOption, ValueScorePair }

trait SortedSetOperations[F[+_]] extends SortedSetApi[F] { self: Redis[F, _] =>
  implicit def conc: Concurrent[F]
  implicit def ctx: ContextShift[F]

  override def zadd(key: Any, score: Double, member: Any, scoreVals: (Double, Any)*)(
      implicit format: Format
  ): F[Resp[Long]] =
    send("ZADD", List(key, score, member) ::: scoreVals.toList.flatMap(x => List(x._1, x._2)))(asInteger)

  override def zadd(
      key: Any,
      updateOption: SortedSetUpdateOption,
      changed: Boolean,
      score: Double,
      member: Any,
      scoreVals: (Double, Any)*
  )(
      implicit format: Format
  ): F[Resp[Long]] = {
    val args =
      if (changed) {
        List(key, updateOption.toString, "CH", score, member) ::: scoreVals.toList.flatMap(x => List(x._1, x._2))
      } else {
        List(key, updateOption.toString, score, member) ::: scoreVals.toList.flatMap(x => List(x._1, x._2))
      }
    send("ZADD", args)(asInteger)
  }

  override def zadd(
      key: Any,
      updateOption: Option[SortedSetUpdateOption],
      changed: Boolean,
      incr: Boolean,
      score: Double,
      member: Any
  )(
      implicit format: Format
  ): F[Resp[Long]] = {
    val subArgs1 =
      if (changed && incr) List("CH", "INCR")
      else if (changed) List("CH")
      else if (incr) List("INCR")
      else Nil
    val subArgs2 =
      updateOption.map(u => List(u.toString)).getOrElse(Nil)

    send("ZADD", List(key) ::: subArgs2 ::: subArgs1 ::: List(score, member))(asInteger)
  }

  override def zrem(key: Any, member: Any, members: Any*)(implicit format: Format): F[Resp[Long]] =
    send("ZREM", List(key, member) ::: members.toList)(asInteger)

  override def zincrby(key: Any, incr: Double, member: Any)(implicit format: Format): F[Resp[Option[Double]]] =
    send("ZINCRBY", List(key, incr, member))(asBulkString(Parse.Implicits.parseDouble))

  override def zcard(key: Any)(implicit format: Format): F[Resp[Long]] =
    send("ZCARD", List(key))(asInteger)

  override def zscore(key: Any, element: Any)(implicit format: Format): F[Resp[Option[Score]]] =
    send("ZSCORE", List(key, element))(asBulkString(Parse.Implicits.parseDouble).map(Score))

  override def zrange[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[List[Option[A]]]] =
    send(if (sortAs == ASC) "ZRANGE" else "ZREVRANGE", List(key, start, end))(asFlatList)

  override def zrangeWithScore[A](key: Any, start: Int = 0, end: Int = -1, sortAs: SortOrder = ASC)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[List[(A, Score)]]] =
    send(if (sortAs == ASC) "ZRANGE" else "ZREVRANGE", List(key, start, end, "WITHSCORES"))(
      asFlatListPairs(parse, Parse.Implicits.parseDouble).map(e => (e._1, Score(e._2)))
    )

  override def zrangebylex[A](key: Any, min: String, max: String, limit: Option[(Int, Int)], sortAs: SortOrder = ASC)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[List[Option[A]]]] =
    if (!limit.isEmpty) {
      val params = limit.toList.flatMap(l => List(key, min, max, "LIMIT", l._1, l._2))
      send(if (sortAs == ASC) "ZRANGEBYLEX" else "ZREVRANGEBYLEX", params)(asFlatList)
    } else {
      send(if (sortAs == ASC) "ZRANGEBYLEX" else "ZREVRANGEBYLEX", List(key, min, max))(asFlatList)
    }

  override def zrangebyscore[A](
      key: Any,
      min: Double = Double.NegativeInfinity,
      minInclusive: Boolean = true,
      max: Double = Double.PositiveInfinity,
      maxInclusive: Boolean = true,
      limit: Option[(Int, Int)],
      sortAs: SortOrder = ASC
  )(implicit format: Format, parse: Parse[A]): F[Resp[List[Option[A]]]] = {

    val (limitEntries, minParam, maxParam) =
      zrangebyScoreWithScoreInternal(min, minInclusive, max, maxInclusive, limit)

    val params = sortAs match {
      case ASC  => ("ZRANGEBYSCORE", key :: minParam :: maxParam :: limitEntries)
      case DESC => ("ZREVRANGEBYSCORE", key :: maxParam :: minParam :: limitEntries)
    }
    send(params._1, params._2)(asFlatList)
  }

  override def zrangebyscoreWithScore[A](
      key: Any,
      min: Double = Double.NegativeInfinity,
      minInclusive: Boolean = true,
      max: Double = Double.PositiveInfinity,
      maxInclusive: Boolean = true,
      limit: Option[(Int, Int)],
      sortAs: SortOrder = ASC
  )(implicit format: Format, parse: Parse[A]): F[Resp[List[(A, Score)]]] = {

    val (limitEntries, minParam, maxParam) =
      zrangebyScoreWithScoreInternal(min, minInclusive, max, maxInclusive, limit)

    val params = sortAs match {
      case ASC  => ("ZRANGEBYSCORE", key :: minParam :: maxParam :: "WITHSCORES" :: limitEntries)
      case DESC => ("ZREVRANGEBYSCORE", key :: maxParam :: minParam :: "WITHSCORES" :: limitEntries)
    }
    send(params._1, params._2)(asFlatListPairs(parse, Parse.Implicits.parseDouble).map(e => (e._1, Score(e._2))))
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
  ): F[Resp[Long]] =
    send(if (reverse) "ZREVRANK" else "ZRANK", List(key, member))(asInteger)

  override def zremrangebyrank(key: Any, start: Int = 0, end: Int = -1)(
      implicit format: Format
  ): F[Resp[Long]] =
    send("ZREMRANGEBYRANK", List(key, start, end))(asInteger)

  override def zremrangebyscore(
      key: Any,
      start: Double = Double.NegativeInfinity,
      end: Double = Double.PositiveInfinity
  )(implicit format: Format): F[Resp[Long]] =
    send("ZREMRANGEBYSCORE", List(key, start, end))(asInteger)

  override def zremrangebylex[A](key: Any, min: String, max: String)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Long]] =
    send("ZREMRANGEBYLEX", List(key, min, max))(asInteger)

  override def zunionstore(dstKey: Any, keys: Iterable[Any], aggregate: Aggregate = SUM)(
      implicit format: Format
  ): F[Resp[Long]] =
    send("ZUNIONSTORE", (Iterator(dstKey, keys.size) ++ keys.iterator ++ Iterator("AGGREGATE", aggregate)).toList)(
      asInteger
    )

  override def zunionstoreWeighted(dstKey: Any, kws: Iterable[Product2[Any, Double]], aggregate: Aggregate = SUM)(
      implicit format: Format
  ): F[Resp[Long]] =
    send(
      "ZUNIONSTORE",
      (Iterator(dstKey, kws.size) ++ kws.iterator.map(_._1) ++ Iterator.single("WEIGHTS") ++ kws.iterator
            .map(_._2) ++ Iterator("AGGREGATE", aggregate)).toList
    )(asInteger)

  override def zinterstore(dstKey: Any, keys: Iterable[Any], aggregate: Aggregate = SUM)(
      implicit format: Format
  ): F[Resp[Long]] =
    send("ZINTERSTORE", (Iterator(dstKey, keys.size) ++ keys.iterator ++ Iterator("AGGREGATE", aggregate)).toList)(
      asInteger
    )

  override def zinterstoreWeighted(dstKey: Any, kws: Iterable[Product2[Any, Double]], aggregate: Aggregate = SUM)(
      implicit format: Format
  ): F[Resp[Long]] =
    send(
      "ZINTERSTORE",
      (Iterator(dstKey, kws.size) ++ kws.iterator.map(_._1) ++ Iterator.single("WEIGHTS") ++ kws.iterator
            .map(_._2) ++ Iterator("AGGREGATE", aggregate)).toList
    )(asInteger)

  override def zcount(
      key: Any,
      min: Double = Double.NegativeInfinity,
      max: Double = Double.PositiveInfinity,
      minInclusive: Boolean = true,
      maxInclusive: Boolean = true
  )(implicit format: Format): F[Resp[Long]] =
    send("ZCOUNT", List(key, Format.formatDouble(min, minInclusive), Format.formatDouble(max, maxInclusive)))(asInteger)

  override def zlexcount[A](key: Any, min: String, max: String)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Long]] =
    send("ZLEXCOUNT", List(key, min, max))(asInteger)

  override def zscan[A](key: Any, cursor: Int, pattern: Any = "*", count: Int = 10)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Option[(Int, List[Option[A]])]]] =
    send(
      "ZSCAN",
      key :: cursor :: ((x: List[Any]) => if (pattern == "*") x else "match" :: pattern :: x)(
            if (count == 10) Nil else List("count", count)
          )
    )(asPair)

  override def zpopmin[A](key: Any, count: Int = 1)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[List[ValueScorePair[A]]]] =
    send("ZPOPMIN", List(key, count))(
      asFlatListPairs[A, Double](parse, Parse.Implicits.parseDouble).map {
        case (v, s) => ValueScorePair[A](Score(s), v)
      }
    )

  override def zpopmax[A](key: Any, count: Int = 1)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[List[ValueScorePair[A]]]] =
    send("ZPOPMAX", List(key, count))(
      asFlatListPairs[A, Double](parse, Parse.Implicits.parseDouble).map {
        case (v, s) => ValueScorePair[A](Score(s), v)
      }
    )

  override def bzpopmin[K, V](
      timeoutInSeconds: Int,
      key: K,
      keys: K*
  )(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): F[Resp[Option[(K, ValueScorePair[V])]]] =
    send("BZPOPMIN", key :: keys.foldRight(List[Any](timeoutInSeconds))(_ :: _)) {
      asTriple[K, V, Double](parseK, parseV, Parse.Implicits.parseDouble) match {
        case None                      => None
        case r: Option[(K, V, Double)] => Some((r.get._1, ValueScorePair(Score(r.get._3), r.get._2)))
        case x                         => println(s"gt $x"); None
      }
    }

  override def bzpopmax[K, V](
      timeoutInSeconds: Int,
      key: K,
      keys: K*
  )(implicit format: Format, parseK: Parse[K], parseV: Parse[V]): F[Resp[Option[(K, ValueScorePair[V])]]] =
    send("BZPOPMAX", key :: keys.foldRight(List[Any](timeoutInSeconds))(_ :: _)) {
      asTriple[K, V, Double](parseK, parseV, Parse.Implicits.parseDouble) match {
        case None                      => None
        case r: Option[(K, V, Double)] => Some((r.get._1, ValueScorePair(Score(r.get._3), r.get._2)))
      }
    }
}
