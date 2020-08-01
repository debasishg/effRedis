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
import algebra.GeoApi
import codecs._

trait GeoOperations[F[_]] extends GeoApi[F] { self: Redis =>
  implicit def blocker: Blocker
  implicit def conc: Concurrent[F]
  implicit def ctx: ContextShift[F]

  private def flattenProduct3(in: Iterable[Product3[Any, Any, Any]]): List[Any] =
    in.iterator.flatMap(x => Iterator(x._1, x._2, x._3)).toList

  override def geoadd(key: Any, members: Iterable[Product3[Any, Any, Any]]): F[Option[Int]] =
    send("GEOADD", key :: flattenProduct3(members))(asInt)

  override def geopos[A](
      key: Any,
      members: Iterable[Any]
  )(implicit format: Format, parse: Parse[A]): F[Option[List[Option[List[Option[A]]]]]] =
    send("GEOPOS", key :: members.toList)(receive(multiBulkNested).map(_.map(_.map(_.map(_.map(parse))))))

  override def geohash[A](
      key: Any,
      members: Iterable[Any]
  )(implicit format: Format, parse: Parse[A]): F[Option[List[Option[A]]]] =
    send("GEOHASH", key :: members.toList)(asList[A])

  override def geodist(key: Any, m1: Any, m2: Any, unit: Option[Any]): F[Option[String]] =
    send("GEODIST", List(key, m1, m2) ++ unit.toList)(asBulk[String])

  override def georadius(
      key: Any,
      longitude: Any,
      latitude: Any,
      radius: Any,
      unit: Any,
      withCoord: Boolean,
      withDist: Boolean,
      withHash: Boolean,
      count: Option[Int],
      sort: Option[Any],
      store: Option[Any],
      storeDist: Option[Any]
  ): F[Option[List[Option[GeoRadiusMember]]]] = {
    val radArgs = List(
      if (withCoord) List("WITHCOORD") else Nil,
      if (withDist) List("WITHDIST") else Nil,
      if (withHash) List("WITHHASH") else Nil,
      sort.fold[List[Any]](Nil)(b => List(b)),
      count.fold[List[Any]](Nil)(b => List("COUNT", b)),
      store.fold[List[Any]](Nil)(b => List("STORE", b)),
      storeDist.fold[List[Any]](Nil)(b => List("STOREDIST", b))
    ).flatten
    send("GEORADIUS", List(key, longitude, latitude, radius, unit) ++ radArgs)(receive(geoRadiusMemberReply))
  }

  override def georadiusbymember[A](
      key: Any,
      member: Any,
      radius: Any,
      unit: Any,
      withCoord: Boolean,
      withDist: Boolean,
      withHash: Boolean,
      count: Option[Int],
      sort: Option[Any],
      store: Option[Any],
      storeDist: Option[Any]
  )(implicit format: Format, parse: Parse[A]): F[Option[List[Option[GeoRadiusMember]]]] = {
    val radArgs = List(
      if (withCoord) List("WITHCOORD") else Nil,
      if (withDist) List("WITHDIST") else Nil,
      if (withHash) List("WITHHASH") else Nil,
      sort.fold[List[Any]](Nil)(b => List(b)),
      count.fold[List[Any]](Nil)(b => List("COUNT", b)),
      store.fold[List[Any]](Nil)(b => List("STORE", b)),
      storeDist.fold[List[Any]](Nil)(b => List("STOREDIST", b))
    ).flatten
    send("GEORADIUSBYMEMBER", List(key, member, radius, unit) ++ radArgs)(receive(geoRadiusMemberReply))
  }

}
