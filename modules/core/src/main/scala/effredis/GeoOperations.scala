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
import Containers._

trait GeoOperations[F[+_]] extends GeoApi[F] { self: Redis[F, _] =>
  implicit def conc: Concurrent[F]
  implicit def ctx: ContextShift[F]

  override def geoadd(key: Any, members: GeoLocation*): F[Resp[Long]] = {
    val l: List[Any] =
      List(key) ::: members.toList.flatMap(m => List(m.longitude.value.toString, m.latitude.value.toString, m.member))
    send("GEOADD", l)(asInteger)
  }

  override def geopos(key: Any, members: Any*)(
      implicit format: Format
  ): F[Resp[List[Option[GeoCoordinate]]]] =
    send("GEOPOS", List(key) ::: members.toList) {
      asList.map {
        case List(Some(lo), Some(la)) => {
          Some(
            GeoCoordinate(
              Longitude(Parse.Implicits.parseDouble(lo.toString.getBytes("UTF-8"))),
              Latitude(Parse.Implicits.parseDouble(la.toString.getBytes("UTF-8")))
            )
          )
        }
        case List(None) => None
        case _          => None
      }
    }

  override def geohash(
      key: Any,
      members: Iterable[Any]
  )(implicit format: Format): F[Resp[List[Option[String]]]] =
    send("GEOHASH", key :: members.toList)(asFlatList[String])

  override def geodist(key: Any, m1: Any, m2: Any, unit: Option[GeoUnit])(
      implicit format: Format
  ): F[Resp[Option[Double]]] =
    send("GEODIST", List(key, m1, m2) ++ List(unit.getOrElse(Containers.m)))(asBulkString[String].map(_.toDouble))

  override def georadius[A](key: Any, geoRadius: GeoRadius, unit: GeoUnit)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Set[Option[A]]]] =
    send("GEORADIUS", List(key) ::: geoRadius.asListString ::: List(unit))(asSet)

  override def georadius[A](key: Any, geoRadius: GeoRadius, unit: GeoUnit, args: GeoRadiusArgs)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[List[GeoRadiusMember]]] =
    send(
      "GEORADIUS",
      List(key) :::
          geoRadius.asListString :::
          List(unit) :::
          args.value
    )(processResponseForGeoRadius(asList, args))

  override def georadiusByMember[A](key: Any, value: Any, distance: Distance, unit: GeoUnit)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Set[Option[A]]]] =
    send("GEORADIUSBYMEMBER", List(key, value) ::: List(distance.value.toString, unit))(asSet)

  override def georadiusByMember[A](key: Any, value: Any, distance: Distance, unit: GeoUnit, args: GeoRadiusArgs)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[List[GeoRadiusMember]]] =
    send(
      "GEORADIUSBYMEMBER",
      List(key, value) :::
          List(distance.value.toString, unit) :::
          args.value
    )(processResponseForGeoRadius(asList, args))

  private def processResponseForGeoRadius(resp: List[Any], args: GeoRadiusArgs) = resp.map { mem =>
    val lmem = mem.asInstanceOf[List[_]]
    // member name
    val mname = lmem.head.asInstanceOf[Option[String]]

    // distance (if present)
    val mdist =
      if (args.withDist) lmem.tail.head.asInstanceOf[Option[String]].map(_.toDouble).map(Distance(_)) else None

    // hash (if present)
    val mhashIdx = if (mdist.isDefined) 2 else 1
    val mhash    = if (args.withHash) lmem(mhashIdx).asInstanceOf[Option[Long]] else None

    // coordinates (if present)
    val coords = if (args.withCoord) {
      val cos = lmem.last.asInstanceOf[List[Option[String]]].map(_.map(_.toDouble))
      Some(GeoCoordinate(Longitude(cos.head.get), Latitude(cos.tail.head.get)))
    } else None

    GeoRadiusMember(mname, mhash, mdist, coords)
  }
}
