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
import resp.RespValues._

trait GeoOperations[F[+_]] extends GeoApi[F] { self: Redis[F, _] =>
  implicit def conc: Concurrent[F]
  implicit def ctx: ContextShift[F]

  override def geoadd(key: Any, members: GeoLocation*): F[Resp[Long]] = {
    val l: List[Any] =
      List(key) ::: members.toList.flatMap(m => List(m.longitude.value.toString, m.latitude.value.toString, m.member))
    send("GEOADD", l)(asInteger)
  }

  def geopos(key: Any, members: Any*)(
      implicit format: Format
  ): F[Resp[List[GeoCoordinate]]] =
    send("GEOPOS", List(key) ::: members.toList) {
      asList.map {
        case List(lo, la) =>
          ValidGeoCoordinate(
            Longitude(Parse.Implicits.parseDouble(lo.toString.getBytes("UTF-8"))),
            Latitude(Parse.Implicits.parseDouble(la.toString.getBytes("UTF-8")))
          )
        case List(REDIS_NIL) => UnknownGeoCoordinate
      }
    }

  /*
  override def geohash[A](
      key: Any,
      members: Iterable[Any]
  )(implicit format: Format, parse: Parse[A]): F[Resp[Option[List[Option[A]]]]] =
    send("GEOHASH", key :: members.toList)(asList[A])

  override def geodist(key: Any, m1: Any, m2: Any, unit: Option[Any]): F[Resp[Option[String]]] =
    send("GEODIST", List(key, m1, m2) ++ unit.toList)(asBulk[String])
   */

//   override def georadius(
//       key: Any,
//       longitude: Any,
//       latitude: Any,
//       radius: Any,
//       unit: Any,
//       withCoord: Boolean,
//       withDist: Boolean,
//       withHash: Boolean,
//       count: Option[Int],
//       sort: Option[Any],
//       store: Option[Any],
//       storeDist: Option[Any]
//   ): F[Resp[Option[List[Option[GeoRadiusMember]]]]] = {
//     val radArgs = List(
//       if (withCoord) List("WITHCOORD") else Nil,
//       if (withDist) List("WITHDIST") else Nil,
//       if (withHash) List("WITHHASH") else Nil,
//       sort.fold[List[Any]](Nil)(b => List(b)),
//       count.fold[List[Any]](Nil)(b => List("COUNT", b)),
//       store.fold[List[Any]](Nil)(b => List("STORE", b)),
//       storeDist.fold[List[Any]](Nil)(b => List("STOREDIST", b))
//     ).flatten
//     send("GEORADIUS", List(key, longitude, latitude, radius, unit) ++ radArgs)(receive(geoRadiusMemberReply))
//   }
//
//   override def georadiusbymember[A](
//       key: Any,
//       member: Any,
//       radius: Any,
//       unit: Any,
//       withCoord: Boolean,
//       withDist: Boolean,
//       withHash: Boolean,
//       count: Option[Int],
//       sort: Option[Any],
//       store: Option[Any],
//       storeDist: Option[Any]
//   )(implicit format: Format, parse: Parse[A]): F[Resp[Option[List[Option[GeoRadiusMember]]]]] = {
//     val radArgs = List(
//       if (withCoord) List("WITHCOORD") else Nil,
//       if (withDist) List("WITHDIST") else Nil,
//       if (withHash) List("WITHHASH") else Nil,
//       sort.fold[List[Any]](Nil)(b => List(b)),
//       count.fold[List[Any]](Nil)(b => List("COUNT", b)),
//       store.fold[List[Any]](Nil)(b => List("STORE", b)),
//       storeDist.fold[List[Any]](Nil)(b => List("STOREDIST", b))
//     ).flatten
//     send("GEORADIUSBYMEMBER", List(key, member, radius, unit) ++ radArgs)(receive(geoRadiusMemberReply))
//   }
}
