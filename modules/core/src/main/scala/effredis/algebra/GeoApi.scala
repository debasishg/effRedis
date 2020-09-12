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

// import codecs.{ Format, Parse }
import codecs.Format
import Containers._

trait GeoApi[F[+_]] {

  def geoadd(key: Any, members: GeoLocation*): F[Resp[Long]]

  def geopos(key: Any, members: Any*)(
      implicit format: Format
  ): F[Resp[List[GeoCoordinate]]]

  /*
  def geohash(key: Any, members: Iterable[Any])(
      implicit format: Format
  ): F[Resp[List[String]]]

  def geodist(key: Any, m1: Any, m2: Any, unit: Option[GeoUnit]): F[Resp[Double]]
   */

//   def georadius(
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
//   ): F[Resp[Option[List[Option[GeoRadiusMember]]]]]
//
//   def georadiusbymember[A](
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
//   )(implicit format: Format, parse: Parse[A]): F[Resp[Option[List[Option[GeoRadiusMember]]]]]
}
