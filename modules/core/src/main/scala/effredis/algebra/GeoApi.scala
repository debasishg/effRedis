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
import Containers._

trait GeoApi[F[+_]] {

  def geoadd(key: Any, members: GeoLocation*): F[Resp[Long]]

  def geopos(key: Any, members: Any*)(
      implicit format: Format
  ): F[Resp[List[Option[GeoCoordinate]]]]

  def geohash(key: Any, members: Iterable[Any])(
      implicit format: Format
  ): F[Resp[List[Option[String]]]]

  def geodist(key: Any, m1: Any, m2: Any, unit: Option[GeoUnit])(
      implicit format: Format
  ): F[Resp[Option[Double]]]

  def georadius[A](key: Any, geoRadius: GeoRadius, unit: GeoUnit)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Set[Option[A]]]]

  def georadius[A](key: Any, geoRadius: GeoRadius, unit: GeoUnit, args: GeoRadiusArgs)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[List[GeoRadiusMember]]]

  def georadiusByMember[A](key: Any, value: Any, distance: Distance, unit: GeoUnit)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[Set[Option[A]]]]

  def georadiusByMember[A](key: Any, value: Any, distance: Distance, unit: GeoUnit, args: GeoRadiusArgs)(
      implicit format: Format,
      parse: Parse[A]
  ): F[Resp[List[GeoRadiusMember]]]
}
