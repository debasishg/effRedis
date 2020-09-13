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
import EffRedisFunSuite._
import Containers._

trait TestGeoScenarios {
  implicit def cs: ContextShift[IO]

  final def geosGeoAdd(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    for {
      x <- geoadd(
            "Sicily",
            GeoLocation(Longitude(13.361389), Latitude(38.115556), "Palermo"),
            GeoLocation(Longitude(15.087269), Latitude(37.502669), "Catania")
          )
      _ <- IO(assert(getResp(x).get == 2))
    } yield ()
  }

  final def geosGeoPos(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    for {
      x <- geoadd(
            "Sicily",
            GeoLocation(Longitude(13.361389), Latitude(38.115556), "Palermo"),
            GeoLocation(Longitude(15.087269), Latitude(37.502669), "Catania")
          )
      _ <- IO(assert(getResp(x).get == 2))

      x <- geopos("Sicily", "Palermo", "Catania", "NonExisting")
      _ <- IO(assert(getRespList[Option[GeoCoordinate]](x).get.filter(_.isDefined).size == 2))
      _ <- IO(assert(getRespList[Option[GeoCoordinate]](x).get.filter(!_.isDefined).size == 1))
    } yield ()
  }

  final def geosGeoRadius(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    for {
      _ <- geoadd(
            "Sicily",
            GeoLocation(Longitude(13.361389), Latitude(38.115556), "Palermo"),
            GeoLocation(Longitude(15.087269), Latitude(37.502669), "Catania")
          )

      x <- georadius("Sicily", GeoRadius(Longitude(15), Latitude(37), Distance(200)), km)
      _ <- IO(assert(getResp(x).get == Set(Some("Palermo"), Some("Catania"))))
      x <- georadius("NonExisting", GeoRadius(Longitude(15), Latitude(37), Distance(200)), km)
      _ <- IO(assert(getResp(x).get == Set.empty))

      x <- georadius(
            "Sicily",
            GeoRadius(Longitude(15), Latitude(37), Distance(200)),
            km,
            GeoRadiusArgs(withCoord = true, withDist = true, withHash = true, Some(4), Some(ASC))
          )
      _ <- IO(assert(getRespListSize(x).get == 2))
      // _ <- IO(println(getResp(x)))
      _ <- IO {
            x match {
              case Value(members) => assert(members.map(_.member.get).toSet == Set("Catania", "Palermo"))
              case _              => false
            }
          }

      x <- georadius(
            "Sicily",
            GeoRadius(Longitude(15), Latitude(37), Distance(200)),
            km,
            GeoRadiusArgs(withCoord = true, withDist = false, withHash = true, Some(4), Some(ASC))
          )
      _ <- IO(assert(getRespListSize(x).get == 2))
      // _ <- IO(println(getResp(x)))
      _ <- IO {
            x match {
              case Value(members) => assert(members.map(_.member.get).toSet == Set("Catania", "Palermo"))
              case _              => false
            }
          }

    } yield ()
  }

  final def geosGeoRadiusByMember(cmd: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    import cmd._
    for {
      x <- geoadd(
            "Sicily",
            GeoLocation(Longitude(13.583333), Latitude(37.316667), "Agrigento"),
            GeoLocation(Longitude(13.361389), Latitude(38.115556), "Palermo"),
            GeoLocation(Longitude(15.087269), Latitude(37.502669), "Catania")
          )
      _ <- IO(assert(getResp(x).get == 3))
      x <- georadiusByMember("Sicily", "Agrigento", Distance(100), km)
      _ <- IO(assert(getResp(x).get == Set(Some("Agrigento"), Some("Palermo"))))
      x <- georadiusByMember(
            "Sicily",
            "Agrigento",
            Distance(100),
            km,
            GeoRadiusArgs(withCoord = true, withDist = true, withHash = true, Some(4), Some(ASC))
          )
      // _ <- IO(println(getResp(x)))
      _ <- IO {
            x match {
              case Value(members) => assert(members.map(_.member.get).toSet == Set("Agrigento", "Palermo"))
              case _              => false
            }
          }
    } yield ()
  }
}
