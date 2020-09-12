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
      x <- geopos("Sicily", "Palermo", "Catania", "NonExisting")
      _ <- IO(assert(getRespList[GeoCoordinate](x).get.filter(_ != UnknownGeoCoordinate).size == 2))
      _ <- IO(assert(getRespList[GeoCoordinate](x).get.filter(_ == UnknownGeoCoordinate).size == 1))

    } yield ()
  }
}
