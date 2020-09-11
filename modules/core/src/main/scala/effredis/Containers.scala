package effredis

object Containers {
  final case class Latitude(value: Double) extends AnyVal
  final case class Longitude(value: Double) extends AnyVal

  sealed trait GeoCoordinate
  final case class ValidGeoCoordinate(longitude: Longitude, latitude: Latitude) extends GeoCoordinate
  case object UnknownGeoCoordinate extends GeoCoordinate

  final case class GeoLocation(longitude: Longitude, latitude: Latitude, member: String)

  sealed trait GeoUnit
  case object m extends GeoUnit
  case object km extends GeoUnit
  case object mi extends GeoUnit
  case object ft extends GeoUnit
}
