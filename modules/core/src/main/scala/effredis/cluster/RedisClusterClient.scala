package effredis.cluster

import java.net.URI

sealed trait Topology

final case class RedisClusterClient(
  initialURIs: Set[URI],
  topology: Topology
)