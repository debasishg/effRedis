package effredis.cluster

sealed trait NodeFlag
case object Master extends NodeFlag
case object Slave extends NodeFlag
case object Myself extends NodeFlag
case object EventualFail extends NodeFlag
case object Fail extends NodeFlag
case object NoAddr extends NodeFlag
case object Handshake extends NodeFlag