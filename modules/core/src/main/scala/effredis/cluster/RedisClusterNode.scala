package effredis.cluster

import java.net.URI
import java.util.BitSet

final private[effredis] case class RedisClusterNode(
  uri: URI,
  nodeId: String,
  connected: Boolean,
  slaveOf: String,
  lastPendingPingSentTimestamp: Long,
  lastPongReceivedTimestamp: Long,
  configEpoch: Long,
  slots: BitSet,
  nodeFlags: Set[NodeFlag],
) {
  /**
    * Return the slots as {@link List}.
    * @return the slots as {@link List}.
    */
    def getSlots(): List[Int] = ???

    def hasSlot(slot: Int): Boolean = {
        slot <= HashSlot.TOTAL_HASH_SLOTS && !slots.isEmpty && slots.get(slot)
    }
  
}