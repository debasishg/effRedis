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

package effredis.cluster

import enumeratum._

sealed abstract class NodeFlag(override val entryName: String) extends EnumEntry

object NodeFlag extends Enum[NodeFlag] {
  case object NoFlags extends NodeFlag("noflags")
  case object Upstream extends NodeFlag("master")
  case object Replica extends NodeFlag("slave")
  case object Myself extends NodeFlag("myself")
  case object EventualFail extends NodeFlag("fail?")
  case object Fail extends NodeFlag("fail")
  case object NoAddr extends NodeFlag("noaddr")
  case object Handshake extends NodeFlag("handshake")

  val values = findValues
}
