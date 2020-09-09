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
package algebra

import effredis.Resp

trait ClusterApi[F[+_]] {
  def clusterNodes: F[Resp[Option[String]]]
  // def clusterSlots: F[Resp[Option[List[Option[List[Option[String]]]]]]]
  def clusterSlots: F[Resp[Option[Any]]]
  def asking: F[Resp[Boolean]]
}
