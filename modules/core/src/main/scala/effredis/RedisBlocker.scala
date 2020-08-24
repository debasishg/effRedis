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
import java.util.concurrent._

trait RedisBlocker {
  def ec: Blocker
}

object RedisBlocker {
  def apply(blocker: Blocker): RedisBlocker =
    new RedisBlocker {
      def ec: Blocker = blocker
    }

  private[effredis] def make[F[+_]: Sync](threadCountInPool: Int = 1): Resource[F, Blocker] =
    Blocker.fromExecutorService(F.delay(Executors.newFixedThreadPool(threadCountInPool)))
}
