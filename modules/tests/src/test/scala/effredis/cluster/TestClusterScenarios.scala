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

import cats.effect._

import effredis.RedisClient

trait TestClusterScenarios {
  implicit def cs: ContextShift[IO]

  final def parseClusterSlots(client: RedisClient[IO, RedisClient.SINGLE.type]): IO[Unit] = {
    println(client.clusterSlots.unsafeRunSync())
    for {
      _ <- client.flushdb
    } yield ()
  }
}
