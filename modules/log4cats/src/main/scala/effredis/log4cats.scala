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

// Adopted from :
// https://github.com/profunktor/redis4cats/blob/master/modules/log4cats/src/main/scala/dev/profunktor/redis4cats/log4cats.scala

package effredis

import io.chrisdavenport.log4cats.Logger

object log4cats {

  implicit def log4CatsInstance[F[_]: Logger]: Log[F] =
    new Log[F] {
      def debug(msg: => String): F[Unit] = F.debug(msg)
      def error(msg: => String): F[Unit] = F.error(msg)
      def info(msg: => String): F[Unit]  = F.info(msg)
    }

}
