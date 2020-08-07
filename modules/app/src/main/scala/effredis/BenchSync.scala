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

import scala.concurrent._
import scala.util.{ Failure, Success }
import com.redis._

object BenchSync extends App {
  Util.run()
}

object Util {
  val redisPoll = new RedisClientPool("localhost", 6379)

  def run(): Unit = {
    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
    for (iteration <- 0 to 4) {
      val s = System.currentTimeMillis()
      setRedisSymbolsAsync(iteration).onComplete {
        case Success(r) => println(s"set redis symbols async in ${System.currentTimeMillis() - s} ms: ${r}")
        case Failure(_) => println("Failure on getting Redis symbols")
      }
    }

    for (iteration <- 0 to 4) {
      val s = System.currentTimeMillis()
      getRedisSymbolsAsync(iteration).onComplete {
        case Success(r) => println(s"got redis symbols async in ${System.currentTimeMillis() - s} ms: ${r}")
        case Failure(_) => println("Failure on getting Redis symbols")
      }
    }
  }

  def setRedisSymbolsAsync(terminalId: Int)(implicit ex: ExecutionContext) =
    Future {
      redisPoll.withClient { client =>
        val _ = client.set(s"key_prefix_${terminalId}", s"value_prefix_${terminalId}")
        ()
      }
    }

  def getRedisSymbolsAsync(terminalId: Int)(implicit ex: ExecutionContext) =
    Future {
      redisPoll.withClient { client =>
        val data = client.get(s"key_prefix_${terminalId}")
        data
      }
    }
}
