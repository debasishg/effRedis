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
package cluster

import io.chrisdavenport.keypool._
import util.ClusterUtils
import java.net.URI
import scala.concurrent.duration._
import cats.data.NonEmptyList
import cats.effect._
import cats.syntax.all._
import log4cats._
import RedisClient._

object ClusterP extends LoggerIOApp {

  val nKeys = 60000
  val nsubs = 6
  def subProgram(cl: RedisClusterClient[IO, SINGLE.type], keyPrefix: String, valuePrefix: String)(
      implicit pool: KeyPool[IO, URI, (RedisClient[IO, SINGLE.type], IO[Unit])]
  ): IO[Unit] =
    for {
      _ <- (0 to nKeys)
            .map(i => cl.set(s"$keyPrefix$i", s"$valuePrefix $i"))
            .toList
            .sequence
    } yield ()

  def program: IO[Unit] =
    RedisClusterClient.make[IO, SINGLE.type](NonEmptyList.one(new URI("http://localhost:7000"))).flatMap { cl =>
      for {
        // optionally the cluster topology can be refreshed to reflect the latest partitions
        // this step schedules that job at a pre-configured interval
        _ <- ClusterUtils.repeatAtFixedRate(30.seconds, cl.topologyCache.expire).start
        _ <- RedisClientPool.poolResource[IO, SINGLE.type](SINGLE).use { pool =>
              implicit val p = pool
              // parallelize the job with fibers
              // can be done when you have parallelizable fragments of jobs
              // also handles cancelation
              (
                subProgram(cl, "k1", "v1").start,
                subProgram(cl, "k2", "v2").start,
                subProgram(cl, "k3", "v3").start,
                subProgram(cl, "k4", "v4").start,
                subProgram(cl, "k4", "v4").start,
                subProgram(cl, "k4", "v4").start
              ).tupled.bracket {
                case (fa, fb, fc, fd, fe, ff) =>
                  (fa.join, fb.join, fc.join, fd.join, fe.join, ff.join).tupled
              } {
                case (fa, fb, fc, fd, fe, ff) =>
                  fa.cancel >> fb.cancel >> fc.cancel >> fd.cancel >> fe.cancel >> ff.cancel
              }
            }
      } yield ()
    }

  override def run(args: List[String]): IO[ExitCode] = {
    val start = System.currentTimeMillis()
    program.unsafeRunSync()
    val elapsed = (System.currentTimeMillis() - start) / 1000
    println(s"Elapsed: $elapsed seconds for 360000 sets")
    println(s"Rate: ${(elapsed * 1000000) / (nKeys * nsubs)} microsecs per set")
    IO(ExitCode.Success)
  }
}
