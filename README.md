# effRedis

[![Build Status](https://travis-ci.org/debasishg/effRedis.svg?branch=master)](https://travis-ci.org/debasishg/effRedis)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.github.debasishg/effredis-core_2.13/badge.svg)](https://maven-badges.herokuapp.com/maven-central/io.github.debasishg/effredis-core_2.13) <a href="https://typelevel.org/cats/"><img src="https://typelevel.org/cats/img/cats-badge.svg" height="40px" align="right" alt="Cats friendly" /></a>
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-brightgreen.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)


Non-blocking, effectful Scala client for Redis implemented using [cats](https://github.com/typelevel/cats) and [cats-effect](https://github.com/typelevel/cats-effect). It uses the existing [scala-redis](https://github.com/debasishg/scala-redis) client as the underlying implementation. I have plans of making a few improvements in the underlying implementation as well, but things will take time. I can only afford a couple of hours per week on this.



# Sample Usage

## Using Single instance

```scala
package effredis

import java.net.URI
import cats.effect._
import cats.implicits._
import log4cats._

object Main extends LoggerIOApp {
  override def run(args: List[String]): IO[ExitCode] =
    RedisClient.make[IO](new URI("http://localhost:6379")).use { cmd =>
      import cmd._

      // just 1 command
      println(set("k1", "v1").unsafeRunSync())

      // List of commands
      println(List(set("k1", "v1"), get("k1")).sequence.unsafeRunSync())
      println(List(set("k1", "v1"), get("k1"), set("k2", 100), incrby("k2", 12)).sequence.unsafeRunSync())

      // Use as applicative
      case class Foo(str: String, num: Long)

      val res = (set("k1", "v1"), set("k2", 100), get("k1"), incrby("k2", 12)).parMapN { (_, _, k1val, k2val) =>
        (k1val, k2val) match {
          case (Value(Some(k1)), Value(Some(k2))) => Foo(k1, k2)
          case err                                => println(s"Error $err")
        }
      }
      println(res.unsafeRunSync())

      // monadic
      val result = for {

        a <- set("k1", "v1")
        b <- set("k2", "v2")
        c <- get("k1")

      } yield (a, b, c)

      println(result.unsafeRunSync())

      // monadic with fail
      val rsult = for {

        a <- set("k1", "vnew")
        b <- set("k2", "v2")
        c <- lpop("k1")
        d <- get("k1")

      } yield List(a, b, c, d)

      println(rsult.unsafeRunSync())

      // applicative
      val rs = (
        set("k1", "vnew"),
        set("k2", "v2"),
        lpop("k1"),
        get("k1")
      ).parMapN((a, b, c, d) => List(a, b, c, d))

      println(rs.unsafeRunSync())

      IO(ExitCode.Success)
    }
}
```

## Using Redis Cluster

```scala
package effredis
package cluster

import java.net.URI
import scala.concurrent.duration._
import cats.effect._
import cats.implicits._
import log4cats._

object Cluster extends LoggerIOApp {

  val nKeys = 60000
  def program: IO[Unit] =
    RedisClusterClient.make[IO](new URI("http://localhost:7000")).flatMap { cl =>
      for {
        // optionally set refresh interval at which the cluster topology will be refreshed
        // we start a fibre here that will expire the cache at the specified interval
        _ <- util.ClusterUtils.repeatAtFixedRate(2.seconds, cl.topologyCache.expire).start
        _ <- RedisClientPool.poolResource[IO].use { pool =>
               implicit val p = pool
               for {
                 _ <- (0 to nKeys)
                       .map(i => cl.set(s"key$i", s"value $i"))
                       .toList
                       .sequence
               } yield ()
             }
      } yield ()
    }

  override def run(args: List[String]): IO[ExitCode] = {
    program.unsafeRunSync()
    IO(ExitCode.Success)
  }
}
```

### Dependencies

Add this to your `build.sbt` for the Core API (depends on `cats-effect`):

```
libraryDependencies += "io.github.debasishg" %% "effredis-core" % Version
```

### Log4cats support

`effredis` uses `log4cats` for internal logging. It is the recommended logging library:

```
libraryDependencies += "io.github.debasishg" %% "effredis-log4cats" % Version
```

(Adopted from [redis4cats](https://github.com/profunktor/redis4cats))

## Running the tests locally

Start Redis locally or using `docker-compose`:

```bash
> docker-compose up
> sbt +test
```
