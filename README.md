# effRedis
Non-blocking, effectful Scala client for Redis implemented using [cats](https://github.com/typelevel/cats) and [cats-effect](https://github.com/typelevel/cats-effect). It uses the existing [scala-redis](https://github.com/debasishg/scala-redis) client as the underlying implementation. I have plans of making a few improvements in the underlying implementation as well, but things will take time. I can only afford a couple of hours per week on this.

**This is an exercise that has just started and it will remain WIP for quite some time now. Feedback welcome.**


# Sample Usage

```scala
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