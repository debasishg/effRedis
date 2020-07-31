package effredis.algebra

import effredis.serialization.{Format, Parse}

trait EvalApi[F[_]] {

  /**
   * evaluates lua code on the server.
   */
  def evalMultiBulk[A](luaCode: String, keys: List[Any], args: List[Any])
                      (implicit format: Format, parse: Parse[A]): F[Option[List[Option[A]]]]

  def evalBulk[A](luaCode: String, keys: List[Any], args: List[Any])
                 (implicit format: Format, parse: Parse[A]): F[Option[A]]

  def evalInt(luaCode: String, keys: List[Any], args: List[Any]): F[Option[Int]]

  def evalMultiSHA[A](shahash: String, keys: List[Any], args: List[Any])
                     (implicit format: Format, parse: Parse[A]): F[Option[List[Option[A]]]]

  def evalSHA[A](shahash: String, keys: List[Any], args: List[Any])
                (implicit format: Format, parse: Parse[A]): F[Option[A]]

  def evalSHABulk[A](shahash: String, keys: List[Any], args: List[Any])
                    (implicit format: Format, parse: Parse[A]): F[Option[A]]

  def scriptLoad(luaCode: String): F[Option[String]]

  def scriptExists(shahash: String): F[Option[Int]]

  def scriptFlush: F[Option[String]]

}