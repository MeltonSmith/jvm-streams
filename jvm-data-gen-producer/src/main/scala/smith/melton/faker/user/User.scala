package smith.melton.faker.user

import org.scalacheck.{Arbitrary, Gen}
import pureconfig.ConfigReader
import pureconfig.generic.semiauto.deriveReader
import smith.melton.faker.CustomResourceLoader

import java.util.UUID

/**
 * @author Melton Smith
 * @since 31.05.2025
 */
final case class User(id: Long, name: String)

object User {
  def users(implicit loader: CustomResourceLoader): Seq[User] =
    loader.loadKey[Seq[User]]("users")

  implicit def userArbitrary(implicit
                                  loader: CustomResourceLoader
                                 ): Arbitrary[User] =
    Arbitrary(Gen.oneOf(users))

  implicit val firstNameConfigReader: ConfigReader[User] = deriveReader
}
