package smith.melton.faker

import pureconfig.{ConfigObjectSource, ConfigReader, ConfigSource}

import java.util.Locale
import scala.reflect.ClassTag

/**
 * @author Melton Smith
 * @since 31.05.2025
 */
class CustomResourceLoader(private val locale: Locale) {

  private val defaultConfig: ConfigObjectSource =
    ConfigSource.resources("customfaker/default.conf")
  private val languageConfig: ConfigObjectSource =
    ConfigSource.resources(s"customfaker/${locale.getLanguage}.conf")
  private val localeConfig: ConfigObjectSource =
    ConfigSource.resources(s"customfaker/${locale.toString}.conf")

  // Fallback Pattern: en-US.conf -> en.conf -> default.conf
  private val conf: ConfigSource =
    localeConfig.optional
      .withFallback(languageConfig.optional)
      .withFallback(defaultConfig)

  def loadKey[A: ConfigReader: ClassTag](key: String): A =
    conf.at(key).loadOrThrow[A]
}


object CustomResourceLoader {
  val default: CustomResourceLoader = new CustomResourceLoader(Locale.ENGLISH)

  object Implicits {
    implicit val defaultResourceLoader: CustomResourceLoader = default
  }
}
