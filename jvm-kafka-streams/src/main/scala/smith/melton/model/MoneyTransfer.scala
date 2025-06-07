package smith.melton.model

import com.fasterxml.jackson.annotation.JsonProperty

/**
 * @author Melton Smith
 * @since 02.06.2025
 */
final case class MoneyTransfer(@JsonProperty("fromUserId") fromUserId: Long,
                               @JsonProperty("toUserId") toUserId: Long,
                               @JsonProperty("amount") amount: Long
                              )
