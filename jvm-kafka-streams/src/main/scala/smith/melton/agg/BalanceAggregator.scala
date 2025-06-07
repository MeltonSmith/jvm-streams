package smith.melton.agg

import com.fasterxml.jackson.annotation.JsonProperty

/**
 * @author Melton Smith
 * @since 08.06.2025
 */
class BalanceAggregator {

  @JsonProperty("currentSum")
  var currentSum: Long = 0

  def add(sum: Long) = {
    currentSum += sum
  }

  def substract(sum: Long) = {
    currentSum -= sum
  }

}
