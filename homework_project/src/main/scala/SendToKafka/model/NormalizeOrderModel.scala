package SendToKafka.model

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.Locale
import scala.util.Try

case class NormalizeOrderModel(
                                order_id: String,
                                restaurant_id: String,
                                restaurant_name: String,
                                district: String,
                                city: String,
                                order_datetime: String,
                                status: String,
                                distance_km: Double,
                                items: Seq[OrderItemModel],
                                bill_amount: Double,
                                total_amount: Double,
                                rating: Double,
                                prep_time_mins: Double,
                                rider_wait_mins: Double,
                                customer_id: String
                              ) {
}
object NormalizeOrderModel {
  // Метод для создания NormalizedOrder из RawOrder
  def fromRaw(raw: RawOrderModel): NormalizeOrderModel = {
    NormalizeOrderModel(
      order_id = safeString(raw.order_id),
      restaurant_id = safeString(raw.restaurant_id),
      restaurant_name = safeString(raw.restaurant_name),
      district = safeString(raw.district),
      city = safeString(raw.city),
      order_datetime = parseDateTime(raw.order_time),
      status = safeString(raw.status),
      distance_km = normalizeDistance(raw.distance),
      items = parseItems(safeString(raw.items)),
      bill_amount = safeDouble(raw.bill_subtotal),
      total_amount = safeDouble(raw.total),
      rating = safeDouble(raw.rating),
      prep_time_mins = safeDouble(raw.prep_time),
      rider_wait_mins = safeDouble(raw.rider_wait_time),
      customer_id = safeString(raw.customer_id)
    )
  }

  private def safeString(value: String): String = Option(value).getOrElse("")

  private def safeDouble(value: String): Double = {
    Try(Option(value).map(_.trim).filter(_.nonEmpty).map(_.toDouble).getOrElse(0.0))
      .getOrElse(0.0)
  }

  private def safeInt(value: String): Int = {
    Try(Option(value).map(_.trim).filter(_.nonEmpty).map(_.toInt).getOrElse(0))
      .getOrElse(0)
  }
  // Парсинг даты (11:38 PM, September 10 2024 -> ISO)
  private def parseDateTime(datetimeStr: String): String = {
    Try {
      val formatter = DateTimeFormatter.ofPattern("h:mm a, MMMM dd yyyy", Locale.ENGLISH)
      LocalDateTime.parse(datetimeStr, formatter).toString
    }.getOrElse {
      println(s"Warning: Failed to parse date '$datetimeStr'")
      LocalDateTime.now().toString
    }
  }
  // Парсинг строки с товарами
  private def parseItems(itemsStr: String): Seq[OrderItemModel] = {
    itemsStr.split(",")
      .map(_.trim)
      .filter(_.nonEmpty)
      .flatMap { item =>
        item.split(" x ", 2) match {
          case Array(qty, name) =>
            Some(OrderItemModel(qty.trim.toInt, name.trim))
          case _ => None
        }
      }
  }

  // Нормализация расстояния
  private def normalizeDistance(distance: String): Double = {
    distance match {
      case d if d.contains("<") => 0.9
      case d => d.replace("km", "").trim.toDouble
    }
  }
}
