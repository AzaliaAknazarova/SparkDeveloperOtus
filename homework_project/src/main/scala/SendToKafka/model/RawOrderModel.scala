package SendToKafka.model

case class RawOrderModel(
                          restaurant_id: String,
                          restaurant_name: String,
                          district: String,
                          city: String,
                          order_id: String,
                          order_time: String,
                          status: String,
                          distance: String,
                          items: String,
                          bill_subtotal: String,
                          total: String,
                          rating: String,
                          prep_time: String,
                          rider_wait_time: String,
                          customer_id: String
                        )