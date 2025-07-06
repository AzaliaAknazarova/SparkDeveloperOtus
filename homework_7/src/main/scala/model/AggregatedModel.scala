package model

case class AggregatedModel(
                              zone: String
                            , tripCount: Long
                            , minDistance: Option[Double]
                            , avgDistance: Option[Double]
                            , maxDistance: Option[Double]
                            , stddevDistance: Option[Double])
