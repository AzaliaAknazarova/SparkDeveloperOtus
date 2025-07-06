package model

import java.sql.Timestamp

case class BookModel(
                 Name: String,
                 Author: String,
                 UserRating: Double,
                 Reviews: Int,
                 Price: Int,
                 Year: Int,
                 Genre: String
               )
