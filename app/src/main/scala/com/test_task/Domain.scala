package com.test_task


object Domain {

  type OrderId = String
  type UserId = String
  type ProductId = String
  type Price = Double

  case class Order(order_id: OrderId, user_id: UserId, product_id: String, price: Price)

}
