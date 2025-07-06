import scala.io.StdIn.readLine

object Main {
  // новая аннотация для скала 3
  @main def run(): Unit = {
    // 1
    val weight = 12.9
    var age = 25
    var name = "Ivan"
    var isStudent = false

    // 2
    println(s"Имя: $name")
    println(s"Возраст: $age")
    println(s"Вес: $weight")
    println(s"Является студентом: $isStudent")

    println("___________________________________")

    // 3
    def sum(a: Int, b: Int): Int = a + b

    // 3.1
    val aVal = 25
    val bVal = 36
    println(s"Сумма двух целых чисел $aVal и $bVal = ${sum(aVal, bVal)}")

    println("___________________________________")
    // 4
    def ageStatus(age: Int): String =
      if (age < 30)
        "Молодой"
      else
        "Взрослый"

    // 4.1
    println(s"$name - ${ageStatus(age)}")

    println("___________________________________")
    // 5
    for (i <- 1 to 10){
      println(i)
    }

    println("___________________________________")
    // 5.1
    val students = List("Ivan", "Anna", "Oleg", "Niki", "Gleb")

    for (student <- students){
      println(student)
    }

    println("___________________________________")
    // 6
    println(s"Введите, пожалуйста, ваше имя")
    name = readLine()

    println(s"Введите, пожалуйста, ваш возраст")
    age = readLine().toInt

    println(s"Вы студент? (true/false)")
    isStudent = readLine().toBoolean


    println(s"Имя: $name")
    println(s"Возраст: $age")
    println(s"Вес: $weight")
    println(s"Является студентом: $isStudent")

    println("___________________________________")
    // 7
    val listNumbers = List.range(1, 11)

    val squares = for (n <- listNumbers) yield n * n
    println(s"Квадраты чисел: $squares")

    println("___________________________________")

    val evens = for (n <- listNumbers if n % 2 == 0) yield n
    println(s"Чётные числа: $evens")

    println("___________________________________")
  }
}