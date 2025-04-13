import scala.io.StdIn.readLine

object Main {
  // новая аннотация для скала 3
  @main def run(): Unit = {
    println("Выберите номер задания (1-6):")

    val task = readLine().toInt // приведение к типу инт тк функция readLine возвращает только стринг значение

    val listInt = List(1, 2, 3, 4, 5)

    task match
      case 1 => greetUser()
      case 2 => sumTwoNumbers()
      case 3 => incrementList(listInt)
      case 4 => checkEvenOdd()
      case 5 => stringLength()
      case 6 => joinStrings()
      case _ => println("Нет такого задания.")
  }

  // 1. Ввод имени + приветствие
  def greetUser(): Unit = {
    println("Введите ваше имя:")
    val name = readLine()
    println(s"Привет, $name!")
  }

  // 2. Сумма двух чисел
  def sumTwoNumbers(): Unit = {
    println("Введите первое число:")
    val num1 = readLine().toInt

    println("Введите второе число:")
    val num2 = readLine().toInt

    val sum = num1 + num2
    println(s"Сумма чисел: $sum")
  }

  // 3. Увеличить каждый элемент списка на 1
  def incrementList(nums: List[Int]): Unit = {
    val incremented = nums.map(_ + 1)
    println("Результат: " + incremented)
  }

  // 4. Проверка четности
  def checkEvenOdd(): Unit = {
    println("Введите число:")
    val num = readLine().toInt
    if num % 2 == 0 then
      println("Число чётное")
    else
      println("Число нечётное")
  }

  // 5. Длина строки
  def stringLength(): Unit = {
    println("Введите строку:")
    val str = readLine()
    println(s"Длина строки: ${str.length}")
  }

  // 6. Объединение строк через пробел
  def joinStrings(): Unit = {
    println("Введите список строк через пробел:")

    // Вводим строку и разбиваем на элементы
    val input = readLine()
    val stringList = input.split(" ").toList

    // Собираем все строки через пробел
    val result = stringList.mkString(" ")
    println(s"Результат: $result")
  }
}