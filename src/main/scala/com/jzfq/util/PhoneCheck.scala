package com.jzfq.util

object PhoneCheck {

  /**
    * 全为正整数，并且倒数第11位为1，标志为有效（并将倒数第11位之前的去掉），其他的为无效；
    *
    * @param number 手机号
    * @return 是否符合规则
    */
  def isPhone(number: String): Boolean = {
    number.takeRight(11).matches("1[0-9]{10}")
  }

  /**
    * 倒数第1~8位，2个数字分别出现4次，如：13811223344、13866778899，为无效；
    *
    * @param number 手机号
    * @return 是否符合规则
    */
  def isRepetition2(number: String): Boolean = {
    number.takeRight(11).matches("1[0-9]{2}([1-9]{2}){4}")

  }

  /**
    * 倒数第1~8位，7个及以上相同数字，如：13887777777、13866666666，为无效；
    *
    * @param number 手机号
    * @return 是否符合规则
    */
  def isRepetition7(number: String): Boolean = {
    number.takeRight(7).matches("""([0-9])\1{6}""")
  }

  /**
    * 倒数第1~8位，7个及以上连续数字，如：13812345678、13881234567，为无效；
    *
    * @param number 手机号
    * @return 是否符合规则
    */
  def isAuto8(number: String): Boolean = {
    number.takeRight(7).equals("1234567") || number.takeRight(7).equals("2345678") || number.takeRight(7).equals("3456789") || number.takeRight(7).equals("9876543") || number.takeRight(7).equals("8765432") || number.takeRight(7).equals("7654321") || number.takeRight(7).equals("0123456") || number.takeRight(7).equals("6543210")
  }

 /* def main(args: Array[String]): Unit = {
    ///0\d{2,3}-\d{7,8}/
    //val regx = "^[0][1-9]{8}$"
    // println(isMobileNumber("1019315949614561"))
    /* println(isPhone("000989813876777777"))
     println(isRepetition2("11112223344"))
     println(isRepetition7("13877777777"))
     println("000989813876777777".takeRight(8))*/
    //
    //println("123456789".matches("""^([1-9]\d{0,2}|9)$"""))
    //println("123956784".matches("""(?!([0-9]).*?\1+$)^[0-9]{9}$"""))
//    println("12345678".equals("12345678"))
   // println("99885546".matches("([1-9]{2}){4}"))

  }*/
}
