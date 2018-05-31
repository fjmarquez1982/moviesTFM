package es.common.util

import java.text.SimpleDateFormat
import java.util.Date

class DateUtil {

  /**
    *
    * @param format
    * @param date
    * @return
    */
  def dateStringFormat(format: String, date: Date): String ={
    val sdf = new SimpleDateFormat(format)
    return sdf.format(date)
  }
}
