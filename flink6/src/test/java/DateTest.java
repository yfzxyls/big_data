import java.util.Calendar;
import java.util.Date;

/**
 * @author yangfuzhao on 2019/1/22.
 */
public class DateTest {

    public static void main(String[] args) {
        Date date = org.apache.commons.lang3.time.DateUtils.truncate(new Date(), Calendar.DATE);
        System.out.println(date.getTime());
    }
}

