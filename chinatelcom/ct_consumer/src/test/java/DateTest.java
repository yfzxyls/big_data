import org.joda.time.DateTime;
import org.junit.Test;

/**
 * Created by yangf on 2018/3/21.
 */
public class DateTest {

    @Test
    public void date(){
        DateTime dateTime = new DateTime("2017-01-02");
        DateTime dateTime1 = new DateTime("2017-05-02");
        DateTime dateTime3 = dateTime.plusMonths(1).dayOfMonth().withMinimumValue();
        System.out.println(dateTime1.toString());
    }
}
