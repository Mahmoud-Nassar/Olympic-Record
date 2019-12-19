package olympic;

import org.junit.Test;
import olympic.business.*;
import static org.junit.Assert.assertEquals;
import static olympic.business.ReturnValue.*;

public class SimpleTest extends AbstractTest{

    @Test
    public void simpleTestCreateAthlete()
    {
        Athlete a = new Athlete();
        a.setId(1);
        a.setName("Artur");
        a.setCountry("Brazil");
        a.setIsActive(true);
        ReturnValue ret = Solution.addAthlete(a);
        assertEquals(OK, ret);
    }

    @Test
    public void testDeleteUser(){
        Athlete a = new Athlete();
        a.setId(10);
        a.setName("Eli");
        a.setCountry("Argentina");
        a.setIsActive(true);

        ReturnValue ret = Solution.deleteAthlete(a);
        assertEquals(NOT_EXISTS , ret);
    }
}