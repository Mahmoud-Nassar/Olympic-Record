
package olympic;

import org.junit.Test;
import olympic.business.*;
import static org.junit.Assert.assertEquals;
import static olympic.business.ReturnValue.*;


public class BasicAPITests extends AbstractTest {
    @Test
    public void addAthleteToSportTest() {

        ReturnValue res;
        Sport s = new Sport();
        s.setId(1);
        s.setName("Basketball");
        s.setCity("Tel Aviv");
        s.setAthletesCount(0);

            res = Solution.addSport(s);
        assertEquals(OK, res);

        Athlete a = new Athlete();
        a.setId(2);
        a.setName("Artur");
        a.setCountry("Brazil");
        a.setIsActive(true);
        ReturnValue ret = Solution.addAthlete(a);
        assertEquals(OK, ret);

        res = Solution.athleteJoinSport(1, 2);
        assertEquals(OK, res);

        res = Solution.athleteJoinSport(1, -19);
        assertEquals(NOT_EXISTS, res);
    }

    @Test
    public void friendsTest() {

        ReturnValue res;
        Athlete a = new Athlete();
        a.setId(4);
        a.setName("Neymar");
        a.setCountry("Brazil");
        a.setIsActive(true);
        ReturnValue ret = Solution.addAthlete(a);
        assertEquals(OK, ret);


        a.setId(5);
        a.setName("Artur");
        a.setCountry("Brazil");
        a.setIsActive(false);
        ret = Solution.addAthlete(a);
        assertEquals(OK, ret);

        res = Solution.makeFriends(4,5);
        assertEquals(OK, res);

        res = Solution.removeFriendship(2,5);
        assertEquals(NOT_EXISTS, res);
    }
}


