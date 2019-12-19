package olympic;

import olympic.business.Athlete;
import olympic.business.ReturnValue;
import olympic.business.Sport;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class MegaTest extends AbstractTest {

    @Test
    public void Test_1_Basic() {
        Sport s = new Sport();
        s.setId(1);
        s.setName("football");
        s.setCity("TLV");

        Athlete a = new Athlete();
        a.setId(1);
        a.setCountry("Israel");
        a.setName("gadi");

        Solution.addAthlete(a);
        Solution.addSport(s);

        assertEquals(s, Solution.getSport(s.getId()));
        assertEquals(a, Solution.getAthleteProfile(a.getId()));
        assertEquals(Sport.badSport(), Solution.getSport(2));

        Solution.athleteJoinSport(s.getId(),a.getId());
        assertEquals(ReturnValue.NOT_EXISTS, Solution.confirmStanding(s.getId(),a.getId(),5));
        assertEquals(ReturnValue.NOT_EXISTS, Solution.confirmStanding(2,a.getId(),5));
        a.setId(2);
        a.setIsActive(true);
        Solution.addAthlete(a);
        Solution.athleteJoinSport(s.getId(),a.getId());
        assertEquals(ReturnValue.OK, Solution.confirmStanding(s.getId(),a.getId(),3));

        assertEquals(ReturnValue.NOT_EXISTS, Solution.athleteLeftSport(s.getId(),3));
        assertEquals(ReturnValue.NOT_EXISTS, Solution.athleteLeftSport(3,a.getId()));
        assertEquals(ReturnValue.OK, Solution.athleteLeftSport(s.getId(),a.getId()));

        Athlete b = new Athlete();
        b.setId(11);
        b.setCountry("Israel");
        b.setName("g");
        b.setIsActive(true);

        Solution.addAthlete(b);
        Solution.athleteJoinSport(s.getId(),b.getId());
        assertEquals(ReturnValue.OK, Solution.makeFriends(a.getId(),b.getId()));
        assertEquals(ReturnValue.ALREADY_EXISTS, Solution.makeFriends(a.getId(),b.getId()));
        assertEquals(ReturnValue.ALREADY_EXISTS, Solution.makeFriends(b.getId(),a.getId()));
        assertEquals(ReturnValue.BAD_PARAMS, Solution.makeFriends(a.getId(),a.getId()));

        assertEquals(ReturnValue.NOT_EXISTS, Solution.removeFriendship(a.getId(),a.getId()));
        assertEquals(ReturnValue.OK, Solution.removeFriendship(b.getId(),a.getId()));

        assertEquals(ReturnValue.NOT_EXISTS, Solution.changePayment(b.getId(),s.getId(),200));

        Athlete c = new Athlete();
        c.setId(22);
        c.setCountry("Canada");
        c.setName("g");

        Solution.addAthlete(c);
        // a && c not in Joined, b in Joined => a not popular
        assertEquals(ReturnValue.OK, Solution.makeFriends(b.getId(),a.getId()));
        assertEquals(ReturnValue.OK, Solution.makeFriends(a.getId(),c.getId()));
        assertFalse(Solution.isAthletePopular(a.getId()));

        assertEquals(ReturnValue.OK, Solution.athleteLeftSport(s.getId(),b.getId()));
        assertTrue(Solution.isAthletePopular(a.getId()));
        Solution.clearTables();

        Solution.addAthlete(a);
        Solution.addAthlete(b);
        Solution.addAthlete(c);
        Solution.addSport(s);
        Solution.athleteJoinSport(s.getId(),a.getId());
        Solution.athleteJoinSport(s.getId(),b.getId());
        Solution.athleteJoinSport(s.getId(),c.getId());

        Solution.confirmStanding(s.getId(),a.getId(),1);
        Solution.confirmStanding(s.getId(),b.getId(),2);
        Solution.confirmStanding(s.getId(),c.getId(),2);

        assertEquals(2, Solution.getTotalNumberOfMedalsFromCountry("Israel").intValue());
        assertEquals(0, Solution.getTotalNumberOfMedalsFromCountry("Canada").intValue());
        Solution.athleteDisqualified(s.getId(),a.getId());
        assertEquals(1, Solution.getTotalNumberOfMedalsFromCountry("Israel").intValue());
        Solution.changePayment(a.getId(),s.getId(),100);
        Solution.changePayment(b.getId(),s.getId(),100);
        Solution.changePayment(c.getId(),s.getId(),100);

        assertEquals(100, Solution.getIncomeFromSport(s.getId()).intValue());

        Athlete d = new Athlete();
        d.setId(22);
        d.setCountry("France");
        d.setName("g");
        d.setIsActive(true);
        Solution.addAthlete(d);
        assertEquals(ReturnValue.ALREADY_EXISTS, Solution.athleteJoinSport(s.getId(),d.getId()));
        Solution.changePayment(d.getId(),s.getId(),300);
        assertEquals(/*100*/300, Solution.getIncomeFromSport(s.getId()).intValue());
        Solution.athleteLeftSport(s.getId(),a.getId());
        Solution.athleteJoinSport(s.getId(),a.getId());
        Solution.confirmStanding(s.getId(),d.getId(),2);
        Solution.confirmStanding(s.getId(),a.getId(),3);
        assertTrue(Solution.getBestCountry().contains("Israel"));  //  should by Israel

    }

    @Test
    public void Test_2_Basic() {
        Solution.clearTables();
        Athlete a = new Athlete();
        Sport s = new Sport();
        a.setId(1);
        a.setName("yossi");
        a.setCountry("israel");
        a.setIsActive(true);
        Solution.addAthlete(a);
        a.setId(2);
        a.setName("gadi");
        a.setCountry("israel");
        a.setIsActive(false);
        Solution.addAthlete(a);
        a.setId(3);
        a.setName("moshe");
        a.setCountry("canada");
        a.setIsActive(true);
        Solution.addAthlete(a);
        s.setId(12);
        s.setName("tennis");
        s.setCity("tel-aviv");
        Solution.addSport(s);
        s.setId(1);
        s.setName("swim");
        s.setCity("tel-aviv");
        Solution.addSport(s);
        s.setId(3);
        s.setName("football");
        s.setCity("haifa");
        Solution.addSport(s);
        Solution.athleteJoinSport(12,1);
        Solution.athleteJoinSport(1,2);
        Solution.makeFriends(1,2);
        Solution.makeFriends(1,3);

        assertFalse(Solution.isAthletePopular(1));
        assertFalse(Solution.isAthletePopular(2));
        assertEquals(100, Solution.getIncomeFromSport(1).intValue());
        assertEquals(Solution.confirmStanding(12, 1, 1), ReturnValue.OK);
        assertEquals(Solution.confirmStanding(1, 2, 1), ReturnValue.NOT_EXISTS);
        assertEquals(1, Solution.getTotalNumberOfMedalsFromCountry("israel").intValue());
        assertTrue(Solution.getBestCountry().contains("israel"));
        assertSame(Solution.athleteDisqualified(1, 2), ReturnValue.OK);
        assertTrue(Solution.getBestCountry().contains("israel"));
        assertSame(Solution.athleteDisqualified(12, 1), ReturnValue.OK);
        assertEquals("", Solution.getBestCountry());
        assertSame(Solution.confirmStanding(12, 1, 1), ReturnValue.OK);   // disqualified athletes can get medals
        Solution.athleteJoinSport(12,3);
        Solution.athleteJoinSport(2,3);
        Solution.confirmStanding(12,3,1);
        Solution.confirmStanding(2,3,2);
        assertEquals(Solution.getTotalNumberOfMedalsFromCountry("canada").intValue(), 1);
        assertTrue(Solution.getBestCountry().contains("canada"));
        assertTrue(Solution.getMostPopularCity().contains("tel-aviv"));
        assertSame(Solution.deleteAthlete(Solution.getAthleteProfile(1)), ReturnValue.OK);
        assertSame(Solution.athleteLeftSport(12, 1), ReturnValue.NOT_EXISTS); // should have been deleted on previous test
        Solution.athleteLeftSport(12,3);
        Solution.athleteLeftSport(2,3);
        Solution.athleteLeftSport(1,2);
      /** TO CHECK IF WE SHOULD CONSIDER THAT THE ATHLETE IS NOT
       * IN PARTICBATING AT THE MOMENT IN THE SPORT I.E IN THIS CASE SHOULD WE
       * COUNT THE MEDALS FOR SUCH ATHLETES OR NOT
       * KNOW IM COUNTOING THOES ATHLETES BUT IN THE TESET THERE ARE NOT COUNTING THEME*/
       ///  assertEquals("", Solution.getBestCountry());
        assertEquals("tel-aviv", Solution.getMostPopularCity());
        Solution.deleteAthlete(Solution.getAthleteProfile(2));
        Solution.deleteAthlete(Solution.getAthleteProfile(3));
        assertTrue(Solution.getBestCountry().isEmpty());
        assertEquals("tel-aviv", Solution.getMostPopularCity());
        Solution.deleteSport(Solution.getSport(12));
        Solution.deleteSport(Solution.getSport(1));
        Solution.deleteSport(Solution.getSport(3));
        assertTrue(Solution.getMostPopularCity().isEmpty());   // no cities in DB
        a.setId(1);
        a.setName("A");
        a.setCountry("A");
        a.setIsActive(true);
        Solution.addAthlete(a);
        a.setId(2);
        Solution.addAthlete(a);
        s.setId(11);
        s.setName("S");
        s.setCity("SA");
        Solution.addSport(s);
        s.setId(22);
        s.setName("S");
        s.setCity("SB");
        Solution.addSport(s);
        Solution.athleteJoinSport(11,1);
        Solution.athleteJoinSport(11,2);
        Solution.athleteJoinSport(22,1);
        Solution.athleteJoinSport(22,2);
        assertTrue(Solution.getMostPopularCity().contains("SB"));   // same AVG for SA and SB. should choose by LEX sort
    }

    @Test
    public void Test_3_getAthleteMedals() {
        Solution.clearTables();
        Sport s = new Sport();
        s.setId(11);
        s.setName("tennis");
        s.setCity("TLV");
        Solution.addSport(s);
        s.setId(22);
        s.setName("golf");
        s.setCity("paris");
        Solution.addSport(s);
        Athlete atl=new Athlete();
        atl.setId(1);
        atl.setIsActive(true);
        atl.setName("name");
        atl.setCountry("country");
        Solution.addAthlete(atl);
        Solution.athleteJoinSport(11,1);
        Solution.athleteJoinSport(22,1);
        Solution.confirmStanding(11,1,1);
        Solution.confirmStanding(22,1,1);
        ArrayList<Integer> expected = new ArrayList<Integer>(Arrays.asList(2,0,0));
        assertEquals(expected, Solution.getAthleteMedals(1));
        expected.clear();
        s.setId(33);
        Solution.addSport(s);
        Solution.athleteJoinSport(33,1);
        Solution.confirmStanding(33,1,2);
        expected.addAll(Arrays.asList(2,1,0));
        assertEquals(expected, Solution.getAthleteMedals(1));
        Solution.athleteDisqualified(11,1);
        expected.clear();
        expected.addAll(Arrays.asList(1,1,0));
        assertEquals(expected, Solution.getAthleteMedals(1));
        Solution.deleteAthlete(Solution.getAthleteProfile(1));
        expected.clear();
        expected.addAll(Arrays.asList(0,0,0));
        assertEquals(expected, Solution.getAthleteMedals(1));
    }

    @Test
    public void Test_4_getMostRatedAthletes() {
        Solution.clearTables();
        Sport s = new Sport();
        s.setId(11);
        s.setCity("haifa");
        s.setName("t");
        Solution.addSport(s);
        Athlete atl = new Athlete();
        atl.setIsActive(true);
        atl.setName("n");
        atl.setCountry("c");
        for (int i : IntStream.range(1, 5).toArray()) {  // 1-4
            atl.setId(i);
            Solution.addAthlete(atl);
            Solution.athleteJoinSport(11, i);
        }
        ArrayList<Integer> expected = new ArrayList<Integer>(IntStream.range(1, 5).boxed().collect(Collectors.toList()));
        assertEquals(expected, Solution.getMostRatedAthletes());  // less than 10 elements
        for (int i : IntStream.range(5, 12).toArray()) {  // 5-11
            atl.setId(i);
            Solution.addAthlete(atl);
            Solution.athleteJoinSport(11, i);
        }
        expected.clear();
        expected.addAll(IntStream.range(1, 11).boxed().collect(Collectors.toList()));
        assertEquals(expected, Solution.getMostRatedAthletes());  // exactly 10 elements (but there're 11 that fit)

        for (int i : IntStream.range(6, 12).toArray())  // 6-11
            Solution.confirmStanding(11, i, 1);
        expected.clear();
        expected.addAll(IntStream.range(6, 12).boxed().collect(Collectors.toList())); // add 6-11
        expected.addAll(IntStream.range(1, 5).boxed().collect(Collectors.toList())); // add 1-4
        assertEquals(expected, Solution.getMostRatedAthletes());  // order has changed
        for (int i : IntStream.range(1, 4).toArray()) { // 1-3
            Solution.confirmStanding(11, i, 1);
            Solution.confirmStanding(11, i, 2);
        }
        expected.clear();
        expected.addAll(IntStream.range(6, 12).boxed().collect(Collectors.toList())); // add 6-11
        expected.addAll(IntStream.range(1, 4).boxed().collect(Collectors.toList())); // add 1-3
        expected.add(4);
        ArrayList<Integer> temp=Solution.getMostRatedAthletes();
        assertEquals(expected, Solution.getMostRatedAthletes());    // order has changed again
    }

    @Test
    public void Test_5_getCloseAthletes() {
        Solution.clearTables();
        Sport s = new Sport();
        s.setCity("haifa");
        s.setName("t");
        for (int i : IntStream.range(11, 14).toArray()) {  // 11-13
            s.setId(i);
            Solution.addSport(s);
        }
        Athlete at = new Athlete();
        at.setName("a");
        at.setCountry("a");
        for (int i : IntStream.range(1, 13).toArray()) {  // 1-5 observers, 6-12 participants
            if(i==6)
                at.setIsActive(true);
            at.setId(i);
            Solution.addAthlete(at);
        }
        ArrayList<Integer> expected = new ArrayList<Integer>();
        expected.addAll(IntStream.range(2, 12).boxed().collect(Collectors.toList()));
        assertEquals(expected, Solution.getCloseAthletes(1));  // no one joined any sport
        Solution.athleteJoinSport(11,1);
        expected.clear();
        expected= new ArrayList<Integer>();
        assertEquals(expected, Solution.getCloseAthletes(1)); // cannot be close to himself
        for (int i : IntStream.range(2,6).toArray())  // 2-5
            Solution.athleteJoinSport(11,i);
        expected.clear();
        expected.addAll(IntStream.range(2,6).boxed().collect(Collectors.toList()));
        assertEquals(expected, Solution.getCloseAthletes(1)); // 2-5 are 100% fit
        Solution.athleteJoinSport(12,1);
        assertEquals(expected, Solution.getCloseAthletes(1)); // 2-5 are 50% fit
        Solution.athleteJoinSport(13,1);
        expected.clear();
        assertEquals(expected, Solution.getCloseAthletes(1)); // 2-5 are only 33.3% fit (>50%)
        Solution.athleteLeftSport(13,1);    // 1 is part of sportID in (11,12)
        for (int i : IntStream.range(6,13).toArray()) {
            Solution.athleteJoinSport(11, i);
            Solution.athleteJoinSport(12, i);
        }
        // now : 2-5 are 50% fit && 6-12 are 100% fit
        // notice expected list should be in ascending athleteID order
        expected.clear();
        expected.addAll(IntStream.range(2,6).boxed().collect(Collectors.toList())); // 2-5
        expected.addAll(IntStream.range(6,12).boxed().collect(Collectors.toList())); //6-11
        assertEquals(expected, Solution.getCloseAthletes(1));
        Solution.deleteAthlete(Solution.getAthleteProfile(1));
        expected.clear();
        assertEquals(expected, Solution.getCloseAthletes(1)); // athlete doesn't exist
    }

    @Test
    public void Test_6_getSportsRecommendation() {
        Solution.clearTables();
        Sport s = new Sport();
        s.setCity("haifa");
        s.setName("t");
        for (int i : IntStream.range(11, 16).toArray()) {  // 11-15
            s.setId(i);
            Solution.addSport(s);
        }
        Athlete at = new Athlete();
        at.setName("a");
        at.setCountry("a");
        for (int i : IntStream.range(1, 13).toArray()) {  // 1-5 observers, 6-12 participants
            if(i==6)
                at.setIsActive(true);
            at.setId(i);
            Solution.addAthlete(at);
        }
        ArrayList<Integer> expected = new ArrayList<Integer>();
        assertEquals(expected, Solution.getSportsRecommendation(1));  // no one joined any sport
        Solution.athleteJoinSport(11,1);
        assertEquals(expected, Solution.getSportsRecommendation(1)); // cannot recommend to himself
        for (int i : IntStream.range(2,6).toArray())  // now 2-5 are close to 1
            Solution.athleteJoinSport(11,i);
        assertEquals(expected, Solution.getSportsRecommendation(1)); // athleteID=1 already joined to "recommended" sports
        Solution.athleteJoinSport(12,1);
        for (int i : IntStream.range(2,6).toArray()) {  // 2-5
            Solution.athleteJoinSport(12, i);
            Solution.athleteJoinSport(13, i);
            Solution.athleteJoinSport(14, i);
        }
        expected.clear();
        expected.addAll(Arrays.asList(13,14));
        assertEquals(expected, Solution.getSportsRecommendation(1));  // 2-5 are 50% close to 1 && recommend 13,14
        Solution.athleteLeftSport(11,1);
        Solution.athleteJoinSport(14,1);
        expected.clear();
        expected.addAll(Arrays.asList(11,13));
        assertEquals(expected, Solution.getSportsRecommendation(1));   // 2-5 are 50% close to 1 && recommend 13,11
        for (int i : IntStream.range(6,11).toArray()) {  // 6-10
            Solution.athleteJoinSport(12, i);
            Solution.athleteJoinSport(13, i);
            Solution.athleteJoinSport(14, i);
        }
        Solution.athleteJoinSport(14,1);
        expected.clear();
        expected.add(13);
        expected.add(11);
        ArrayList<Integer> result=Solution.getSportsRecommendation(1);
        assertEquals(expected, Solution.getSportsRecommendation(1));   // only recommendation is 13
        Solution.deleteAthlete(Solution.getAthleteProfile(1));
        expected.clear();
        assertEquals(expected, Solution.getSportsRecommendation(1)); // athleteID=1 doesn't exist
    }

}








