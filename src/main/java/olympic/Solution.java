package olympic;

import olympic.business.Athlete;
import olympic.business.ReturnValue;
import olympic.business.Sport;
import olympic.data.DBConnector;
import olympic.data.PostgreSQLErrorCodes;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import static olympic.business.ReturnValue.*;

public class Solution {
    public static void createTables() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {

           pstmt = connection.prepareStatement("CREATE TABLE Sport\n" +
                    "(\n" +
                    "    sport_id integer,\n" +
                    "    sport_name text NOT NULL,\n" +
                    "    city text NOT NULL,\n" +
                    "    athlete_counter integer default(0),\n" +
                    "    PRIMARY KEY(sport_id),\n" +
                    "    CHECK(sport_id > 0),\n" +
                    "    CHECK(athlete_counter > -1),\n" +
                    "    unique(sport_id)\n" +
                    ")");
            pstmt.execute();

            pstmt = connection.prepareStatement("CREATE TABLE Athlete\n" +
                    "(\n" +
                    "   athlete_id integer,\n" +
                    "    athlete_name text NOT NULL,\n" +
                    "    country text NOT NULL,\n" +
                    "    active boolean NOT NULL,\n" +
                    "    PRIMARY KEY (athlete_id),\n" +
                    "    CHECK (athlete_id > 0),\n" +
                    "    unique (athlete_id)\n" +
                    ")");
            pstmt.execute();

            pstmt = connection.prepareStatement("CREATE TABLE Medals\n" +
                    "(\n" +
                    "    sport_id integer,\n" +
                    "    athlete_id integer,\n" +
                    "    place integer,\n" +
                    "    CHECK (athlete_id > 0),\n" +
                    "    CHECK (sport_id > 0),\n" +
                    "    CHECK (place > 0),\n" +
                    "    CHECK (place < 4),\n" +
                    "    FOREIGN KEY (sport_id) REFERENCES Sport(sport_id) " +
                    "ON DELETE CASCADE,\n" +
                    "    FOREIGN KEY (athlete_id) REFERENCES Athlete(athlete_id) " +
                    "ON DELETE CASCADE\n" +
                    ")");
            pstmt.execute();

            pstmt = connection.prepareStatement("CREATE TABLE ObserveOrParticipate\n" +
                    "(\n" +
                    "   athlete_id integer,\n" +
                    "    sport_id integer,\n" +
                    "    money_paid integer default(100),\n" +
                    "    PRIMARY KEY (athlete_id,sport_id),\n" +
                    "    CHECK (athlete_id > 0),\n" +
                    "    CHECK (sport_id > 0),\n" +
                    "    unique (athlete_id,sport_id),\n" +
                    "    FOREIGN KEY (athlete_id) REFERENCES Athlete(athlete_id) " +
                    "ON DELETE CASCADE,\n" +
                    "    FOREIGN KEY (sport_id) REFERENCES Sport(sport_id) " +
                    "ON DELETE CASCADE\n" +
                    ")");
            pstmt.execute();

            pstmt = connection.prepareStatement("CREATE TABLE Friends\n" +
                    "(\n" +
                    "    athlete1_id integer,\n" +
                    "    athlete2_id integer,\n" +
                    "    CHECK (athlete1_id > 0),\n" +
                    "    CHECK (athlete2_id > 0),\n" +
                    "    CHECK (athlete1_id <> athlete2_id),\n" +
                    "    unique (athlete1_id,athlete2_id),\n" +
                    "    PRIMARY KEY (athlete1_id,athlete2_id),\n" +
                    "    FOREIGN KEY (athlete1_id) REFERENCES Athlete(athlete_id) " +
                    "ON DELETE CASCADE,\n" +
                    "    FOREIGN KEY (athlete2_id) REFERENCES Athlete(athlete_id) " +
                    "ON DELETE CASCADE\n" +
                    ")");
            pstmt.execute();

            pstmt = connection.prepareStatement("CREATE VIEW ActiveData as SELECT " +
                    "active, athlete_id FROM Athlete");
            pstmt.execute();

            pstmt = connection.prepareStatement("CREATE VIEW NUMSPORTSINCITY as " +
                    "SELECT city, COUNT(sport_id) as sports_count" +
                    " FROM Sport GROUP BY city\n");
            pstmt.execute();

            pstmt = connection.prepareStatement("CREATE VIEW NUMATHLETESINCITY" +
                    " as SELECT s.city,\n" +
                    " count(a.athlete_id) AS athlete_count\n" +
                    " FROM observeorparticipate o,\n" +
                    " sport s,\n" +
                    " athlete a\n" +
                    " WHERE o.sport_id = s.sport_id AND a.athlete_id = o.athlete_id AND" +
                    " a.active = true\n" +
                    " GROUP BY s.city;");
            pstmt.execute();


            pstmt = connection.prepareStatement("CREATE VIEW ZeroCites as" +
                    " SELECT s2.city, COUNT((SELECT a.athlete_id FROM" +
                    " observeorparticipate o,\n" +
                    " sport s, athlete a WHERE o.sport_id = s.sport_id" +
                    " AND a.athlete_id = o.athlete_id AND a.active = true" +
                    " AND S.city=s2.city)) as athlete_count" +
                    " from Sport s2 where s2.city NOT IN (SELECT n.city" +
                    " FROM NUMATHLETESINCITY n) GROUP BY s2.city");
            pstmt.execute();

            pstmt = connection.prepareStatement("CREATE VIEW AllNumAthletesInCity"+
                    " AS ((SELECT * FROM NUMATHLETESINCITY)" +
                    " UNION (SELECT * FROM ZeroCites))");
            pstmt.execute();

            pstmt = connection.prepareStatement("CREATE VIEW GoldMedals" +
                    " as (SELECT athlete_id, COUNT(place) " +
                    "as medals FROM Medals WHERE place=1 GROUP BY" +
                    " athlete_id) UNION (SELECT a.athlete_id," +
                    " COUNT((SELECT m.athlete_id FROM" +
                    " Medals m WHERE m.athlete_id=a.athlete_id and place=1))" +
                    " as medals FROM Athlete a WHERE a.athlete_id" +
                    " NOT IN (SELECT athlete_id FROM Medals WHERE place=1)" +
                    " GROUP BY a.athlete_id)");
            pstmt.execute();

            pstmt = connection.prepareStatement("CREATE VIEW SilverMedals" +
                    " as (SELECT athlete_id, COUNT(place) " +
                    "as medals FROM Medals WHERE place=2 GROUP BY" +
                    " athlete_id) UNION (SELECT a.athlete_id," +
                    " COUNT((SELECT m.athlete_id FROM" +
                    " Medals m WHERE m.athlete_id=a.athlete_id AND m.place=2))" +
                    " as medals FROM Athlete a WHERE a.athlete_id" +
                    " NOT IN (SELECT athlete_id FROM Medals m2 WHERE m2.place=2)" +
                    " GROUP BY a.athlete_id)");
            pstmt.execute();

            pstmt = connection.prepareStatement("CREATE VIEW BronzeMedals" +
                    " as (SELECT athlete_id, COUNT(place) " +
                    "as medals FROM Medals WHERE place=3 GROUP BY" +
                    " athlete_id) UNION (SELECT a.athlete_id," +
                    " COUNT((SELECT m.athlete_id FROM" +
                    " Medals m WHERE m.athlete_id=a.athlete_id AND m.place=3))" +
                    " as medals FROM Athlete a WHERE a.athlete_id" +
                    " NOT IN (SELECT athlete_id FROM Medals m2 WHERE m2.place=3)" +
                    " GROUP BY a.athlete_id)");
            pstmt.execute();

            pstmt = connection.prepareStatement("CREATE VIEW GoldPoints" +
                    " as SELECT athlete_id, (medals*3) as gold_points" +
                    " FROM GoldMedals");
            pstmt.execute();

            pstmt = connection.prepareStatement("CREATE VIEW SilverPoints" +
                    " as SELECT athlete_id, (medals*2) as silver_points" +
                    " FROM SilverMedals");
            pstmt.execute();

            pstmt = connection.prepareStatement("CREATE VIEW BronzePoints " +
                    "as SELECT athlete_id, (medals) as bronze_points FROM BronzeMedals");
            pstmt.execute();


        } catch (SQLException e) {
              //e.printStackTrace();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
               //e.printStackTrace();
            }
        }
    }

    public static void clearTables() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {

            pstmt = connection.prepareStatement("DELETE FROM Athlete");
            pstmt.execute();

            pstmt = connection.prepareStatement("DELETE FROM ObserveOrParticipate");
            pstmt.execute();

            pstmt = connection.prepareStatement("DELETE FROM Sport");
            pstmt.execute();

            pstmt = connection.prepareStatement("DELETE FROM Friends");
            pstmt.execute();

            pstmt = connection.prepareStatement("DELETE FROM Medals");
            pstmt.execute();

        } catch (SQLException e) {
            //e.printStackTrace()();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
    }

    public static void dropTables() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {

            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS GoldPoints");
            pstmt.execute();

            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS SilverPoints");
            pstmt.execute();

            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS BronzePoints");
            pstmt.execute();

            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS BronzeMedals");
            pstmt.execute();

            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS SilverMedals");
            pstmt.execute();

            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS GoldMedals");
            pstmt.execute();

            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS AllNumAthletesInCity");
            pstmt.execute();

            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS ZeroCites");
            pstmt.execute();

            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS FriendsSports");
            pstmt.execute();

            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS NUMATHLETESINCITY");
            pstmt.execute();

            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS NUMSPORTSINCITY");
            pstmt.execute();

            pstmt = connection.prepareStatement("DROP VIEW IF EXISTS ActiveData");
            pstmt.execute();

            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS ObserveOrParticipate");
            pstmt.execute();

            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Friends");
            pstmt.execute();

            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Medals");
            pstmt.execute();

            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Athlete");
            pstmt.execute();

            pstmt = connection.prepareStatement("DROP TABLE IF EXISTS Sport");
            pstmt.execute();

        } catch (SQLException e) {
           // e.printStackTrace();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
               // e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
    }

    public static ReturnValue addAthlete(Athlete athlete) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("INSERT INTO Athlete" +
                    " VALUES (?,?,?,?)");
            pstmt.setInt(1,athlete.getId());
            pstmt.setString(2, athlete.getName());
            pstmt.setString(3, athlete.getCountry());
            pstmt.setBoolean(4,athlete.getIsActive());
            pstmt.execute();

        } catch (SQLException e) {
            //e.printStackTrace()();
            if(Integer.valueOf(e.getSQLState()) ==
                    PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue()){
                return ALREADY_EXISTS;
            }
            if (Integer.valueOf(e.getSQLState()) ==
                    PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue()
                    ||
                    Integer.valueOf(e.getSQLState()) ==
                            PostgreSQLErrorCodes.CHECK_VIOLATION.getValue()){
                return BAD_PARAMS;
            }
            else {
                return ERROR;
            }
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return OK;
    }

    public static Athlete getAthleteProfile(Integer athleteId) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ResultSet results;
        try {
            pstmt = connection.prepareStatement(
                    "SELECT * FROM Athlete " +
                            "where athlete_id = ?");
            pstmt.setInt(1,athleteId);
            results = pstmt.executeQuery();
            if (results.next()) {
                boolean is_active = results.getBoolean(4);
                Athlete athlete = new Athlete();
                athlete.setId(athleteId);
                athlete.setName(results.getString(2));
                athlete.setCountry(results.getString(3));
                athlete.setIsActive(is_active);
                results.close();
                return athlete;
            }
            else {
                results.close();
            }

        } catch (SQLException e) {
            //e.printStackTrace()();
                return Athlete.badAthlete();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return Athlete.badAthlete();
    }

    public static ReturnValue deleteAthlete(Athlete athlete) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(
                    "DELETE FROM Athlete " +
                            "where athlete_id = ?");
            pstmt.setInt(1,athlete.getId());
            int affectedRows = pstmt.executeUpdate();

            if (affectedRows==0){
                return NOT_EXISTS;
            }

            pstmt = connection.prepareStatement(
                    "DELETE FROM ObserveOrParticipate " +
                            "where athlete_id = ?");
            pstmt.setInt(1,athlete.getId());
            pstmt.executeUpdate();


            pstmt = connection.prepareStatement(
                    "DELETE FROM Friends " +
                            "where athlete1_id = ? or athlete2_id= ?");
            pstmt.setInt(1,athlete.getId());
            pstmt.setInt(2,athlete.getId());
            pstmt.executeUpdate();

            pstmt = connection.prepareStatement(
                    "DELETE FROM Medals " +
                            "where athlete_id = ?");
            pstmt.setInt(1,athlete.getId());
            pstmt.executeUpdate();

        } catch (SQLException e) {
          //  e.printStackTrace();
            return ERROR;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return OK;
    }

    public static ReturnValue addSport(Sport sport) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("INSERT INTO Sport" +
                    " VALUES (?,?,?,?)");
            pstmt.setInt(1,sport.getId());
            pstmt.setString(2, sport.getName());
            pstmt.setString(3, sport.getCity());
            pstmt.setInt(4,sport.getAthletesCount());
            pstmt.execute();

        } catch (SQLException e) {
           // e.printStackTrace();
            if(Integer.valueOf(e.getSQLState()) ==
                    PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue()){
                return ALREADY_EXISTS;
            }
            else if (Integer.valueOf(e.getSQLState()) ==
                    PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue()){
                return BAD_PARAMS;
            }
            else if (Integer.valueOf(e.getSQLState()) ==
                    PostgreSQLErrorCodes.NOT_NULL_VIOLATION.getValue()
                    ||
                    Integer.valueOf(e.getSQLState()) ==
                            PostgreSQLErrorCodes.CHECK_VIOLATION.getValue()){
                return BAD_PARAMS;
            }
            else {
                return ERROR;
            }
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
        return OK;
    }

    public static Sport getSport(Integer sportId) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ResultSet results;
        try {
            pstmt = connection.prepareStatement(
                    "SELECT * FROM Sport " +
                            "where sport_id = ?");
            pstmt.setInt(1,sportId);
            results = pstmt.executeQuery();
            if (results.next()) {
                int athletes_count = results.getInt(4);
                Sport sport = new Sport();
                sport.setId(sportId);
                sport.setName(results.getString(2));
                sport.setCity(results.getString(3));
                sport.setAthletesCount(athletes_count);
                results.close();
                return sport;
            }
            else {
                results.close();
            }

        } catch (SQLException e) {
            //e.printStackTrace()();
            return Sport.badSport();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return Sport.badSport();
    }

    public static ReturnValue deleteSport(Sport sport)  {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(
                    "DELETE FROM Sport " +
                            "where sport_id = ?");
            pstmt.setInt(1, sport.getId());
            if (pstmt.executeUpdate()<1){
                return NOT_EXISTS;
            }

            pstmt = connection.prepareStatement(
                    "DELETE FROM ObserveOrParticipate " +
                            "where sport_id = ?");
            pstmt.setInt(1, sport.getId());
            pstmt.executeUpdate();

            pstmt = connection.prepareStatement(
                    "DELETE FROM Medals " +
                            "where sport_id = ?");
            pstmt.setInt(1, sport.getId());
            pstmt.executeUpdate();

        } catch (SQLException e) {
            //  e.printStackTrace();
            return ERROR;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return OK;
    }

    public static ReturnValue athleteJoinSport(Integer sportId, Integer athleteId) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {

            pstmt = connection.prepareStatement("INSERT INTO ObserveOrParticipate" +
                    " VALUES (?,?,?)");
            pstmt.setInt(1,athleteId);
            pstmt.setInt(2, sportId);
            pstmt.setInt(3, 100);
            pstmt.execute();

            pstmt = connection.prepareStatement(
                    "SELECT * FROM Athlete " +
                            "where athlete_id = ? AND active=true");
            pstmt.setInt(1,athleteId);
            ResultSet results = pstmt.executeQuery();

            if (results.next()) {
                pstmt = connection.prepareStatement(
                        "UPDATE Sport " +
                                "SET athlete_counter = athlete_counter + 1 " +
                                "where sport_Id = ?");
                pstmt.setInt(1, sportId);
                pstmt.executeUpdate();
            }

        } catch (SQLException e) {
            ///e.printStackTrace();
            if(Integer.valueOf(e.getSQLState()) ==
                    PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue()){
                return ALREADY_EXISTS;
            }
            else if (Integer.valueOf(e.getSQLState()) ==
                    PostgreSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()){
                return NOT_EXISTS;
            }
            else {
                return ERROR;
            }
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
               // e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
               // e.printStackTrace();
            }
        }
        return OK;
    }

    public static ReturnValue athleteLeftSport(Integer sportId, Integer athleteId) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {

            pstmt = connection.prepareStatement(
                    "SELECT * FROM ObserveOrParticipate " +
                            "where athlete_id = ? and sport_id = ?");
            pstmt.setInt(1,athleteId);
            pstmt.setInt(2,sportId);
            ResultSet results = pstmt.executeQuery();

            if (!results.next()) {
                return  NOT_EXISTS;
            }

            pstmt = connection.prepareStatement(
                    "SELECT active FROM ActiveData " +
                            "where athlete_id = ?");
            pstmt.setInt(1,athleteId);
            results = pstmt.executeQuery();
            results.next();
            boolean is_active=results.getBoolean(1);

            pstmt = connection.prepareStatement(
                    "DELETE FROM ObserveOrParticipate " +
                            "where athlete_id = ? and sport_id = ?");
            pstmt.setInt(1,athleteId);
            pstmt.setInt(2,sportId);
            pstmt.executeUpdate();

            if (is_active) {
                pstmt = connection.prepareStatement(
                        "UPDATE Sport " +
                                "SET athlete_counter = athlete_counter - 1 " +
                                "where sport_Id = ?");
                pstmt.setInt(1, sportId);
                pstmt.executeUpdate();
            }

        } catch (SQLException e) {
            //e.printStackTrace();
            return ERROR;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
              //  e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
               // e.printStackTrace();
            }
        }
        return OK;
    }

    public static ReturnValue confirmStanding(Integer sportId, Integer athleteId, Integer place) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {

            pstmt = connection.prepareStatement(
                    "SELECT * FROM ObserveOrParticipate O, Athlete A" +
                            " where A.athlete_id = ? AND" +
                            " A.athlete_id = O.athlete_id AND O.sport_id= ? AND active=true");
            pstmt.setInt(1,athleteId);
            pstmt.setInt(2,sportId);
            ResultSet results = pstmt.executeQuery();

            if (!results.next()) {
                return  NOT_EXISTS;
            }

            pstmt = connection.prepareStatement("SELECT * FROM Medals" +
                    " WHERE athlete_id= ? AND Sport_id= ? ");
            pstmt.setInt(1,athleteId);
            pstmt.setInt(2,sportId);
            results = pstmt.executeQuery();

            if (results.next()) {
                pstmt = connection.prepareStatement("UPDATE Medals set place= ?" +
                        " WHERE athlete_id= ? and sport_id=?");
                pstmt.setInt(1,place);
                pstmt.setInt(2, athleteId);
                pstmt.setInt(3, sportId);
                pstmt.execute();
            }
            else {
                pstmt = connection.prepareStatement("INSERT INTO Medals" +
                        " VALUES (?,?,?)");
                pstmt.setInt(1, sportId);
                pstmt.setInt(2, athleteId);
                pstmt.setInt(3, place);
                pstmt.execute();
            }
        } catch (SQLException e) {
            //e.printStackTrace();
            if(Integer.valueOf(e.getSQLState()) ==
                    PostgreSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()){
                return NOT_EXISTS;
            }
            else if (Integer.valueOf(e.getSQLState()) ==
                            PostgreSQLErrorCodes.CHECK_VIOLATION.getValue()){
                return BAD_PARAMS;
            }
            else {
                return ERROR;
            }
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return OK;
    }

    public static ReturnValue athleteDisqualified(Integer sportId, Integer athleteId){
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(
                    "DELETE FROM Medals " +
                            "where sport_id = ? AND athlete_id= ?");
            pstmt.setInt(1, sportId);
            pstmt.setInt(2, athleteId);
            pstmt.executeUpdate();

        } catch (SQLException e) {
            //  e.printStackTrace();
            return ERROR;
        } finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return OK;
    }

    public static ReturnValue makeFriends(Integer athleteId1, Integer athleteId2) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("INSERT INTO Friends" +
                    " VALUES (?,?)");
            pstmt.setInt(1,athleteId1);
            pstmt.setInt(2, athleteId2);
            pstmt.execute();
            pstmt.setInt(1,athleteId2);
            pstmt.setInt(2, athleteId1);
            pstmt.execute();

        } catch (SQLException e) {
            //e.printStackTrace()();
            if(Integer.valueOf(e.getSQLState()) ==
                    PostgreSQLErrorCodes.UNIQUE_VIOLATION.getValue()){
                return ALREADY_EXISTS;
            }
            if(Integer.valueOf(e.getSQLState()) ==
                    PostgreSQLErrorCodes.FOREIGN_KEY_VIOLATION.getValue()){
                return NOT_EXISTS;
            }
            if (Integer.valueOf(e.getSQLState()) ==
                            PostgreSQLErrorCodes.CHECK_VIOLATION.getValue()){
                return BAD_PARAMS;
            }
            else {
                return ERROR;
            }
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return OK;
    }

    public static ReturnValue removeFriendship(Integer athleteId1, Integer athleteId2) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {

            pstmt = connection.prepareStatement("DELETE FROM Friends" +
                    " where athlete1_id= ? AND athlete2_id= ?");
            pstmt.setInt(1,athleteId1);
            pstmt.setInt(2, athleteId2);
            if (pstmt.executeUpdate()<1){
                return NOT_EXISTS;
            }
            pstmt.setInt(1,athleteId2);
            pstmt.setInt(2, athleteId1);
            pstmt.executeUpdate();

        } catch (SQLException e) {
            //e.printStackTrace()();
            return ERROR;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return OK;
    }

    public static ReturnValue changePayment(Integer athleteId, Integer sportId, Integer payment) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {

            pstmt = connection.prepareStatement(
                    "SELECT * FROM Athlete " +
                            "where athlete_id = ? AND active= false");
            pstmt.setInt(1,athleteId);
            ResultSet results1,results2;
            results1 = pstmt.executeQuery();

            pstmt = connection.prepareStatement(
                    "SELECT * FROM Sport " +
                            "where sport_id = ?");
            pstmt.setInt(1,sportId);
            results2 = pstmt.executeQuery();

            if ((!results1.next())||(!results2.next())) {
                return NOT_EXISTS;
            }

            pstmt = connection.prepareStatement(
                    "SELECT athlete_id FROM athlete where athlete_id = ?" +
                            " AND active=false");
            pstmt.setInt(1,athleteId);
            ResultSet results = pstmt.executeQuery();

            if (!results.next()) {
                return NOT_EXISTS;
            }

            pstmt = connection.prepareStatement(
                    "UPDATE ObserveOrParticipate " +
                            "SET money_paid = ? " +
                            "where athlete_id = ? AND sport_id= ?");
            pstmt.setInt(1,payment);
            pstmt.setInt(2,athleteId);
            pstmt.setInt(3, sportId);
            int affectedRows = pstmt.executeUpdate();

            if (affectedRows==0){
                return NOT_EXISTS;
            }


        } catch (SQLException e) {
            //e.printStackTrace()();
            if (Integer.valueOf(e.getSQLState()) ==
                    PostgreSQLErrorCodes.CHECK_VIOLATION.getValue()){
                return BAD_PARAMS;
            }
            else{
                return ERROR;
            }
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return OK;
    }

    public static Boolean isAthletePopular(Integer athleteId) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {

            pstmt = connection.prepareStatement(
                    "SELECT * FROM Athlete " +
                            "where athlete_id = ?");
            pstmt.setInt(1,athleteId);
            ResultSet results = pstmt.executeQuery();

            if (!results.next()) {
                return  false;
            }
        /** FriendsSports*/
            pstmt = connection.prepareStatement(
                    "SELECT sport_id FROM ObserveOrParticipate " +
                            "where athlete_id IN (SELECT athlete1_id FROM Friends WHERE athlete2_id= ?)");
            pstmt.setInt(1,athleteId);
            results=pstmt.executeQuery();
            if (!results.next()){
                return true;
            }

            pstmt = connection.prepareStatement(
                    "SELECT soprt_id FROM (SELECT sport_id FROM ObserveOrParticipate" +
                            " where athlete_id IN (SELECT athlete1_id FROM" +
                            " Friends WHERE athlete2_id= ?)) " +
                    "where sport_id NOT IN" +
                            " (SELECT soprt_id FROM ObserveOrParticipate where athlete_id= ?)");
            pstmt.setInt(1,athleteId);
            pstmt.setInt(2,athleteId);
            results = pstmt.executeQuery();

            if (results.next()){
                return false;
            }

        }catch (SQLException e) {
            //e.printStackTrace()();
            return false;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
               // e.printStackTrace();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace();
            }
        }
        return true;
    }

    public static Integer getTotalNumberOfMedalsFromCountry(String country) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {

            pstmt = connection.prepareStatement(
                    "SELECT count(m.athlete_id) FROM Athlete a, Medals m " +
                            "where a.country = ? AND m.athlete_id= a.athlete_id");
            pstmt.setString(1,country);
            ResultSet results = pstmt.executeQuery();

            if (results.next()) {
                return results.getInt(1);
            }
        }catch (SQLException e) {
            //e.printStackTrace()();
            return 0;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return 0;
    }

    public static Integer getIncomeFromSport(Integer sportId) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {

            pstmt = connection.prepareStatement(
                    "SELECT SUM(money_paid) FROM Athlete a,ObserveOrParticipate o " +
                            "where o.sport_id = ? AND o.athlete_id= a.athlete_id " +
                            "AND a.active=false");
            pstmt.setInt(1,sportId);
            ResultSet results = pstmt.executeQuery();

            if (results.next()) {
                return results.getInt(1);
            }
        }catch (SQLException e) {
            //e.printStackTrace()();
            return 0;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return 0;
    }

    public static String getBestCountry() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(
                    "SELECT a.country, COUNT(m.athlete_id) FROM Medals m,Athlete a " +
                            "where m.athlete_id= a.athlete_id GROUP BY a.country ORDER BY " +
                            "COUNT(m.athlete_id) DESC,a.country ASC");
            ResultSet results = pstmt.executeQuery();

            if (results.next()) {
                return results.getString(1);
            }
        }catch (SQLException e) {
            //e.printStackTrace()();
            return null;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return "";
    }

    public static String getMostPopularCity(){
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement("SELECT A.city, " +
                    "athlete_count/sports_count"+
                    " as avg FROM AllNumAthletesInCity A, NUMSPORTSINCITY S" +
                    " WHERE A.CITY=S.CITY ORDER BY avg DESC,A.city DESC");
            ResultSet results = pstmt.executeQuery();

            if (results.next()) {
                return results.getString(1);
            }
        }catch (SQLException e) {
            //e.printStackTrace()();
            return null;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
        return "";
    }

    public static ArrayList<Integer> getAthleteMedals(Integer athleteId) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> medals=new ArrayList<>();
        try {
            int gold_num=0,silver_num=0,bronze_num=0;
            ResultSet results;

            pstmt = connection.prepareStatement("SELECT medals FROM GoldMedals" +
                    " WHERE athlete_id= ?");
            pstmt.setInt(1,athleteId);
            results = pstmt.executeQuery();
            if (results.next()) {
                 gold_num=results.getInt(1);
            }

            pstmt = connection.prepareStatement("SELECT medals FROM SilverMedals" +
                    " WHERE athlete_id= ?");
            pstmt.setInt(1,athleteId);
            results = pstmt.executeQuery();
            if (results.next()) {
                silver_num=results.getInt(1);
            }

            pstmt = connection.prepareStatement("SELECT medals FROM BronzeMedals" +
                    " WHERE athlete_id= ?");
            pstmt.setInt(1,athleteId);
            results = pstmt.executeQuery();
            if (results.next()) {
                bronze_num=results.getInt(1);
            }
            medals.add(0,gold_num);
            medals.add(1,silver_num);
            medals.add(2,bronze_num);
            return medals;
        }catch (SQLException e) {
            //e.printStackTrace()();
            medals.add(0,0);
            medals.add(1,0);
            medals.add(2,0);
            return medals;
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
    }

    public static ArrayList<Integer> getMostRatedAthletes() {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> athletes=new ArrayList<>();
        try {
            ResultSet results;

            pstmt = connection.prepareStatement("SELECT s.athlete_id, " +
                    "gold_points+silver_points+bronze_points as total_points " +
                    "FROM GoldPoints g, SilverPoints s, BronzePoints b " +
                    "WHERE s.athlete_id=g.athlete_id AND " +
                    "b.athlete_id=g.athlete_id " +
                    "ORDER BY total_points DESC, s.athlete_id ASC");
            results = pstmt.executeQuery();
            int i=0;
            while (results.next()&&i<10) {
                int value=results.getInt(1);
                athletes.add(i,value);
                i++;
            }
            return athletes;
        }catch (SQLException e) {
            //e.printStackTrace()();
            return new ArrayList<>();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
    }

    public static ArrayList<Integer> getCloseAthletes(Integer athleteId) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> athletes=new ArrayList<>();
        try {
            int i=0;
            ResultSet results;

            pstmt = connection.prepareStatement("SELECT * FROM" +
                    " Athlete WHERE athlete_id= ?");
            pstmt.setInt(1,athleteId);
            results = pstmt.executeQuery();

            if (!results.next()) {
                return new ArrayList<>();
            }

            pstmt = connection.prepareStatement("SELECT sport_id FROM" +
                    " ObserveOrParticipate WHERE athlete_id= ?");
            pstmt.setInt(1,athleteId);
            results = pstmt.executeQuery();

            if (!results.next()) {
                pstmt = connection.prepareStatement("SELECT athlete_id FROM" +
                        " Athlete WHERE athlete_id <> ? ORDER by athlete_id ASC");
                pstmt.setInt(1,athleteId);

                results = pstmt.executeQuery();
                while (results.next()&&i<10) {
                    int value=results.getInt(1);
                    athletes.add(i,value);
                    i++;
                }
            }
            else {
                pstmt = connection.prepareStatement("SELECT o.athlete_id, " +
                        "COUNT(o.sport_id) FROM ObserveOrParticipate o " +
                        "WHERE o.sport_id IN (SELECT o2.sport_id FROM" +
                        " ObserveOrParticipate o2 WHERE athlete_id= ? ) " +
                        "group BY o.athlete_id HAVING COUNT(o.sport_id) >=" +
                        " (((SELECT COUNT(sport_id) FROM observeOrParticipate " +
                        "WHERE athlete_id= ?)+1)/2) AND o.athlete_id <> ? ORDER" +
                        " BY o.athlete_id ASC");
                pstmt.setInt(1,athleteId);
                pstmt.setInt(2,athleteId);
                pstmt.setInt(3,athleteId);
                results = pstmt.executeQuery();
                while (results.next()&&i<10) {
                    int value=results.getInt(1);
                    athletes.add(i,value);
                    i++;
                }
            }
            return athletes;
        }catch (SQLException e) {
            //e.printStackTrace()();
            return new ArrayList<>();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
    }

    public static ArrayList<Integer> getSportsRecommendation(Integer athleteId) {
        Connection connection = DBConnector.getConnection();
        PreparedStatement pstmt = null;
        ArrayList<Integer> athletes=new ArrayList<>();
        try {
            int i=0;
            ResultSet results;

            pstmt = connection.prepareStatement("SELECT * FROM" +
                    " Athlete WHERE athlete_id= ?");
            pstmt.setInt(1,athleteId);
            results = pstmt.executeQuery();

            if (!results.next()) {
                return new ArrayList<>();
            }

            pstmt = connection.prepareStatement("SELECT o.athlete_id, " +
                    "COUNT(o.sport_id) FROM ObserveOrParticipate o " +
                    "WHERE o.sport_id IN (SELECT o2.sport_id FROM" +
                    " ObserveOrParticipate o2 WHERE athlete_id= ? ) " +
                    "group BY o.athlete_id HAVING COUNT(o.sport_id) >=" +
                    " (((SELECT COUNT(sport_id) FROM observeOrParticipate " +
                    "WHERE athlete_id= ?)+1)/2) AND o.athlete_id <> ? ORDER" +
                    " BY o.athlete_id ASC");
            pstmt.setInt(1,athleteId);
            pstmt.setInt(2,athleteId);
            pstmt.setInt(3,athleteId);
            results = pstmt.executeQuery();
            if (!results.next()) {
                    return new ArrayList<>();
            }


            pstmt = connection.prepareStatement("SELECT o.sport_id," +
                    " COUNT(o.sport_id) FROM ObserveOrParticipate" +
                    " o WHERE o.athlete_id IN (SELECT o3.athlete_id" +
                    " FROM ObserveOrParticipate o3 WHERE o3.sport_id IN" +
                    " (SELECT o2.sport_id FROM ObserveOrParticipate o2" +
                    " WHERE athlete_id= ? ) group BY o3.athlete_id " +
                    " HAVING COUNT(o3.sport_id) >= (((SELECT COUNT(sport_id)" +
                    " FROM observeOrParticipate WHERE athlete_id= ?)+1)/2)" +
                    " AND o3.athlete_id <> ? ORDER BY o3.athlete_id ASC)" +
                    " AND o.sport_id NOT IN(SELECT sport_id FROM" +
                    " ObserveOrParticipatE WHERE athlete_id= ?) " +
                    " GROUP BY o.sport_id ORDER BY COUNT(o.sport_id)" +
                    " DESC,o.sport_id ASC ");
            pstmt.setInt(1,athleteId);
            pstmt.setInt(2,athleteId);
            pstmt.setInt(3,athleteId);
            pstmt.setInt(4,athleteId);
            results = pstmt.executeQuery();
                while (results.next()&&i<3) {
                    int value=results.getInt(1);
                    athletes.add(i,value);
                    i++;
                }


                return athletes;
        }catch (SQLException e) {
            //e.printStackTrace()();
            return new ArrayList<>();
        }
        finally {
            try {
                pstmt.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
            try {
                connection.close();
            } catch (SQLException e) {
                //e.printStackTrace()();
            }
        }
    }
}



