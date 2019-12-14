package olympic.business;

import java.util.Objects;


public class Sport {
    @Override
    public int hashCode() {
        return Objects.hash(id, name, city, athletesCount);
    }

    int id = -1;
    String name = null;
    String city = null;
    int athletesCount = 0;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public int getAthletesCount() {
        return athletesCount;
    }

    public void setAthletesCount(int athletesCount) {
        this.athletesCount = athletesCount;
    }

    public static Sport badSport()
    {
        return new Sport();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Sport sport = (Sport) o;
        return id == sport.id &&
                athletesCount == sport.athletesCount &&
                Objects.equals(name, sport.name) &&
                Objects.equals(city, sport.city);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Song{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", city='").append(city).append('\'');
        sb.append(", athletesCount='").append(athletesCount).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
