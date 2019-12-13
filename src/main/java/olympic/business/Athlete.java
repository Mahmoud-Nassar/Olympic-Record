package olympic.business;
public class Athlete {
    int id = -1;
    String name = null;
    String country = null;
    boolean active = false;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean getIsActive() {
        return active;
    }

    public void setIsActive(boolean active){this.active=active;}

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public static Athlete badAthlete()
    {
        return new Athlete();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Athlete)) return false;

        Athlete athlete = (Athlete) o;

        if (getId() != athlete.getId()) return false;
        if (getCountry() != null ? !getCountry().equals(athlete.getCountry()) : athlete.getCountry() != null) return false;
        if (getIsActive() != athlete.getIsActive()) return false;
        return getName() != null ? getName().equals(athlete.getName()) : athlete.getName() == null;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("Athlete{");
        sb.append("id=").append(id);
        sb.append(", name='").append(name).append('\'');
        sb.append(", country='").append(country).append('\'');
        sb.append(", active='").append(active).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
