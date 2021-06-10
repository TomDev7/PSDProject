package spendreport;

public class MyAlert {

    int id;
    int window;
    int whichStatisctic;
    double howMuchExceeded;

    public MyAlert(int window, int whichStatisctic, double howMuchExceeded) {
        this.window = window;
        this.whichStatisctic = whichStatisctic;
        this.howMuchExceeded = howMuchExceeded;
    }

    @Override
    public String toString() {
        return "MyAlert{" +
                ", window=" + window +
                ", whichStatisctic=" + whichStatisctic +
                ", howMuchExceeded=" + howMuchExceeded +
                '}';
    }
}
