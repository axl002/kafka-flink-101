import com.rethinkdb.RethinkDB;
import com.rethinkdb.net.Connection;
import com.rethinkdb.net.Cursor;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

// read and print everything from poeapi.itemCount
public class test {


    public static final RethinkDB r = RethinkDB.r;
    public static void main(String[] args){

        Connection conn = r.connection().hostname("35.166.62.31").port(28015).connect();
        conn.use("poeapi");
        Cursor cursor = r.table("itemCount").changes().run(conn);
        System.out.println("iterator created");
        for (Object doc : cursor) {

            //String str = "ZZZZL <%= dsn %> AFFF <%= AFG %>";
            //System.out.println(str);
            //Pattern pattern = Pattern.compile("\\s\\|\\s(.*?)~doo~");
            //Matcher matcher = pattern.matcher(doc.toString());
            //while (matcher.find()) {
            //matcher.find();
            //System.out.println(matcher.group(1));
            //System.out.println(matcher.group(0));
            //}


            System.out.println(doc);
        }
        cursor.close();
        conn.close();


    }
}
