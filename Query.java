import java.util.Properties;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import java.io.FileInputStream;

/**
 * Runs queries against a back-end database
 */
public class Query {
    private static Properties configProps = new Properties();

    private static String imdbUrl;
    private static String customerUrl;

    private static String postgreSQLDriver;
    private static String postgreSQLUser;
    private static String postgreSQLPassword;

    // DB Connection
    private Connection _imdb;
    private Connection _customer_db;

    // Canned queries

    private String _search_sql = "SELECT * FROM movie WHERE name like ? ORDER BY id";
    private PreparedStatement _search_statement;

    private String _director_mid_sql = "SELECT y.* "
                     + "FROM movie_directors x, directors y "
                     + "WHERE x.mid = ? and x.did = y.id";
    private PreparedStatement _director_mid_statement;
    
    private String _actor_mid_sql = "SELECT y.* "
                     + "FROM casts x, actor y "
                     + "WHERE x.mid = ? and x.pid = y.id";
    private PreparedStatement _actor_mid_statement;

    private String _renter_mid_sql = "SELECT aid "
                     + "FROM Rentals r "
                     + "WHERE r.mid = ?";
    private PreparedStatement _renter_mid_statement;

    private String _director_fast_sql = "SELECT x.id, z.* "
                     + "FROM movie x, movie_directors y, directors z "
                     + "WHERE upper(x.name) like upper(?) and x.id = y.mid and y.did = z.id "
                     + "ORDER BY x.id";
    private PreparedStatement _director_fast_statement;

    private String _actor_fast_sql = "SELECT x.id, z.* "
                     + "FROM movie x, casts y, actor z "
                     + "WHERE upper(x.name) like upper(?) and x.id = y.mid and y.pid = z.id "
                     + "ORDER BY x.id";
    private PreparedStatement _actor_fast_statement;    

    /* uncomment, and edit, after your create your own customer database */
    private String _movie_sql = "SELECT * FROM movie WHERE id = ?";
    private PreparedStatement _movie_statement;

    private String _who_has_this_movie_sql = "SELECT aid FROM rentals WHERE mid = ?";
    private PreparedStatement _who_has_this_movie_statement;    
   
    private String _customer_login_sql = "SELECT * FROM accounts WHERE username = ? AND password = ?";
    private PreparedStatement _customer_login_statement;
    
    private String _customer_info_sql = "SELECT fname, lname FROM accounts WHERE id = ?";
    private PreparedStatement _customer_info_statement;

    private String _plan_maxrent_sql = "SELECT maxrent FROM plans p, accounts a WHERE p.pname = a.plan AND a.id = ?";
    private PreparedStatement _plan_maxrent_statement;
    
    private String _all_plans_sql = "SELECT * FROM plans";
    private PreparedStatement _all_plans_statement;

    private String _current_rentals_sql = "SELECT count(*) from rentals WHERE aid = ?";
    private PreparedStatement _current_rentals_statement;

    private String _begin_transaction_read_write_sql = "BEGIN TRANSACTION READ WRITE";
    private PreparedStatement _begin_transaction_read_write_statement;

    private String _commit_transaction_sql = "COMMIT TRANSACTION";
    private PreparedStatement _commit_transaction_statement;

    private String _rollback_transaction_sql = "ROLLBACK TRANSACTION";
    private PreparedStatement _rollback_transaction_statement;

    private String _customer_movie_rental_archive_sql = "INSERT INTO pastrentals VALUES (?,?);";
    private PreparedStatement _customer_movie_rental_archive_statement;
   
    private String _customer_data_sql = "SELECT x.fname, x.lname, y.pname, y.maxrent, y.fee "
                     + "FROM accounts x, plans y WHERE x.aid = ? and x.pid = y.pid";
    private PreparedStatement _customer_data_statement;   

    private String _customer_movie_return_sql = "DELETE FROM rentals WHERE aid=? AND mid=?;";
    private PreparedStatement _customer_movie_return_statement;

    private String _customer_plan_check_sql = "SELECT p.max_movies FROM plans p INNER JOIN accounts a on a.plan = p.plan WHERE a.aid = ?";
    private PreparedStatement _customer_plan_check_statement;

    private String _customer_movie_rent_sql = "INSERT INTO rentals VALUES (?,?)";
    private PreparedStatement _customer_movie_rent_statement;

    private String _customer_change_plan_sql = "UPDATE accounts SET plan = ? WHERE id = ?";
    private PreparedStatement _customer_change_plan_statement;
    
    private String _list_plans_sql = "SELECT * FROM plans";
    private PreparedStatement _list_plans_statement; 

    private String _list_rentals_sql = "SELECT mid FROM rentals WHERE aid = ?";
    private PreparedStatement _list_rentals_statement;

    private String _update_plan_sql = "UPDATE accounts SET pid = ? WHERE aid = ?";
    private PreparedStatement _update_plan_statement; 

    public Query() {
    }

    /**********************************************************/
    /* Connections to postgres databases */

    public void openConnection() throws Exception {
        configProps.load(new FileInputStream("dbconn.config"));
        
        
        imdbUrl        = configProps.getProperty("imdbUrl");
        customerUrl    = configProps.getProperty("customerUrl");
        postgreSQLDriver   = configProps.getProperty("postgreSQLDriver");
        postgreSQLUser     = configProps.getProperty("postgreSQLUser");
        postgreSQLPassword = configProps.getProperty("postgreSQLPassword");


        /* load jdbc drivers */
        Class.forName(postgreSQLDriver).newInstance();

        /* open connections to TWO databases: imdb and the customer database */
        _imdb = DriverManager.getConnection(imdbUrl, // database
                postgreSQLUser, // user
                postgreSQLPassword); // password

        _customer_db = DriverManager.getConnection(customerUrl, // database
                postgreSQLUser, // user
                postgreSQLPassword); // password
                
    }

    public void closeConnection() throws Exception {
        _imdb.close();
        _customer_db.close();
    }

    /**********************************************************/
    /* prepare all the SQL statements in this method.
      "preparing" a statement is almost like compiling it.  Note
       that the parameters (with ?) are still not filled in */

    public void prepareStatements() throws Exception {

        _search_statement = _imdb.prepareStatement(_search_sql);
        _director_mid_statement = _imdb.prepareStatement(_director_mid_sql);
       _actor_mid_statement = _imdb.prepareStatement(_actor_mid_sql);
       _renter_mid_statement = _customer_db.prepareStatement(_renter_mid_sql);
       
       /* uncomment after you create your customers database */
        
        _customer_login_statement = _customer_db.prepareStatement(_customer_login_sql);
        _begin_transaction_read_write_statement = _customer_db.prepareStatement(_begin_transaction_read_write_sql);
        _commit_transaction_statement = _customer_db.prepareStatement(_commit_transaction_sql);
        _rollback_transaction_statement = _customer_db.prepareStatement(_rollback_transaction_sql);

        /* add here more prepare statements for all the other queries you need */
        /* . . . . . . */
        
        _customer_data_statement = _customer_db.prepareStatement(_customer_data_sql);
        _customer_info_statement = _customer_db.prepareStatement(_customer_info_sql);
        _plan_maxrent_statement = _customer_db.prepareStatement(_plan_maxrent_sql);
        _current_rentals_statement = _customer_db.prepareStatement(_current_rentals_sql);
        _all_plans_statement = _customer_db.prepareStatement(_all_plans_sql);
        _customer_change_plan_statement = _customer_db.prepareStatement(_customer_change_plan_sql);
        _customer_movie_rental_archive_statement = _customer_db.prepareStatement(_customer_movie_rental_archive_sql);
        _customer_movie_return_statement = _customer_db.prepareStatement(_customer_movie_return_sql);
        _customer_plan_check_statement = _customer_db.prepareStatement(_customer_plan_check_sql);
        _customer_movie_rent_statement = _customer_db.prepareStatement(_customer_movie_rent_sql);
        _movie_statement = _customer_db.prepareStatement(_movie_sql);
        _list_rentals_statement = _customer_db.prepareStatement(_list_rentals_sql);
        _who_has_this_movie_statement = _customer_db.prepareStatement(_who_has_this_movie_sql);
	    
    }


    /**********************************************************/
    /* suggested helper functions  */

    public int helper_compute_remaining_rentals(int cid) throws Exception {
        /* how many movies can she/he still rent ? */
        /* you have to compute and return the difference between the customer's plan
           and the count of oustanding rentals */
        _plan_maxrent_statement.clearParameters();
        _plan_maxrent_statement.setInt(1,cid);   
        ResultSet maxrent_set = _plan_maxrent_statement.executeQuery();
        _current_rentals_statement.clearParameters();
        _current_rentals_statement.setInt(1,cid);
        ResultSet rentals_num_set = _current_rentals_statement.executeQuery();
        maxrent_set.next();
        int max = maxrent_set.getInt(1);
        int out = 0;
        if (rentals_num_set.next()) out = rentals_num_set.getInt(1);
        maxrent_set.close();
        rentals_num_set.close();
        return (max - out);
    }

    public String helper_compute_customer_name(int cid) throws Exception {
        /* you find  the first + last name of the current customer */
        _customer_info_statement.clearParameters();
        _customer_info_statement.setInt(1,cid);
        ResultSet info_set = _customer_info_statement.executeQuery();
        info_set.next();
        String name = info_set.getString(1) + " " + info_set.getString(2);
        info_set.close();
        return (name);
    }

    public boolean helper_check_plan(String plan_id) throws Exception {
        /* is plan_id a valid plan id ?  you have to figure out */
        _all_plans_statement.clearParameters();
        ResultSet plans_set = _all_plans_statement.executeQuery();
        boolean flag = false;
        while(plans_set.next()){
            if(plans_set.getString(1).equals(plan_id)) flag = true;
        }
        plans_set.close();
        return flag;
    }

    public boolean helper_check_movie(int mid) throws Exception {
        /* is mid a valid movie id ? you have to figure out  */

        boolean valid;

        _movie_statement.clearParameters();
        _movie_statement.setInt(1,mid);
        ResultSet movie_set = _movie_statement.executeQuery();
        if (movie_set.next()) valid = true;
        else valid = false;
        movie_set.close();

        return valid;
    }

    private int helper_who_has_this_movie(int mid) throws Exception {
        /* find the customer id (cid) of whoever currently rents the movie mid; return -1 if none */

        int cid;

        _who_has_this_movie_statement.clearParameters();
        _who_has_this_movie_statement.setInt(1,mid);
        ResultSet who_set = _who_has_this_movie_statement.executeQuery();
        if (who_set.next()){
            cid = who_set.getInt(1);
            System.out.println("Customer: " + cid);
        } 
        else cid = -1;
        who_set.close();
        return (cid);
    }

    /**********************************************************/
    /* login transaction: invoked only once, when the app is started  */
    public int transaction_login(String username, String password) throws Exception {
        /* authenticates the user, and returns the user id, or -1 if authentication fails */

        /* Uncomment after you create your own customers database */
        
        int cid;

        _customer_login_statement.clearParameters();
        _customer_login_statement.setString(1,username);
        _customer_login_statement.setString(2,password);
        ResultSet cid_set = _customer_login_statement.executeQuery();
        if (cid_set.next()) cid = cid_set.getInt(1);
        else cid = -1;
        cid_set.close();
        return(cid);
    }

    public void transaction_personal_data(int cid) throws Exception {
           /* println the customer's personal data: name, and plan number */

        System.out.println( "Name: " + 
                            helper_compute_customer_name(cid) + 
                            "\nOpen Rental slots: " + 
                            helper_compute_remaining_rentals(cid)
        );
    }


    /**********************************************************/
    /* main functions in this project: */

    public void transaction_search(int cid, String movie_title)
            throws Exception {
        /* searches for movies with matching titles: SELECT * FROM movie WHERE name LIKE movie_title */
        /* prints the movies, directors, actors, and the availability status:
           AVAILABLE, or UNAVAILABLE, or YOU CURRENTLY RENT IT */

        /* set the first (and single) '?' parameter */
        _search_statement.clearParameters();
        _search_statement.setString(1, '%' + movie_title + '%');

        ResultSet movie_set = _search_statement.executeQuery();
        while (movie_set.next()) {
            int mid = movie_set.getInt(1);
            System.out.println("ID: " + mid + " NAME: "
                    + movie_set.getString(2) + " YEAR: "
                    + movie_set.getString(3));
            /* do a dependent join with directors */
            _director_mid_statement.clearParameters();
            _director_mid_statement.setInt(1, mid);
            ResultSet director_set = _director_mid_statement.executeQuery();
            while (director_set.next()) {
                System.out.println("\t\tDirector: " + director_set.getString(3)
                        + " " + director_set.getString(2));
            }
            director_set.close();
          
             /* now you need to retrieve the actors, in the same manner */

            _actor_mid_statement.clearParameters();
            _actor_mid_statement.setInt(1, mid);
            ResultSet actor_set = _actor_mid_statement.executeQuery();
            while (actor_set.next()) {
                System.out.println("\t\tActor: " + actor_set.getString(3)
                        + " " + actor_set.getString(2));
            }
            actor_set.close();

            /* then you have to find the status: of "AVAILABLE" "YOU HAVE IT", "UNAVAILABLE" */

            _renter_mid_statement.clearParameters();
            _renter_mid_statement.setInt(1, mid);
            ResultSet renter_set = _renter_mid_statement.executeQuery();

            int renterID = -1;

            //if movie is rented out then get matching customer id
            if (renter_set.next())
                renterID = renter_set.getInt(1);

            System.out.println("Is the movie available?");

            if (renterID == -1)
                System.out.println("Movie is Available for rent!");
            else
                if (renterID == cid)
                    System.out.println("You have the movie out silly!");
                else
                    System.out.println("Sorry but the movie is already rented out!");

            renter_set.close();
        }
        System.out.println();
    }

    public void transaction_choose_plan(int cid, String pid) throws Exception {
        /* updates the customer's plan to pid: UPDATE accounts SET plan = ? WHERE id = ? */
        /* remember to enforce consistency ! */
        _begin_transaction_read_write_statement.executeUpdate();
        int required_min = 3;
        _all_plans_statement.clearParameters();
        ResultSet plans_set = _all_plans_statement.executeQuery();
        while(plans_set.next()){
            if(plans_set.getString(1).equals(pid)) required_min = plans_set.getInt(3);
        }
        plans_set.close();
        
        int out = 0;
        _current_rentals_statement.clearParameters();
        _current_rentals_statement.setInt(1,cid);
        ResultSet rentals_num_set = _current_rentals_statement.executeQuery();
        if (rentals_num_set.next()) out = rentals_num_set.getInt(1);
        rentals_num_set.close();
        
        if(out <= required_min){
            _customer_change_plan_statement.clearParameters();
            _customer_change_plan_statement.setString(1,pid);
            _customer_change_plan_statement.setInt(2,cid);
            _customer_change_plan_statement.executeUpdate();
            _commit_transaction_statement.executeUpdate();
            System.out.println("plan changed to " + pid);
        }
        else _rollback_transaction_statement.executeUpdate();
        System.out.println("You have to many movies rented! \nReturn some and try again.");
    }

    public void transaction_list_plans() throws Exception {
        /* println all available plans: SELECT * FROM plan */
        _all_plans_statement.clearParameters();
        ResultSet plans_set = _all_plans_statement.executeQuery();
        while(plans_set.next()){
            System.out.println(
                "PLAN NAME: " +
                plans_set.getString(1) +
                " FEE: $" +
                plans_set.getInt(2) +
                " MAX RENTALS: " +
                plans_set.getInt(3)
            );
        }
        plans_set.close();
    }
    
    public void transaction_list_user_rentals(int cid) throws Exception {
        /* prints all movies rented by the current user*/
        
        _list_rentals_statement.clearParameters();
        _list_rentals_statement.setInt(1,cid);
        ResultSet rentals_set = _list_rentals_statement.executeQuery();

        while (rentals_set.next()) {
            int mid = rentals_set.getInt(1);
            _movie_statement.clearParameters();
            _movie_statement.setInt(1,mid);
            
            ResultSet movie_set = _movie_statement.executeQuery();
                movie_set.next();
                System.out.println(
                    "ID: " +
                    movie_set.getString(1) +
                    " NAME: " +
                    movie_set.getString(2) +
                    " YEAR: " +
                    movie_set.getString(3)
                );
            movie_set.close();
        }
        rentals_set.close();
    }

    public void transaction_rent(int cid, int mid) throws Exception {
        if(helper_compute_remaining_rentals(cid) > 0){ //check that user has open rental slot
            if(helper_check_movie(mid)){ //check that movie is valid
                if(helper_who_has_this_movie(mid) == -1){ //check if movie is available to rent
                    _customer_movie_rent_statement.clearParameters();
                    _customer_movie_rent_statement.setInt(1,cid);
                    _customer_movie_rent_statement.setInt(2,mid);
                    _customer_movie_rent_statement.execute();
                    _commit_transaction_statement.execute();
                    System.out.println("Movie rented. You have " + helper_compute_remaining_rentals(cid) + " rentals left");
                }
                else System.out.println("Movie is already rented out.");
            }
            else System.out.println("Please enter a valid movie ID number.");
        }
        else System.out.println("You cannot rent any more movies. \nPlease return a movie before renting another one");
    }
    public void transaction_return(int cid, int mid) throws Exception {
        if(helper_check_movie(mid) && (helper_who_has_this_movie(mid) == cid)){
            _begin_transaction_read_write_statement.execute();
            _customer_movie_rental_archive_statement.clearParameters();
            _customer_movie_rental_archive_statement.setInt(1, cid);
            _customer_movie_rental_archive_statement.setInt(2, mid);
            _customer_movie_rental_archive_statement.execute();
            _customer_movie_return_statement.clearParameters();
            _customer_movie_return_statement.setInt(1, cid);
            _customer_movie_return_statement.setInt(2, mid);
            _customer_movie_return_statement.execute();
            _commit_transaction_statement.execute();
        }
    }

    public void transaction_fast_search(int cid, String movie_title)
            throws Exception {
        /* like transaction_search, but uses joins instead of independent joins
           Needs to run three SQL queries: (a) movies, (b) movies join directors, (c) movies join actors
           Answers are sorted by mid.
           Then merge-joins the three answer sets */

        /* set the first (and single) '?' parameter */
      _search_statement.clearParameters();
        _search_statement.setString(1, '%' + movie_title + '%');
        ResultSet movie_set = _search_statement.executeQuery();

        /* retrieve directors */
        _director_fast_statement.clearParameters();
        _director_fast_statement.setString(1, '%' + movie_title + '%');
        ResultSet director_set = _director_fast_statement.executeQuery();

 
        /* retrieve the actors */
        _actor_fast_statement.clearParameters();
        _actor_fast_statement.setString(1, '%' + movie_title + '%');
        ResultSet actor_set = _actor_fast_statement.executeQuery();

        director_set.next();
        actor_set.next();

        while (movie_set.next()) {
            int mid = movie_set.getInt(1);
            System.out.println("ID: " + mid + " NAME: "
                    + movie_set.getString(2) + " YEAR: "
                    + movie_set.getString(3));
        
            do {
                if (director_set.getInt(1) == mid) {
                    System.out.println("\t\tDirector: " + director_set.getString(4)
                            + " " + director_set.getString(3));
                }
                else {
                    break;
                }
            } while (director_set.next());

            do {
                if (actor_set.getInt(1) == mid) {
                    System.out.println("\t\tActor: " + actor_set.getString(4)
                            + " " + actor_set.getString(3));
                }
                else {
                    break;
                } 
            } while (actor_set.next());
        }
        movie_set.close();
        director_set.close();
        actor_set.close();
        System.out.println();
    }
}
