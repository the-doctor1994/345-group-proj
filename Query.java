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

    /* uncomment, and edit, after your create your own customer database */
   
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

    private String _customer_movie_return_sql = "UPDATE MovieRentals SET cid=NULL, status='closed' WHERE cid=? AND mid=?;";
    private PreparedStatement _customer_movie_return_statement;

    private String _customer_movie_check_sql = "SELECT count(*) FROM MovieRentals WHERE cid = ? AND status = 'open';";
    private PreparedStatement _customer_movie_check_statement;

    private String _customer_plan_check_sql = "SELECT r.max_movies FROM RentalPlans r INNER JOIN Customers c on c.pid = r.pid WHERE c.cid = ?";
    private PreparedStatement _customer_plan_check_statement;

    private String _customer_movie_rent_sql = "INSERT INTO MovieRentals VALUES (?,?,'open')";
    private PreparedStatement _customer_movie_rent_statement;


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
	   _customer_movie_return_statement = _imdb.prepareStatement(_customer_movie_return_sql);
	   _customer_plan_check_statement = _imdb.prepareStatement(_customer_plan_check_sql);
	   _customer_movie_check_statement = _imdb.prepareStatement(_customer_movie_check_sql);
	   _customer_movie_rent_statement = _imdb.prepareStatement(_customer_movie_rent_sql);
       _actor_mid_statement = _imdb.prepareStatement(_actor_mid_sql);
       _renter_mid_statement = _customer_db.prepareStatement(_renter_mid_sql);
       
       /* uncomment after you create your customers database */
        
        _customer_login_statement = _customer_db.prepareStatement(_customer_login_sql);
        _begin_transaction_read_write_statement = _customer_db.prepareStatement(_begin_transaction_read_write_sql);
        _commit_transaction_statement = _customer_db.prepareStatement(_commit_transaction_sql);
        _rollback_transaction_statement = _customer_db.prepareStatement(_rollback_transaction_sql);

        /* add here more prepare statements for all the other queries you need */
        /* . . . . . . */
        
        _customer_info_statement = _customer_db.prepareStatement(_customer_info_sql);
        _plan_maxrent_statement = _customer_db.prepareStatement(_plan_maxrent_sql);
        _current_rentals_statement = _customer_db.prepareStatement(_current_rentals_sql);
        _all_plans_statement = _customer_db.prepareStatement(_all_plans_sql);
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
        return (max - out);
    }

    public String helper_compute_customer_name(int cid) throws Exception {
        /* you find  the first + last name of the current customer */
        _customer_info_statement.clearParameters();
        _customer_info_statement.setInt(1,cid);
        ResultSet info_set = _customer_info_statement.executeQuery();
        info_set.next();
        return (info_set.getString(1) + " " + info_set.getString(2));
    }

    public boolean helper_check_plan(int plan_id) throws Exception {
        /* is plan_id a valid plan id ?  you have to figure out */
        return true;
    }

    public boolean helper_check_movie(int mid) throws Exception {
        /* is mid a valid movie id ? you have to figure out  */
        return true;
    }

    private int helper_who_has_this_movie(int mid) throws Exception {
        /* find the customer id (cid) of whoever currently rents the movie mid; return -1 if none */
        return (77);
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
        return(cid);
         
        //return (55);
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

    public void transaction_choose_plan(int cid, int pid) throws Exception {
        /* updates the customer's plan to pid: UPDATE customers SET plid = pid */
        /* remember to enforce consistency ! */
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
    }
    
    public void transaction_list_user_rentals(int cid) throws Exception {
        /* println all movies rented by the current user*/
    }

    public void transaction_rent(int cid, int mid) throws Exception {
	if(helper_check_movie(mid)){
	    _begin_transaction_read_write_statement.execute();
	    _customer_movie_check_statement.clearParameters();
	    _customer_movie_check_statement.setInt(1, cid);
	    ResultSet check_movies = _customer_movie_check_statement.executeQuery();
	    check_movies.first();
	    int rented = check_movies.getInt(1);
	    _customer_plan_check_statement.clearParameters();
	    _customer_plan_check_statement.setInt(1, cid);
	    ResultSet check_plan = _customer_plan_check_statement.executeQuery();
	    check_plan.first();
	    int max_movies = check_plan.getInt(1);
	    if(rented < max_movies){
		_customer_movie_rent_statement.clearParameters();
		_customer_movie_rent_statement.setInt(1,mid);
		_customer_movie_rent_statement.setInt(2,cid);
		_customer_movie_rent_statement.execute();
		_commit_transaction_statement.execute();
	    }
	    else{
		_rollback_transaction_statement.execute();
	    }
	    
	}
    }
    public void transaction_return(int cid, int mid) throws Exception {
	if(helper_check_movie(mid) && (helper_who_has_this_movie(mid) == cid)){
	    _begin_transaction_read_write_statement.execute();
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
    }

}
