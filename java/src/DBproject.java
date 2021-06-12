/*
 * Template JAVA User Interface
 * =============================
 *
 * Database Management Systems
 * Department of Computer Science &amp; Engineering
 * University of California - Riverside
 *
 * Target DBMS: 'Postgres'
 *
 */


import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.ArrayList;

/**
 * This class defines a simple embedded SQL utility class that is designed to
 * work with PostgreSQL JDBC drivers.
 *
 */

public class DBproject{
	//reference to physical database connection
	private Connection _connection = null;
	static BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	
	public DBproject(String dbname, String dbport, String user, String passwd) throws SQLException {
		System.out.print("Connecting to database...");
		try{
			// constructs the connection URL
			String url = "jdbc:postgresql://localhost:" + dbport + "/" + dbname;
			System.out.println ("Connection URL: " + url + "\n");
			
			// obtain a physical connection
	        this._connection = DriverManager.getConnection(url, user, passwd);
	        System.out.println("Done");
		}catch(Exception e){
			System.err.println("Error - Unable to Connect to Database: " + e.getMessage());
	        System.out.println("Make sure you started postgres on this machine");
	        System.exit(-1);
		}
	}
	
	/**
	 * Method to execute an update SQL statement.  Update SQL instructions
	 * includes CREATE, INSERT, UPDATE, DELETE, and DROP.
	 * 
	 * @param sql the input SQL string
	 * @throws java.sql.SQLException when update failed
	 * */
	public void executeUpdate (String sql) throws SQLException { 
		// creates a statement object
		Statement stmt = this._connection.createStatement ();

		// issues the update instruction
		stmt.executeUpdate (sql);

		// close the instruction
	    stmt.close ();
	}//end executeUpdate

	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and outputs the results to
	 * standard out.
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQueryAndPrintResult (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		/*
		 *  obtains the metadata object for the returned result set.  The metadata
		 *  contains row and column info.
		 */
		ResultSetMetaData rsmd = rs.getMetaData ();
		int numCol = rsmd.getColumnCount ();
		int rowCount = 0;
		
		//iterates through the result set and output them to standard out.
		boolean outputHeader = true;
		while (rs.next()){
			if(outputHeader){
				for(int i = 1; i <= numCol; i++){
					System.out.print(rsmd.getColumnName(i) + "\t");
			    }
			    System.out.println();
			    outputHeader = false;
			}
			for (int i=1; i<=numCol; ++i)
				System.out.print (rs.getString (i) + "\t");
			System.out.println ();
			++rowCount;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the results as
	 * a list of records. Each record in turn is a list of attribute values
	 * 
	 * @param query the input query string
	 * @return the query result as a list of records
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public List<List<String>> executeQueryAndReturnResult (String query) throws SQLException { 
		//creates a statement object 
		Statement stmt = this._connection.createStatement (); 
		
		//issues the query instruction 
		ResultSet rs = stmt.executeQuery (query); 
	 
		/*
		 * obtains the metadata object for the returned result set.  The metadata 
		 * contains row and column info. 
		*/ 
		ResultSetMetaData rsmd = rs.getMetaData (); 
		int numCol = rsmd.getColumnCount (); 
		int rowCount = 0; 
	 
		//iterates through the result set and saves the data returned by the query. 
		boolean outputHeader = false;
		List<List<String>> result  = new ArrayList<List<String>>(); 
		while (rs.next()){
			List<String> record = new ArrayList<String>(); 
			for (int i=1; i<=numCol; ++i) 
				record.add(rs.getString (i)); 
			result.add(record); 
		}//end while 
		stmt.close (); 
		return result; 
	}//end executeQueryAndReturnResult
	
	/**
	 * Method to execute an input query SQL instruction (i.e. SELECT).  This
	 * method issues the query to the DBMS and returns the number of results
	 * 
	 * @param query the input query string
	 * @return the number of rows returned
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	public int executeQuery (String query) throws SQLException {
		//creates a statement object
		Statement stmt = this._connection.createStatement ();

		//issues the query instruction
		ResultSet rs = stmt.executeQuery (query);

		int rowCount = 0;

		//iterates through the result set and count nuber of results.
		if(rs.next()){
			rowCount++;
		}//end while
		stmt.close ();
		return rowCount;
	}
	
	/**
	 * Method to fetch the last value from sequence. This
	 * method issues the query to the DBMS and returns the current 
	 * value of sequence used for autogenerated keys
	 * 
	 * @param sequence name of the DB sequence
	 * @return current value of a sequence
	 * @throws java.sql.SQLException when failed to execute the query
	 */
	
	public int getCurrSeqVal(String sequence) throws SQLException {
		Statement stmt = this._connection.createStatement ();
		
		ResultSet rs = stmt.executeQuery (String.format("Select currval('%s')", sequence));
		if (rs.next()) return rs.getInt(1);
		return -1;
	}

	/**
	 * Method to close the physical connection if it is open.
	 */
	public void cleanup(){
		try{
			if (this._connection != null){
				this._connection.close ();
			}//end if
		}catch (SQLException e){
	         // ignored.
		}//end try
	}//end cleanup

	/**
	 * The main execution method
	 * 
	 * @param args the command line arguments this inclues the <mysql|pgsql> <login file>
	 */
	public static void main (String[] args) {
		if (args.length != 3) {
			System.err.println (
				"Usage: " + "java [-classpath <classpath>] " + DBproject.class.getName () +
		            " <dbname> <port> <user>");
			return;
		}//end if
		
		DBproject esql = null;
		
		try{
			System.out.println("(1)");
			
			try {
				Class.forName("org.postgresql.Driver");
			}catch(Exception e){

				System.out.println("Where is your PostgreSQL JDBC Driver? " + "Include in your library path!");
				e.printStackTrace();
				return;
			}
			
			System.out.println("(2)");
			String dbname = args[0];
			String dbport = args[1];
			String user = args[2];
			
			esql = new DBproject (dbname, dbport, user, "");
			
			boolean keepon = true;
			while(keepon){
				System.out.println("MAIN MENU");
				System.out.println("---------");
				System.out.println("1. Add Doctor");
				System.out.println("2. Add Patient");
				System.out.println("3. Add Appointment");
				System.out.println("4. Make an Appointment");
				System.out.println("5. List appointments of a given doctor");
				System.out.println("6. List all available appointments of a given department");
				System.out.println("7. List total number of different types of appointments per doctor in descending order");
				System.out.println("8. Find total number of patients per doctor with a given status");
				System.out.println("9. < EXIT");
				
				switch (readChoice()){
					case 1: AddDoctor(esql); break;
					case 2: AddPatient(esql); break;
					case 3: AddAppointment(esql); break;
					case 4: MakeAppointment(esql); break;
					case 5: ListAppointmentsOfDoctor(esql); break;
					case 6: ListAvailableAppointmentsOfDepartment(esql); break;
					case 7: ListStatusNumberOfAppointmentsPerDoctor(esql); break;
					case 8: FindPatientsCountWithStatus(esql); break;
					case 9: keepon = false; break;
				}
			}
		}catch(Exception e){
			System.err.println (e.getMessage ());
		}finally{
			try{
				if(esql != null) {
					System.out.print("Disconnecting from database...");
					esql.cleanup ();
					System.out.println("Done\n\nBye !");
				}//end if				
			}catch(Exception e){
				// ignored.
			}
		}
	}

	public static int readChoice() {
		int input;
		// returns only if a correct value is given.
		do {
			System.out.print("Please make your choice: ");
			try { // read the integer, parse it and break.
				input = Integer.parseInt(in.readLine());
				break;
			}catch (Exception e) {
				System.out.println("Your input is invalid!");
				continue;
			}//end try
		}while (true);
		return input;
	}//end readChoice

	public static void AddDoctor(DBproject esql) {//1
		try{
			String max_doc_id_query = "SELECT MAX(D.doctor_ID) FROM Doctor D";
			List<List<String>> max_doc_id = esql.executeQueryAndReturnResult(max_doc_id_query);
			int max_id = 0;
			for(List<String> a: max_doc_id){
				for(String b: a){
					max_id = Integer.parseInt(b) + 1;
				}
			}

			String query = "INSERT INTO Doctor VALUES (";

			// System.out.print("\tEnter Doctor ID: "); //type: int, not null, primary key
			String doc_id = "'" + Integer.toString(max_id) + "', ";

			System.out.print("\tEnter Doctor Name: ");//type: varchar(128)
			String doc_name = "'" + in.readLine() + "', ";

			System.out.print("\tEnter Doctor Speciality: ");//type: varchar(24)
			String doc_spec = "'" + in.readLine() + "', ";

			System.out.print("\tEnter Doctor Department ID: ");//type: int, not null, did reference department(dept_id)
			String doc_dept_id = "'" + in.readLine() + "')";

			query += doc_id + doc_name + doc_spec + doc_dept_id;

			// System.out.println(query);

			esql.executeUpdate(query);
		}catch(Exception e){
			System.err.println (e.getMessage());
		}
	}

	public static void AddPatient(DBproject esql) {//2
		try{
			String max_pat_id_query = "SELECT MAX(P.patient_ID) FROM Patient P";
			List<List<String>> max_pat_id = esql.executeQueryAndReturnResult(max_pat_id_query);
			int max_id = 0;
			for(List<String> a: max_pat_id){
				for(String b: a){
					max_id = Integer.parseInt(b) + 1;
				}
			}

			String query = "INSERT INTO Patient VALUES (";

			// System.out.print("\tEnter Patient ID: "); //type: int, not null, primary key
			String patient_id = "'" + Integer.toString(max_id) + "', ";

			System.out.print("\tEnter Patient Name: ");//type: varchar(128), not null
			String patient_name = "'" + in.readLine() + "', ";

			System.out.print("\tEnter Patient Gender (F|M): ");//type: _gender,  not null
			String raw_gender = in.readLine();
			while(!raw_gender.equals("F") && !raw_gender.equals("M")){
				System.out.print("\tPlease enter a valid gender (F|M): ");
				raw_gender = in.readLine();
			}
			String patient_gender = "'" + raw_gender + "', ";

			System.out.print("\tEnter Patient Age: ");//type: int, not null
			String patient_age = "'" + in.readLine() + "', ";

			System.out.print("\tEnter Patient Address: ");//type: varchar(256)
			String patient_addr = "'" + in.readLine() + "', ";

			String patient_appt_num = "'0')"; //type: int, assumption: this is a new patient with 0 appointments initially

			query += patient_id + patient_name + patient_gender + patient_age + patient_addr + patient_appt_num;

			// System.out.println(query);

			esql.executeUpdate(query);
		}catch(Exception e){
			System.err.println (e.getMessage());
		}
	}

	public static void AddAppointment(DBproject esql) {//3
		try{
			String max_appt_id_query = "SELECT MAX(A.appnt_ID) FROM Appointment A";
			List<List<String>> max_appt_id = esql.executeQueryAndReturnResult(max_appt_id_query);
			int max_id = 0;
			for(List<String> a: max_appt_id){
				for(String b: a){
					max_id = Integer.parseInt(b) + 1;
				}
			}

			String query = "INSERT INTO Appointment VALUES (";

			// System.out.print("\tEnter Appointment ID: "); //type: int, not null, primary key
			String appt_id = "'" + Integer.toString(max_id) + "', ";

			System.out.print("\tEnter Appointment Date (MM/DD/YYYY): ");//type: date, not null
			String appt_date = "TO_DATE('" + in.readLine() + "', " + "'MM/DD/YYYY'), ";

			System.out.print("\tEnter Appointment Time Slot (HH:MM-HH:MM): ");//type: varchar(11)
			String appt_time_slot = "'" + in.readLine() + "', ";

			System.out.print("\tEnter Appointment Status (PA|AC|AV|WL): ");//type: _status
			String raw_status = in.readLine();
			while(!raw_status.equals("PA") && !raw_status.equals("AC") && !raw_status.equals("AV") && !raw_status.equals("WL")){
				System.out.print("\tPlease enter a valid appointment status (PA|AC|AV|WL): ");
				raw_status = in.readLine();
			}
			String appt_status = "'" + raw_status + "')";

			query += appt_id + appt_date + appt_time_slot + appt_status;

			// System.out.println(query);

			esql.executeUpdate(query);
		}catch(Exception e){
			System.err.println (e.getMessage());
		}
	}


	public static void MakeAppointment(DBproject esql) {//4
		// Given a patient, a doctor and an appointment of the doctor that s/he wants to take, add an appointment to the DB
		try{
			String query = "";

			esql.executeQuery(query);
		}catch(Exception e){
			System.err.println (e.getMessage());
		}
	}

	public static void ListAppointmentsOfDoctor(DBproject esql) {//5
		// For a doctor ID and a date range, find the list of active and available appointments of the doctor
		try{
			String query = "SELECT * FROM Appointment A, (SELECT * FROM has_appointment WHERE doctor_id = ";

			System.out.print("\tEnter Doctor ID: ");
			String doc_id = in.readLine();
			query += doc_id + ") AS temp WHERE temp.appt_id = A.appnt_ID AND A.status = 'AC' OR A.status = 'AV' AND A.adate BETWEEN '";

			System.out.print("\tEnter Date Range (MM/DD/YYYY-MM/DD/YYYY): ");
			String date_range = in.readLine();
			String[] date_range_arr = date_range.split("-");
			query += date_range_arr[0] + "' AND '" + date_range_arr[1] + "'";

			// System.out.println(query);

			esql.executeQueryAndPrintResult(query);
		}catch(Exception e){
			System.err.println (e.getMessage());
		}
	}

	public static void ListAvailableAppointmentsOfDepartment(DBproject esql) {//6
		// For a department name and a specific date, find the list of available appointments of the department
// -- find department id's
// -- find doctors associated with department id's
// -- find list of available appointments under the doctors we just found
		try{
			String query = "SELECT DISTINCT * FROM Appointment A, (SELECT H.appt_id, H.doctor_id FROM has_appointment H, (SELECT D.doctor_ID FROM Doctor D, ";

			System.out.print("\tEnter Department Name: ");
			String dept_name = in.readLine();

			System.out.print("\tEnter Date (MM/DD/YYYY): ");
			String sel_date = in.readLine();

			String dept_ids = "(SELECT dept_ID FROM Department WHERE name = '" + dept_name + "') AS temp ";

			query += dept_ids + "WHERE temp.dept_ID = D.did) AS temp2) AS temp3 WHERE temp3.appt_id = A.appnt_ID AND A.status = 'AV' AND A.adate = TO_DATE('" + sel_date + "', 'MM/DD/YYYY')";

			// System.out.println(query);

			esql.executeQueryAndPrintResult(query);
		}catch(Exception e){
			System.err.println (e.getMessage());
		}
	}

	public static void ListStatusNumberOfAppointmentsPerDoctor(DBproject esql) {//7
		// Count number of different types of appointments per doctors and list them in descending order
		try{
			String query = "SELECT PA.doctor_id, COALESCE(PA.count,0) AS Past, COALESCE(AC.count,0) AS active, COALESCE(AV.count,0) AS Available, COALESCE(WL.count,0) AS Waitlisted FROM (SELECT H.doctor_id, COUNT(*) FROM has_appointment H, Appointment A WHERE H.appt_id = A.appnt_ID AND A.status = 'PA' GROUP BY H.doctor_id) AS PA LEFT JOIN (SELECT H.doctor_id, COUNT(*) FROM has_appointment H, Appointment A WHERE H.appt_id = A.appnt_ID AND A.status = 'AC' GROUP BY H.doctor_id) AS AC ON PA.doctor_id = AC.doctor_id LEFT JOIN (SELECT H.doctor_id, COUNT(*) FROM has_appointment H, Appointment A WHERE H.appt_id = A.appnt_ID AND A.status = 'AV' GROUP BY H.doctor_id) AS AV ON AC.doctor_id = AV.doctor_id LEFT JOIN (SELECT H.doctor_id, COUNT(*) FROM has_appointment H, Appointment A WHERE H.appt_id = A.appnt_ID AND A.status = 'WL' GROUP BY H.doctor_id) AS WL ON AV.doctor_id = WL.doctor_id GROUP BY PA.doctor_id, pa.count, ac.count, av.count, wl.count ORDER BY PA.doctor_id DESC, pa.count DESC, ac.count DESC, av.count DESC, wl.count DESC";
			esql.executeQueryAndPrintResult(query);
		}catch(Exception e){
			System.err.println (e.getMessage());
		}
	}

	
	public static void FindPatientsCountWithStatus(DBproject esql) {//8
		// Find how many patients per doctor there are with a given status (i.e. PA, AC, AV, WL) and list that number per doctor.
		try{
			String query = "";

			esql.executeQuery(query);
		}catch(Exception e){
			System.err.println (e.getMessage());
		}
	}
}