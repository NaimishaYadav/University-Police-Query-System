import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class Populate {

	public static void main(String[] args) {
		Connection jdbcConnection = null;
		try{
		jdbcConnection = getConnection();
		loadZones(jdbcConnection);
		loadOfficers(jdbcConnection);
		loadRoutes(jdbcConnection);
		loadIncidents(jdbcConnection);
		}catch(SQLException sqle)
		{
		System.out.println("SQL Exception while connecting to database "+ sqle.getMessage());	
		}
	}

	
	/*Loading Incident Data */
	private static void loadIncidents(Connection jdbcConnection) {
		try {
			Statement stmt= jdbcConnection.createStatement();
			stmt.execute("delete from Incident where 1=1");
			//System.out.println("Data deleted from before loading if exists");
		} catch (SQLException sqle) {
			System.out.println("SQLException while deleting past data in loadIncidents: "+sqle.getMessage());
		}
		
		String zonesql = "insert into INCIDENT values(?, ?, ST_GeomFromText(?))";
		ResultSet rs = null;
		PreparedStatement pstmt = getPreparedStmt(jdbcConnection, zonesql);
		File file = new File("incident.txt");
		BufferedReader br = fileBufferlines(file);

		String st = null;
		String incident = null;
		String tokens[] = null;
		try {
			while ((st = br.readLine()) != null) {
				tokens = st.split(",", -1);
				pstmt.setInt(1, Integer.parseInt(tokens[0]));
				incident=tokens[1];
				if(incident.contains("\""))
					pstmt.setString(2, incident.substring(incident.indexOf("\"")+1, incident.lastIndexOf("\"")));
				else
					pstmt.setString(2,incident);
				StringBuilder locationCoordinates = new StringBuilder("POINT(");
				locationCoordinates.append(tokens[2].trim()+" ");
				locationCoordinates.append(tokens[3]);
				locationCoordinates.append(")");
				pstmt.setString(3, locationCoordinates.toString());
				pstmt.executeUpdate();
				
			}
		} catch (IOException e) {
			System.out.println("IOException in loadIncidents: "+e.getMessage());
		} catch (NumberFormatException nfe) {
			System.out.println("NumberFormatException in loadIncidents: "+nfe.getMessage());
		} catch (SQLException sqle) {
			System.out.println("SQLException in loadIncidents: "+sqle.getMessage());
		}finally {
			try {
				pstmt.close();
			}catch(SQLException sqle) {
				System.out.println("Exception in finally block of loadIncidents"+sqle.getMessage());
			}
		}
		System.out.println("Incident Data loaded");
	}

	/*Loading Route Data*/
	private static void loadRoutes(Connection jdbcConnection) {
		try {
			Statement stmt= jdbcConnection.createStatement();
			stmt.execute("delete from Route where 1=1");
		} catch (SQLException e1) {
			System.out.println("SQLException in loadRoutes while deleting past data");
		}
		
		String zonesql = "insert into Route values(?, ?, ST_GeomFromText(?))";
		ResultSet rs = null;
		PreparedStatement pstmt = getPreparedStmt(jdbcConnection, zonesql);
		File file = new File("route.txt");
		BufferedReader br = fileBufferlines(file);
		String st = null;
		String tokens[] = null;
		try {
			while ((st = br.readLine()) != null) {
				tokens = st.split(",", -1);
				pstmt.setInt(1, Integer.parseInt(tokens[0]));
				pstmt.setString(2, tokens[1]);
				StringBuilder routeCoordinates = new StringBuilder("LINESTRING(");
				for(int count=2;count<tokens.length;count++){
					routeCoordinates.append(tokens[count]);
					if(count%2==0){
						routeCoordinates.append(" ");
					}else{
						if(count>2 && tokens.length!= count+1  )
						routeCoordinates.append(", ");
					}
				}
				routeCoordinates.append(")");
				pstmt.setString(3, routeCoordinates.toString());
				pstmt.executeUpdate();
				
			}
		} catch (IOException e) {
			System.out.println("IOException in loadRoutes: "+e.getMessage());
		} catch (NumberFormatException nfe) {
			System.out.println("NumberFormatException in loadRoutes: "+nfe.getMessage());
		} catch (SQLException sqle) {
			System.out.println("SQLException in loadRoutes: "+sqle.getMessage());
		}finally {
			try {
				pstmt.close();
			}catch(SQLException sqle) {
				System.out.println("Exception in finally block of loadRoutes"+sqle.getMessage());
			}
		}
		
		System.out.println("Route Data loaded");
	}

	/*Loading Officers Data*/
	private static void loadOfficers(Connection jdbcConnection) throws SQLException {
		try {
			Statement stmt= jdbcConnection.createStatement();
			stmt.execute("delete from Officer where 1=1");
		} catch (SQLException sqle) {
			System.out.println("SQLException in loadOfficers while deleting past data: "+sqle.getMessage());
			throw sqle;
		} 
		
		String zonesql = "insert into Officer values(?, ?, ?, ST_GeomFromText(?))";
		ResultSet rs = null;
		PreparedStatement pstmt = getPreparedStmt(jdbcConnection, zonesql);
		File file = new File("officer.txt");
		BufferedReader br = fileBufferlines(file);
		String officerName=null;
		String st = null;
		String tokens[] = null;
		try {
			while ((st = br.readLine()) != null) {
				tokens = st.split(",", -1);
				pstmt.setInt(1, Integer.parseInt(tokens[0]));
				officerName=tokens[1];
				if(officerName.contains("\""))
					pstmt.setString(2, officerName.substring(officerName.indexOf("\"")+1, officerName.lastIndexOf("\"")));
				else
					pstmt.setString(2,officerName);
				pstmt.setString(3, tokens[2]);
				StringBuilder locationCoordinates = new StringBuilder("POINT(");
				locationCoordinates.append(tokens[3].trim()+" ");
				locationCoordinates.append(tokens[4]);
				locationCoordinates.append(")");
				pstmt.setString(4, locationCoordinates.toString());
				pstmt.executeUpdate();
				
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NumberFormatException nfe) {
			nfe.printStackTrace();
		} catch (SQLException sqle) {
			sqle.printStackTrace();
		}finally {
			try {
				pstmt.close();
			}catch(SQLException sqle) {
				System.out.println("Exception in finally block of loadOfficers"+sqle.getMessage());
			}
		}
		System.out.println("Officer Data loaded");
	}

	/*Loading Zone Data*/
	private static void loadZones(Connection jdbcConnection) {
		
		try {
			Statement stmt= jdbcConnection.createStatement();
			stmt.execute("delete from zone where 1=1");
			//Data deleted from Zone before loading if exists
		} catch (SQLException e1) {
			System.out.println("SQLException while deleting past data in loadZones");
		}
		String zonesql = "insert into zone values(?, ?, ?, ?, ST_GeomFromText(?))";
		ResultSet rs = null;
		PreparedStatement pstmt = getPreparedStmt(jdbcConnection, zonesql);
		File file = new File("zone.txt");
		BufferedReader br = fileBufferlines(file);

		String zoneName = null;
		String st = null;
		String tokens[] = null;
		
		String startLong, startLat, vertexLong, vertexLat;
		try {
			while ((st = br.readLine()) != null) {
				tokens = st.split(",", -1);
				pstmt.setInt(1, Integer.parseInt(tokens[0]));
				zoneName=tokens[1];
				if(zoneName.contains("\""))
					pstmt.setString(2, zoneName.substring(zoneName.indexOf("\"")+1, zoneName.lastIndexOf("\"")));
				else
					pstmt.setString(2,zoneName);
				pstmt.setString(3, tokens[2]);
				pstmt.setString(4, tokens[3]);
				startLong = tokens[4];
				startLat = tokens[5];
				StringBuilder zoneCoordinates = new StringBuilder("POLYGON((");
				for(int count=4;count<tokens.length;count+=2){
					vertexLong = tokens[count];
					vertexLat = tokens[count+1];
					zoneCoordinates.append(vertexLong+" "+vertexLat+", ");
				}
				zoneCoordinates.append(startLong+" ");
				zoneCoordinates.append(startLat);
				zoneCoordinates.append("))");
				pstmt.setString(5, zoneCoordinates.toString());
				pstmt.executeUpdate();				
			}
		} catch (IOException e) {
			System.out.println("IOException while loadZone: "+e.getMessage());
		} catch (NumberFormatException nfe) {
			System.out.println("NumberFormatException while loadZone: "+nfe.getMessage());
		} catch (SQLException sqle) {
			System.out.println("SQLException while loadZone: "+sqle.getMessage());
		}finally {
			try {
				pstmt.close();
			}catch(SQLException sqle) {
				System.out.println("Exception in finally block of loadZones"+sqle.getMessage());
			}
		}
		System.out.println("Zone Data loaded");
	}

	private static BufferedReader fileBufferlines(File file) {
		BufferedReader br=null;
		
		try {
			br = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		return br;
	}

	private static PreparedStatement getPreparedStmt(Connection jdbcConnection, String sql) {
		PreparedStatement pstmt=null;
		try {
			pstmt = jdbcConnection.prepareStatement(sql);
		} catch (SQLException sqle) {
			sqle.printStackTrace();
		}
		return pstmt;
	}
	
	/*Connecting to database*/
	private static Connection getConnection() throws  SQLException {
	Connection jdbcConnection=null;
	BufferedReader br=null;
	File file = new File("db.properties");
		try{
        br = new BufferedReader(new FileReader(file));
		}catch (FileNotFoundException e) {
		e.printStackTrace();
		}
		String line = null;       
		try{
		Class.forName("com.mysql.cj.jdbc.Driver");
		String host_name = br.readLine();
		if (host_name.equals("") || null==host_name)
		{
			System.out.println("Host name is missing in db.properties file");
			throw new SQLException(":Exiting....!");
		}
        String port = br.readLine();
		if (port.equals("") || null==port)
		{
			System.out.println("Port is missing in db.properties file");
			throw new SQLException(":Exiting...!");
		}
		String db_name =  br.readLine();
		if (db_name.equals("") || null==db_name)
		{
			System.out.println("Database name is missing in db.properties file");
			throw new SQLException(":Exiting...!");
		}
        String username =  br.readLine(); 
		if (username.equals("") || null==username)
		{
			System.out.println("User name is missing in db.properties file");
				throw new SQLException(":Exiting...!");
		}
			
        String  password =  br.readLine();
		if ( null==password|| password.equals(""))
		{
			System.out.println("Password is missing in db.properties file");
				throw new SQLException(":Exiting...!");
		}	
		
		String url = "jdbc:mysql://" + host_name + ":" + port + "/" + db_name ;  		
		jdbcConnection = DriverManager.getConnection(url,username,password);
		}catch (IOException e) {
			e.printStackTrace();
		}
		catch(ClassNotFoundException cnfe){
			System.out.println("ClassNotFoundException while getConnection: "+ cnfe.getMessage());
		}		
		return jdbcConnection;
	}

}
