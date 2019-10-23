import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class Hw {

	public static void main(String[] args) {
		try{
		String queryTobeUsed =args[1];
		if("q1".equalsIgnoreCase(queryTobeUsed)){
			rangeQuery(getConnection(), args);
		}else if("q2".equalsIgnoreCase(queryTobeUsed)){
			pointQuery(getConnection(),args);
		}else if("q3".equalsIgnoreCase(queryTobeUsed)){
			findSquad(getConnection(),args);
		}else if("q4".equalsIgnoreCase(queryTobeUsed)){
			routeCoverage(getConnection(),args);
		}
		}catch(SQLException sqle)
		{
		System.out.println("SQL Exception while connecting to database "+ sqle.getMessage());	
		}catch(Exception e){
		}		
	}
	
	/*Route Coverage*/
	private static void routeCoverage(Connection jdbcConnection, String[] args) throws Exception {
		
	String routeSql = "select * from Route where UNIQUE_ROUTE_NUMBER=?";	
	ResultSet rsRoute = null;
	PreparedStatement pstmtRoute = getPreparedStmt(jdbcConnection, routeSql);
	try {
		pstmtRoute.setInt(1, Integer.parseInt(args[2]));
		
		rsRoute = pstmtRoute.executeQuery();
		if(!rsRoute.next()){
			System.out.println("Given Route does not exist");
			throw new Exception();
		}
		
	} catch (NumberFormatException | SQLException e) {
		System.out.println("Exception in  while accessing database: "+ e.getMessage());
	}finally {
		try {
			pstmtRoute.close();
		}catch(SQLException sqle) {
			System.out.println("Exception in finally block of "+sqle.getMessage());
		}
	}
		String routeCovSql = "select zn.ZONEID, zn.ZONENAME from zone zn, Route rt where st_intersects(zn.zone_polygon, rt.ROUTE) and rt.UNIQUE_ROUTE_NUMBER=? order by zn.ZONEID";
		ResultSet rs = null;
		PreparedStatement pstmt1 = getPreparedStmt(jdbcConnection, routeCovSql);
		try {
			 pstmt1.setInt(1, Integer.parseInt(args[2]));
			rs = pstmt1.executeQuery();
			if (rs.next()) {
				System.out.println("\nZoneID\tZoneName");
				System.out.println("----------------");
				do{
				System.out.println(rs.getInt(1)+"\t"+rs.getString(2));
				}while(rs.next());
			}
			else {
				System.out.println("Patrol route does not pass through any of the zone for given route number");
			}
				
		
		} catch (SQLException sqle) {
			System.out.println("Exception in routCoverage while accessing database: "+sqle.getMessage());
		}finally {
			try {
				pstmt1.close();
			}catch(SQLException sqle) {
				System.out.println("Exception in finally block of routeCoverage"+sqle.getMessage());
			}
		}
	}
	
	/*Find Squad*/
	private static void findSquad(Connection jdbcConnection, String[] args) throws Exception {
		
		String zoneSql="select zn.ZONENAME from zone zn where zn.CURRENT_SQUAD_NUMBER=?";
		ResultSet rs1 = null;
		PreparedStatement pstmt1 = getPreparedStmt(jdbcConnection, zoneSql);
		
		try {
			 pstmt1.setInt(1, Integer.parseInt(args[2]));
			rs1 = pstmt1.executeQuery();
			if(rs1.next()){
				do{
					System.out.println("\nSqaud "+args[2]+ " is now patrolling: "+rs1.getString(1));
				}while(rs1.next());
			}else{
			System.out.println("Squad "+args[2]+ " is not assigned to any zone");
				 throw new Exception();
			}
		} catch (NumberFormatException e) {
			System.out.println("Number format Exception in findSquad accessing database: "+e.getMessage());
                        throw new Exception();
		} 
		finally {
			try {
				pstmt1.close();
			}catch(SQLException sqle) {
				System.out.println("Exception in finally block of findSquad first query: "+sqle.getMessage());
			}
		}
		
	String squadSql = "select off.UNIQUE_BADGE_NUMBER, (case when st_contains(zn.ZONE_POLYGON,off.OFFICER_LOCATION) THEN 'IN' ELSE 'OUT' END) INRANGE,"+
	" off.OFFICER_NAME from Officer off, zone zn where off.SQUAD_NUMBER = zn.CURRENT_SQUAD_NUMBER and off.SQUAD_NUMBER=? order by off.UNIQUE_BADGE_NUMBER";
		ResultSet rs2 = null;
		PreparedStatement pstmt2 = getPreparedStmt(jdbcConnection, squadSql);
		try {
		pstmt2.setInt(1, Integer.parseInt(args[2]));
			
			rs2 = pstmt2.executeQuery();
			
			if(rs2.next()){
				System.out.println("BadgeNo\t\tRange\t\tOfficerName");
				System.out.println("-------------------------------------------");
				do{
					System.out.println(rs2.getInt(1)+"\t\t"+rs2.getString(2)+"\t\t"+rs2.getString(3));
				}while(rs2.next());
			}else{
				System.out.println("There are no officers assigned to the given squad");
				 throw new Exception();		 
														
			}
   
		} catch (NumberFormatException | SQLException e) {
			System.out.println(" Error while retrieving officer details"+e.getMessage());
						 
		}finally {
			try {
				rs2.close();
				pstmt2.close();
				jdbcConnection.close();
			}catch(SQLException sqle) {
				System.out.println("Exception in finally block of findSquad second query"+sqle.getMessage());
			}
		}
	}
	
	/*Point Query*/
	private static void pointQuery(Connection jdbcConnection, String[] args) throws Exception
	{
	String incidentSql = "select * from incident where unique_incident_id=?";	
	ResultSet rsIncident = null;
	PreparedStatement pstmtIncident = getPreparedStmt(jdbcConnection, incidentSql);
	try {
		pstmtIncident.setInt(1, Integer.parseInt(args[2]));
		
		rsIncident = pstmtIncident.executeQuery();
		if(!rsIncident.next()){
			System.out.println("Given Incident does not exist");
			throw new Exception();
		}
		
	} catch (NumberFormatException | SQLException e) {
		System.out.println("Exception in pointQuery while accessing database: "+ e.getMessage());
	}finally {
		try {
			pstmtIncident.close();
		}catch(SQLException sqle) {
			System.out.println("Exception in finally block of pointQuery"+sqle.getMessage());
		}
	}
	
	
	String pointSql = "select off.UNIQUE_BADGE_NUMBER, st_distance_sphere(inc.INCIDENT_LOCATION,off.OFFICER_LOCATION) distance, off.OFFICER_NAME from officer off, incident inc where st_distance_sphere(inc.INCIDENT_LOCATION,off.OFFICER_LOCATION)<? "+
				"and inc.UNIQUE_INCIDENT_ID =? order by distance";
		ResultSet rs = null;
		PreparedStatement pstmt = getPreparedStmt(jdbcConnection, pointSql);
		try {
			pstmt.setInt(1, Integer.parseInt(args[3]));
		pstmt.setInt(2, Integer.parseInt(args[2]));
			
			rs = pstmt.executeQuery();
			
			if(rs.next()){
				System.out.println("\nBadgeNo\t\tDistance\tOfficerName");
				System.out.println("-------------------------------------------");
				do{
					System.out.println(rs.getInt(1)+"\t\t"+Math.round(Double.parseDouble(rs.getString(2)))+"m"+"\t\t"+rs.getString(3));
				}while(rs.next());
			}else{
				System.out.println("No officer found within the given distance of incident");
			}
		
		} catch (NumberFormatException | SQLException e) {
			System.out.println("Exception in pointQuery while accessing database: "+ e.getMessage());
		}finally {
			try {
				pstmt.close();
			}catch(SQLException sqle) {
				System.out.println("Exception in finally block of pointQuery"+sqle.getMessage());
			}
		}
	}

	/*Range Query*/
	private static void rangeQuery(Connection jdbcConnection, String[] args) {
		
		String rangesql = "select inc.UNIQUE_INCIDENT_ID, ST_X(inc.INCIDENT_LOCATION), ST_Y(inc.INCIDENT_LOCATION), inc.INCIDENT_TYPE from Incident inc "
				+ "where st_contains(ST_GEOMETRYFromText(?),inc.INCIDENT_LOCATION) order by inc.UNIQUE_INCIDENT_ID";
		ResultSet rs = null;
		PreparedStatement pstmt = getPreparedStmt(jdbcConnection, rangesql);
		String startLong, startLat,vertexLong, vertexLat;
		startLong = args[3];
		startLat = args[4];
		StringBuilder queryPart1 = new StringBuilder("POLYGON((");
		for(int i=0, argCount=3; i< Integer.parseInt(args[2])*2;argCount+=2,i+=2){
			vertexLong = args[argCount];
			vertexLat = args[argCount+1];
			queryPart1.append(vertexLong+" "+vertexLat+", ");
		}
		queryPart1.append(startLong +" "+ startLat);
		queryPart1.append("))");
		try {
			pstmt.setString(1, queryPart1.toString());
			rs = pstmt.executeQuery();
			
			if(rs.next()){
				System.out.println("\nIncidentId\tLocation\t\t\tIncidentType");
				System.out.println("------------------------------------------------------------");
				do
				{
				System.out.println(rs.getInt(1)+"\t\t"+rs.getString(3)+", "+rs.getString(2)+"\t\t"+rs.getString(4));
				}while(rs.next());
			}
			else 
			{
				System.out.println("There are no incidents in a given polygon");
			}
		} catch (SQLException e) {
			System.out.println("Exception in rangeQuery while accessing database: "+e.getMessage());
		}finally {
			try {
				pstmt.close();
			}catch(SQLException sqle) {
				System.out.println("Exception in finally block of rangeQuery"+sqle.getMessage());
			}
		}
	}
	
	private static PreparedStatement getPreparedStmt(Connection jdbcConnection, String sql) {
		PreparedStatement pstmt=null;
		try {
			pstmt = jdbcConnection.prepareStatement(sql);
		} catch (SQLException sqle) {
			System.out.println("Exception while getting prepared statement : "+sqle.getMessage());
		}
		return pstmt;
	}

	private static Connection getConnection() throws SQLException {
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
        String password =  br.readLine();	
		if ( null==password|| password.equals(""))
		{
			System.out.println("Password is missing in db.properties file");
				throw new SQLException(":Exiting...!");
		}
		String url = "jdbc:mysql://" + host_name + ":" + port + "/" + db_name ;		
		jdbcConnection = DriverManager.getConnection(url,username,password);
		}catch (IOException e) {
			System.out.println("IO Exception in getting database jdbc connection method, while reading db.properties : "+e.getMessage());
		}
		catch(ClassNotFoundException cnfe){
			System.out.println("Exception in database driver: "+cnfe.getMessage());
		} 
		return jdbcConnection;
	}

}
