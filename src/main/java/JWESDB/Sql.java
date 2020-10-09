package JWESDB;
  
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
  
public class Sql {
  
  static final String JDBC_DRIVER = "com.mysql.jdbc.Driver"; 
  String addr = "jdbc:mysql://"; Connection mysqlConn = null;
  static int startingId = 0;
  
  Sql(String db_address, int port, String db, String user_name, String password) throws SQLException { 
	  addr += db_address + ":" + port+"/" + db +"?useSSL=false";
	  mysqlConn = DriverManager.getConnection(addr, user_name, password);
  }
  
  public void insertInDB(List<VCF_Variant> variants, HashMap<String, String> infoMap, HashMap<String, String> infoMapType, HashMap<String, String> formatMap, HashMap<String, String> formatMapType, boolean createTable) throws SQLException { 
	  
	  if(createTable) {
		  createTable();
		  createVariantTable();
		  createInfoTable(infoMap, infoMapType);
		  createSampleTable(formatMap, formatMapType);
	  }else {
		  startingId = getAutoIncrement("WES_VARIANT");
		  System.out.println(startingId);
		  insertVariantEntries(variants);
		  insertInfoEntries(variants, infoMap, infoMapType);
		  insertSampleEntries(variants, formatMap, formatMapType);
	  }
  }
  
  void createTable() throws SQLException {
	  String query = "CREATE SCHEMA IF NOT EXISTS `JWESDB` DEFAULT CHARACTER SET utf8 ;\n";

	  Statement stmt = mysqlConn.createStatement();
	  stmt.executeUpdate(query) ;
  }
  
  void createVariantTable() throws SQLException {
	  String query = "CREATE TABLE IF NOT EXISTS `JWESDB`.`WES_VARIANT` (\n" + 
	  		"  `WESV_Id` INT NOT NULL AUTO_INCREMENT,\n" + 
	  		"  `WESV_Crom_Number` VARCHAR(100) NULL DEFAULT 'NA',\n" + 
	  		"  `WESV_Crom_Position` INT NULL DEFAULT 0,\n" + 
	  		"  `WESV_Identifier` VARCHAR(45) NULL DEFAULT 'NA',\n" + 
	  		"  `WESV_Ref_Base` VARCHAR(250) NULL DEFAULT 'NA',\n" + 
	  		"  `WESV_Alt_Base` VARCHAR(250) NULL DEFAULT 'NA',\n" + 
	  		"  `WESV_Quality` INT NULL DEFAULT 0,\n" + 
	  		"  `WESV_Filter` VARCHAR(100) NULL DEFAULT 'NA',\n" + 
	  		"  `WESV_Filter_Desc` VARCHAR(250) NULL DEFAULT 'NA',\n" + 
	  		"  `WESV_Datetime` DATETIME NULL DEFAULT CURRENT_TIMESTAMP,\n" + 
	  		"  `WESV_Archived` VARCHAR(45) NULL DEFAULT 'false',\n" + 
	  		"  PRIMARY KEY (`WESV_Id`),\n" + 
	  		"  INDEX `pos` (`WESV_Crom_Position` ASC),\n" + 
	  		"  INDEX `rsid` (`WESV_Identifier` ASC))\n" + 
	  		"ENGINE = InnoDB\n" + 
	  		"AUTO_INCREMENT = 1\n" + 
	  		"DEFAULT CHARACTER SET = utf8;";
	  
	  Statement stmt = mysqlConn.createStatement();
	  stmt.executeUpdate(query) ;
  }
  
  void insertVariantEntries(List<VCF_Variant> variants) throws SQLException {
  
	  String query = "INSERT INTO `JWESDB`.`WES_VARIANT`\n" 
		  + "(`WESV_Crom_Number`,\n" 
		  + "`WESV_Crom_Position`,\n" 
		  + "`WESV_Identifier`,\n"
		  + "`WESV_Ref_Base`,\n" 
		  + "`WESV_Alt_Base`,\n" 
		  + "`WESV_Quality`,\n" 
		  + "`WESV_Filter`,\n" 
		  + "`WESV_Filter_Desc`,\n" 
		  + "`WESV_Archived`)" 
		  + " values (?, ?, ?, ?, ?, ?, ?, ?, ?)";
  
	  PreparedStatement preparedStmt = null;
	  
	  try { 
		  	preparedStmt = mysqlConn.prepareStatement(query);
		  	mysqlConn.setAutoCommit(false);
		  for(VCF_Variant variant : variants) {
			  preparedStmt.setString(1, variant.getChrom_number()); 
			  preparedStmt.setLong(2, variant.getChrom_position()); 
			  preparedStmt.setString(3, variant.getChrom_id()); 
			  preparedStmt.setString(4, variant.getRef_base());
			  preparedStmt.setString(5, variant.getAlt_base()); 
			  preparedStmt.setFloat(6,variant.getQuality()); 
			  preparedStmt.setString(7, variant.getFilter());
			  preparedStmt.setString(8, variant.getDescription());
			  preparedStmt.setString(9, "false");
			  preparedStmt.addBatch();  		  
		  }
		 
		  //preparedStmt.execute(); 
		  preparedStmt.executeBatch();
		  mysqlConn.commit();
		  
	  } catch (SQLException se) { 
		  se.printStackTrace();
		  throw se; 
	  } 
	  finally { 
		  preparedStmt.close(); 
	  } 
  }
  
  void createInfoTable(HashMap<String, String> infoMap, HashMap<String, String> infoMapType) throws SQLException {
  
	  String infoCreateQuery = "CREATE TABLE IF NOT EXISTS `JWESDB`.`WES_INFO` (\n"
			  + "  `WESI_Id` INT NOT NULL AUTO_INCREMENT,\n";
	  
	  Set<String> keys = infoMap.keySet();
	  
	  for(String key : keys) {
		  switch(infoMapType.get(key)) {
  		case "String":
  			infoCreateQuery += "`WESI_" + key + "` VARCHAR(100) NULL DEFAULT 'NA',\n"
  					+ "`WESI_" + key + "_Description` VARCHAR(250) NULL DEFAULT 'NA',\n";
  			break;
  		case "Float":
  			infoCreateQuery += "`WESI_" + key + "` FLOAT NULL DEFAULT 0,\n"
  				  + "`WESI_" + key + "_Description` VARCHAR(250) NULL DEFAULT 'NA',\n";
  			break;
  		case "Integer":
  			infoCreateQuery += "`WESI_" + key + "` INT NULL DEFAULT 0, \n"
  				  + "`WESI_" + key + "_Description` VARCHAR(250) NULL DEFAULT 'NA',\n";
  			break;
  		case "Flag":
  			infoCreateQuery += "`WESI_" + key + "` TINYINT NULL DEFAULT 0,\n"
  				  + "`WESI_" + key + "_Description` VARCHAR(250) NULL DEFAULT 'NA',\n";
  			break;
		  }
	  }

	  infoCreateQuery += "`WESI_WESV_Id` INT NULL DEFAULT 0,\n" + 
	  		"  `WESI_Datetime` DATETIME NULL DEFAULT CURRENT_TIMESTAMP,\n" + 
	  		"  `WESI_Archived` VARCHAR(45) NULL DEFAULT 'false',\n" + 
	  		"  PRIMARY KEY (`WESI_Id`))\n" + 
	  		"ENGINE = InnoDB\n" + 
	  		"AUTO_INCREMENT = 9\n" + 
	  		"DEFAULT CHARACTER SET = utf8;";

	  Statement stmt = mysqlConn.createStatement();
	  stmt.executeUpdate(infoCreateQuery) ;
  }
  
  void insertInfoEntries(List<VCF_Variant> variants, HashMap<String, String> infoMap, HashMap<String, String> infoMapType) throws SQLException {
	  
	  String infoInsertQuery = "INSERT INTO `JWESDB`.`WES_INFO` ( \n";

	  Set<String> keys = infoMap.keySet();
	  
	  int infoCounter = 0;
	  
	  for(String key : keys) {
		  switch(infoMapType.get(key)) {
  		case "String":
  			infoInsertQuery += "`WESI_" + key + "`, \n"
  					+ "`WESI_" + key + "_Description` ,\n";
  			infoCounter += 2;
  			break;
  		case "Float":
  			infoInsertQuery += "`WESI_" + key + "`, \n"
  					+ "`WESI_" + key + "_Description` ,\n";
  			infoCounter += 2;
  			break;
  		case "Integer":
  			infoInsertQuery += "`WESI_" + key + "`, \n"
  					+ "`WESI_" + key + "_Description` ,\n";
  			infoCounter += 2;
  			break;
  		case "Flag":
  			infoInsertQuery += "`WESI_" + key + "`, \n"
  					+ "`WESI_" + key + "_Description` ,\n";
  			infoCounter += 2;
  			break;
		  }
	  }
	  
	  infoInsertQuery += "`WESI_WESV_Id`) \n";
	  infoCounter++;
	  
	  infoInsertQuery += " values (";
	  
	  for(int i =0; i < infoCounter; i++) {
		  if(i + 1 == infoCounter)
			  infoInsertQuery += "?)";
		  else
			  infoInsertQuery += "?,";
	  }
	  
	  PreparedStatement preparedStmt = null;
	  	  
	  int counter = 1;
	  int idCounter = startingId;
	  
	  try { 
		  	preparedStmt = mysqlConn.prepareStatement(infoInsertQuery);
		  	mysqlConn.setAutoCommit(false);
		  	  for(VCF_Variant v : variants)   {
		  		  VCF_Info info = v.getInfo();
				  for(String key : keys) {
					    
						switch(infoMapType.get(key)) {
				  		case "String":
					  		preparedStmt.setString(counter, info.getInfoString(key));
					  		counter++;
					  		preparedStmt.setString(counter, infoMap.get(key));
					  		counter++;
				  			break;
				  		case "Float":
					  		preparedStmt.setFloat(counter, info.getInfoFloat(key));
					  		counter++;
					  		preparedStmt.setString(counter, infoMap.get(key));
					  		counter++;
				  			break;
				  		case "Integer":
							preparedStmt.setInt(counter, info.getInfoInteger(key));
							counter++;
					  		preparedStmt.setString(counter, infoMap.get(key));
							counter++;
				  			break;
				  		case "Flag":
							preparedStmt.setBoolean(counter, info.getInfoFlag(key));
							counter++;
					  		preparedStmt.setString(counter, infoMap.get(key));
							counter++;
				  			break;
						  }
				  }
		  		  preparedStmt.setInt(counter, idCounter);
				  counter = 1;
				  idCounter++;
				  preparedStmt.addBatch();
		  	  }
		  	  preparedStmt.executeBatch();
			  mysqlConn.commit();
		  
	  } catch (SQLException se) { 
		  se.printStackTrace();
		  throw se; 
	  } 
	  finally { 
		  preparedStmt.close(); 
	  }
  }
  
  void createSampleTable(HashMap<String, String> formatMap, HashMap<String, String> formatMapType) throws SQLException {
	  
	  String query = "CREATE TABLE IF NOT EXISTS `JWESDB`.`WES_SAMPLE` ( \n"
			  + "  `WESS_Id` INT NOT NULL AUTO_INCREMENT,\n";
	  
	  Set<String> keys = formatMap.keySet();

	  for(String key : keys) {
		  switch(formatMapType.get(key)) {
  		case "String":
  			query += "`WESS_" + key + "` VARCHAR(100) NULL DEFAULT 'NA',\n"
  				  + "`WESS_" + key + "_Description` VARCHAR(250) NULL DEFAULT 'NA',\n";
  			break;
  		case "Float":
  			query += "`WESS_" + key + "` FLOAT NULL DEFAULT 0,\n"
  				  + "`WESS_" + key + "_Description` VARCHAR(250) NULL DEFAULT 'NA',\n";
  			break;
  		case "Integer":
  			query += "`WESS_" + key + "` INT NULL DEFAULT 0, \n"
  				  + "`WESS_" + key + "_Description` VARCHAR(250) NULL DEFAULT 'NA',\n";
  			break;
  		case "Flag":
  			query += "`WESS_" + key + "` TINYINT NULL DEFAULT 0,\n"
  				  + "`WESS_" + key + "_Description` VARCHAR(250) NULL DEFAULT 'NA',\n";
  			break;
		  }
	  }

	  query += "`WESS_WESV_Id` INT NULL DEFAULT 0,\n" + 
	  		"  `WESS_Datetime` DATETIME NULL DEFAULT CURRENT_TIMESTAMP,\n" + 
	  		"  `WESS_Archived` VARCHAR(45) NULL DEFAULT 'false',\n" + 
	  		"  PRIMARY KEY (`WESS_Id`))\n" + 
	  		"ENGINE = InnoDB\n" + 
	  		"AUTO_INCREMENT = 5\n" + 
	  		"DEFAULT CHARACTER SET = utf8;";

	  Statement stmt = mysqlConn.createStatement();
	  stmt.executeUpdate(query) ;
  } 
  
  void insertSampleEntries(List<VCF_Variant> variants, HashMap<String, String> sampleMap, HashMap<String, String> sampleMapType) throws SQLException {
	  String sampleInsertQuery = "INSERT INTO `JWESDB`.`WES_SAMPLE` ( \n";

	  Set<String> keys = sampleMap.keySet();
	  
	  int sampleCounter = 0;
	  
	  for(String key : keys) {
		  switch(sampleMapType.get(key)) {
  		case "String":
  			sampleInsertQuery += "`WESS_" + key + "`, \n"
  					+ "`WESS_" + key + "_Description` ,\n";
  			sampleCounter += 2;
  			break;
  		case "Float":
  			sampleInsertQuery += "`WESS_" + key + "`, \n"
  					+ "`WESS_" + key + "_Description` ,\n";
  			sampleCounter += 2;
  			break;
  		case "Integer":
  			sampleInsertQuery += "`WESS_" + key + "`, \n"
  					+ "`WESS_" + key + "_Description` ,\n";
  			sampleCounter += 2;
  			break;
  		case "Flag":
  			sampleInsertQuery += "`WESS_" + key + "`, \n"
  					+ "`WESS_" + key + "_Description` ,\n";
  			sampleCounter += 2;
  			break;
		  }
	  }
	  
	  sampleInsertQuery += "`WESS_WESV_Id`) \n";
	  sampleCounter++;
	  
	  sampleInsertQuery += " values (";
	  
	  for(int i =0; i < sampleCounter; i++) {
		  if(i + 1 == sampleCounter)
			  sampleInsertQuery += "?)";
		  else
			  sampleInsertQuery += "?,";
	  }
	  
	  PreparedStatement preparedStmt = null;
	  	  
	  int counter = 1;
	  int idCounter = startingId;
	  
	  try { 
		  	preparedStmt = mysqlConn.prepareStatement(sampleInsertQuery);
		  	mysqlConn.setAutoCommit(false);
		  	
		  	for(VCF_Variant v : variants)   {
		  		List<VCF_Sample> l = v.getFormat();
		  		System.out.println(l.size());
		  		for(VCF_Sample sample : l) {
  
				  for(String key : keys) {
					  switch(sampleMapType.get(key)) {
			  		case "String":
				  		preparedStmt.setString(counter, sample.getSampleString(key));
				  		counter++;
				  		preparedStmt.setString(counter, sampleMap.get(key));
				  		counter++;
			  			break;
			  		case "Float":
				  		preparedStmt.setFloat(counter, sample.getSampleFloat(key));
				  		counter++;
				  		preparedStmt.setString(counter, sampleMap.get(key));
				  		counter++;
			  			break;
			  		case "Integer":
						preparedStmt.setInt(counter, sample.getSampleInteger(key));
						counter++;
				  		preparedStmt.setString(counter, sampleMap.get(key));
						counter++;
			  			break;
			  		case "Flag":
						preparedStmt.setBoolean(counter, sample.getSampleFlag(key));
						counter++;
				  		preparedStmt.setString(counter, sampleMap.get(key));
						counter++;
			  			break;
					  }
				  }
				  preparedStmt.setInt(counter, idCounter);
				  counter = 1;
				  idCounter++;
				  preparedStmt.addBatch();
		  		}
		  	} 
			  preparedStmt.executeBatch();
			  mysqlConn.commit();
		  
		  
	  } catch (SQLException se) { 
		  se.printStackTrace();
		  throw se; 
	  } 
	  finally { 
		  preparedStmt.close(); 
	  }
  }
  
  int getAutoIncrement(String table) throws SQLException {
	  String query = "SELECT AUTO_INCREMENT FROM information_schema.tables WHERE table_name = '"+ table +"' AND table_schema = DATABASE( );\n";
	  Statement stmt = mysqlConn.createStatement();
	  ResultSet rs = stmt.executeQuery(query) ;
	  int r = 0;
	  while (rs.next()) {
		  r = Integer.valueOf(rs.getString(1));
	  }
	      
	  return r;
  }
}
 