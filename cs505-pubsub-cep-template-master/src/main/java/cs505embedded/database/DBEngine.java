package cs505embedded.database;

import com.google.gson.reflect.TypeToken;
import org.apache.commons.dbcp2.*;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPool;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Type;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;


public class DBEngine {

    private DataSource ds;

    public DBEngine() {

        try {
            //Name of database
            String databaseName = "myDatabase";

            //Driver needs to be identified in order to load the namespace in the JVM
            String dbDriver = "org.apache.derby.jdbc.EmbeddedDriver";
            Class.forName(dbDriver).newInstance();

            //Connection string pointing to a local file location
            String dbConnectionString = "jdbc:derby:memory:" + databaseName + ";create=true";
            ds = setupDataSource(dbConnectionString);

            /*
            if(!databaseExist(databaseName)) {
                System.out.println("No database, creating " + databaseName);
                initDB();
            } else {
                System.out.println("Database found, removing " + databaseName);
                delete(Paths.get(databaseName).toFile());
                System.out.println("Creating " + databaseName);
                initDB();
            }
             */

            initDB();


        }

        catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public static DataSource setupDataSource(String connectURI) {
        //
        // First, we'll create a ConnectionFactory that the
        // pool will use to create Connections.
        // We'll use the DriverManagerConnectionFactory,
        // using the connect string passed in the command line
        // arguments.
        //
        ConnectionFactory connectionFactory = null;
        connectionFactory = new DriverManagerConnectionFactory(connectURI, null);


        //
        // Next we'll create the PoolableConnectionFactory, which wraps
        // the "real" Connections created by the ConnectionFactory with
        // the classes that implement the pooling functionality.
        //
        PoolableConnectionFactory poolableConnectionFactory =
                new PoolableConnectionFactory(connectionFactory, null);

        //
        // Now we'll need a ObjectPool that serves as the
        // actual pool of connections.
        //
        // We'll use a GenericObjectPool instance, although
        // any ObjectPool implementation will suffice.
        //
        ObjectPool<PoolableConnection> connectionPool =
                new GenericObjectPool<>(poolableConnectionFactory);

        // Set the factory's pool property to the owning pool
        poolableConnectionFactory.setPool(connectionPool);

        //
        // Finally, we create the PoolingDriver itself,
        // passing in the object pool we created.
        //
        PoolingDataSource<PoolableConnection> dataSource =
                new PoolingDataSource<>(connectionPool);

        return dataSource;
    }

    public void initDB() {
        
        //Create hospital table
        String makeHosp = "CREATE TABLE hospital" +
        					"(" +
        					" id int, " + 
        					"name varchar(255), " +
        					"address varchar(255), " + 
        					"city varchar(255), " + 
        					"state varchar(2), " +
        					"zip varchar(5), " +
        					"type varchar(255), " +
        					"beds int, " +
        					"beds_avail int, " +
        					"county varchar(255), " +
        					"countyfips varchar(255), " +
        					"country varchar(3), " +
        					"latitude varchar(255), " +
        					"longitude varchar(255), " +
        					"naics_code varchar(255), " +
        					"webiste varchar(255), " +
        					"owner varchar(255), " +
        					"trauma varchar(255), " +
        					"helipad varchar(1)" +
        					")";
        
        String makeZip = "CREATE TABLE zip" +
        					"(" + 
        					"zipcode varchar(5), "+
        					"name varchar(255), " +
        					"city varchar(255), " +
        					"state varchar(2), " +
        					"county varchar(255)" +
        					")";
        
        String makeDistances = "CREATE TABLE distances" +
        						"(" +
        						"zip_from varchar(5), " +
        						"zip_to varchar(5), " +
        						"distance decimal(10, 3)" +
        						")";
        
        String makePatients = "CREATE TABLE patients" + 
        						"(" +
        						"first_name varchar(255), " +
        						"last_name varchar(255), " +
        						"mrn varchar(255), " +
        						"zipcode varchar(5), " +
        						"patient_status_code int, " +
        						"location int" +
        						")";

        try {
            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(makeHosp);
                }
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(makeZip);
                }
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(makeDistances);
                }
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(makePatients);
                }
                
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    }
    public void insertTestData() {
    	String hospTest = "INSERT INTO hospital" +
				" " +
				"11740202, " + 
				"\"UNIVERSITY OF LOUISVILLE HOSPITAL\", " + 
				"\"530 S JACKSON ST\", " + 
				"\"LOUISVILLE\"" + 
				"\"KY\", " +
				"\"40202\", " + 
				"\"GENERAL ACUTE CARE\", " + 
				"404, " + "404, " +
				"\"JEFFERSON\", " + 
				"\"21111\", " + 
				"\"USA\", " + 
				"\"38.24832296\", " + 
				"\"-85.74433596\", " + 
				"\"622110\", " + 
				"\"http://www.ulh.org\", " + 
				"\"NON-PROFIT\", " + 
				"\"LEVEL I\", " + 
				"\"Y\"" + ")";
			
		System.out.println(hospTest);
		try(Connection conn = ds.getConnection()) {
		try (Statement stmt = conn.createStatement()) {
		stmt.executeUpdate(hospTest);
		}
		} catch(Exception ex) {
		ex.printStackTrace();
		}
	
		String zipTest = "INSERT INTO zip VALUES(\"42201\", \"ABERDEEN\", \"KY\", \"ABERDEEN\", \"KY\", \"BUTLER\")";
		
		System.out.println(zipTest);
		try(Connection conn = ds.getConnection()) {
			try (Statement stmt = conn.createStatement()) {
					stmt.executeUpdate(zipTest);
			}
	
		} catch(Exception ex) {
			ex.printStackTrace();
			}
		zipTest = "INSERT INTO zip VALUES(\"42202\", \"ADAIRVILLE\", \"KY\", \"ADAIRVILLE\", \"KY\", \"LOGAN\")"; 
		try(Connection conn = ds.getConnection()) {
			try (Statement stmt = conn.createStatement()) {
					stmt.executeUpdate(zipTest);
			}
	
		} catch(Exception ex) {
			ex.printStackTrace();
			}
		
		String distancesTest = "INSERT INTO distances VALUES(\"42201\", \"42202\", 1)";
		try(Connection conn = ds.getConnection()) {
			try (Statement stmt = conn.createStatement()) {
					stmt.executeUpdate(distancesTest);
			}
	
		} catch(Exception ex) {
			ex.printStackTrace();
			}
		
		distancesTest = "INSERT INTO distances VALUES(\"42202\", \"42201\", 1)";
		try(Connection conn = ds.getConnection()) {
			try (Statement stmt = conn.createStatement()) {
					stmt.executeUpdate(distancesTest);
			}
	
		} catch(Exception ex) {
			ex.printStackTrace();
			}
		
		String patientTest = "INSERT INTO patients VALUES(\"John\", \"Doe\", \"asfagwg\", \"402202\", 0, 0)";
		try(Connection conn = ds.getConnection()) {
			try (Statement stmt = conn.createStatement()) {
					stmt.executeUpdate(patientTest);
			}
	
		} catch(Exception ex) {
			ex.printStackTrace();
			}
	}
    
    	
    	
    public void readDataIntoDB() throws FileNotFoundException {
    	String hospFileName = "/cs505-pubsub-cep-template-master/hospitals.csv";
    	String zipFileName = "/home/mssz222/Documents/cs505-final/cs505-pubsub-cep-template-master/target/kyzipdetails.csv";
    	String zipDistFileName = "/home/mssz222/Documents/cs505-final/cs505-pubsub-cep-template-master/target/kyzipdistance.csv";
    	
    	File hospFile = new File(hospFileName);
    	System.out.println(hospFile.getAbsolutePath());
    	//parse hospitals file
    	Scanner hospScanner = new Scanner(new File(hospFileName));
    	hospScanner.useDelimiter(",");
    	hospScanner.nextLine();
    	System.out.println("Reading hospital data");
    	while(hospScanner.hasNextLine()) {
    		String newLine = hospScanner.nextLine().strip();;
    		String splitLine[] = newLine.split(",");
    		int id = Integer.parseInt(splitLine[0]);
    		String name = splitLine[1];
    		String address = splitLine[2];
    		String city = splitLine[3];
    		String state = splitLine[4];
    		String zip = splitLine[5];
    		String type = splitLine[6];
    		int beds = Integer.parseInt(splitLine[7]);
    		int beds_avail = beds;
    		String county = splitLine[8];
    		String county_fips = splitLine[9];
    		String country = splitLine[10];
    		String latitude = splitLine[11];
    		String longitude = splitLine[12];
    		String naics_code = splitLine[13];
    		String website = splitLine[14];
    		String owner = splitLine[15];
    		String trauma = splitLine[16];
    		String helipad = splitLine[17];
    		
    		
    		String insertHospRow = "INSERT INTO hospital" +
    								"" +
    								id + ", " +
    								name + ", " +
    								city + ", " +
    								state + ", " +
    								type + ", " +
    								beds + ", " +
    								beds_avail + ", " +
    								county + ", " +
    								county_fips + ", " +
    								country + ", " + 
    								latitude + ", " +
    								longitude + ", " +
    								naics_code + ", " +
    								website + ", " +
    								owner + ", " +
    								trauma + ", " +
    								helipad + ", " +
    								")";
    		System.out.println(insertHospRow);
    		try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(insertHospRow);
                }
    		} catch(Exception ex) {
    			ex.printStackTrace();
    		}
    	} //done with reading in all hospital data
    	hospScanner.close();
    	//parse zip code file
    	Scanner zipScanner = new Scanner(new File(zipFileName));
    	zipScanner.useDelimiter(",");
    	zipScanner.nextLine();
    	while(zipScanner.hasNextLine()) {
    		String newLine = zipScanner.nextLine();
    		String split[] = newLine.split(",");
    		String zip = split[0];
    		String name = split[1];
    		String city = split[2];
    		String state = split[3];
    		String county = split[4];
    		
    		String insertZipRow = "INSERT INTO zip VALUES" +
					"(" +
					zip + ", " +
					name + ", " +
					city + ", " +
					state + ", " +
					county + ", " +
					")";
    		try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(insertZipRow);
                }
    		} catch(Exception ex) {
    			ex.printStackTrace();
    		}
    	} //done reading in zip data
    	zipScanner.close();
    	Scanner distScanner = new Scanner(new File(zipDistFileName));
    	distScanner.useDelimiter(",");
    	distScanner.nextLine();
    	
    	while(distScanner.hasNextLine()) {
    		String newLine = distScanner.nextLine();
    		String split[] = newLine.split(",");
    		String zip_from = split[0];
    		String zip_to = split[1];
    		String distance = split[2];
    		
    		String insertZipDistRow = "INSERT INTO distances VALUES" +
					"(" +
					zip_from + ", " +
					zip_to + ", " +
					distance + ", " +
					")";
    		
    		try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {
                    stmt.executeUpdate(insertZipDistRow);
                }
    		} catch(Exception ex) {
    			ex.printStackTrace();
    		}
    	}
    	distScanner.close();
    }
    

    void delete(File f) throws IOException {
        if (f.isDirectory()) {
            for (File c : f.listFiles())
                delete(c);
        }
        if (!f.delete())
            throw new FileNotFoundException("Failed to delete file: " + f);
    }

    public int executeUpdate(String stmtString) {
        int result = -1;
        try {
            Connection conn = ds.getConnection();
            try {
                Statement stmt = conn.createStatement();
                result = stmt.executeUpdate(stmtString);
                stmt.close();
            } catch (Exception e) {

                e.printStackTrace();
            } finally {
                conn.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return  result;
    }

    public int dropTable(String tableName) {
        int result = -1;
        try {
            Connection conn = ds.getConnection();
            try {
                String stmtString = null;

                stmtString = "DROP TABLE " + tableName;

                Statement stmt = conn.createStatement();

                result = stmt.executeUpdate(stmtString);

                stmt.close();
            } catch (Exception e) {

                e.printStackTrace();
            } finally {
                conn.close();
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }
        return result;
    }

    public int resetData() {
    	int result = 0; //0 for fail, 1 for success
    	String hospital = "hospital";
    	String zip = "zip";
    	String distances = "distances";
    	String patients = "patients";
    	if(dropTable(hospital) != -1 && dropTable(zip) != -1 && dropTable(distances) != -1 && dropTable(patients) != -1) {
    		result = 1;
    	}
    	
    	return result;
    }
    public boolean databaseExist(String databaseName)  {
        boolean exist = false;
        try {

            if(!ds.getConnection().isClosed()) {
                exist = true;
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return exist;
    }

    public boolean tableExist(String tableName)  {
        boolean exist = false;

        ResultSet result;
        DatabaseMetaData metadata = null;

        try {
            metadata = ds.getConnection().getMetaData();
            result = metadata.getTables(null, null, tableName.toUpperCase(), null);

            if(result.next()) {
                exist = true;
            }
        } catch(java.sql.SQLException e) {
            e.printStackTrace();
        }

        catch(Exception ex) {
            ex.printStackTrace();
        }
        return exist;
    }

    public List<Map<String,String>> getAccessLogs() {
        List<Map<String,String>> accessMapList = null;
        try {

            accessMapList = new ArrayList<>();

            Type type = new TypeToken<Map<String, String>>(){}.getType();

            String queryString = null;

            //fill in the query
            queryString = "SELECT * FROM accesslog";

            try(Connection conn = ds.getConnection()) {
                try (Statement stmt = conn.createStatement()) {

                    try(ResultSet rs = stmt.executeQuery(queryString)) {

                        while (rs.next()) {
                            Map<String, String> accessMap = new HashMap<>();
                            accessMap.put("remote_ip", rs.getString("remote_ip"));
                            accessMap.put("access_ts", rs.getString("access_ts"));
                            accessMapList.add(accessMap);
                        }

                    }
                }
            }

        } catch(Exception ex) {
            ex.printStackTrace();
        }

        return accessMapList;
    }
    
    public HashMap<String, String> getHospitalInfo(int hospID) {
    	System.out.println("Called getHospitalInfo");
    	HashMap<String, String> hospitalInfo = new HashMap<String, String>();
    	String queryString = "SELECT beds, beds_avail, zip FROM hospital WHERE" +
    							" hospital.id=" + hospID;
    	
    	String test = "SELECT * from hospital";
    	
    	System.out.println(queryString);
    	
    	try(Connection conn = ds.getConnection()) {
            try (Statement stmt = conn.createStatement()) {

                try(ResultSet rs = stmt.executeQuery(test)) {
                    while (rs.next()) {
                    	System.out.println("Inside while loop");
                        hospitalInfo.put("total_beds", rs.getString("beds"));
                        hospitalInfo.put("available_beds", rs.getString("beds_avail"));
                        hospitalInfo.put("zipcode", rs.getString("zip"));
                    }

                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    	
    	return hospitalInfo;
    	
    }
    
    public HashMap<String, String> getPatientInfo(String mrn) {
    	HashMap<String, String> patientInfo = new HashMap<String, String>();
    	String queryString = "SELECT location FROM patients WHERE patients.mrn = " +
    							mrn;
    	try(Connection conn = ds.getConnection()) {
            try (Statement stmt = conn.createStatement()) {

                try(ResultSet rs = stmt.executeQuery(queryString)) {

                    while (rs.next()) {
                        patientInfo.put("mrn", mrn);
                        patientInfo.put("patient_location", rs.getString("location"));
                    }

                }
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
    	
    	
    	return patientInfo;
    }
    
    
    public HashMap<String, String> getTestCounter() {
    	HashMap<String, String> testInfo = new HashMap<String, String>();
    	//patient_status_code < 2, negative
    	//patient_status_code >= 2, positive
    	String negQueryString = "SELECT COUNT(patient_status_code) FROM patients WHERE patients.patient_status_code < 2";
    	String posQueryString = "SELECT COUNT(patient_status_code) FROM patients WHERE patients.patient_status_code >= 2";
    	try(Connection conn = ds.getConnection()) {
            try (Statement stmt = conn.createStatement()) {

                try(ResultSet rs = stmt.executeQuery(posQueryString)) {

                    while (rs.next()) {
                        testInfo.put("positive_test:", rs.getString("COUNT(patient_status_code)"));
                        
                    }

                }
            try (Statement st = conn.createStatement()) {

                  try(ResultSet rs = st.executeQuery(negQueryString)) {

                      while (rs.next()) {
                          testInfo.put("negative_test:", rs.getString("COUNT(patient_status_code)"));
                      }

                  }  
            }
          } 
    	} catch(Exception ex) {
            ex.printStackTrace();
          }
    	
    	return testInfo;
    }

}

	
