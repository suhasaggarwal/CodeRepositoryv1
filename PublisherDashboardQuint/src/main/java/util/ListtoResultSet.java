package util;

import java.sql.ResultSet;
import java.util.List;
import com.mockrunner.mock.jdbc.MockResultSet;

public class ListtoResultSet {

	 public static ResultSet getResultSet(List<String> headers, List<List<String>> data) throws Exception {
		 
	        // validation
	        if (headers == null || data == null) {
	            throw new Exception("null parameters");
	        }
	 
	        if (headers.size() != data.size()) {
	         //   throw new Exception("parameters size are not equals");
	        }
	 
	        // create a mock result set
	        MockResultSet mockResultSet = new MockResultSet("myResultSet");
	 
	        // add header
	        for (String string : headers) {         
	            mockResultSet.addColumn(string);
	        }
	 
	        // add data
	        for (List<String> list : data) {
	            mockResultSet.addRow(list);
	        }
	 
	        return mockResultSet;
	    }

}
