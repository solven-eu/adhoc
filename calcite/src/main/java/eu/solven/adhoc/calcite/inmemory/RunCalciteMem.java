/**
 * The MIT License
 * Copyright (c) 2025 Benoit Chatain Lacelle - SOLVEN
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package eu.solven.adhoc.calcite.inmemory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

public class RunCalciteMem {

	// https://gist.github.com/andriika/e9f3c34c4e29ace79806af5c2f318a88
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Class.forName("org.apache.calcite.jdbc.Driver");

		Properties info = new Properties();
		info.setProperty("lex", "JAVA");
		Connection connection = DriverManager.getConnection("jdbc:calcite:", info);

		CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
		SchemaPlus rootSchema = calciteConnection.getRootSchema();
		Schema schema = new CustomSchema();
		rootSchema.add("hr", schema);

		Statement statement = calciteConnection.createStatement();
		ResultSet rs = statement.executeQuery("select * from hr.employees as e where e.age >= 30");

		while (rs.next()) {
			long id = rs.getLong("id");
			String name = rs.getString("name");
			int age = rs.getInt("age");
			System.out.println("id: " + id + "; name: " + name + "; age: " + age);
		}

		rs.close();
		statement.close();
		connection.close();
	}
}
