using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Threading;

namespace common
{
    public static class AzureSQLOperations
    {
        // Returns ReadOnly Or WriteOnly
        public static void GetReadIntentStatus(string connectionString)
        {
            SqlConnection sqlConnection = new SqlConnection();
            SqlDataReader dr = null;
            try
            {
                sqlConnection.ConnectionString = connectionString;
                sqlConnection.Open();
                SqlCommand cmd = new SqlCommand("SELECT DATABASEPROPERTYEX('testdb', 'Updateability')", sqlConnection);
                dr = cmd.ExecuteReader();
                while (dr.Read())
                {
                    Console.WriteLine($"Data Reading From :  {dr[0]}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                sqlConnection.Close();
            }


        }

        // Returns the Count
        public static void ReadCountFromAzureSql(string connectionString)
        {
            SqlConnection sqlConnection = new SqlConnection();
            try
            {
                sqlConnection.ConnectionString = connectionString;
                sqlConnection.Open();
                SqlCommand cmd = new SqlCommand("Select Count(*) from Employee", sqlConnection);
                Thread.Sleep(3000);
                Console.WriteLine("Number of Rows READ_ONLY Replica : " + (int)cmd.ExecuteScalar());

            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            finally
            {
                sqlConnection.Close();
            }
        }

        // Add Data Into Employee Table
        public static void WriteDataIntoAzureSql(string connectionString)
        {
            SqlConnection sqlConnection = new SqlConnection();
            try
            {
                // Started
                sqlConnection.ConnectionString = connectionString;
                sqlConnection.Open();
                SqlCommand cmd = null;
                for (int i = 0; i < 1000000; i++)
                {
                    Thread.Sleep(100);
                    string commandQuery = $"Insert into Employee values({i},'Test:{i}',{i})";
                    cmd = new SqlCommand(commandQuery, sqlConnection);
                    cmd.ExecuteNonQuery();
                    Console.WriteLine("Employee Added : " + i);
                }
            }
            catch (Exception ex)
            {

                Console.WriteLine(ex.Message);
            }
            finally
            {
                sqlConnection.Close();
            }
        }


        // Bulk Upload  Data
        public static void BulkUpload(string connectionString)
        {

            DataTable tbl = new DataTable();
            tbl.Columns.Add(new DataColumn("EmpId", typeof(Int32)));
            tbl.Columns.Add(new DataColumn("EmpName", typeof(string)));
            tbl.Columns.Add(new DataColumn("Salary", typeof(Int32)));
            for (int i = 0; i < 100000; i++)
            {
                DataRow dr = tbl.NewRow();
                dr["EmpId"] = i;
                dr["EmpName"] = "Test"+i;
                dr["Salary"] = i;              

                tbl.Rows.Add(dr);
            }

            SqlConnection con = new SqlConnection(connectionString);
            //create object of SqlBulkCopy which help to insert  
            SqlBulkCopy objbulk = new SqlBulkCopy(con);

            //assign Destination table name  
            objbulk.DestinationTableName = "Employee";

            objbulk.ColumnMappings.Add("EmpId", "EmpId");
            objbulk.ColumnMappings.Add("EmpName", "EmpName");
            objbulk.ColumnMappings.Add("Salary", "Salary");
            

            con.Open();
            //insert bulk Records into DataBase.  
            objbulk.WriteToServer(tbl);
            con.Close();
        }


        // Update Operation

        public static void UpdateAzureSQLData(string connectionString)
        {
            while (true)
            {
                SqlConnection sqlConnection = new SqlConnection();
                try
                {
                    // Started
                    sqlConnection.ConnectionString = connectionString;
                    sqlConnection.Open();
                    
                    SqlCommand cmd = null; 
                    for (int i = 1; i < 1000000; i++)
                    {
                        string empName = "Updated Test" + i.ToString();
                        Thread.Sleep(100);
                        string commandQuery = $"Update Employee Set EmpName='{empName}' Where EmpId =" + i;
                        cmd = new SqlCommand(commandQuery, sqlConnection);
                        cmd.ExecuteNonQuery();
                        Console.WriteLine("Employee Updated : " + i);
                    }
                }
                catch (Exception ex)
                {

                    Console.WriteLine(ex.Message);
                }
                finally
                {
                    sqlConnection.Close();
                }
            }
         
        }
    }
}