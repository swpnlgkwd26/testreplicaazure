using System;
using System.Collections.Generic;
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
    }
}