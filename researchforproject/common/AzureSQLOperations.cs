using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Threading;

namespace common
{
    public  class AzureSQLOperations
    {


        public static void GetEmployeeDataInfinite(string connectionString)
        {
            using (SqlConnection sqlConnection = new SqlConnection())
            {
                
                try
                {
                    sqlConnection.ConnectionString = connectionString;
                    sqlConnection.Open();
                   var transaction= sqlConnection.BeginTransaction(IsolationLevel.Serializable);
                    while (true)
                    {

                        SqlCommand cmd = new SqlCommand("Select * from Employee", sqlConnection, transaction);
                        SqlDataReader dr = cmd.ExecuteReader();
                        while (dr.Read())
                        {
                            //Thread.Sleep(500);
                            Console.WriteLine($"EmpId :{dr[0].ToString()} , EmpName : {dr[1].ToString()}, Salary :{dr[2].ToString()}");
                        }
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
        public static void GetEmployeeData(string connectionString)
        {
            SqlConnection sqlConnection = new SqlConnection();
            try
            {
                sqlConnection.ConnectionString = connectionString;
                sqlConnection.Open();
                for (int i = 0; i < 500000; i++)
                {

                    SqlCommand cmd = new SqlCommand("Select * from Employee where EmpId=" + i.ToString(), sqlConnection);
                    SqlDataReader dr = cmd.ExecuteReader();
                    if (dr.Read())
                    {
                        Thread.Sleep(500);
                        Console.WriteLine($"EmpId :{dr[0].ToString()} , EmpName : {dr[1].ToString()}, Salary :{dr[2].ToString()}");
                    }
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

        public static DataTable CreateDataTable(int start, int end)
        {


            DataTable tbl = new DataTable();
            tbl.Columns.Add(new DataColumn("EmpId", typeof(Int32)));
            tbl.Columns.Add(new DataColumn("EmpName", typeof(string)));
            tbl.Columns.Add(new DataColumn("Salary", typeof(Int32)));
            for (int i = start; i < end; i++)
            {
                DataRow dr = tbl.NewRow();
                dr["EmpId"] = i;
                dr["EmpName"] = "Test" + i;
                dr["Salary"] = i;

                tbl.Rows.Add(dr);
            }
            return tbl;

        }

        public static List<DataTable> GetDataTable(string connectionString)
        {
            
            List<DataTable> dataTables = new List<DataTable>();
            int start = 0;
            int end = start + 100000;
            for (int i = 0; i < 15; i++)
            {

                var tbl = CreateDataTable(start, end);
                dataTables.Add(tbl);

                start = end;
                end = start + 100000;
            }

            return dataTables;
        }

        public static void BulkDataUpload(DataTable tbl, string connectionString)
        {
            SqlConnection con = new SqlConnection(connectionString);
            //create object of SqlBulkCopy which help to insert  
            con.Open();
            using (var objbulk = new SqlBulkCopy(con))
            {
                objbulk.DestinationTableName = "Employee";
                objbulk.BulkCopyTimeout = 0;
                // By default all the records in the source  will be written to target table
                // To reduce the memory consumed by sqlbulkcopy we can use BatchSize
                objbulk.BatchSize = 10000;
                objbulk.NotifyAfter = 100000;
                objbulk.SqlRowsCopied += (sender, eventArgs) => Console.WriteLine(" ** Wrote : " + eventArgs.RowsCopied + " Records **");

                objbulk.ColumnMappings.Add("EmpId", "EmpId");
                objbulk.ColumnMappings.Add("EmpName", "EmpName");
                objbulk.ColumnMappings.Add("Salary", "Salary");
                objbulk.WriteToServer(tbl);
            }
            con.Close();           
            
        }

        public static void BulkDataUploadWithTransaction(DataTable tbl, string connectionString)
        {
            SqlConnection con = new SqlConnection(connectionString);
            //create object of SqlBulkCopy which help to insert  
            con.Open();
            var transaction = con.BeginTransaction(IsolationLevel.Serializable);
            using (var objbulk = new SqlBulkCopy(con,SqlBulkCopyOptions.KeepIdentity,transaction))
            {
                
                objbulk.DestinationTableName = "Employee";
                objbulk.BulkCopyTimeout = 0;
                // By default all the records in the source  will be written to target table
                // To reduce the memory consumed by sqlbulkcopy we can use BatchSize
                objbulk.BatchSize = 10000;
                objbulk.NotifyAfter = 100000;
                objbulk.SqlRowsCopied += (sender, eventArgs) => Console.WriteLine(" ** Wrote : " + eventArgs.RowsCopied + " Records **");

                objbulk.ColumnMappings.Add("EmpId", "EmpId");
                objbulk.ColumnMappings.Add("EmpName", "EmpName");
                objbulk.ColumnMappings.Add("Salary", "Salary");
                try
                {
                    objbulk.WriteToServer(tbl);
                    transaction.Commit();
                }
                catch (Exception)
                {

                    transaction.Rollback();
                }
               
            }
            con.Close();

        }

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

        public static  void TruncateTableEmp(string connectionString)
        {
            SqlConnection sqlConnection = new SqlConnection();
            try
            {
                sqlConnection.ConnectionString = connectionString;
                sqlConnection.Open();
                SqlCommand cmd = new SqlCommand("Truncate Table Employee", sqlConnection);
                cmd.ExecuteNonQuery();
               // Thread.Sleep(3000);
                Console.WriteLine("Table Truncated ");

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

        public static void CreateClusteredIndex(string connectionString)
        {
            SqlConnection sqlConnection = new SqlConnection();
            try
            {
                sqlConnection.ConnectionString = connectionString;
                sqlConnection.Open();
                SqlCommand cmd = new SqlCommand("CREATE NONCLUSTERED INDEX IX_EmpIndex ON Employee(EmpName) ", sqlConnection);

                //SqlCommand cmd = new SqlCommand("ALTER INDEX ALL ON Employee Rebuild ", sqlConnection);
                cmd.ExecuteNonQuery();
                // Thread.Sleep(3000);
                Console.WriteLine("Index Created on EmpId");

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

        public static void DropClusteredIndex(string connectionString)
        {
            SqlConnection sqlConnection = new SqlConnection();
            try
            {
                sqlConnection.ConnectionString = connectionString;
                sqlConnection.Open();
                SqlCommand cmd = new SqlCommand("Drop Index IX_EmpIndex ON [dbo].Employee", sqlConnection);
                cmd.ExecuteNonQuery();
                // Thread.Sleep(3000);
                Console.WriteLine("Index Deleted on EmpId");

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