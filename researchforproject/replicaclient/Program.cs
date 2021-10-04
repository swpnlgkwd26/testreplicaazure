using common;
using System;
using System.Data.SqlClient;
using System.Threading;

namespace replicaclient
{
    class Program
    {

        // Checked Bulk Operations
        // Check Insert Data
        // Checked Bulk Update
        static void Main(string[] args)
        {
            // Application Intent is READ_WRITE
            string connectionString = Utility.GetConnectionString(false);
            Console.WriteLine("Press 1 Bulk Upload and 2 for Simple Insert Operation Press 3 for updating data");
            int i = Convert.ToInt32(Console.ReadLine());
            if (i==1)
            {
                AzureSQLOperations.BulkDataUpload(connectionString);
            }
            else if(i==2)
            {

                // Insert Data into Azure SQL
                AzureSQLOperations.WriteDataIntoAzureSql(connectionString);
               
            }
            else if (i==3)
            {
                AzureSQLOperations.UpdateAzureSQLData(connectionString);

            }
            Console.ReadLine();
        }
    }
}
