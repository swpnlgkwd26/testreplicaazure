using common;
using System;
using System.Data.SqlClient;
using System.Threading;

namespace replicaclient
{
    class Program
    {

        static void Main(string[] args)
        {
            // Application Intent is READ_WRITE
            string connectionstring = Utility.GetConnectionString(false);
            // Insert Data into Azure SQL
            AzureSQLOperations.WriteDataIntoAzureSql(connectionstring);
            Console.ReadLine();
        }
    }
}
