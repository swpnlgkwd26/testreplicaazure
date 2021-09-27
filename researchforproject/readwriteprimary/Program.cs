using common;
using System;
using System.Data.SqlClient;
using System.Threading;

namespace readwriteprimary
{
    class Program
    {
        public static void GetEmployeeCountReadWriteMode(string connectionString)
        {
            AzureSQLOperations.GetReadIntentStatus(connectionString);

            while (true)
            {
                AzureSQLOperations.ReadCountFromAzureSql(connectionString);
            }
        }
        static void Main(string[] args)
        {

            // Application Intent is READ_WRITE
            string connectionString = Utility.GetConnectionString(false);
            GetEmployeeCountReadWriteMode(connectionString);
            Console.ReadLine();
        }
    }
}
