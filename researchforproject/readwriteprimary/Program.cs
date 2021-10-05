using common;
using System;
using System.Data.SqlClient;
using System.Threading;

namespace readwriteprimary
{
    class Program
    {

        public static  void GetEmployeeData(string  connectionString)
        {
            AzureSQLOperations.GetEmployeeData(connectionString);
        }
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

            Thread.Sleep(5000);
            // Application Intent is READ_WRITE
            string connectionString = Utility.GetConnectionString(false);
            AzureSQLOperations.GetEmployeeDataInfinite(connectionString);
           // GetEmployeeData(connectionString);
            //GetEmployeeCountReadWriteMode(connectionString);
            Console.ReadLine();
        }
    }
}
