using common;
using System;
using System.Data.SqlClient;
using System.Threading;

namespace readonlyreplica
{
    class Program
    {
        public static void GetEmployeesCountReadonlyMode(string connectionString)
        {
            // Get the Status :  ReadWrite or Readonly.. from here we can find out from where the 
            // application is reading the data.
            AzureSQLOperations.GetReadIntentStatus(connectionString);
            while (true)
            {
                AzureSQLOperations.ReadCountFromAzureSql(connectionString);
            }

        }
        static void Main(string[] args)
        {
            // Get Data From Replica : Application Intent = ReadOnly
            string connectionString = Utility.GetConnectionString(true);
            GetEmployeesCountReadonlyMode(connectionString);

            Console.ReadLine();
        }
    }
}
