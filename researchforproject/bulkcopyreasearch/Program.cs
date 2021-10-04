using common;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace bulkcopyreasearch
{
    class Program
    {
        public static void SerialLoadTest(string connectionString)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            stopwatch.Start();
            // Get the DataTables
            List<DataTable> tables = AzureSQLOperations.GetDataTable(connectionString);
            foreach (var table in tables)
            {
                AzureSQLOperations.BulkDataUpload(table, connectionString);
            }

            stopwatch.Stop();
            double d = (double)(stopwatch.ElapsedMilliseconds / 1000);
            Console.WriteLine("Serial Time Elapsed : " + stopwatch.ElapsedMilliseconds + " Seconds :  " + d.ToString());
            AzureSQLOperations.ReadCountFromAzureSql(connectionString);

        }

        public static void ParallelLoadTest(string connectionString)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            stopwatch.Start();
            // Get the DataTables
            List<DataTable> tables = AzureSQLOperations.GetDataTable(connectionString);
            Parallel.ForEach(tables, table =>
            {
                AzureSQLOperations.BulkDataUpload(table, connectionString);
            });
            stopwatch.Stop();
            double d = (double)(stopwatch.ElapsedMilliseconds / 1000);
            Console.WriteLine("Parallel Time Elapsed : " + stopwatch.ElapsedMilliseconds + " Seconds :  " + d);
            AzureSQLOperations.ReadCountFromAzureSql(connectionString);

        }

        static void Main(string[] args)
        {
            var connectionString = Utility.GetConnectionString(false);

            AzureSQLOperations.TruncateTableEmp(connectionString);
            SerialLoadTest(connectionString); // Serially
            Thread.Sleep(3000);

            AzureSQLOperations.TruncateTableEmp(connectionString);
            Thread.Sleep(3000);
            ParallelLoadTest(connectionString); //Parallel
            AzureSQLOperations.TruncateTableEmp(connectionString);

            Console.ReadLine();
        }
    }
}
