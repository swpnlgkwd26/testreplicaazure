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

        //  Scenarion 1 .... Bulk Copy Insert  with Serial and Parallel way
        public static void SerialLoadTest(string connectionString)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            stopwatch.Reset();
            stopwatch.Start();
            // Get the DataTables
            List<DataTable> tables = AzureSQLOperations.GetDataTable(connectionString);
            foreach (var table in tables)
            {
                AzureSQLOperations.BulkDataUpload(table, connectionString);
            }

            stopwatch.Stop();
            double d = ((double)stopwatch.ElapsedMilliseconds / 1000);
            Console.WriteLine("Serial Time Elapsed in MilliSecond : " + stopwatch.ElapsedMilliseconds + " Seconds :  " + d.ToString());
            AzureSQLOperations.ReadCountFromAzureSql(connectionString);

        }
        public static void ParallelLoadTest(string connectionString)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            stopwatch.Reset();
            stopwatch.Start();
            // Get the DataTables
            List<DataTable> tables = AzureSQLOperations.GetDataTable(connectionString);
            Parallel.ForEach(tables, table =>
            {
                AzureSQLOperations.BulkDataUpload(table, connectionString);
                //AzureSQLOperations.BulkDataUploadWithTransaction(table, connectionString);
            });
            stopwatch.Stop();
            double d = ((double)stopwatch.ElapsedMilliseconds / 1000);
            Console.WriteLine("Parallel Time Elapsed in MilliSecond: " + stopwatch.ElapsedMilliseconds + " Seconds :  " + d);
            AzureSQLOperations.ReadCountFromAzureSql(connectionString);

        }


        //  Scenario 2 .... Bulk Copy Insert  with Serial and Parallel  With Transaction
        public static void SerialLoadTestTransaction(string connectionString)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            stopwatch.Reset();
            stopwatch.Start();
            // Get the DataTables
            List<DataTable> tables = AzureSQLOperations.GetDataTable(connectionString);
            foreach (var table in tables)
            {
                //AzureSQLOperations.BulkDataUpload(table, connectionString);
                AzureSQLOperations.BulkDataUploadWithTransaction(table, connectionString);
            }

            stopwatch.Stop();
            double d = ((double)stopwatch.ElapsedMilliseconds / 1000);
            Console.WriteLine("Serial Time Elapsed in MilliSecond: " + stopwatch.ElapsedMilliseconds + " Seconds :  " + d.ToString());
            AzureSQLOperations.ReadCountFromAzureSql(connectionString);

        }
        public static void ParallelLoadTestTransaction(string connectionString)
        {
            Stopwatch stopwatch = Stopwatch.StartNew();
            stopwatch.Reset();
            stopwatch.Start();
            // Get the DataTables
            List<DataTable> tables = AzureSQLOperations.GetDataTable(connectionString);
            Parallel.ForEach(tables, table =>
            {
                //AzureSQLOperations.BulkDataUpload(table, connectionString);
                AzureSQLOperations.BulkDataUploadWithTransaction(table, connectionString);
            });
            stopwatch.Stop();
            double d = ((double)stopwatch.ElapsedMilliseconds / 1000);
            Console.WriteLine("Parallel Time Elapsed in MilliSecond: " + stopwatch.ElapsedMilliseconds + " Seconds :  " + d);
            AzureSQLOperations.ReadCountFromAzureSql(connectionString);

        }


        // Scenarion 3 ..   Bulk Copy Insert  with Serial and Parallel way With Transaction and Drop , Recreating Index 
        public static void SerialLoadTestTransactionWithIndex(string connectionString)
        {
            AzureSQLOperations.DropClusteredIndex(connectionString);
            Stopwatch stopwatch = Stopwatch.StartNew();
            stopwatch.Reset();
            stopwatch.Start();
            // Get the DataTables
            List<DataTable> tables = AzureSQLOperations.GetDataTable(connectionString);
            foreach (var table in tables)
            {
                //AzureSQLOperations.BulkDataUpload(table, connectionString);
                AzureSQLOperations.BulkDataUploadWithTransaction(table, connectionString);
            }

            stopwatch.Stop();
            double d = ((double)stopwatch.ElapsedMilliseconds / 1000);
            Console.WriteLine("Serial Time Elapsed using Index in MilliSecond: " + stopwatch.ElapsedMilliseconds + " Seconds :  " + d.ToString());
            AzureSQLOperations.ReadCountFromAzureSql(connectionString);
            AzureSQLOperations.CreateClusteredIndex(connectionString);
        }
        public static void ParallelLoadTestTransactionWithIndex(string connectionString)
        {
            AzureSQLOperations.DropClusteredIndex(connectionString);
            Stopwatch stopwatch = Stopwatch.StartNew();
            stopwatch.Reset();
            stopwatch.Start();
            // Get the DataTables
            List<DataTable> tables = AzureSQLOperations.GetDataTable(connectionString);
            Parallel.ForEach(tables, table =>
            {
                //AzureSQLOperations.BulkDataUpload(table, connectionString);
                AzureSQLOperations.BulkDataUploadWithTransaction(table, connectionString);
            });
            stopwatch.Stop();
            double d = ((double)stopwatch.ElapsedMilliseconds / 1000);
            Console.WriteLine("Parallel Time Elapsed using Index in MilliSecond: " + stopwatch.ElapsedMilliseconds + " Seconds :  " + d);
            AzureSQLOperations.ReadCountFromAzureSql(connectionString);
            AzureSQLOperations.CreateClusteredIndex(connectionString);

        }





        static void Main(string[] args)
        {
            var connectionString = Utility.GetConnectionString(false);


            Console.WriteLine("Scenario 1");
            Console.WriteLine("*** Operation  : Serial without Transaction and  Recreating Index***");
            AzureSQLOperations.TruncateTableEmp(connectionString);
            SerialLoadTest(connectionString); // Serially
                                              //  Thread.Sleep(3000);
            Console.WriteLine();


            Console.WriteLine("*** Operation  : Parallel without Transaction and Recreating Index***");
         //   AzureSQLOperations.TruncateTableEmp(connectionString);
            //    Thread.Sleep(3000);
            ParallelLoadTest(connectionString); //Parallel
           


            Console.WriteLine("\n \n===================================================================");
            Console.WriteLine("\n \n===================================================================");

            // Console.ReadLine();

            Console.WriteLine("Scenario 2");
            Console.WriteLine("*** Operation  : Serial with Transaction ***");
           // AzureSQLOperations.TruncateTableEmp(connectionString);
            SerialLoadTestTransaction(connectionString); // Serially with Transacation

            Console.WriteLine();

            Console.WriteLine("*** Operation  : Parallel with Transaction ***");
          //  AzureSQLOperations.TruncateTableEmp(connectionString);
            ParallelLoadTestTransaction(connectionString); // Parallel Transaction


            Console.WriteLine("\n \n===================================================================");
            Console.WriteLine("\n \n===================================================================");

           // Console.ReadLine();


            Console.WriteLine("Scenario 3");
            Console.WriteLine("*** Operation  : Serial with Transaction and Recreating Index ***");
           // AzureSQLOperations.TruncateTableEmp(connectionString);
            SerialLoadTestTransactionWithIndex(connectionString); // Serially with Transacation
                                                                  //Thread.Sleep(3000);
            Console.WriteLine();

            Console.WriteLine("*** Operation  : Parallel with Transaction and Recreating Using Index***");
         //   AzureSQLOperations.TruncateTableEmp(connectionString);
            ParallelLoadTestTransactionWithIndex(connectionString); // Parallel Transaction
                                                                    // Thread.Sleep(3000);

            Console.WriteLine("\n \n===================================================================");


            Console.WriteLine("\n \n===================================================================");
            Console.ReadLine();

        }
    }
}
