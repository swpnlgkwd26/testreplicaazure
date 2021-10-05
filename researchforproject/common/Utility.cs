using System;

namespace common
{
    public static class Utility
    {
        // Returns ConnectionString
        public static string GetConnectionString(bool readOnly)
        {
            string connectionString = "Server=tcp:swpnlgkwd.database.windows.net,1433;Initial Catalog=testdb;Persist Security Info=False;User ID=swpnlgkwd;Password=Password@1234;MultipleActiveResultSets=true;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
            if (readOnly)
            {
                connectionString = connectionString + "ApplicationIntent=ReadOnly;";
            }
            return connectionString;
        }
    }

    // Table Creation
//    Create table Employee
//(
// EmpId int ,
// EmpName Varchar(30),
// Salary int
// )


}
