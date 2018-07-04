using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Threading.Tasks;
using Microsoft.SqlServer.Server;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.Core;
using MongoDB.Driver.Linq;

namespace SQL_Mongo
{
    class Program
    {
        static void Main(string[] args)
        {
            //sql server
            string ConnectionString = @"Data Source = 172.16.24.103; user id=sa; password=Diamond2013; Initial Catalog = EpeleDB";
            SqlConnection conn = new SqlConnection(ConnectionString);
            conn.Open();

            string query = "SELECT [IdVar], [IniDat], [FinDat], [ValMax], [ValMin], [Val], [Valsdv], [Rlb], [RlbSta], [Rlbchk], [Rbl] FROM [EpeleDB].[dbo].[Msi_His_Var] " +
                "WHERE [IdVar] = 50002 ORDER BY [IniDat]";
            SqlCommand queryCommand = new SqlCommand(query, conn);
            SqlDataReader queryCommandReader = queryCommand.ExecuteReader();
            DataTable dataTable = new DataTable();
            dataTable.Load(queryCommandReader);
            //mongo
            var connString = "mongodb://localhost:27017";
            var client = new MongoClient(connString);
            var database = client.GetDatabase("basudate");
            var collec = database.GetCollection<BsonDocument>("sql");

            for (int i = 0; i < dataTable.Rows.Count; i++)
            {
                int? idvar = null;
                string inidat = String.Empty;
                string findat = String.Empty;
                double? valmax = null;
                double? valmin = null;
                double? val = null;
                double? valsdv = null;
                int? rlb = null;
                string rlbsta = String.Empty;
                string rlbchk = String.Empty;
                int? rbl = null;
                int rowid = i;
                foreach (DataColumn column in dataTable.Columns)
                {
                    idvar = Convert.ToInt32((dataTable.Rows[i]["idVar"]));
                    inidat = Convert.ToString((dataTable.Rows[i]["inidat"]));
                    findat = Convert.ToString((dataTable.Rows[i]["findat"]));
                    valmax = Convert.ToDouble((dataTable.Rows[i]["valmax"]));
                    valmin = Convert.ToDouble((dataTable.Rows[i]["valmin"]));
                    val = Convert.ToDouble((dataTable.Rows[i]["val"]));
                    valsdv = Convert.ToDouble((dataTable.Rows[i]["valsdv"]));
                    rlb = Convert.ToInt32((dataTable.Rows[i]["rlb"]));
                    rlbsta = Convert.ToString((dataTable.Rows[i]["rlbsta"]));
                    rlbchk = Convert.ToString((dataTable.Rows[i]["rlbchk"]));
                    rbl = Convert.ToInt32((dataTable.Rows[i]["rbl"]));
                }
                var documnt = new BsonDocument
                {
                {"RowID", rowid },
                { "idvar", idvar },
                { "initdat", inidat },
                { "findat", findat },
                { "valmax", valmax },
                { "valmin", valmin },
                { "val", val },
                { "valsdv", valsdv },
                { "rlb", rlb },
                { "rlbsta", rlbchk },
                { "rbl", rbl }
                };
                collec.InsertOneAsync(documnt);
            }
            Console.Read();
            conn.Close();
        }
    }
}