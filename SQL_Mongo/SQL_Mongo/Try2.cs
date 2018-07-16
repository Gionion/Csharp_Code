using System;
using System.Data;
using System.Data.SqlClient;
using System.Globalization;
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
            string ConnectionString = @"Data Source = 172.16.24.103; user id=x; password=xxx; Initial Catalog = xDB";
            SqlConnection conn = new SqlConnection(ConnectionString);
            conn.Open();
            
            int idvariable = 50972;
            while (idvariable < 53075)
            {
                DateTime fecha = new DateTime(2013, 8, 7);
                DateTime fecha_fin = new DateTime(2013, 8, 8);

                while (fecha < DateTime.Now) {
                    string fecha_insert = fecha.ToString("yyyy-MM-dd HH:mm:ss");
                    string fecha_insert_fin = fecha_fin.ToString("yyyy-MM-dd HH:mm:ss");
                    //sql server
                    string query = "SELECT [IdVar], CONVERT(DateTime,[IniDat],131) AS [IniDat], CONVERT(DateTime,[FinDat],131) AS [FinDat], [ValMax], [ValMin], [Val], [Valsdv], [Rlb], [RlbSta], [Rlbchk], [Rbl] FROM [EpeleDB].[dbo].[Msi_His_Var] " +
                        "WHERE [IdVar] = " + idvariable + " AND [Inidat] BETWEEN '" + fecha_insert + "' AND '" + fecha_insert_fin + "' ORDER BY [IniDat]";

                    SqlCommand queryCommand = new SqlCommand(query, conn);
                    SqlDataReader queryCommandReader = queryCommand.ExecuteReader();
                    DataTable dataTable = new DataTable();
                    dataTable.Load(queryCommandReader);
                    //mongo
                    var connString = "mongodb://localhost:27017";
                    var client = new MongoClient(connString);
                    var database = client.GetDatabase("xDB");
                    var collec = database.GetCollection<BsonDocument>("xcolec");

                    for (int i = 0; i < dataTable.Rows.Count; i++)
                    {
                        int? idvar = null;
                        DateTime? inidat = null;
                        DateTime? findat = null;
                        double? valmax = null;
                        double? valmin = null;
                        double? val = null;
                        double? valsdv = null;
                        int? rlb = null;
                        string rlbsta = String.Empty;
                        string rlbchk = String.Empty;
                        int? rbl = null;
                        foreach (DataColumn column in dataTable.Columns)
                        {
                            idvar = Convert.ToInt32((dataTable.Rows[i]["idVar"]));
                            inidat = Convert.ToDateTime(dataTable.Rows[i]["inidat"]);
                            findat = Convert.ToDateTime((dataTable.Rows[i]["findat"]));
                            if(dataTable.Rows[i]["valmax"] == DBNull.Value)
                            {
                                valmax = dataTable.Rows[i]["valmax"] is DBNull ? 0 : double.Parse(Convert.ToString(dataTable.Rows[i]["valmax"]));
                            }
                            else
                            {
                                valmax = Convert.ToDouble((dataTable.Rows[i]["valmax"]));
                            }
                            if (dataTable.Rows[i]["valmin"] == DBNull.Value)
                            {
                                valmin = dataTable.Rows[i]["valmin"] is DBNull ? 0 : double.Parse(Convert.ToString(dataTable.Rows[i]["valmin"]));
                            }
                            else
                            {
                                valmin = Convert.ToDouble((dataTable.Rows[i]["valmin"]));
                            }
                            val = Convert.ToDouble((dataTable.Rows[i]["val"]));
                            if (dataTable.Rows[i]["valsdv"] == DBNull.Value) {
                                valsdv = dataTable.Rows[i]["valsdv"] is DBNull ? 0 : double.Parse(Convert.ToString(dataTable.Rows[i]["valsdv"]));
                            }
                            else
                            {
                                valsdv = Convert.ToDouble((dataTable.Rows[i]["valsdv"]));
                            }
                            if (dataTable.Rows[i]["rlb"] == DBNull.Value)
                            {
                                rlb = dataTable.Rows[i]["rlb"] is DBNull ? 0 : Int32.Parse(Convert.ToString(dataTable.Rows[i]["rlb"]));
                            }
                            else
                            {
                                rlb = Convert.ToInt32((dataTable.Rows[i]["rlb"]));
                            }
                            rlbsta = Convert.ToString((dataTable.Rows[i]["rlbsta"]));
                            rlbchk = Convert.ToString((dataTable.Rows[i]["rlbchk"]));
                            rbl = Convert.ToInt32((dataTable.Rows[i]["rbl"]));
                        }
                        var documnt = new BsonDocument
                        {
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
                        collec.InsertOne(documnt);
                    }
                    fecha = fecha.AddDays(1);
                    fecha_fin = fecha_fin.AddDays(1);
                }
                idvariable++;  
            }
            conn.Close();
        }
    }
}
