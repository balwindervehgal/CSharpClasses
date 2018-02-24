#region Namespaces
using System;
using System.Data;
using Microsoft.SqlServer.Dts.Runtime;
using System.Windows.Forms;
using System.IO;
using System.Data.SqlClient;
#endregion

namespace ST_f1f319851cae48c2b8ecfc5395d8e547
{
    /// <summary>
    /// ScriptMain is the entry point class of the script.  Do not change the name, attributes,
    /// or parent of this class.
    /// </summary>
	[Microsoft.SqlServer.Dts.Tasks.ScriptTask.SSISScriptTaskEntryPointAttribute]
    public partial class ScriptMain : Microsoft.SqlServer.Dts.Tasks.ScriptTask.VSTARTScriptObjectModelBase
    {
        
        string currentTs = DateTime.Now.ToString("yyyyMMddHHmmss");


        public void Main()
        {
            // TODO: Add your code here
            string destFilePath = Dts.Variables["User::destFilePath"].Value.ToString();
            string colDelimiter = Dts.Variables["User::colDelimiter"].Value.ToString();
            string fileExtension = Dts.Variables["User::fileExtension"].Value.ToString();

            string sqlQuery = "SELECT Schema_name(schema_id) AS SchemaName,name AS TableName FROM   sys.tables WHERE type in ('U') order by 1,2";
            try
            {
                DataTable dt = getTableList(sqlQuery);

                createTextFilesForTableList(dt, destFilePath, colDelimiter, fileExtension);
                Dts.TaskResult = (int)ScriptResults.Success;
            }
            catch (Exception ex)
            {
                using (StreamWriter sw = File.CreateText(Dts.Variables["User::LogFilePath"].Value.ToString() + "\\" + "ErrorLog_" + currentTs + ".log"))
                {
                    sw.WriteLine(ex.ToString());
                    Dts.TaskResult = (int)ScriptResults.Failure;
                }

            }
            
        }

        private DataTable getTableList(string sqlQuery)
        {
            DataTable dt = new DataTable();
            SqlConnection conn = new SqlConnection();

            try
            {
                conn = (SqlConnection)(Dts.Connections["CONN"].AcquireConnection(Dts.Transaction));
                SqlCommand sqlCmd = new SqlCommand(sqlQuery, conn);
                dt.Load(sqlCmd.ExecuteReader());
            }
            catch (Exception ex)
            {
                using (StreamWriter sw = File.CreateText(Dts.Variables["User::LogFilePath"].Value.ToString() + "\\" + "ErrorLog_" + currentTs + ".log"))
                {
                    sw.WriteLine(ex.ToString());
                    Dts.TaskResult = (int)ScriptResults.Failure;
                }

            }
            finally
            {
                if (conn != null)
                    conn.Dispose();
            }
            return dt;
        }

        private void createTextFilesForTableList(DataTable dataTable, string destFilePath, string colDelimiter, string fileExtension)
        {
            SqlConnection conn = new SqlConnection();

            try
            {
                conn = (SqlConnection)(Dts.Connections["CONN"].AcquireConnection(Dts.Transaction));

                foreach (DataRow dtRow in dataTable.Rows)
                {
                    string schName = "";
                    string tblName = "";
                    object[] array = dtRow.ItemArray;
                    schName = array[0].ToString();
                    tblName = array[1].ToString();

                    string fileFullName = destFilePath + "\\" + schName + "_" + tblName + fileExtension;

                    if (File.Exists(fileFullName) == true)
                        File.Delete(fileFullName);

                    string sqlDataQuery = "Select * From [" + schName + "].[" + tblName + "]";
                    SqlCommand sqlCmdData = new SqlCommand(sqlDataQuery, conn);
                    DataTable dtData = new DataTable();
                    dtData.Load(sqlCmdData.ExecuteReader());

                    StreamWriter sw = null;

                    sw = new StreamWriter(fileFullName, false);

                    int ColumnCount = dtData.Columns.Count;
                    for (int ic = 0; ic < ColumnCount; ic++)
                    {
                        sw.Write("\"" + dtData.Columns[ic] + "\"");
                        if (ic < ColumnCount - 1)
                        {
                            sw.Write(colDelimiter);
                        }
                    }
                    sw.Write(sw.NewLine);

                    foreach (DataRow dr in dtData.Rows)
                    {
                        for (int ir = 0; ir < ColumnCount; ir++)
                        {
                            if (!Convert.IsDBNull(dr[ir]))
                            {
                                sw.Write("\"" + dr[ir].ToString() + "\"");
                            }
                            if (ir < ColumnCount - 1)
                            {
                                sw.Write(colDelimiter);
                            }
                        }
                        sw.Write(sw.NewLine);

                    }

                    sw.Close();
                }
                //}
            }
            catch (Exception ex)
            {
                using (StreamWriter sw = File.CreateText(Dts.Variables["User::LogFilePath"].Value.ToString() + "\\" + "ErrorLog_" + currentTs + ".log"))
                {
                    sw.WriteLine(ex.ToString());
                    Dts.TaskResult = (int)ScriptResults.Failure;
                }

            }
            finally
            {

            }

        }

        enum ScriptResults
        {
            Success = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Success,
            Failure = Microsoft.SqlServer.Dts.Runtime.DTSExecResult.Failure
        };
        #endregion

    }
}