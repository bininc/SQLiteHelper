using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.SQLite;
using System.Data;
using System.Diagnostics;
using System.Collections;
using System.Data.Common;
using DBHelper;
using DBHelper.BaseHelper;
using CommonLib;
using CommLiby;
using DBHelper.Common;

namespace SQLiteHelper
{
    public class SQLiteHelper : DBHelper.DBHelper
    {
        /// <summary>
        /// 数据库操作类
        /// </summary>
        /// <param name="connectionString">数据库连接字符串</param>
        public SQLiteHelper(string connectionString) : base(connectionString)
        {

        }

        public override int ExecuteNonQuery(CommandInfo cmdInfo)
        {
            //Create a connection
            using (SQLiteConnection connection = new SQLiteConnection())
            {
                try
                {
                    connection.ConnectionString = ConnectionString;
                    // Create a new SQLite command
                    SQLiteCommand cmd = new SQLiteCommand();
                    //Prepare the command
                    PrepareCommand(cmd, connection, cmdInfo.Text, null, cmdInfo.Type, cmdInfo.Parameters);

                    //Execute the command
                    int val = cmd.ExecuteNonQuery();
                    cmd.Dispose();
                    return val;
                }
                catch (Exception ex)
                {
                    if (Debugger.IsAttached)
                        throw new Exception(ex.Message);
                    else
                        LogHelper.Error(ex, "SQLiteHelper.ExecuteNonQuery");
                    return -1;
                }
            }
        }

        public override int ExecuteProcedure(string storedProcName, params DbParameter[] parameters)
        {
            using (SQLiteConnection conn = new SQLiteConnection())
            {
                conn.ConnectionString = ConnectionString;
                SQLiteCommand cmd = new SQLiteCommand();
                try
                {
                    PrepareCommand(cmd, conn, storedProcName, null, CommandType.StoredProcedure, parameters);
                    int i = cmd.ExecuteNonQuery();
                    return i;
                }
                catch (Exception ex)
                {
                    if (Debugger.IsAttached)
                        throw new Exception(ex.Message);
                    else
                        LogHelper.Error(ex, "SQLiteHelper.ExecuteProcedure");
                    return -1;
                }
                finally
                {
                    cmd.Dispose();
                }
            }
        }

        public override int ExecuteProcedureTran(string storedProcName, params DbParameter[] parameters)
        {
            using (SQLiteConnection conn = new SQLiteConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                SQLiteTransaction tran = conn.BeginTransaction();
                SQLiteCommand cmd = new SQLiteCommand();
                try
                {
                    PrepareCommand(cmd, conn, storedProcName, tran, CommandType.StoredProcedure, parameters);
                    int i = cmd.ExecuteNonQuery();
                    tran.Commit();
                    return i;
                }
                catch (Exception ex)
                {
                    tran.Rollback();
                    if (Debugger.IsAttached)
                        throw new Exception(ex.Message);
                    else
                        LogHelper.Error(ex, "SQLiteHelper.ExecuteProcedureTran");
                    return -1;
                }
                finally
                {
                    tran.Dispose();
                    cmd.Dispose();
                }
            }
        }

        public override DbDataReader ExecuteReader(string sqlString, params DbParameter[] dbParameter)
        {
            SQLiteConnection conn = new SQLiteConnection();
            conn.ConnectionString = ConnectionString;
            SQLiteCommand cmd = new SQLiteCommand();
            SQLiteDataReader rdr = null;
            try
            {
                //Prepare the command to execute
                PrepareCommand(cmd, conn, sqlString, null, CommandType.Text, dbParameter);
                rdr = cmd.ExecuteReader(CommandBehavior.CloseConnection);
                return rdr;
            }
            catch (Exception ex)
            {
                System.Diagnostics.Debug.WriteLine(ex);
                rdr?.Close();
                cmd.Dispose();
                conn.Close();
            }
            return null;
        }

        public override int ExecuteSqlsTran(List<CommandInfo> cmdList, int num = 5000)
        {
            if (cmdList == null || cmdList.Count == 0) return -1;

            using (SQLiteConnection conn = new SQLiteConnection())
            {
                conn.ConnectionString = ConnectionString;
                conn.Open();
                int allCount = 0;

                //Stopwatch watch = new Stopwatch();
                while (cmdList.Count > 0)
                {
                    //watch.Reset();
                    //watch.Start();                
                    var submitSQLs = cmdList.Take(num);
                    SQLiteTransaction tx = conn.BeginTransaction();
                    SQLiteCommand cmd = new SQLiteCommand();
                    int count = 0;
                    try
                    {
                        foreach (CommandInfo c in submitSQLs)
                        {
                            try
                            {
                                if (!string.IsNullOrEmpty(c.Text))
                                {
                                    PrepareCommand(cmd, conn, c.Text, tx, c.Type, c.Parameters);
                                    int res = cmd.ExecuteNonQuery();
                                    if (c.EffentNextType == EffentNextType.ExcuteEffectRows && res == 0)
                                    {
                                        throw new Exception("SQLite:违背要求" + c.Text + "必须有影响行");
                                    }
                                    count += res;
                                }
                            }
                            catch (Exception ex)
                            {
                                if (c.FailRollback)
                                    throw ex;
                            }
                        }
                        tx.Commit();
                    }
                    catch (Exception ex)
                    {
                        tx.Rollback();
                        if (Debugger.IsAttached)
                            throw new Exception(ex.Message);
                        else
                            LogHelper.Error(ex, "SQLiteHelper.ExecuteSqlsTran");
                        count = 0;
                        break;
                    }
                    finally
                    {
                        cmd.Dispose();
                        tx.Dispose();
                        allCount += count;
                    }

                    int removeCount = cmdList.Count >= num ? num : cmdList.Count; //每次最多执行1000行
                    cmdList.RemoveRange(0, removeCount);
                    //watch.Stop();
                    //Console.WriteLine(cmdList.Count + "-" + allCount + "-" + watch.ElapsedMilliseconds / 1000);
                }
                return allCount;
            }
        }

        public override int ExecuteSqlTran(CommandInfo cmdInfo)
        {
            //Create a connection
            using (SQLiteConnection connection = new SQLiteConnection())
            {
                connection.ConnectionString = ConnectionString;
                connection.Open();
                SQLiteTransaction tran = connection.BeginTransaction();
                try
                {
                    // Create a new SQLite command
                    SQLiteCommand cmd = new SQLiteCommand();
                    //Prepare the command
                    PrepareCommand(cmd, connection, cmdInfo.Text, tran, cmdInfo.Type, cmdInfo.Parameters);

                    //Execute the command
                    int val = cmd.ExecuteNonQuery();
                    cmd.Dispose();
                    return val;
                }
                catch (Exception ex)
                {
                    tran.Rollback();
                    if (Debugger.IsAttached)
                        throw new Exception(ex.Message);
                    else
                        LogHelper.Error(ex, "SQLiteHelper.ExecuteSqlTran");
                    return -1;
                }
                finally
                {
                    tran.Dispose();
                }
            }
        }

        public override DataBaseType GetCurrentDataBaseType()
        {
            return DataBaseType.Sqlite;
        }

        public override string GetPageRowNumSql(string dataSql, int startRowNum, int endRowNum)
        {
            return $"{dataSql} limit {endRowNum - startRowNum} offset {startRowNum}";
        }

        public override string GetRowLimitSql(string dataSql, int rowLimit)
        {
            return $"{dataSql} limit {rowLimit}";
        }

        public override DataSet Query(string sqlString, params DbParameter[] dbParameter)
        {
            DataSet ds = new DataSet("ds");
            using (SQLiteConnection connection = new SQLiteConnection(_connectionString))
            {
                try
                {
                    using (SQLiteCommand cmd = new SQLiteCommand())
                    {
                        PrepareCommand(cmd, connection, sqlString, null, CommandType.Text, dbParameter);
                        using (SQLiteDataAdapter command = new SQLiteDataAdapter())
                        {
                            command.SelectCommand = cmd;
                            command.Fill(ds, "dt");
                        }
                    }
                }
                catch (Exception ex)
                {
                    if (System.Diagnostics.Debugger.IsAttached)
                        throw new Exception(ex.Message);
                    else
                        LogHelper.Error(ex, "SQLiteHelper.Query");
                }
            }
            return ds;
        }

        public override object QueryScalar(string sqlString, CommandType cmdType = CommandType.Text, params DbParameter[] dbParameter)
        {
            using (SQLiteConnection connection = new SQLiteConnection(_connectionString))
            {
                using (SQLiteCommand cmd = new SQLiteCommand())
                {
                    PrepareCommand(cmd, connection, sqlString, null, cmdType, dbParameter);
                    object obj = cmd.ExecuteScalar();
                    if ((Equals(obj, null)) || (Equals(obj, DBNull.Value)))
                    {
                        return null;
                    }
                    else
                    {
                        return obj;
                    }
                }
            }
        }

        public override bool TableExists(string tableName)
        {
            return Exists($"select count(*) from sqlite_master where type='table' and name='{tableName}'");
        }

        public override bool TestConnectionString()
        {
            DataTable dt = QueryTable("select 1");
            if (dt.IsNotEmpty())
            {
                if (dt.Rows[0][0].ToString() == "1")
                {
                    return true;
                }
            }
            return false;
        }
    }
}
