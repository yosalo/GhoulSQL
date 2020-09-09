using Dapper;
using MySql.Data.MySqlClient;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace GhoulSQL.Sql
{
    public class DAOBase
    {
        protected string _connection;
        protected IEnumerable<string> _slaveConnections;
        private IDbTransaction _transaction = null;

        protected bool Debug
        {
            get
            {
                try
                {
                    if (Configs.DB == null) return false;
                    return Configs.DB.Debug;
                }
                catch
                {
                    return false;
                }
            }
        }
        private void PrintSql(string sql, object args)
        {
            if (this.Debug)
            {
                if (args is DynamicParameters)
                {
                    var param = (DynamicParameters)args;
                    var parameters = new Dictionary<string, object>();
                    foreach (var parameterName in param.ParameterNames)
                        parameters.Add(parameterName, param.Get<object>(parameterName));
                    args = parameters;
                }
                //Logging.Log.Debug<DAOBase>(sql, args, "sql_trace");
            }
        }

        public DAOBase() : this(Configs.DB.MasterConnection, Configs.DB.SlaveConnections) { }
        public DAOBase(string connection)
        {
            this._connection = connection;
        }
        /// <summary>
        /// Initializes a new instance of the <see cref="DAOBase"/> class.
        /// </summary>
        /// <param name="connection">The connection.</param>
        /// <param name="slaveConnections">Slave db.</param>
        public DAOBase(string connection, IEnumerable<string> slaveConnections) : this(connection)
        {
            this._slaveConnections = slaveConnections;
        }

        /// <summary>
        /// News the database connection.
        /// </summary>
        /// <param name="useSlave">use slave or not</param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException">Db connection can not be null</exception>
        public IDbConnection NewDbConnection(bool useSlave = false)
        {
            var connection = this._connection;
            Dapper.DefaultTypeMap.MatchNamesWithUnderscores = true;
            if (useSlave && this._slaveConnections != null && this._slaveConnections.Count() > 0)
                connection = this._slaveConnections.Random();
            if (string.IsNullOrEmpty(connection))
                throw new SystemException("Db connection can not be null");
            return new MySqlConnection(connection);
        }

        public T Transaction<T>(Func<IDbTransaction, T> action, Predicate<T> predicate = null)
        {
            using (var connection = this.NewDbConnection())
            {
                connection.Open();
                var transaction = OpenTransaction(connection);
                try
                {
                    var ret = action(transaction);

                    if (predicate != null && !predicate(ret))
                    {
                        RollBackTransaction();
                        return ret;
                    }

                    CommitTransaction();
                    return ret;
                }
                catch (Exception ex)
                {
                    RollBackTransaction();
                    //Logging.Log.Exception<DAOBase>(ex, "Transaction exception");
                    //Thorw exception 
                    throw ex;
                }
                finally
                {
                    transaction.Dispose();
                    transaction = null;
                    connection.Close();
                    connection.Dispose();
                }
            }
        }

        public void Transaction(params Action<IDbTransaction>[] actions)
        {
            using (var connection = this.NewDbConnection())
            {
                connection.Open();
                var transaction = OpenTransaction(connection);
                try
                {
                    foreach (var action in actions)
                        action(transaction);
                    CommitTransaction();
                }
                catch (Exception ex)
                {
                    RollBackTransaction();
                    //Logging.Log.Exception<DAOBase>(ex, "Transaction exception");
                    throw ex;
                }
                finally
                {
                    transaction.Dispose();
                    transaction = null;
                }
            }
        }
        public IDbTransaction OpenTransaction(IDbConnection connection = null)
        {
            if (connection == null)
            {
                connection = new MySqlConnection(this._connection);
                connection.Open();
            }

            this._transaction = connection.BeginTransaction();
            return _transaction;
        }
        public void CommitTransaction()
        {
            if (_transaction == null)
                throw new ApplicationException("When committing the transaction, the transaction cannot be empty!");
            _transaction.Commit();
        }
        public void RollBackTransaction()
        {
            if (_transaction == null)
                throw new ApplicationException("When committing the transaction, the transaction cannot be empty!");
            _transaction.Rollback();
        }

        #region 便捷调用

        protected bool IsContain(string tableName, string fieldName, object value)
        {
            var builder = new SqlBuilder().Table(tableName)
                .Where($"{fieldName}=@{fieldName.Replace("`", "")}", value);

            using (var conn = this.NewDbConnection())
            {
                var sql = builder.ToSql();
                this.PrintSql(sql, builder.Parameters);
                return conn.Query(sql, builder.Parameters).Count() > 0;
            }
        }

        protected bool IsContain(string tableName, IDictionary<string, object> parameters)
        {
            var builder = new SqlBuilder().Table(tableName);
            foreach (var item in parameters)
                builder.Where($"{item.Key}=@{item.Key.Replace("`", "")}", item.Value);

            using (var conn = this.NewDbConnection())
            {
                var sql = builder.ToSql();
                this.PrintSql(sql, builder.Parameters);
                return conn.Query(sql, builder.Parameters).Count() > 0;
            }
        }

        protected T Insert<T>(string sql, DynamicParameters parameters)
        {
            this.PrintSql(sql, parameters);
            return this.Transaction<T>(it => it.Connection.ExecuteScalar<T>(sql, parameters));
        }

        protected T ExecuteScalar<T>(string sql, DynamicParameters parameters)
        {
            using (var conn = this.NewDbConnection())
            {
                return this.ExecuteScalar<T>(conn, sql, parameters);
            }
        }
        protected T ExecuteScalar<T>(IDbConnection conn, string sql, DynamicParameters parameters)
        {
            if (conn == null)
                return this.ExecuteScalar<T>(sql, parameters);

            this.PrintSql(sql, parameters);
            return conn.ExecuteScalar<T>(sql, parameters);
        }

        protected int Execute(string sql, object parameters)
        {
            using (var conn = this.NewDbConnection())
            {
                return this.Execute(conn, sql, parameters);
            }
        }

        protected int Execute(IDbConnection conn, string sql, object parameters)
        {
            if (conn == null)
                return this.Execute(sql, parameters);

            this.PrintSql(sql, parameters);
            return conn.Execute(sql, parameters);
        }

        #region Single
        protected T Single<T>(string sql, DynamicParameters parameters, bool useSlave = false)
        {
            using (var conn = this.NewDbConnection(useSlave))
            {
                return this.Single<T>(conn, sql, parameters, useSlave);
            }
        }

        protected T Single<T>(IDbConnection conn, string sql, DynamicParameters parameters, bool useSlave = false)
        {
            if (conn == null)
                return this.Single<T>(sql, parameters, useSlave);

            //if (!sql.ToLower().EndsWith("limit 1"))
            //    sql += " limit 1";

            this.PrintSql(sql, parameters);
            return conn.QuerySingleOrDefault<T>(sql, parameters);
        }

        protected R Single<T, T2, R>(string sql, DynamicParameters parameters, Func<T, T2, R> map, string splitOn = "Id")
        {
            return this.Query<T, T2, R>(sql, parameters, map, splitOn).FirstOrDefault();
        }

        protected R Single<T, T2, R>(IDbConnection conn, string sql, DynamicParameters parameters, Func<T, T2, R> map, string splitOn = "Id")
        {
            if (conn == null)
                return this.Single<T, T2, R>(sql, parameters, map, splitOn);

            return this.Query<T, T2, R>(conn, sql, parameters, map, splitOn).FirstOrDefault();
        }

        #region Async
        protected Task<T> SingleAsync<T>(string sql, object parameters, bool useSlave = false)
        {
            using (var conn = this.NewDbConnection(useSlave))
            {
                return this.SingleAsync<T>(conn, sql, parameters, useSlave);
            }
        }

        protected Task<T> SingleAsync<T>(IDbConnection conn, string sql, object parameters, bool useSlave = false)
        {
            if (conn == null)
                return this.SingleAsync<T>(sql, parameters, useSlave);

            //if (!sql.ToLower().EndsWith("limit 1"))
            //    sql += " limit 1";

            this.PrintSql(sql, parameters);
            return conn.QueryFirstAsync<T>(sql, parameters);
        }
        #endregion

        #endregion

        protected IEnumerable<T> Query<T>(string sql, object parameters, bool useSlave = false)
        {
            using (var conn = this.NewDbConnection(useSlave))
            {
                if (conn.State != ConnectionState.Open)
                    conn.Open();
                this.PrintSql(sql, parameters);
                return conn.Query<T>(sql, parameters);
            }
        }

        protected IEnumerable<T> Query<T>(IDbConnection conn, string sql, object parameters, bool useSlave = false)
        {
            if (conn == null)
                return this.Query<T>(sql, parameters, useSlave);
            this.PrintSql(sql, parameters);

            return conn.Query<T>(sql, parameters);
        }

        protected IEnumerable<R> Query<T, T2, R>(string sql, DynamicParameters parameters, Func<T, T2, R> map, string splitOn = "id")
        {
            using (var conn = this.NewDbConnection())
            {
                this.PrintSql(sql, parameters);
                return conn.Query<T, T2, R>(sql, map, parameters, splitOn: splitOn);
            };
        }

        protected IEnumerable<R> Query<T, T2, R>(IDbConnection conn, string sql, DynamicParameters parameters, Func<T, T2, R> map, string splitOn = "id")
        {
            if (conn == null)
                return this.Query<T, T2, R>(sql, parameters, map, splitOn);

            this.PrintSql(sql, parameters);
            return conn.Query<T, T2, R>(sql, map, parameters, splitOn: splitOn);
        }

        /// <summary>
        /// Page query
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="sql">The SQL.</param>
        /// <param name="parameters">The parameters.</param>
        /// <param name="pageIndex">Index of the page.</param>
        /// <param name="pageSize">Size of the page.</param>
        /// <param name="mapTotals">yes or not</param>
        /// <param name="useSlave">if set to <c>true</c> [use slave].</param>
        /// <returns></returns>
        protected Pagination<T> Pagination<T>(string sql, DynamicParameters parameters,
            int pageIndex, int pageSize,
            Func<dynamic, Hashtable> totalsMap = null, bool useSlave = false)
        {
            using (var conn = this.NewDbConnection(useSlave))
            {
                this.PrintSql(sql, parameters);

                var multi = conn.QueryMultiple(sql, parameters);
                var data = multi.Read<T>();
                int totalCount;
                Hashtable totalsTable = null;
                if (totalsMap != null)
                {
                    var totals = multi.ReadFirstOrDefault();
                    totalCount = (int)totals.Count;
                    totalsTable = totalsMap(totals);
                }
                else
                    totalCount = multi.Read<int>().Single();

                return new Pagination<T>(data, pageIndex, pageSize, totalCount, totalsTable);
            }
        }

        protected Pagination<R> Pagination<T, T2, R>(string sql, DynamicParameters parameters, int pageIndex, int pageSize,
            Func<T, T2, R> map, string splitOn = "id",
            Func<dynamic, Hashtable> totalsMap = null, bool useSlave = false)
        {
            using (var conn = this.NewDbConnection(useSlave))
            {
                var lastSemicolonIndex = sql.LastIndexOf(";");
                var querySql = sql.Substring(0, lastSemicolonIndex + 1);
                var totalSql = sql.Substring(lastSemicolonIndex + 1);

                var data = this.Query<T, T2, R>(conn, querySql, parameters, map, splitOn);

                this.PrintSql(totalSql, parameters);
                int totalCount;
                Hashtable totalsTable = null;
                if (totalsMap != null)
                {
                    var totals = conn.QuerySingle(totalSql, parameters);
                    totalCount = (int)totals.Count;
                    totalsTable = totalsMap(totals);
                }
                else
                    totalCount = conn.QuerySingle<int>(totalSql, parameters);

                return new Pagination<R>(data, pageIndex, pageSize, totalCount, totalsTable);
            }
        }
        #endregion

        protected T GetEntityBykey<T>(string keyName, object keyValue) where T : class
        {
            var builder = new SqlBuilder().Table(nameof(T), "et").Value($"et.{keyName}", keyValue);
            return this.Single<T>(builder.ToSql(), builder.Parameters);
        }

        /// <summary>
        /// get data
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="builderFunc"></param>
        /// <param name="conditions"></param>
        /// <returns></returns>
        protected T GetEntity<T>(Func<ConditionHash, SqlBuilder> builderFunc, ConditionHash conditions) where T : class
        {
            return GetEntity<T>(null, builderFunc, conditions);
        }

        protected T GetEntity<T>(IDbConnection conn, Func<ConditionHash, SqlBuilder> builderFunc, ConditionHash conditions) where T : class
        {
            if (builderFunc == null)
            {
                return default(T);
            }

            var builder = builderFunc(conditions);

            builder.Limit(0, 1);

            return this.Single<T>(conn, builder.ToSql(), builder.Parameters);
        }

        /// <summary>
        /// list data
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="builderFunc"></param>
        /// <param name="conditions"></param>
        /// <returns></returns>
        protected IEnumerable<T> ListEntities<T>(Func<ConditionHash, SqlBuilder> builderFunc, ConditionHash conditions) where T : class
        {
            if (builderFunc == null)
            {
                return default(IEnumerable<T>);
            }

            var builder = builderFunc(conditions);
            return this.Query<T>(builder.ToSql(), builder.Parameters);
        }

        /// <summary>
        /// page data
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="builderFunc"></param>
        /// <param name="conditions"></param>
        /// <returns></returns>
        protected Pagination<T> PageEntities<T>(Func<ConditionHash, SqlBuilder> builderFunc, ConditionHash conditions) where T : class
        {
            if (builderFunc == null)
            {
                return default(Pagination<T>);
            }

            var builder = builderFunc(conditions).Page(conditions.PageIndex, conditions.PageSize);

            return this.Pagination<T>(builder.ToSql(), builder.Parameters, conditions.PageIndex, conditions.PageSize);
        }

        /// <summary>
        /// Count
        /// </summary>
        /// <param name="builderFunc"></param>
        /// <param name="conditions"></param>
        /// <returns></returns>
        protected int QueryCount(Func<ConditionHash, SqlBuilder> builderFunc, ConditionHash conditions)
        {
            var builder = builderFunc(conditions)
                .ClearSelect().Select("COUNT(0) as COUNT");

            return this.ExecuteScalar<int>(builder.ToSql(), builder.Parameters);
        }

        protected int QueryCount(SqlBuilder sqlBuilder)
        {
            var builder = sqlBuilder
                .ClearSelect()
                .Select("COUNT(0) as COUNT");

            return this.ExecuteScalar<int>(builder.ToSql(), builder.Parameters);
        }
    }
}
