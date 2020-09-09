using Dapper;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.RegularExpressions;
using System.Linq;

namespace GhoulSQL.Sql
{
    /// <summary>
    /// 这是一个SQL生成工具类，避免繁琐的SQL拼接工作
    /// </summary>
    public class SqlBuilder
    {
        /// <summary>
        /// 拼接模式（SqlBuilder会根据不同的拼接模式生成不同的Sql）
        /// </summary>
        enum Mode
        {
            SELECT = 0,
            INSERT = 1,
            UPDATE = 2,
            DELETE = 3,
            REPLACE = 4,
        }

        Mode _mode = Mode.SELECT;
        bool _selectScopeIdentify = false;
        DynamicParameters _parameters = new DynamicParameters();
        IDictionary<string, List<object>> sqlParts;
        public SqlBuilder()
        {
            this.sqlParts = new Dictionary<string, List<object>>();
        }
        /// <summary>
        /// SqlBuilder的快速声明方式，通过此方法，您可以快速调用，例如:
        /// SqlBuilder.Instance().Table....而不需要New SqlBuilder().Table...
        /// </summary>
        /// <returns></returns>
        public static SqlBuilder Instance()
        {
            return new SqlBuilder();
        }

        private void AddSqlPart(string name, object value)
        {
            if (!this.sqlParts.ContainsKey(name))
                this.sqlParts.Add(name, new List<object>());
            this.sqlParts[name].Add(value);
        }
        private void RemoveSqlPart(string name)
        {
            if (this.sqlParts.ContainsKey(name))
                this.sqlParts.Remove(name);
        }
        private List<object> GetSqlPart(string name)
        {
            if (!this.sqlParts.ContainsKey(name)) return null;
            return this.sqlParts[name];
        }

        /// <summary>
        /// 用于生成插入语句，只允许调用一次
        /// </summary>
        /// <param name="tableName">要插入的表名.</param>
        /// <returns></returns>
        public SqlBuilder Insert(string tableName)
        {
            this._mode = Mode.INSERT;
            this.AddSqlPart("table", tableName);
            return this;
        }

        /// <summary>
        /// Duplicates the specified update keys.
        /// </summary>
        /// <param name="updateKeys">需要更新的Key.</param>
        /// <param name="replace">是否替换，true则直接替换，false则 = oldValue+newValue </param>
        /// <returns></returns>
        public SqlBuilder Duplicate(string fieldName, string fieldValue)
        {
            if (!this.IsInsert) return this;
            if (string.IsNullOrEmpty(fieldName) || string.IsNullOrEmpty(fieldValue)) return this;

            this.AddSqlPart("duplicate_update", new KeyValuePair<string, string>(fieldName, fieldValue));
            return this;
        }

        /// <summary>
        /// Insert语句的数据，在Insert之后调用，可多次调用，代表插入不同列的值.
        /// 例如: SqlBuilder().Insert('Table').Value("Name","Name").Value("Age",13)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName">字段名.</param>
        /// <param name="fieldValue">字段值.</param>
        /// <param name="isLiteral">是否原生，例如在调用GETDATE()等方法是，传入“GETDATE()”会直接写在SQL中.</param>
        /// <returns></returns>
        public SqlBuilder Value<T>(string fieldName, T fieldValue, bool isLiteral = false)
        {
            if (isLiteral)
            {
                this.AddSqlPart("insert_value", new object[] { fieldName, fieldValue });
            }
            else
            {
                var parameterName = fieldName.Replace("`", "");
                this.AddSqlPart("insert_value", new object[] { fieldName, "@" + parameterName });
                this._parameters.Add(parameterName, fieldValue);
            }
            return this;
        }

        /// <summary>
        /// 是否查询更改（用在Insert语句中）
        /// </summary>
        /// <returns></returns>
        public SqlBuilder SelectIdentity()
        {
            if (this._mode != Mode.INSERT) return this;
            this._selectScopeIdentify = true;
            return this;
        }

        /// <summary>
        /// 用户生成Update语句，只允许调用一次
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public SqlBuilder Update(string tableName)
        {
            this._mode = Mode.UPDATE;
            this.AddSqlPart("table", tableName);
            return this;
        }

        /// <summary>
        /// 在生成Update语句时使用。例如:
        /// SqlBuilder.Instance().Update("Talbe").Set("Name","New Name").Set...
        /// <typeparam name="T"></typeparam>
        /// <param name="fieldName">字段名.</param>
        /// <param name="fieldValue">字段值.</param>
        /// <param name="isLiteral">是否原生，例如在调用GETDATE()等方法是，传入“GETDATE()”会直接写在SQL中.</param>
        /// <returns></returns>
        public SqlBuilder Set<T>(string fieldName, T fieldValue, bool isLiteral = false)
        {
            //if (fieldValue == null) return this;

            if (isLiteral)
            {
                this.AddSqlPart("update_value", string.Format("{0}={1}", fieldName, fieldValue));
            }
            else
            {
                string setStatement = string.Empty;
                // 纯字段
                var isField = Regex.IsMatch(fieldName, @"^\w+$");
                if (isField)
                {
                    setStatement = $"`{fieldName}`=@{fieldName}";
                    this._parameters.Add(fieldName.Replace("`", ""), fieldValue);
                }
                else
                {
                    var parameterName = this.ParseParametersName(fieldName).FirstOrDefault().Replace("@", "").Replace("`", "");
                    if (string.IsNullOrEmpty(parameterName)) return this;
                    setStatement = $"`{parameterName}`=@{parameterName}";
                    this._parameters.Add(parameterName, fieldValue);
                }
                this.AddSqlPart("update_value", setStatement);

            }
            return this;
        }

        /// <summary>
        /// 在生成Update语句时使用。例如:
        /// SqlBuilder.Instance().Update("Talbe").Set(CheckName(),"Name","New Name").Set...
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="isExtend">是否扩展Set语句，如果为True，则后续增加Where，使用本重载可避免判断If(condition.IsContains<T>..),增加连贯性 ,.</param>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="fieldValue">The field value.</param>
        /// <param name="isLiteral">if set to <c>true</c> [is literal].</param>
        /// <returns></returns>
        public SqlBuilder Set<T>(bool isExtend, string fieldName, T fieldValue, bool isLiteral = false)
        {
            if (isExtend)
                return this.Set(fieldName, fieldValue, isLiteral);

            return this;
        }

        /// <summary>
        /// 用于生成删除语句，只允许调用一次
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns></returns>
        public SqlBuilder Delete(string tableName)
        {
            this._mode = Mode.DELETE;

            this.AddSqlPart("table", tableName);
            return this;
        }

        /// <summary>
        /// 用于生成查询语句设置表名，可多次调用。例如:
        /// SqlBuilder.Instance().Table("Table1").Table("Table2")
        /// 这样会生成Sql select ... from Table1,Table2...
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="alias">The alias.</param>
        /// <returns></returns>
        public SqlBuilder Table(string tableName, string alias = null)
        {
            var tableStr = string.IsNullOrEmpty(alias) ? tableName : string.Format("{0} {1}", tableName, alias);
            this.AddSqlPart("table", tableStr);
            return this;
        }

        public SqlBuilder Join(string table, string alias, string onCondition, string joinType = "")
        {
            var joinStr = string.Format("{0} JOIN {1} {2} ON {3}", joinType, table, alias, onCondition);
            this.AddSqlPart("join", joinStr);
            return this;
        }

        public SqlBuilder StraightJoin(string table, string alias, string onCondition)
        {
            var joinStr = string.Format(" STRAIGHT_JOIN {0} {1} ON {2}", table, alias, onCondition);
            this.AddSqlPart("join", joinStr);
            return this;
        }

        public SqlBuilder LeftJoin(string table, string alias, string onCondition)
        {
            return Join(table, alias, onCondition, " LEFT");
        }

        public SqlBuilder RightJoin(string table, string alias, string onCondition)
        {
            return Join(table, alias, onCondition, " Right");
        }

        public SqlBuilder InnerJoin(string table, string alias, string onCondition)
        {
            return Join(table, alias, onCondition, " Inner");
        }

        //寻找参数正则
        Regex _parameterReg = new Regex(@"@\w+", RegexOptions.IgnoreCase);
        private IEnumerable<string> ParseParametersName(string plainText)
        {
            var matches = _parameterReg.Matches(plainText);
            foreach (Match match in matches)
                yield return match.Groups[0].Value;
        }

        /// <summary>
        /// Where条件，可在Select,Update,Delete模式下使用。例如:
        /// Where("Name=@Name","Hello").Where("Age=1")
        /// </summary>
        /// <param name="isExtend">是否扩展Where语句，如果为True，则后续增加Where，使用本重载可避免判断If(condition.IsContains<T>..),增加连贯性 ,</param>
        /// <param name="condition">The condition.</param>
        /// <param name="args">The arguments.</param>
        /// <returns></returns>
        public SqlBuilder Where(bool isExtend, string condition, params object[] args)
        {
            if (isExtend)
                return this.Where(condition, args);

            return this;
        }

        /// <summary>
        /// Where条件，可在Select,Update,Delete模式下使用。例如:
        /// Where("Name=@Name","Hello").Where("Age=1")
        /// </summary>
        /// <param name="condition">Where条件，条件中带有@时认为需要传入参数，该方法会自动生成对应的SqlParameter..</param>
        /// <param name="args"参数，数量必须等于条件中带@变量的数量，否则此条件不成立。</param>
        /// <returns></returns>
        public SqlBuilder Where(string condition, params object[] args)
        {
            return this.WhereEnumerable(condition, args);
        }

        public SqlBuilder WhereEnumerable<T>(string condition, IEnumerable<T> args)
        {
            //获取条件中的参数
            //var matches = _parameterReg.Matches(condition);
            //if (matches == null || matches.Count != args.Length) return this;
            var parameters = ParseParametersName(condition).ToList();

            // 如果参数数量等于条件中参数数量，则说明未使用匿名实例，按顺序匹配参数
            if (parameters.Count != 0 && (args.Count() == parameters.Count))
            {
                for (var i = 0; i < parameters.Count; i++)
                    this._parameters.Add(parameters[i].Replace("`", ""), args.ElementAt(i));
            }
            else
            {
                foreach (var arg in args)
                    this._parameters.AddDynamicParams(arg);
            }

            this.AddSqlPart("where", condition);
            return this;
        }

        public SqlBuilder WhereIn<T>(bool isExtend, string fieldName, IEnumerable<T> args)
        {
            if (isExtend)
                this.WhereIn(fieldName, args);
            return this;
        }

        public SqlBuilder WhereIn<T>(string fieldName, IEnumerable<T> args)
        {
            string[] inArgs = new string[args.Count()];
            for (var i = 0; i < args.Count(); i++)
                inArgs[i] = $"@{fieldName.Replace(".", "_")}_IN_{i}";
            var conditions = $"{fieldName} in ({string.Join(",", inArgs)})";
            return this.WhereEnumerable(conditions, args);
        }

        public SqlBuilder Select(params string[] selects)
        {
            foreach (var select in selects)
                this.AddSqlPart("select", select);
            return this;
        }

        /// <summary>
        /// 清除已声明的Select语句，常用于对复用SQL Builder的复写。
        /// </summary>
        /// <returns></returns>
        public SqlBuilder ClearSelect()
        {
            this.RemoveSqlPart("select");
            return this;
        }

        public SqlBuilder ForUpdate()
        {
            this.AddSqlPart("forupdate", true);
            return this;
        }

        /// <summary>
        /// 分页
        /// </summary>
        /// <param name="pageIndex">页码.</param>
        /// <param name="pageSize">页查询数.</param>
        /// <param name="isCountTotal">是否附加Count语句，True的话最终生成的语句中多一个SELECT COUNT(0) ...语句.</param>
        /// <param name="totals">用于做聚合查询的语句，如: Sum(Amount) as Amount,Sum(Balance) as Balance</param>
        /// <returns></returns>
        public SqlBuilder Page(int pageIndex, int pageSize, bool isCountTotal = true, string totals = "")
        {
            int skip = (pageIndex - 1) * pageSize;
            this.AddSqlPart("page_args", skip);
            this.AddSqlPart("page_args", pageSize);
            this.AddSqlPart("page_args", isCountTotal);
            this.AddSqlPart("page_args", totals);
            return this;
        }

        public SqlBuilder OrderBy(string field, string orderType = "DESC")
        {
            if (string.IsNullOrEmpty(field))
                field = "1";

            string orderStr = string.Format("{0} {1}", field, orderType);
            this.AddSqlPart("orderby", orderStr);

            return this;
        }

        public SqlBuilder GroupBy(params string[] fields)
        {
            foreach (var field in fields)
                this.AddSqlPart("groupby", field);

            return this;
        }

        public SqlBuilder Limit(int offset, int rows)
        {
            this.AddSqlPart("limit", offset);
            this.AddSqlPart("limit", rows);
            return this;
        }

        public string ToSql()
        {
            switch (this._mode)
            {
                case Mode.INSERT: return PasrseInsertSql();
                case Mode.UPDATE: return ParseUpdateSql();
                case Mode.DELETE: return ParseDeleteSql();
                default: return ParseSelectSql();
            }
        }

        private string PasrseInsertSql()
        {
            var tableParts = this.GetSqlPart("table");
            if (tableParts == null || tableParts.Count != 1) throw new Exception("Insert require a table.Only One!");
            var table = tableParts[0].ToString();

            var valueParts = this.GetSqlPart("insert_value");
            if (valueParts == null || valueParts.Count == 0) throw new Exception("Insert require name and value");

            string[] keys = new string[valueParts.Count];
            string[] values = new string[valueParts.Count];
            for (var i = 0; i < valueParts.Count; i++)
            {
                var valArr = (object[])valueParts[i];
                keys[i] = valArr[0].ToString();
                values[i] = valArr[1].ToString();
            }

            var sql = string.Format("Insert into {0} ({1}) values ({2})", table, string.Join(",", keys), string.Join(",", values));

            var duplicate = this.GetSqlPart("duplicate_update");
            if (duplicate != null)
            {
                sql += " ON DUPLICATE KEY UPDATE ";

                bool isFirst = true;
                foreach (KeyValuePair<string, string> item in duplicate)
                {
                    if (isFirst) isFirst = false;
                    else sql += ",";

                    var valueExp = $"{item.Key}={item.Value}";
                    sql += valueExp;
                }

            }

            if (_selectScopeIdentify)
                sql += ";select last_insert_id() as id;";

            return sql;

        }
        private string ParseUpdateSql()
        {
            var tableParts = this.GetSqlPart("table");
            if (tableParts == null || tableParts.Count != 1) throw new Exception("Update require a table.Only One!");
            var table = tableParts[0].ToString();

            var valueParts = this.GetSqlPart("update_value");
            if (valueParts == null || valueParts.Count == 0) throw new Exception("Update require name and value");

            string[] setArr = new string[valueParts.Count];
            for (var i = 0; i < valueParts.Count; i++)
            {
                setArr[i] = valueParts[i].ToString();
            }

            var whereParts = this.GetSqlPart("where");
            var whereStr = whereParts == null ? "" : (" where " + string.Join(" and ", whereParts));

            return string.Format("update {0} set {1}", table, string.Join(",", setArr)) + whereStr;
        }
        private string ParseDeleteSql()
        {
            var tableParts = this.GetSqlPart("table");
            if (tableParts == null || tableParts.Count != 1) throw new Exception("Delete require a table.Only One!");
            var table = tableParts[0].ToString();

            var whereParts = this.GetSqlPart("where");
            var whereStr = whereParts == null ? "" : (" where " + string.Join(" and ", whereParts));

            return string.Format("delete from {0} ", table) + whereStr;
        }
        private string ParseSelectSql()
        {
            var sb = new StringBuilder();


            var selectParts = this.GetSqlPart("select");
            var selectStr = selectParts == null ? "*" : string.Join(",", selectParts);

            var tableParts = this.GetSqlPart("table");
            if (tableParts == null) throw new Exception("Table Can't be null.");
            var tableStr = string.Join(",", tableParts);


            var pageParts = this.GetSqlPart("page_args");
            var isPage = pageParts != null && pageParts.Count == 4;
            var forUpdateParts = this.GetSqlPart("forupdate");
            var forUpdate = forUpdateParts != null && forUpdateParts.Count == 1 && (bool)forUpdateParts[0];


            sb.AppendFormat("select {0} from {1}", selectStr, tableStr);

            var joinParts = this.GetSqlPart("join");
            var joinStr = joinParts == null ? "" : string.Join(" ", joinParts);

            var whereParts = this.GetSqlPart("where");
            var whereStr = whereParts == null ? "" : (" where " + string.Join(" and ", whereParts));

            var groupbyParts = this.GetSqlPart("groupby");
            var groupbyStr = groupbyParts == null ? "" : (" group by " + string.Join(",", groupbyParts));

            var orderbyParts = this.GetSqlPart("orderby");
            var orderbyStr = orderbyParts == null ? "" : (" order by " + string.Join(",", orderbyParts));

            var limitParts = this.GetSqlPart("limit");
            var limitStr = limitParts != null && limitParts.Count == 2 ? (" limit " + string.Join(",", limitParts)) : "";


            sb.Append(joinStr).Append(whereStr).Append(groupbyStr).Append(orderbyStr).Append(limitStr);

            var isPageCountTotal = false;
            if (isPage)
            {
                isPageCountTotal = (bool)pageParts[2];

                var sql = $"{sb.ToString()} limit {pageParts[0]}, {pageParts[1]}";
                if (forUpdate)
                    sql += " for update";
                if (isPageCountTotal)
                {
                    var pageTotals = pageParts[3].ToString();
                    if (!string.IsNullOrEmpty(pageTotals))
                        pageTotals = " ," + pageTotals;

                    sql += string.Format(";select count(0) as Count{1} from ({0}) CT", sb.ToString(), pageTotals);
                }
                return sql;
            }
            else
            {
                if (forUpdate)
                    sb.Append(" for update");
                return sb.ToString();
            }

        }

        /// <summary>
        /// 分页查询是否有附加获取全部总数语句
        /// </summary>
        /// <value>
        ///   <c>true</c> if this instance is count total; otherwise, <c>false</c>.
        /// </value>
        public bool IsCountTotal
        {
            get
            {
                var pageParts = this.GetSqlPart("page_args");
                var isPage = pageParts != null && pageParts.Count == 3;
                if (isPage)
                {
                    return (bool)pageParts[2];
                }
                return false;
            }

        }

        public DynamicParameters Parameters
        {
            get
            {
                return this._parameters;
            }
        }

        public bool IsUpdate
        {
            get
            {
                return this._mode == Mode.UPDATE;
            }
        }

        public bool IsInsert
        {
            get
            {
                return this._mode == Mode.INSERT;
            }
        }

        public bool IsDelete
        {
            get
            {
                return this._mode == Mode.DELETE;
            }
        }

        public bool IsSelect
        {
            get
            {
                return this._mode == Mode.SELECT;
            }
        }


    }

    /// <summary>
    /// 锁类型
    /// http://www.cnblogs.com/wuyifu/archive/2013/11/28/3447870.html
    /// </summary>
    public enum LockType
    {
        /// <summary>
        /// NOLOCK（不加锁）   
        /// 此选项被选中时，SQL Server 在读取或修改数据时不加任何锁。 
        /// 在这种情况下，用户有可能读取到未完成事务（Uncommited Transaction）或回滚(Roll Back)中的数据, 即所谓的“脏数据”。
        /// </summary>
        NOLOCK = 0,
        /// <summary>
        /// HOLDLOCK（保持锁）   
        /// 此选项被选中时，SQL Server 会将此共享锁保持至整个事务结束，而不会在途中释放。  
        /// </summary>
        HOLDLOCK = 1,
        /// <summary>
        /// UPDLOCK（修改锁）   
        /// 此选项被选中时，SQL Server 在读取数据时使用修改锁来代替共享锁，并将此锁保持至整个事务或命令结束。
        /// 使用此选项能够保证多个进程能同时读取数据但只有该进程能修改数据
        /// </summary>
        UPDLOCK = 2
    }
}
