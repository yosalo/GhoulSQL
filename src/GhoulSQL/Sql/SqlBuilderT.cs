using Dapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace GhoulSQL.Sql
{
    /// <summary>
    /// 新版SqlBuilder，基于旧版做扩展，可以使用表达式做参数
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class SqlBuilder<T>
    {
        public SqlBuilder Builder { get; private set; } = new SqlBuilder();
        public ConditionHash Conditions { get; private set; }

        public SqlBuilder()
        {
        }

        /// <summary>
        /// 传入一个ConditionHash实例，在调用Set,When，Between时可以直接调用
        /// </summary>
        /// <param name="conditions">The conditions.</param>
        public SqlBuilder(ConditionHash conditions)
        {
            this.Conditions = conditions;
        }

        private string GetFieldName<T1, T2>(Expression<Func<T1, T2>> exp, bool isDbName = true)
        {
            if (isDbName)
            {
                object[] member = new object[0];
                if (exp.Body is UnaryExpression) //对象是不是一元运算符  
                    member = ((MemberExpression)((UnaryExpression)exp.Body).Operand)
                     .Member.GetCustomAttributes(typeof(System.Runtime.Serialization.DataMemberAttribute), true);

                if (exp.Body is MemberExpression) //对象是不是访问的字段或属性  
                    member = ((MemberExpression)exp.Body).Member.GetCustomAttributes(typeof(System.Runtime.Serialization.DataMemberAttribute), true);

                if (exp.Body is ParameterExpression) //对象是不是参数表达式 
                    member = ((ParameterExpression)exp.Body).Type.GetCustomAttributes(typeof(System.Runtime.Serialization.DataMemberAttribute), true);

                if (member.Count() > 0)
                    return ((System.Runtime.Serialization.DataMemberAttribute)member.FirstOrDefault()).Name;
            }
            else
            {
                if (exp.Body is UnaryExpression) //对象是不是一元运算符  
                {
                    return ((MemberExpression)((UnaryExpression)exp.Body).Operand).Member.Name;
                }

                if (exp.Body is MemberExpression) //对象是不是访问的字段或属性  
                {
                    return ((MemberExpression)exp.Body).Member.Name;
                }

                if (exp.Body is ParameterExpression) //对象是不是参数表达式 
                {
                    return ((ParameterExpression)exp.Body).Type.Name;
                }
            }

            return "";
        }

        private Type GetFieldType<T1, T2>(Expression<Func<T1, T2>> exp)
        {
            return exp.Body.Type;
        }

        private string GetParameterName<T1, T2>(Expression<Func<T1, T2>> exp)
        {
            return exp.Parameters.First().Name;
        }

        private string GetAlias(string parameterName)
        {
            var parames = parameterName.Replace("`", "").Split(new[] { '.' }, StringSplitOptions.RemoveEmptyEntries);
            if (parames.Length == 2)
                return parames[0];

            return string.Empty;
        }
        private string GetFieldName(string parameterName)
        {
            var parames = parameterName.Replace("`", "").Split(new[] { '.' }, StringSplitOptions.RemoveEmptyEntries);
            if (parames.Length == 2)
                return parames[1];
            if (parames.Length == 1)
                return parames[0];

            return parameterName;
        }

        private string CombineFieldName(string alias, string fieldName)
        {
            if (string.IsNullOrEmpty(alias))
                return fieldName;

            if (!this.Builder.IsSelect)
                return $"`{fieldName}`";

            return $"{alias}.{fieldName}";
        }

        private WhereCompare GetWhereCompare<TEntity, TProperty>(Expression<Func<TEntity, TProperty>> exp,
            WhereCompare defaultCompare = WhereCompare.Equal)
        {
            var fieldName = GetFieldName(exp);
            return GetWhereCompare(fieldName, defaultCompare);
        }

        private WhereCompare GetWhereCompare(string compareName, WhereCompare defaultCompare = WhereCompare.Equal)
        {
            Preconditions.CheckNotNull(Conditions, "Conditions");
            if (!compareName.EndsWith("_WhereCompare"))
                compareName = (compareName + "_WhereCompare");

            if (!Conditions.IsContains<WhereCompare>(compareName))
                return defaultCompare;

            return Conditions.Get<WhereCompare>(compareName);
        }

        /// <summary>
        /// Tables the specified alias.
        /// </summary>
        /// <param name="alias">表简称</param>
        /// <returns></returns>
        public SqlBuilder<T> Table(string alias)
        {
            return this.Table<T>(alias);
        }
        /// <summary>
        /// Tables the specified alias.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="alias">表简称</param>
        /// <returns></returns>
        public SqlBuilder<T> Table<TEntity>(string alias)
        {
            this.Builder.Table(GetTableName<TEntity>(), alias);
            return this;
        }

        /// <summary>
        /// get table name
        /// </summary>
        /// <typeparam name="Tentity"></typeparam>
        /// <returns></returns>
        private string GetTableName<Tentity>()
        {
            string tableName = typeof(Tentity).Name;
            var tableAttribute = typeof(Tentity).GetCustomAttributes(typeof(Dapper.Contrib.Extensions.TableAttribute), true);
            if (tableAttribute.Count() > 0)
                tableName = ((Dapper.Contrib.Extensions.TableAttribute)tableAttribute.FirstOrDefault()).Name;
            return tableName;
        }

        public SqlBuilder<T> Insert()
        {
            this.Builder.Insert(GetTableName<T>());
            return this;
        }
        /// <summary>
        /// Insert时使用，ON DUPLICATE KEY UPDATE
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        public SqlBuilder<T> Duplicate(Expression<Func<T, object>> key, string value)
        {
            this.Builder.Duplicate(GetFieldName(key), value);
            return this;
        }
        /// <summary>
        /// Insert时使用，ON DUPLICATE KEY UPDATE
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <param name="value">The value.</param>
        /// <returns></returns>
        public SqlBuilder<T> Duplicate(string fieldName, string value)
        {
            this.Builder.Duplicate(fieldName, value);
            return this;
        }

        public SqlBuilder<T> Update()
        {
            this.Builder.Update(GetTableName<T>());
            return this;
        }
        public SqlBuilder<T> Delete()
        {
            this.Builder.Delete(GetTableName<T>());
            return this;
        }
        public SqlBuilder<T> ForUpdate()
        {
            this.Builder.ForUpdate();
            return this;
        }

        public SqlBuilder<T> Value<TProperty>(Expression<Func<T, TProperty>> exp, TProperty fieldValue)
        {
            var fieldName = GetFieldName(exp);
            Preconditions.CheckNotNullOrEmpty(fieldName, "Value FieldName");
            this.Builder.Value($"`{fieldName}`", fieldValue);
            return this;
        }
        public SqlBuilder<T> ValueLiteral(Expression<Func<T, object>> exp, string literal)
        {
            var fieldName = GetFieldName(exp);
            Preconditions.CheckNotNullOrEmpty(fieldName, "Value FieldName");
            this.Builder.Value(fieldName, literal, true);
            return this;
        }
        /// <summary>
        /// Update操作是Set的字段
        /// </summary>
        /// <typeparam name="TProperty">The type of the property.</typeparam>
        /// <param name="exp">要Set的字段</param>
        /// <param name="containCondition">ConditionHash中获取该字段值进行判断的高阶方法</param>
        /// <returns></returns>
        public SqlBuilder<T> Set<TProperty>(Expression<Func<T, TProperty>> exp, Func<TProperty, bool> containCondition = null)
        {
            Preconditions.CheckNotNull(this.Conditions, "Conditions");
            var entityName = GetFieldName(exp, false);
            Preconditions.CheckNotNullOrEmpty(entityName, "Set FieldName");
            var isContain = containCondition == null ? this.Conditions.IsContains<TProperty>(entityName) :
                this.Conditions.IsContains<TProperty>(entityName, containCondition);

            var fieldName = GetFieldName(exp);

            if (isContain)
            {
                var parameter = this.Conditions.Get<TProperty>(entityName);
                this.Builder.Set(fieldName, parameter);
            }
            return this;
        }
        public SqlBuilder<T> Set<T1>(bool isExtend, Expression<Func<T, object>> exp, T1 fieldValue, bool isLiteral = false)
        {
            if (isExtend)
                this.Set(exp, fieldValue, isLiteral);
            return this;
        }
        public SqlBuilder<T> Set<T1>(Expression<Func<T, object>> exp, T1 fieldValue, bool isLiteral = false)
        {
            var fieldName = GetFieldName(exp);
            Preconditions.CheckNotNullOrEmpty(fieldName, "Set");
            this.Builder.Set(fieldName, fieldValue, isLiteral);
            return this;
        }

        #region Join
        /// <summary>
        /// Join表操作
        /// </summary>
        /// <typeparam name="TEntity">表实体类.</typeparam>
        /// <param name="exp">用于比较的字段</param>
        /// <param name="condition">比较条件，例如 = t1.Id / <> t1.Status s</param>
        /// <param name="joinType">Type of the join.</param>
        /// <returns></returns>
        public SqlBuilder<T> Join<TEntity>(Expression<Func<TEntity, object>> exp, string condition, string joinType = "")
        {
            var alias = GetParameterName(exp);
            Preconditions.CheckNotNullOrEmpty(condition, "condition");
            var fieldName = GetFieldName(exp);
            Preconditions.CheckNotNullOrEmpty(fieldName, "exp");
            var tableName = GetTableName<TEntity>();
            this.Builder.Join(tableName, alias, $"{alias}.{fieldName} {condition}", joinType);
            return this;
        }
        /// <summary>
        /// Join表操作
        /// </summary>
        /// <typeparam name="TEntity">要Join的表</typeparam>
        /// <typeparam name="TProperty">The type of the property.</typeparam>
        /// <param name="joinExp">要Join的表以及需要进行连表的字段</param>
        /// <param name="equalsExp">相等于Join表连表字段的表字段</param>
        /// <returns></returns>
        public SqlBuilder<T> Join<TEntity, TProperty>(Expression<Func<TEntity, TProperty>> joinExp, Expression<Func<T, TProperty>> equalsExp, string joinType = "")
        {
            var joinAlias = GetParameterName(joinExp);
            var joinFieldName = GetFieldName(joinExp);
            Preconditions.CheckNotNullOrEmpty(joinFieldName, "joinExp");

            var alias = GetParameterName(equalsExp);
            var fieldName = GetFieldName(equalsExp);
            Preconditions.CheckNotNullOrEmpty(fieldName, "equalsExp");


            var tableName = GetTableName<TEntity>();
            this.Builder.Join(tableName, joinAlias, $"{joinAlias}.{joinFieldName} = {alias}.{fieldName}", joinType);
            return this;
        }

        public SqlBuilder<T> StraightJoin<TEntity>(Expression<Func<TEntity, object>> exp, string condition)
        {
            var alias = GetParameterName(exp);
            Preconditions.CheckNotNullOrEmpty(condition, "condition");
            var fieldName = GetFieldName(exp);
            Preconditions.CheckNotNullOrEmpty(fieldName, "exp");
            var tableName = GetTableName<TEntity>();
            this.Builder.StraightJoin(tableName, alias, $"{alias}.{fieldName} {condition}");
            return this;
        }

        public SqlBuilder<T> StraightJoin<TEntity, TProperty>(Expression<Func<TEntity, TProperty>> joinExp, Expression<Func<T, TProperty>> equalsExp)
        {
            var joinAlias = GetParameterName(joinExp);
            var joinFieldName = GetFieldName(joinExp);
            Preconditions.CheckNotNullOrEmpty(joinFieldName, "joinExp");

            var alias = GetParameterName(equalsExp);
            var fieldName = GetFieldName(equalsExp);
            Preconditions.CheckNotNullOrEmpty(fieldName, "equalsExp");


            var tableName = GetTableName<TEntity>();
            this.Builder.StraightJoin(tableName, joinAlias, $"{joinAlias}.{joinFieldName} = {alias}.{fieldName}");
            return this;
        }

        public SqlBuilder<T> LeftJoin<TEntity, TProperty>(Expression<Func<TEntity, TProperty>> joinExp, Expression<Func<T, TProperty>> equalsExp)
        {
            this.Join(joinExp, equalsExp, " Left");
            return this;
        }

        public SqlBuilder<T> LeftJoin<TEntity>(Expression<Func<TEntity, object>> exp, string condition)
        {
            this.Join(exp, condition, " Left");
            return this;
        }

        public SqlBuilder<T> RightJoin<TEntity>(Expression<Func<TEntity, object>> exp, string condition)
        {
            this.Join(exp, condition, " Right");
            return this;
        }

        public SqlBuilder<T> RightJoin<TEntity, TProperty>(Expression<Func<TEntity, TProperty>> joinExp, Expression<Func<T, TProperty>> equalsExp)
        {
            this.Join(joinExp, equalsExp, " Right");
            return this;
        }

        public SqlBuilder<T> InnerJoin<TEntity>(Expression<Func<TEntity, object>> exp, string condition)
        {
            this.Join(exp, condition, " Inner");
            return this;
        }

        public SqlBuilder<T> InnerJoin<TEntity, TProperty>(Expression<Func<TEntity, TProperty>> joinExp, Expression<Func<T, TProperty>> equalsExp)
        {
            this.Join(joinExp, equalsExp, " Inner");
            return this;
        }

        #endregion

        #region When
        /// <summary>
        /// 当Conditions中存在字段名时加上Where,且自行判断Conditions中是否包含WhereCompare，默认为WhereCompare.Equals
        /// </summary>
        /// <typeparam name="TProperty">The type of the property.</typeparam>
        /// <param name="exp">表达式的变量名必须与表简称相同，假如 Table t1,表达式为 t1=>t1.Status </param>
        /// <param name="containCondition">The contain condition.</param>
        /// <returns></returns>
        public SqlBuilder<T> When<TProperty>(Expression<Func<T, TProperty>> exp,
            Func<TProperty, bool> containCondition = null)
        {
            var compare = GetWhereCompare<T, TProperty>(exp);
            return this.When<TProperty>(exp, containCondition, compare);
        }
        /// <summary>
        /// 当Conditions中存在字段名时加上Where,且自行判断Conditions中是否包含WhereCompare，默认为WhereCompare.Equals
        /// </summary>
        /// <typeparam name="TEnity">The type of the enity.</typeparam>
        /// <typeparam name="TProperty">The type of the property.</typeparam>
        /// <param name="exp">The exp.</param>
        /// <param name="containCondition">The contain condition.</param>
        /// <returns></returns>
        public SqlBuilder<T> When<TEnity, TProperty>(Expression<Func<TEnity, TProperty>> exp,
            Func<TProperty, bool> containCondition = null)
        {
            var compare = GetWhereCompare<TEnity, TProperty>(exp);
            return this.When<TEnity, TProperty>(exp, containCondition, compare);
        }
        /// <summary>
        /// 当Conditions中存在字段名时加上Where
        /// </summary>
        /// <typeparam name="TProperty">The type of the property.</typeparam>
        /// <param name="exp">表达式的变量名必须与表简称相同，假如 Table t1,表达式为 t1=>t1.Status </param>
        /// <param name="containCondition">The contain condition.</param>
        /// <param name="compare">The compare.</param>
        /// <returns></returns>
        public SqlBuilder<T> When<TProperty>(Expression<Func<T, TProperty>> exp,
        Func<TProperty, bool> containCondition, WhereCompare compare)
        {
            return this.When<T, TProperty>(exp, containCondition, compare);
        }

        /// <summary>
        /// 当Conditions中存在字段名时加上Where
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <typeparam name="TProperty">The type of the property.</typeparam>
        /// <param name="exp">表达式的变量名必须与表简称相同，假如 Table t1,表达式为 t1=>t1.Status </param>
        /// <param name="containCondition">The contain condition.</param>
        /// <param name="compare">The compare.</param>
        /// <returns></returns>
        public SqlBuilder<T> When<TEntity, TProperty>(Expression<Func<TEntity, TProperty>> exp,
        Func<TProperty, bool> containCondition, WhereCompare compare = WhereCompare.Equal)
        {
            var alias = GetParameterName(exp);

            //因为实体定义与DB定义字段不相同,所以先判断Condition是否满足
            var enittyName = GetFieldName(exp, false);
            Preconditions.CheckNotNull(Conditions, "Conditions");
            Preconditions.CheckNotNullOrEmpty(enittyName, "Where");

            var isContain = containCondition == null ? this.Conditions.IsContains<TProperty>(enittyName) :
                this.Conditions.IsContains<TProperty>(enittyName, containCondition);

            var fieldName = GetFieldName(exp);
            var parameterName = fieldName;

            if (isContain)
            {
                var parameter = this.Conditions.Get<TProperty>(enittyName);
                if (compare == WhereCompare.Like && parameter is string)
                {
                    this.Builder.Where($"{CombineFieldName(alias, fieldName)} like @{parameterName}", $"%{parameter}%");
                }
                else
                {
                    var compareSymbol = "=";
                    switch (compare)
                    {
                        case WhereCompare.NoEqual: compareSymbol = "<>"; break;
                        case WhereCompare.Greater: compareSymbol = ">"; break;
                        case WhereCompare.GreaterOrEqual: compareSymbol = ">="; break;
                        case WhereCompare.Less: compareSymbol = "<"; break;
                        case WhereCompare.LessOrEqual: compareSymbol = "<="; break;
                    }
                    this.Builder.Where($"{CombineFieldName(alias, fieldName)} {compareSymbol} @{parameterName}", parameter);
                }
            }
            return this;
        }


        /// <summary>
        /// When查询
        /// </summary>
        /// <typeparam name="TProperty">要查询的字段对应的类型.</typeparam>
        /// <param name="fieldName">需要参与SQL查询的字段，支持 Name, b.Name, `Key`等.</param>
        /// <param name="parameterName">要获取的参数名，例如表FieldName为Username，但传过来的值是FromUsername等。空则直接获取fieldName.</param>
        /// <param name="containCondition">判断是否参与查询的条件</param>
        /// <returns></returns>
        public SqlBuilder<T> When<TProperty>(string fieldName, string parameterName = null,
            Func<TProperty, bool> containCondition = null)
        {
            if (string.IsNullOrEmpty(parameterName))
                parameterName = GetFieldName(fieldName);

            var compare = GetWhereCompare(parameterName);
            return this.When<TProperty>(fieldName, containCondition, parameterName, compare);
        }

        /// <summary>
        /// When查询
        /// </summary>
        /// <typeparam name="TProperty">要查询的字段对应的类型.</typeparam>
        /// <param name="fieldName">需要参与SQL查询的字段，支持 Name, b.Name, `Key`等.</param>
        /// <param name="containCondition">判断是否参与查询的条件</param>
        /// <param name="parameterName">要获取的参数名，例如表FieldName为Username，但传过来的值是FromUsername等。空则直接获取fieldName.</param>
        /// <param name="compare">The compare.</param>
        /// <returns></returns>
        public SqlBuilder<T> When<TProperty>(string fieldName, Func<TProperty, bool> containCondition,
            string parameterName = null, WhereCompare compare = WhereCompare.Equal)
        {
            var alias = GetAlias(fieldName);
            Preconditions.CheckNotNull(Conditions, "Conditions");

            if (string.IsNullOrEmpty(parameterName))
                parameterName = GetFieldName(fieldName);

            Preconditions.CheckNotNullOrEmpty(fieldName, "Where");

            var isContain = containCondition == null ? this.Conditions.IsContains<TProperty>(parameterName) :
                this.Conditions.IsContains<TProperty>(parameterName, containCondition);

            if (isContain)
            {
                var parameter = this.Conditions.Get<TProperty>(parameterName);
                if (compare == WhereCompare.Like && parameter is string)
                {
                    this.Builder.Where($"{fieldName} like @{parameterName}", $"%{parameter}%");
                }
                else
                {
                    var compareSymbol = "=";
                    switch (compare)
                    {
                        case WhereCompare.NoEqual: compareSymbol = "<>"; break;
                        case WhereCompare.Greater: compareSymbol = ">"; break;
                        case WhereCompare.GreaterOrEqual: compareSymbol = ">="; break;
                        case WhereCompare.Less: compareSymbol = "<"; break;
                        case WhereCompare.LessOrEqual: compareSymbol = "<="; break;
                    }
                    this.Builder.Where($"{fieldName} {compareSymbol} @{parameterName}", parameter);
                }
            }
            return this;
        }

        #endregion

        #region Between
        /// <summary>
        /// 当Conditions中存在字段名时加上Between
        /// </summary>
        /// <typeparam name="TProperty">The type of the property.</typeparam>
        /// <param name="exp">表达式的变量名必须与表简称相同，假如 Table t1,表达式为 t1=>t1.Status </param>
        /// <param name="containCondition">The contain condition.</param>
        /// <returns></returns>
        public SqlBuilder<T> Between<TProperty>(Expression<Func<T, TProperty>> exp, Func<object, bool> containCondition = null)
        {
            Preconditions.CheckNotNull(Conditions, "Conditions");
            var enittyName = GetFieldName(exp, false);
            Preconditions.CheckNotNullOrEmpty(enittyName, "Between FieldName");

            var isContain = containCondition == null ? this.Conditions.IsContains<TProperty[]>(enittyName, CommonFunc.IsArray<TProperty>(2)) :
                this.Conditions.IsContains<TProperty[]>(enittyName, containCondition);

            var fieldName = GetFieldName(exp);
            var parameterName = fieldName;

            if (isContain)
            {
                if (typeof(TProperty) == typeof(DateTimeOffset?) || typeof(TProperty) == typeof(DateTimeOffset))
                {
                    var parameter = this.Conditions.GetArray<DateTimeOffset>(enittyName);
                    return this.Between(exp, parameter.Select(it => it.ToString("o")).ToArray());
                }
                else
                {
                    var parameter = this.Conditions.GetArray<TProperty>(enittyName);
                    return this.Between(exp, parameter.Select(it => (object)it).ToArray());
                }
            }
            return this;
        }

        /// <summary>
        /// 当Conditions中存在字段名时加上Between
        /// </summary>
        /// <typeparam name="TProperty">The type of the property.</typeparam>
        /// <param name="exp">表达式的变量名必须与表简称相同，假如 Table t1,表达式为 t1=>t1.Status </param>
        /// <param name="parameters">parameters必须为object数组是由于SqlBuilder的Where内容为Object数组.</param>
        /// <returns></returns>
        public SqlBuilder<T> Between<TProperty>(Expression<Func<T, TProperty>> exp, params object[] parameters)
        {
            var alias = GetParameterName(exp);
            var fieldName = GetFieldName(exp);
            Preconditions.CheckNotNullOrEmpty(fieldName, "WhereBetween");
            Preconditions.CheckArgumentRange(parameters.Length, "Paraneters Length", 2, 2);
            this.Builder.Where($"{CombineFieldName(alias, fieldName)} between @{fieldName}1 and @{fieldName}2", parameters);
            return this;
        }
        #endregion

        #region Where

        /// <summary>
        /// Wheres the specified exp.
        /// </summary>
        /// <param name="exp">The exp.</param>
        /// <param name="condition">表达式的变量名必须与表简称相同，假如 Table t1,表达式为 t1=>t1.Status.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns></returns>
        public SqlBuilder<T> Where(Expression<Func<T, object>> exp, string condition, params object[] parameters)
        {
            return this.Where<T>(exp, condition, parameters);
        }
        /// <summary>
        /// Wheres the specified is extend.
        /// </summary>
        /// <param name="isExtend">if set to <c>true</c> [is extend].</param>
        /// <param name="exp">表达式的变量名必须与表简称相同，假如 Table t1,表达式为 t1=>t1.Status</param>
        /// <param name="condition">The condition.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns></returns>
        public SqlBuilder<T> Where(bool isExtend, Expression<Func<T, object>> exp, string condition, params object[] parameters)
        {
            if (isExtend)
                return this.Where(exp, condition, parameters);
            return this;
        }
        /// <summary>
        /// Wheres the specified is extend.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="isExtend">if set to <c>true</c> [is extend].</param>
        /// <param name="exp">表达式的变量名必须与表简称相同，假如 Table t1,表达式为 t1=>t1.Status</param>
        /// <param name="condition">The condition.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns></returns>
        public SqlBuilder<T> Where<TEntity>(bool isExtend, Expression<Func<TEntity, object>> exp, string condition, params object[] parameters)
        {
            if (isExtend)
                return this.Where<TEntity>(exp, condition, parameters);
            return this;
        }
        /// <summary>
        /// Wheres the specified exp.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="exp">表达式的变量名必须与表简称相同，假如 Table t1,表达式为 t1=>t1.Status</param>
        /// <param name="condition">The condition.</param>
        /// <param name="parameters">The parameters.</param>
        /// <returns></returns>
        public SqlBuilder<T> Where<TEntity>(Expression<Func<TEntity, object>> exp, string condition, params object[] parameters)
        {
            var alias = GetParameterName(exp);
            Preconditions.CheckNotNullOrEmpty(condition, "condition");
            var fieldName = GetFieldName(exp);
            Preconditions.CheckNotNullOrEmpty(fieldName, "exp");
            this.Builder.Where($"{CombineFieldName(alias, fieldName)} {condition}", parameters);
            return this;
        }

        public SqlBuilder<T> Where(string condition, params object[] parameters)
        {
            this.Builder.Where(condition, parameters);
            return this;
        }
        public SqlBuilder<T> Where(bool isExtend, string condition, params object[] parameters)
        {
            this.Builder.Where(isExtend, condition, parameters);
            return this;
        }
        #endregion

        #region WhereIn
        public SqlBuilder<T> WhereIn<TProperty>(Expression<Func<T, TProperty>> exp, IEnumerable<TProperty> args)
        {
            if (args == null || args.Count() <= 0)
            {
                return this;
            }

            var fieldName = GetFieldName(exp);
            if (this.Builder.IsSelect)
            {
                fieldName = this.CombineFieldName(this.GetParameterName(exp), fieldName);
            }
            this.Builder.WhereIn(fieldName, args);
            return this;
        }

        public SqlBuilder<T> WhereIn<T2, TProperty>(Expression<Func<T2, TProperty>> exp, IEnumerable<TProperty> args)
        {
            if (args == null || args.Count() <= 0)
            {
                return this;
            }

            var fieldName = GetFieldName(exp);
            if (this.Builder.IsSelect)
            {
                fieldName = this.CombineFieldName(this.GetParameterName(exp), fieldName);
            }
            this.Builder.WhereIn(fieldName, args);
            return this;
        }
        #endregion

        /// <summary>
        /// 自动使用Conditions中的PageIndex，PageSize做分页操作
        /// </summary>
        /// <param name="isCountTotal">if set to <c>true</c> [is count total].</param>
        /// <param name="totals">用于做聚合查询的语句，如: Sum(Amount) as Amount,Sum(Balance) as Balance</param>
        /// <returns></returns>
        public SqlBuilder<T> Page(bool isCountTotal = true, string totals = "")
        {
            Preconditions.CheckNotNull(this.Conditions, "Conditions");
            return this.Page(Conditions.PageIndex, Conditions.PageSize, isCountTotal, totals);
        }
        public SqlBuilder<T> Page(int pageIndex, int pageSize, bool isCountTotal = true, string totals = "")
        {
            this.Builder.Page(pageIndex, pageSize, isCountTotal, totals);
            return this;
        }

        public SqlBuilder<T> OrderBy()
        {
            Preconditions.CheckNotNull(this.Conditions, "Conditions");

            if (Conditions.IsContains<string>("OrderBy"))
            {
                var orderType = Conditions.Get<bool>("ASC", false) ? "ASC" : "DESC";
                this.Builder.OrderBy(Conditions.Get<string>("OrderBy"), orderType);
            }

            return this;
        }

        /// <summary>
        /// Orders the by.
        /// </summary>
        /// <param name="exp">表达式的变量名必须与表简称相同，假如 Table t1,表达式为 t1=>t1.Status</param>
        /// <returns></returns>
        public SqlBuilder<T> OrderBy(Expression<Func<T, object>> exp, OrderType orderType = OrderType.DESC)
        {
            return this.OrderBy<T>(exp, orderType);
        }
        /// <summary>
        /// Orders the by.
        /// </summary>
        /// <typeparam name="TEntity">The type of the entity.</typeparam>
        /// <param name="exp">表达式的变量名必须与表简称相同，假如 Table t1,表达式为 t1=>t1.Status </param>
        /// <param name="orderType">Type of the order.</param>
        /// <returns></returns>
        public SqlBuilder<T> OrderBy<TEntity>(Expression<Func<TEntity, object>> exp, OrderType orderType = OrderType.DESC)
        {
            var alias = GetParameterName(exp);
            var fieldName = GetFieldName(exp);
            Preconditions.CheckNotNullOrEmpty(fieldName, "OrderBy");
            this.Builder.OrderBy(CombineFieldName(alias, fieldName), orderType.ToString());
            return this;
        }

        /// <summary>
        /// Groups the by.
        /// </summary>
        /// <param name="exp">表达式的变量名必须与表简称相同，假如 Table t1,表达式为 t1=>t1.Status</param>
        /// <returns></returns>
        public SqlBuilder<T> GroupBy(Expression<Func<T, object>> exp)
        {
            return this.GroupBy<T>(exp);
        }
        public SqlBuilder<T> GroupBy<TEntity>(Expression<Func<TEntity, object>> exp)
        {
            exp.Parameters.First();
            var alias = GetParameterName(exp);
            var fieldName = GetFieldName(exp);
            Preconditions.CheckNotNullOrEmpty(CombineFieldName(alias, fieldName), "GroupBy");
            this.Builder.GroupBy(CombineFieldName(alias, fieldName));
            return this;
        }

        public SqlBuilder<T> Limit(int offset, int rows)
        {
            this.Builder.Limit(offset, rows);
            return this;
        }

        public SqlBuilder<T> Select(params string[] selects)
        {
            this.Builder.Select(selects);
            return this;
        }

        /// <summary>
        /// Selects the specified exps.
        /// </summary>
        /// <param name="exps">表达式的变量名必须与表简称相同，假如 Table t1,表达式为 t1=>t1.Status</param>
        /// <returns></returns>
        public SqlBuilder<T> Select(params Expression<Func<T, object>>[] exps)
        {
            foreach (var exp in exps)
                this.Select<T>(exp);
            return this;
        }
        public SqlBuilder<T> Select<TEntity>(params Expression<Func<TEntity, object>>[] exps)
        {
            foreach (var exp in exps)
                this.Select<TEntity>(exp);
            return this;
        }
        public SqlBuilder<T> Select<TEntity>(Expression<Func<TEntity, object>> exp, string selectAlias = "")
        {
            var fieldName = GetFieldName(exp);
            Preconditions.CheckNotNullOrEmpty(fieldName, "Select FieldName");
            var alias = GetParameterName(exp);

            this.Builder.Select($"{alias}.{fieldName} {selectAlias}");

            return this;
        }

        public SqlBuilder<T> SelectIdentity()
        {
            this.Builder.SelectIdentity();
            return this;
        }

        public SqlBuilder<T> ClearSelect()
        {
            this.Builder.ClearSelect();
            return this;
        }

        public string ToSql()
        {
            return this.Builder.ToSql();
        }

        public DynamicParameters Parameters
        {
            get
            {
                return this.Builder.Parameters;
            }
        }

    }

    public enum OrderType
    {
        DESC = 0,
        ASC = 1
    }

    /// <summary>
    /// Where条件
    /// </summary>
    public enum WhereCompare
    {
        /// <summary>
        /// 等于
        /// </summary>
        Equal = 0,
        /// <summary>
        /// 不等于
        /// </summary>
        NoEqual = 1,
        /// <summary>
        /// 大于
        /// </summary>
        Greater = 2,
        /// <summary>
        /// 大于等于
        /// </summary>
        GreaterOrEqual = 3,
        /// <summary>
        /// 小于
        /// </summary>
        Less = 4,
        /// <summary>
        /// 小于等于
        /// </summary>
        LessOrEqual = 5,
        /// <summary>
        /// Like % s %
        /// </summary>
        Like = 6
    }
}
