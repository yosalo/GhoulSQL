using System;
using System.Collections;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Linq;
using System.Linq.Expressions;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace GhoulSQL
{
    /// <summary>
    /// Conditional hash, used to pass query parameters, and generate SQL statements with SqlBuilder
    /// </summary>
    /// <seealso cref="System.Collections.Hashtable" />
    public class ConditionHash : Hashtable
    {
        public static ConditionHash Empty
        {
            get
            {
                return new ConditionHash();
            }
        }

        public static ConditionHash New(string key, object value)
        {
            return new ConditionHash().Push(key, value);
        }

        public ConditionHash() : base() { }
        public ConditionHash(IDictionary d) : base(d) { }
        public ConditionHash(Object obj, params string[] requiredKeys) : this()
        {
            Type t = obj.GetType();

            PropertyInfo[] properties = t.GetProperties();
            foreach (PropertyInfo property in properties)
            {
                var propertyVal = property.GetValue(obj);
                if (requiredKeys.Contains(property.Name) ||
                    propertyVal is int ||
                    propertyVal is short ||
                    propertyVal is long ||
                    propertyVal is float ||
                    propertyVal is decimal ||
                    propertyVal is string)
                    this.Add(property.Name.ToLower(), propertyVal);
            }
        }

        public override void Remove(object key)
        {
            if (!base.ContainsKey(key.ToString().ToLower()))
            {
                return;
            }

            if (key is string)
                base.Remove(key.ToString().ToLower());
            else
                base.Remove(key);
        }

        public override object this[object key]
        {
            get
            {
                if (key is string)
                    return base[key.ToString().ToLower()];
                return base[key];
            }
            set
            {
                if (key is string)
                    key = key.ToString().ToLower();
                base[key] = value;
            }
        }

        /// <summary>
        /// PageIndex
        /// </summary>
        /// <value>
        /// The index of the page.
        /// </value>
        public int PageIndex
        {
            get
            {
                return this.Get("PageIndex", 1);
            }
        }
        /// <summary>
        /// PageSize
        /// </summary>
        /// <value>
        /// The size of the page.
        /// </value>
        public int PageSize
        {
            get
            {
                return this.Get("PageSize", 20);
            }
        }
        /// <summary>
        /// Get key value
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="name">The name.</param>
        /// <param name="defaultValue">The default value.</param>
        /// <returns></returns>
        public T Get<T>(string name, T defaultValue = default(T))
        {
            return Parse<T>(name, out T value, defaultValue) ? value : defaultValue;
        }

        /// <summary>
        /// Parse key
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="name">The name.</param>
        /// <param name="value">The value.</param>
        /// <param name="defaultValue">The default value.</param>
        /// <returns>If success return true then false</returns>
        public bool Parse<T>(string name, out T value, T defaultValue = default(T))
        {
            name = name.ToLower();
            if (!this.ContainsKey(name) || this[name] == null)
            {
                value = defaultValue;
                return false;
            }
            if (this[name] is T)
            {
                value = (T)this[name];
                return true;
            }
            else if (IsNum<T>(name))
            {
                return Converter.Parse<T>(this[name], out value, defaultValue);
            }
            else if (typeof(T).IsEnum)
            {
                try
                {
                    value = (T)Enum.Parse(typeof(T), this[name].ToString());
                    if (!Enum.IsDefined(typeof(T), value))
                    {
                        value = defaultValue;
                        return false;
                    }
                    return true;
                }
                catch
                {
                    value = defaultValue;
                    return false;
                }
            }
            else if (typeof(IEnumerable).IsAssignableFrom(typeof(T)))
            {
                try
                {
                    var json = JsonConvert.SerializeObject(this[name]);
                    value = JsonConvert.DeserializeObject<T>(json);
                    return true;
                }
                catch
                {
                    value = defaultValue;
                    return false;
                }
            }
            else
            {
                return Converter.Parse<T>(this[name], out value, defaultValue);
            }
        }

        public T[] GetArray<T>(string name, T[] defaultValue = null)
        {
            name = name.ToLower();
            if (!this.Contains(name) || this[name] == null) return defaultValue;
            if (this[name] is T[])
                return this[name] as T[];

            try
            {
                return JsonConvert.DeserializeObject<T[]>(JsonConvert.SerializeObject(this[name]));
            }
            catch
            {
                return defaultValue;
            }
        }

        /// <summary>
        /// Specifies whether the value of Key is a numeric type
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="name">The name.</param>
        /// <returns>
        ///   <c>true</c> if the specified name is number; otherwise, <c>false</c>.
        /// </returns>
        public bool IsNum<T>(string name)
        {
            name = name.ToLower();
            var numType = new[] { typeof(short), typeof(int), typeof(long), typeof(float), typeof(double), typeof(decimal) };
            return numType.Contains(typeof(T)) && numType.Contains(this[name].GetType());
        }

        /// <summary>
        /// Check if the key exists
        /// </summary>
        /// <param name="name">The name.</param>
        /// <param name="arrLen">If the content is an array, the length of the array is judged。
        /// 3 conditon:
        /// 1:Do not check the length
        /// 2:Length must be greater than zero
        /// 3:Length equal to specified number
        /// </param>
        /// <returns></returns>
        public bool IsRequired(string name, int arrLen = -1)
        {
            name = name.ToLower();

            if (!this.ContainsKey(name)) return false;

            var value = this[name];
            if (value is string)
            {
                return !string.IsNullOrEmpty((string)value);
            }
            else if (value is Array)
            {
                var valLength = (value as Array).Length;
                if (arrLen < -1) return true;
                else if (arrLen == -1) return valLength > 0;
                else return arrLen == valLength;
            }
            else if (value is JArray)
            {
                var valLength = (value as JArray).Count;
                if (arrLen < -1) return true;
                else if (arrLen == -1) return valLength > 0;
                else return arrLen == valLength;
            }
            else
                return value != null;
        }
        /// <summary>
        /// Check whether it contains the specified Key and the value of type T
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="name">The name.</param>
        /// <param name="arrLen">Check the length of the content of the array type, the default -1 is not check the length.</param>
        /// <returns>
        ///   <c>true</c> if the specified name is required; otherwise, <c>false</c>.
        /// </returns>
        public bool IsContains<T>(string name, int arrLen = -1)
        {
            name = name.ToLower();

            if (!this.ContainsKey(name.ToLower()) || this[name.ToLower()] == null) return false;

            if (!this.Parse(name, out T val) || val == null) return false;

            //If the type is an iterable type, determine the length
            if (typeof(T).IsAssignableFrom(typeof(IEnumerable)))
            {
                var valLength = (val as IEnumerable<object>).Count();
                if (arrLen < -1) return true;
                else if (arrLen == -1) return valLength > 0;
                else return arrLen == valLength;
            }

            return true;
        }

        /// <summary>
        /// Check whether it contains the specified Key and the value of type T
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="name">The name.</param>
        /// <param name="condition">conditon</param>
        /// <returns>
        ///   <c>true</c> if the specified name is required; otherwise, <c>false</c>.
        /// </returns>
        public bool IsContains<T>(string name, Func<T, bool> condition)
        {
            name = name.ToLower();

            if (!this.ContainsKey(name) || this[name] == null) return false;

            if (!this.Parse(name, out T val) || val == null) return false;

            return condition(val);
        }

        /// <summary>
        /// Additional content
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="data">The data.</param>
        /// <returns></returns>
        public ConditionHash Push(string key, object data)
        {
            this.Add(key, data);
            return this;
        }

        public override void Add(object key, object value)
        {
            if (this.ContainsKey(key.ToString().ToLower()))
            {
                this[key] = value;
                return;
            }

            base.Add(key.ToString().ToLower(), value);
        }

        public string Diff<T>(T data)
        {
            var dataType = data.GetType();
            var dataPropertys = dataType.GetProperties().ToDictionary(it => it.Name.ToLower(), it => it);
            StringBuilder sb = new StringBuilder();
            foreach (var key in this.Keys)
            {
                var keyStr = key.ToString();
                if (!dataPropertys.ContainsKey(keyStr)) continue;

                var property = dataPropertys[keyStr];

                var conditionValue = this.CallGenericMethod("Get",
                    dataPropertys[keyStr].PropertyType,
                    new object[] { keyStr, dataPropertys[keyStr].PropertyType.GetDefaultValue() });

                if (conditionValue == null) continue;

                if (conditionValue is string && string.IsNullOrEmpty(conditionValue.ToString()))
                    conditionValue = "<NULL>";

                var propertyVal = property.GetValue(data);
                if (propertyVal is string && string.IsNullOrEmpty(propertyVal.ToString()))
                    propertyVal = "<NULL>";

                if (propertyVal != conditionValue)
                    sb.Append($"{property.Name} : {propertyVal} -> {conditionValue}{Environment.NewLine}");
            }

            return sb.ToString();
        }

        /// <summary>
        /// Add OrderBy, if OrderBy already exists, do nothing
        /// </summary>
        /// <param name="fieldName">Name of the field.</param>
        /// <returns></returns>
        public ConditionHash OrderBy(string fieldName, bool asc = false)
        {
            if (this.IsContains<string>("OrderBy"))
                return this;
            else
            {
                this["OrderBy"] = fieldName;
                this["ASC"] = asc;
            }

            return this;
        }
    }

    public class ConditionHash<T> : ConditionHash
    {
        public static ConditionHash<T> New(Expression<Func<T, object>> exp, object data)
        {
            var conditions = new ConditionHash<T>().Push(exp, data);
            return conditions;
        }

        public ConditionHash<T> Push(Expression<Func<T, object>> exp, object data)
        {
            var key = ExpressionHelper.GetFieldName(exp);
            base.Push(key, data);
            return this;
        }
    }
}
