using Newtonsoft.Json.Linq;
using System;
using System.Linq;
using System.Collections;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;

namespace GhoulSQL
{
    /// <summary>
    /// 通用高阶函数
    /// </summary>
    public static class CommonFunc
    {
        /// <summary>
        /// 字符串不为空
        /// </summary>
        /// <param name="str">The string.</param>
        /// <returns></returns>
        public static bool StringNotEmpty(string str) => !string.IsNullOrEmpty(str);
        /// <summary>
        /// 数组内容为两个日期对象
        /// </summary>
        /// <param name="arr">The arr.</param>
        /// <returns>
        ///   <c>true</c> if [is time between] [the specified arr]; otherwise, <c>false</c>.
        /// </returns>
        public static bool IsTimeBetween(object arr)
        {
            if (arr == null) return false;

            if (!(arr is Array || arr is IEnumerable)) return false;

            try
            {
                var data = JsonConvert.DeserializeObject<DateTimeOffset[]>(JsonConvert.SerializeObject(arr));
                return data.Length == 2;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// 内容为指定类型数据，且满足指定长度
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="length">The length.</param>
        /// <returns></returns>
        public static Func<Array, bool> IsArray<T>(int length)
        {
            return (arr) => arr.Length == length && arr.GetValue(0) is T && arr.GetValue(1) is T;
        }

        /// <summary>
        /// 数组不为空，内容不为空
        /// </summary>
        /// <param name="arr">The arr.</param>
        /// <returns></returns>
        public static bool ArraryNotEmpty<T>(T[] arr) => arr != null && arr.Length > 0;

        /// <summary>
        /// 整型需大于零
        /// </summary>
        /// <param name="num"></param>
        /// <returns></returns>
        public static bool IntGreaterZero(int num) => num > 0;
    }
}
