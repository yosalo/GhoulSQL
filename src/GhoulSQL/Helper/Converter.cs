using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Net;
using System.Text;

namespace GhoulSQL
{
    public static class Converter
    {
        /// <summary>
        /// 合并一组可枚举的对象为字符串
        /// </summary>
        /// <param name="enumerable">被枚举对象</param>
        /// <param name="seperator">分隔符</param>
        /// <returns>字符串</returns>
        public static string ConcatEnumerable(IEnumerable enumerable, string seperator)
        {
            IEnumerator enumerator = enumerable.GetEnumerator();
            if (!enumerator.MoveNext())
            {
                return string.Empty;
            }
            StringBuilder builder = new StringBuilder(0x80);
            builder.Append(enumerator.Current.ToString());
            while (enumerator.MoveNext())
            {
                builder.Append(seperator + enumerator.Current);
            }
            return builder.ToString();
        }

        /// <summary>
        /// 获取8位整型值。
        /// </summary>
        /// <param name="src">长整型值</param>
        /// <returns></returns>
        public static byte ToByte(long src)
        {
            if (src > 0xff)
            {
                return 0xff;
            }
            if (src < 0)
            {
                return 0;
            }
            return (byte)src;
        }

        /// <summary>
        /// 获得网络字节数组。
        /// </summary>
        /// <param name="src">短整型值</param>
        /// <returns></returns>
        public static byte[] ToBytes(short src)
        {
            return BitConverter.GetBytes(IPAddress.HostToNetworkOrder(src));
        }

        /// <summary>
        /// 获得网络字节数组。
        /// </summary>
        /// <param name="src">整型值</param>
        /// <returns></returns>
        public static byte[] ToBytes(int src)
        {
            return BitConverter.GetBytes(IPAddress.HostToNetworkOrder(src));
        }

        /// <summary>
        /// 获得网络字节数组。
        /// </summary>
        /// <param name="src">长整型值</param>
        /// <returns></returns>
        public static byte[] ToBytes(long src)
        {
            return BitConverter.GetBytes(IPAddress.HostToNetworkOrder(src));
        }

        /// <summary>
        /// 获得时间类型值
        /// </summary>
        /// <param name="src">源对象</param>
        /// <param name="defaultVal">转换失败时期望返回的时间类型值</param>
        /// <returns></returns>
        public static DateTime ToDatetime(object src, DateTime defaultVal)
        {
            DateTime time;
            if ((src != null) && DateTime.TryParse(src.ToString(), out time))
            {
                return time;
            }
            return defaultVal;
        }

        /// <summary>
        /// 获得时间类型值
        /// </summary>
        /// <param name="src">源对象</param>
        /// <returns></returns>
        public static DateTime ToDatetime(object src)
        {
            return ToDatetime(src, default(DateTime));
        }

        /// <summary>
        /// 获得双精度值。
        /// </summary>
        /// <param name="src">源对象</param>
        /// <param name="defaultVal">转换失败时期望返回的双精度值</param>
        /// <returns></returns>
        public static double ToDouble(object src, double defaultVal)
        {
            double num;
            if ((src != null) && double.TryParse(src.ToString(), out num))
            {
                return num;
            }
            return defaultVal;
        }

        /// <summary>
        /// 获得双精度值。
        /// </summary>
        /// <param name="src">源对象</param>
        /// <returns></returns>
        public static double ToDouble(object src)
        {
            return ToDouble(src, 0d);
        }

        /// <summary>
        /// 获取整型值。
        /// </summary>
        /// <param name="src">源对象,长整型值</param>
        /// <returns></returns>
        public static int ToInt(long src)
        {
            if (src > 0x7fffffff)
            {
                return 0x7fffffff;
            }
            if (src < -2147483648)
            {
                return -2147483648;
            }
            return (int)src;
        }

        /// <summary>
        /// 获得整型值。
        /// </summary>
        /// <param name="src">源对象</param>
        /// <returns></returns>
        public static int ToInt(object src)
        {
            return ToInt(src, 0);
        }

        /// <summary>
        /// 获得整型值。
        /// </summary>
        /// <param name="src">源对象</param>
        /// <param name="defaultVal">转换失败时期望返回的整型值</param>
        /// <returns></returns>
        public static int ToInt(object src, int defaultVal)
        {
            int num;
            if ((src != null) && int.TryParse(src.ToString().Trim(), out num))
            {
                return num;
            }
            return defaultVal;
        }

        /// <summary>
        /// 获得整型值。
        /// </summary>
        /// <param name="src">源对象</param>
        /// <param name="defaultVal">转换失败时期望返回的整型值</param>
        /// <param name="scale">源字符串的进位制，如16、10、8、2等</param>
        /// <returns></returns>
        public static int ToInt(object src, int defaultVal, int scale)
        {
            try
            {
                return Convert.ToInt32(src.ToString().Trim(), scale);
            }
            catch
            {
                return defaultVal;
            }
        }

        /// <summary>
        /// 获得长整型值。
        /// </summary>
        /// <param name="src">源对象</param>
        /// <param name="defaultVal">转换失败时期望返回的长整型值</param>
        /// <returns></returns>
        public static long ToLong(object src, long defaultVal)
        {
            long num;
            if ((src != null) && long.TryParse(src.ToString(), out num))
            {
                return num;
            }
            return defaultVal;
        }

        /// <summary>
        /// 获得长整型值。
        /// </summary>
        /// <param name="src">源对象</param>
        /// <param name="defaultVal">转换失败时期望返回的长整型值</param>
        /// <param name="scale">源字符串的进位制，如16、10、8、2等</param>
        /// <returns></returns>
        public static long ToLong(object src, long defaultVal, int scale)
        {
            try
            {
                return Convert.ToInt64(src.ToString().Trim(), scale);
            }
            catch
            {
                return defaultVal;
            }
        }

        /// <summary>
        /// 获得16位整型值。
        /// </summary>
        /// <param name="src">源对象,长整型值</param>
        /// <returns></returns>
        public static short ToShort(long src)
        {
            if (src > 0x7fff)
            {
                return 0x7fff;
            }
            if (src < -32768)
            {
                return -32768;
            }
            return (short)src;
        }

        /// <summary>
        /// 获得16位整型值。
        /// </summary>
        /// <param name="src">源对象</param>
        /// <returns></returns>
        public static short ToShort(object src)
        {
            return ToShort(src, 0);
        }

        /// <summary>
        /// 获得16位整型值。
        /// </summary>
        /// <param name="src">源对象</param>
        /// <param name="defaultVal">转换失败时期望返回的整型值</param>
        /// <returns></returns>
        public static short ToShort(object src, short defaultVal)
        {
            short num;
            if ((src != null) && short.TryParse(src.ToString().Trim(), out num))
            {
                return num;
            }
            return defaultVal;
        }

        /// <summary>
        /// 获得16位整型值。
        /// </summary>
        /// <param name="src">源对象</param>
        /// <param name="defaultVal">转换失败时期望返回的整型值</param>
        /// <param name="scale">源字符串的进位制，如16、10、8、2等</param>
        /// <returns></returns>
        public static short ToShort(object src, short defaultVal, int scale)
        {
            try
            {
                return Convert.ToInt16(src.ToString().Trim(), scale);
            }
            catch
            {
                return defaultVal;
            }
        }

        /// <summary>
        /// 获得字符串值。
        /// <para>该方法用于依据一个对象，始终得到一个不为空的字符串（除非调用者将 defaultVal 设置为空）。</para>
        /// <para>它等价于在程序中对象判空、ToString、IsNullOrEmpty等处理。</para>
        /// </summary>
        /// <param name="src">源对象</param>
        /// <returns></returns>
        public static string ToStr(object src)
        {
            return ToStr(src, "", true);
        }

        /// <summary>
        /// 获得字符串值。
        /// <para>该方法会将 string.Empty 转换为 defaultValue。</para>
        /// <para>该方法用于依据一个对象，始终得到一个不为空的字符串（除非调用者将 defaultVal 设置为空）。</para>
        /// <para>它等价于在程序中对象判空、ToString、IsNullOrEmpty等处理。</para>
        /// </summary>
        /// <param name="src">源对象</param>
        /// <param name="defaultVal">转换失败时期望返回的字符串值</param>
        /// <returns></returns>
        public static string ToStr(object src, string defaultVal)
        {
            return ToStr(src, defaultVal, true);
        }

        /// <summary>
        /// 获得字符串值。
        /// <para>该方法会将 string.Empty 转换为 defaultValue。</para>
        /// <para>该方法用于依据一个对象，始终得到一个不为空的字符串（除非调用者将 defaultVal 设置为空）。</para>
        /// <para>它等价于在程序中对象判空、ToString、IsNullOrEmpty等处理。</para>
        /// </summary>
        /// <param name="src">源对象</param>
        /// <param name="defaultVal">转换失败时期望返回的字符串值</param>
        /// <param name="disallowEmpty">是否不允许空值（将 string.Empty 转换为 defaultValue）</param>
        /// <returns></returns>
        public static string ToStr(object src, string defaultVal, bool disallowEmpty)
        {
            if (src == null)
            {
                return defaultVal;
            }
            if (disallowEmpty && (src.ToString().Length == 0))
            {
                return defaultVal;
            }
            return src.ToString();
        }

        /// <summary>
        /// 获取字节流的指定位置的值
        /// </summary>
        /// <param name="src">数据流</param>
        /// <param name="startIndex">开始读取的位置</param>
        /// <returns></returns>
        public static byte ReadByte(byte[] src, int startIndex)
        {
            byte[] destinationArray = new byte[1];
            Array.Copy(src, startIndex, destinationArray, 0, 1);
            return destinationArray[0];
        }

        /// <summary>
        /// 获取字节流的指定位置的值序列
        /// </summary>
        /// <param name="src">数据流</param>
        /// <param name="startIndex">开始读取的位置</param>
        /// <param name="length">要获取的数据的长度</param>
        /// <returns></returns>
        public static byte[] ReadBytes(byte[] src, int startIndex, int length)
        {
            byte[] destinationArray = new byte[length];
            Array.Copy(src, startIndex, destinationArray, 0, length);
            return destinationArray;
        }

        /// <summary>
        /// 返回由网络字节数组中指定位置的两个字节转换来的 32 位有符号整型 (NetworkToHostOrder)。
        /// </summary>
        /// <param name="src">网络字节数组</param>
        /// <param name="startIndex">开始读取的位置</param>
        /// <returns></returns>
        public static int ReadInt(byte[] src, int startIndex)
        {
            return IPAddress.NetworkToHostOrder(BitConverter.ToInt32(src, startIndex));
        }

        /// <summary>
        /// 返回由网络字节数组中指定位置的两个字节转换来的 64 位有符号整型 (NetworkToHostOrder)。
        /// </summary>
        /// <param name="src">网络字节数组</param>
        /// <param name="startIndex">开始读取的位置</param>
        /// <returns></returns>
        public static long ReadLong(byte[] src, int startIndex)
        {
            return IPAddress.NetworkToHostOrder(BitConverter.ToInt64(src, startIndex));
        }

        /// <summary>
        /// 返回由网络字节数组中指定位置的两个字节转换来的 16 位有符号整型 (NetworkToHostOrder)。
        /// </summary>
        /// <param name="src">网络字节数组</param>
        /// <param name="startIndex">开始获取位置的索引</param>
        /// <returns></returns>
        public static short ReadShort(byte[] src, int startIndex)
        {
            return IPAddress.NetworkToHostOrder(BitConverter.ToInt16(src, startIndex));
        }

        /// <summary>
        /// 获取字节流的字符串表现形式（默认UTF8编码方式）
        /// </summary>
        /// <param name="src">字节流数据对象</param>
        /// <param name="startIndex">开始获取位置的索引</param>
        /// <param name="length">要获取的字符串的长度</param>
        /// <returns></returns>
        public static string ReadString(byte[] src, int startIndex, int length)
        {
            return ReadString(src, Encoding.UTF8, startIndex, length);
        }

        /// <summary>
        /// 获取字节流的字符串表现形式
        /// </summary>
        /// <param name="src">字节流数据对象</param>
        /// <param name="encoding">编码格式</param>
        /// <param name="startIndex">开始获取位置的索引</param>
        /// <param name="length">要获取的字符串的长度</param>
        /// <returns></returns>
        public static string ReadString(byte[] src, Encoding encoding, int startIndex, int length)
        {
            byte[] destinationArray = new byte[length];
            Array.Copy(src, startIndex, destinationArray, 0, length);
            return encoding.GetString(destinationArray);
        }
        /// <summary>
        /// byte[] 转 Uint
        /// </summary>
        /// <param name="src">byte[]对象</param>
        /// <param name="startIndex">开始位置</param>
        /// <returns></returns>
        public static uint ReadUInt(byte[] src, int startIndex)
        {
            return (uint)IPAddress.NetworkToHostOrder((long)BitConverter.ToUInt32(src, startIndex));
        }

        /// <summary>
        /// 获得网络字节数组。
        /// </summary>
        /// <param name="src">整型值</param>
        /// <param name="bytesRightLen">要靠右保留的byte个数</param>
        /// <returns></returns>
        public static byte[] RightBytes(byte[] src, int bytesRightLen)
        {
            byte[] destinationArray = new byte[bytesRightLen];
            Array.Copy(src, src.Length - bytesRightLen, destinationArray, 0, bytesRightLen);
            return destinationArray;
        }

        /// <summary>
        /// 将以分隔符分隔的字符串转为int数组
        /// </summary>
        /// <param name="str">要分隔的字符串</param>
        /// <param name="seperators">分隔符数组</param>
        /// <returns>int数组</returns>
        public static int[] SplitToInt32Array(string str, string[] seperators)
        {
            string[] strArray = str.Split(seperators, StringSplitOptions.RemoveEmptyEntries);
            int[] numArray = new int[strArray.Length];
            for (int i = strArray.Length - 1; i >= 0; i--)
            {
                int num2;
                if (!int.TryParse(strArray[i], out num2))
                {
                    return null;
                }
                numArray[i] = num2;
            }
            return numArray;
        }

        /// <summary>
        /// 将以分隔符分隔的字符串转为int数组
        /// </summary>
        /// <param name="str">要分隔的字符串</param>
        /// <param name="seperator">分隔符</param>
        /// <returns>int数组</returns>
        public static int[] SplitToInt32Array(string str, string seperator)
        {
            string[] strArray2 = str.Split(new string[] { seperator }, StringSplitOptions.RemoveEmptyEntries);
            int[] numArray = new int[strArray2.Length];
            for (int i = strArray2.Length - 1; i >= 0; i--)
            {
                int num2;
                if (!int.TryParse(strArray2[i], out num2))
                {
                    return null;
                }
                numArray[i] = num2;
            }
            return numArray;
        }

        /// <summary>
        /// 将实例转化为指定泛型
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj">转换实例.</param>
        /// <param name="defaultValue">转换失败返回默认值.</param>
        /// <returns></returns>
        public static T To<T>(this object obj, T defaultValue = default(T))
        {
            try
            {
                var convertsionType = typeof(T);
                //判断convertsionType类型是否为泛型，因为nullable是泛型类,
                if (convertsionType.IsGenericType &&
                    //判断convertsionType是否为nullable泛型类
                    convertsionType.GetGenericTypeDefinition().Equals(typeof(Nullable<>)))
                {
                    if (obj == null || obj.ToString().Length == 0)
                    {
                        return defaultValue;
                    }

                    //如果convertsionType为nullable类，声明一个NullableConverter类，该类提供从Nullable类到基础基元类型的转换
                    NullableConverter nullableConverter = new NullableConverter(convertsionType);
                    //将convertsionType转换为nullable对的基础基元类型
                    convertsionType = nullableConverter.UnderlyingType;
                }

                return (T)Convert.ChangeType(obj, convertsionType);
            }
            catch
            {
                return defaultValue;
            }
        }

        /// <summary>
        /// 将实例转换为指定泛型
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="obj">转换实例.</param>
        /// <param name="value">转换结果.</param>
        /// <param name="defaultValue">转换失败默认值.</param>
        /// <returns>转换是否成功</returns>
        public static bool Parse<T>(this object obj, out T value, T defaultValue = default(T))
        {
            try
            {
                var convertsionType = typeof(T);
                //判断convertsionType类型是否为泛型，因为nullable是泛型类,
                if (convertsionType.IsGenericType &&
                    //判断convertsionType是否为nullable泛型类
                    convertsionType.GetGenericTypeDefinition().Equals(typeof(Nullable<>)))
                {
                    if (obj == null || obj.ToString().Length == 0)
                    {
                        value = defaultValue;
                        return false;
                    }

                    //如果convertsionType为nullable类，声明一个NullableConverter类，该类提供从Nullable类到基础基元类型的转换
                    NullableConverter nullableConverter = new NullableConverter(convertsionType);
                    //将convertsionType转换为nullable对的基础基元类型
                    convertsionType = nullableConverter.UnderlyingType;
                }

                if (typeof(DateTimeOffset).IsAssignableFrom(typeof(T)))
                    value = Converter.To<T>(DateTimeOffset.Parse(obj.ToString()));
                else
                    value = (T)Convert.ChangeType(obj, convertsionType);
                return true;
            }
            catch
            {
                value = defaultValue;
                return false;
            }
        }

        /// <summary>
        /// 将时间转成UnixTimeStamp
        /// </summary>
        /// <param name="time">The time.</param>
        /// <returns></returns>
        public static int ToUnix(this DateTime time)
        {
            return time.ToUniversalTime().ToTimestamp();
        }

        public static int ToTimestamp(this DateTime utcTime)
        {
            return (int)(utcTime.Subtract(new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc))).TotalSeconds;
        }

        public static DateTime ToDatetime(this long milliseconds)
        {
            DateTime start = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            DateTime date = start.AddMilliseconds(milliseconds).ToLocalTime();
            return date;
        }
    }
}
