using System;
using System.Collections.Generic;
using System.Linq;

namespace GhoulSQL
{
    public static class EnumerateExtension
    {
        /// <summary>
        /// 随机列表中一个值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="data">The data.</param>
        /// <returns></returns>
        /// <exception cref="System.ArgumentNullException"></exception>
        public static T Random<T>(this IEnumerable<T> data)
        {
            if (data == null || data.Count() == 0)
                throw new ArgumentNullException();

            var rnd = new Random();
            var index = rnd.Next(0, data.Count());
            return data.ElementAt(index);
        }

        public static void ForEach<T>(this IEnumerable<T> data, Action<T> action)
        {
            foreach (var item in data)
                action(item);
        }

        public static IEnumerable<T> After<T>(this IEnumerable<T> source, T item)
        {
            var index = 0;
            foreach (T i in source)
            {
                if (i.Equals(item)) break;

                index++;
            }

            return source.Skip(index);
        }

        public static void AddRange<T, S>(this IDictionary<T, S> source, IDictionary<T, S> collection)
        {
            if (collection == null)
            {
                throw new ArgumentNullException("Collection is null");
            }

            foreach (var item in collection)
            {
                if (!source.ContainsKey(item.Key))
                {
                    source.Add(item.Key, item.Value);
                }
            }
        }

        public static V Get<K, V>(this IDictionary<K, V> source, K key, V defaultValue = default(V))
        {
            if (source.ContainsKey(key))
                return source[key];

            return defaultValue;
        }

    }
}

