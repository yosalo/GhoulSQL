using System;
using System.Linq;
using System.Collections.Generic;
using System.Reflection;
using System.Text;

namespace GhoulSQL
{
    public static class ReflectionHelper
    {
        /// <summary>
        /// 反射调用实例的泛型方法
        /// </summary>
        /// <param name="instance">对象实例</param>
        /// <param name="method">要调用的泛型方法</param>
        /// <param name="genericType">泛型所使用的类型</param>
        /// <param name="parameters">调用方法使用的参数</param>
        /// <returns></returns>
        public static object CallGenericMethod(this object instance, MethodInfo method, Type genericType, params object[] parameters)
        {
            MethodInfo generic = method.MakeGenericMethod(genericType);
            return generic?.Invoke(instance, parameters);
        }

        /// <summary>
        /// 反射调用实例的泛型方法
        /// </summary>
        /// <param name="instance">对象实例</param>
        /// <param name="method">要调用的泛型方法</param>
        /// <param name="genericType">泛型所使用的类型</param>
        /// <param name="parameters">调用方法使用的参数</param>
        /// <returns></returns>
        public static object CallGenericMethod(this object instance, string method, Type genericType, params object[] parameters)
        {
            var methodInfo = instance.GetType().GetMethod(method);
            if (methodInfo == null) return null;

            return instance.CallGenericMethod(methodInfo, genericType, parameters);
        }
    }
}
