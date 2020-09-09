using System;

namespace GhoulSQL
{
    public static class TypeExtension
    {
        public static object GetDefaultValue(this Type type)
        {
            if (type.IsValueType)
            {
                return Activator.CreateInstance(type);
            }
            return null;
        }
    }
}
