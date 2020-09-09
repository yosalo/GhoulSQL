using System;
using System.Linq.Expressions;

namespace GhoulSQL
{
    public class ExpressionHelper
    {
        public static string GetFieldName<T1, T2>(Expression<Func<T1, T2>> exp)
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
            return "";
        }
    }
}
