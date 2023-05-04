using System;
using System.Linq;
using System.Linq.Expressions;

namespace Tycho;

internal static class ObjectExtensions
{
    public static string GetExpressionMemberName(this Expression method)
    {
        if (method is LambdaExpression lex)
        {
            if (lex.Body.NodeType == ExpressionType.Convert)
            {
                return (((UnaryExpression)lex.Body).Operand as MemberExpression).Member.Name;
            }

            if (lex.Body.NodeType == ExpressionType.MemberAccess)
            {
                return (lex.Body as MemberExpression).Member.Name;
            }
        }

        throw new TychoDbException("The provided expression is not valid member expression");
    }

    public static string GetSafeTypeName(this Type type)
    {
        return
            !type.IsGenericType || type.IsGenericTypeDefinition
                ? !type.IsGenericTypeDefinition
                    ? type.Name
                    : type.Name.Replace('`', '_')
                : $"{GetSafeTypeName(type.GetGenericTypeDefinition())}__{string.Join(',', type.GetGenericArguments().Select(x => x.GetSafeTypeName()))}__";
    }
}
