using System;
using System.Linq;
using System.Linq.Expressions;

namespace TychoDB;

internal static class ObjectExtensions
{
    public static string GetExpressionMemberName(this Expression method)
    {
        if (method is not LambdaExpression lex)
        {
            throw new TychoException("The provided expression is not valid member expression");
        }

        return lex.Body.NodeType switch
        {
            ExpressionType.Convert =>
                (((UnaryExpression)lex.Body).Operand as MemberExpression)?.Member.Name
                ?? throw new TychoException("The provided expression is not valid member expression (Convert)"),
            ExpressionType.MemberAccess =>
                (lex.Body as MemberExpression)?.Member.Name
                ?? throw new TychoException("The provided expression is not valid member expression (MemberAccess)"),
            _ => throw new TychoException("The provided expression is not valid member expression"),
        };
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
