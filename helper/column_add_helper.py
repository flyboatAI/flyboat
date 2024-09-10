from sympy import symbols, sympify


def merge_data(origin_data, add_fields):
    for add_field in add_fields:
        name = add_field.get("name")
        for item in origin_data:
            expression = add_field.get("expression")
            expr = sympify(expression)
            for k in item.keys():
                x = symbols(k)
                expr = expr.subs(x, item[k])
            r = expr.evalf()
            item[name] = float(r)
    return origin_data
