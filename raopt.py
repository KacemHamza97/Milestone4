import re
import radb
import radb.ast
import radb.parse


def select_number(ra):
    return str(ra).count('\\select')


def cross_number(ra):
    return str(ra).count('\\cross')


def joint_number(ra):
    return str(ra).count('\\join')


def tris(x, cross_list):
    """this function is used to sort the conditions with the same order as the cross tables order"""
    cross_list_name = [x.rel if isinstance(x, radb.ast.RelRef) else x.relname for x in cross_list]
    x1 = cross_list_name.index(x.inputs[0].rel)
    x2 = cross_list_name.index(x.inputs[1].rel)
    return x1 + x2


def break_select(ra):
    """ Breaks up Complex selection into simple selection operations."""
    # valExpreBinary_list_ra will contain all the valExprBinaryOp objects in the right order.
    valExpreBinary_list_ra = [ra.cond.inputs[1]]
    inputs_0 = ra.cond.inputs[0]
    prev_inputs_0 = None
    while (type(inputs_0.inputs[0]) == radb.ast.ValExprBinaryOp):
        valExpreBinary_list_ra.append(inputs_0.inputs[1])
        prev_inputs_0 = inputs_0
        inputs_0 = inputs_0.inputs[0]
    if type(inputs_0.inputs[0]) != radb.ast.ValExprBinaryOp and prev_inputs_0 == None:
        valExpreBinary_list_ra.append(inputs_0)
    elif prev_inputs_0 != None:
        valExpreBinary_list_ra.append(prev_inputs_0.inputs[0])

    # The creation of the desired Selection object.
    select_index_number = len(valExpreBinary_list_ra)
    res = radb.ast.Select(cond=valExpreBinary_list_ra[0], input=ra.inputs[0])
    for i in range(1, select_index_number):
        res = (radb.ast.Select(cond=valExpreBinary_list_ra[i], input=res))
    return res


def clean_query(sql_query):
    """this function eliminates extra whitespaces"""
    return re.sub("\s\s+", " ", sql_query).strip()


def cross_tolist(cross_object):
    """this function returns a list containing the tables from a cross object"""
    cross_object_list = [cross_object.inputs[1]]
    test_cross = cross_object.inputs[0]
    if not isinstance(test_cross, radb.ast.Cross):
        cross_object_list.append(test_cross)
        return cross_object_list
    while (isinstance(test_cross.inputs[0], radb.ast.Cross)):
        cross_object_list.append(test_cross.inputs[1])
        test_cross = test_cross.inputs[0]
    cross_object_list.extend(test_cross.inputs[::-1])
    return cross_object_list


def extract_cross_select(ra):
    """this function returns a list containing the select_objects inside a cross object(s)"""
    cross_select_list = [ra.inputs[1]]
    test_cross = ra.inputs[0]

    while (isinstance(test_cross, radb.ast.Cross)):
        cross_select_list.append(test_cross.inputs[1])
        test_cross = test_cross.inputs[0]
    cross_select_list.append(test_cross)

    return cross_select_list


def rule_merge_selections_cross(ra):
    """this function merge the select_objects inside the cross_object(s)"""
    L = extract_cross_select(ra)
    selections = [merge_select(s) if isinstance(s, radb.ast.Select) else s for s in L]
    n = len(selections)
    res = selections[-1]
    for i in range(n - 2, -1, -1):
        res = radb.ast.Cross(res, selections[i])
    return res


def split_selection_cross(ra):
    """this function returns: list of select_conditions and a list of
            tables (inside a cross object) in the form of a tuple"""
    cross_list = []
    list_selection_cond = [ra.cond]
    test_cross = ra.inputs[0]
    while (isinstance(test_cross, radb.ast.Select)):
        list_selection_cond.append(test_cross.cond)
        test_cross = test_cross.inputs[0]

    if isinstance(test_cross, radb.ast.Cross):
        cross_list = cross_tolist(test_cross)

    return list_selection_cond, cross_list


def is_cross_select(s):
    """this function returns True if both terms of a select_condition are AttRef
                and have the same attribute name (T.name) else False"""
    return isinstance(s.inputs[0], radb.ast.AttrRef) and isinstance(s.inputs[1], radb.ast.AttrRef) and (s.inputs[
        0].name == s.inputs[1].name or s.inputs[0].name[1:] == s.inputs[1].name[1:])


def remaining_select(select_list):
    """returns the select_conditions that are not pushed down"""
    return [s for s in select_list if not is_cross_select(s)]


def is_neither(s):
    """used to extract select_conditions that are not pushed_down and that
        are not used in push_step1 and push_step2 functions"""
    return isinstance(s.inputs[0], radb.ast.AttrRef) and isinstance(s.inputs[1], radb.ast.AttrRef) and str(s).count(
        '.') == 2


def replace(table, remaining_list, dd):
    """this function used to replace table with the suitable select_obj
                    while parsing the ra object recursiely"""
    L = []
    for cond in remaining_list:
        if isinstance(table, radb.ast.Rename):
            if table.relname is None:
                a = dd[table.inputs[0].rel].get(cond.inputs[0].name, False)
                if a:
                    L.append(cond)
            else:
                if table.relname == cond.inputs[0].rel:
                    a = dd[table.inputs[0].rel].get(cond.inputs[0].name, False)
                    if a:
                        L.append(cond)
        else:
            if cond.inputs[0].rel == None:
                a = dd[table.rel].get(cond.inputs[0].name, False)
                if a:
                    L.append(cond)
            elif table.rel == cond.inputs[0].rel:
                a = dd[table.rel].get(cond.inputs[0].name, False)
                if a:
                    L.append(cond)

    n = len(L)
    if n == 0:
        return table
    if n == 1:
        return radb.ast.Select(L[0], table)
    elif n > 1:
        res = radb.ast.Select(L[0], table)
        for i in range(1, n):
            res = radb.ast.Select(L[i], res)

        return res


def swap(s):
    """swap two consecutive selections"""
    if isinstance(s, radb.ast.Select):
        if isinstance(s.inputs[0], radb.ast.Select):
            return radb.ast.Select(s.inputs[0].cond, radb.ast.Select(s.cond, s.inputs[0].inputs[0]))


def select_rest(rest_list, input):
    """used to add the is_neither(select) selections"""
    res = rest_list[0]
    if len(rest_list) == 1:
        return radb.ast.Select(res, input)
    elif len(rest_list) > 1:
        res_input = radb.ast.Select(res, input)
        for i in range(1, len(rest_list)):
            res = radb.ast.Select(rest_list[i], res_input)
        return res


def push_step1(s_cond_list, cross_list):
    """push only the select_objects used for the cross between tables"""
    if len(s_cond_list) == 0:
        return cross_list[0]
    return radb.ast.Select(s_cond_list[0], radb.ast.Cross(push_step1(s_cond_list[1:], cross_list[1:]), cross_list[0]))


def push_step2(remaining_list, cross_list, dd):
    """push the select_objects that are not used for the cross"""
    if len(cross_list) == 1:
        return replace(cross_list[0], remaining_list, dd)
    return radb.ast.Cross(push_step2(remaining_list, cross_list[1:], dd), replace(
        cross_list[0], remaining_list, dd))


def push_step3(s_cond_list, remaining_list, cross_list, dd):
    """push_step1 and push_step2 combined(problem python doesn't use referencing so
                I am obliged to recreate and return another object"""
    if len(cross_list) == 1:
        return replace(cross_list[0], remaining_list, dd)
    return radb.ast.Select(s_cond_list[0],
                           radb.ast.Cross(push_step3(s_cond_list[1:], remaining_list, cross_list[1:], dd), replace(
                               cross_list[0], remaining_list, dd)))


def push_down_rule_selection(ra, dd):
    """used to push_down selections"""
    s_cond_list, cross_list = split_selection_cross(ra)
    remaining_s_list = remaining_select(s_cond_list)[::-1]
    s_cond_list = [e for e in s_cond_list if e not in remaining_s_list]
    s_cond_list.sort(key=lambda x: tris(x, cross_list))
    rest = [e for e in remaining_s_list if is_neither(e)]
    remaining_s_list = [e for e in remaining_s_list if e not in rest]

    if len(remaining_s_list) == 0 and len(rest) == 0:
        return push_step1(s_cond_list, cross_list)
    elif len(remaining_s_list) == 0 and len(rest) != 0:
        return swap(select_rest(rest, push_step1(s_cond_list, cross_list)))
    elif len(remaining_s_list) != 0 and len(s_cond_list) == 0 and len(rest) == 0:
        return push_step2(remaining_s_list, cross_list, dd)
    elif len(remaining_s_list) != 0 and len(s_cond_list) == 0 and len(rest) != 0:
        return select_rest(rest, push_step2(remaining_s_list, cross_list, dd))
    elif len(remaining_s_list) != 0 and len(s_cond_list) != 0 and len(rest) == 0:
        res = push_step3(s_cond_list, remaining_s_list, cross_list, dd)
        return res
    elif len(remaining_s_list) != 0 and len(s_cond_list) != 0 and len(rest) != 0:
        return swap(select_rest(rest, push_step3(s_cond_list, remaining_s_list, cross_list, dd)))


def merge_select(select_object):
    """merge select_objects"""
    if isinstance(select_object, radb.ast.RelRef):  # it is a simple select or just a table
        return select_object

    valEcprBinaryOp_list = [select_object.cond]
    test_select = select_object.inputs[0]
    if isinstance(test_select, radb.ast.RelRef):  # it is a simple select or just a table
        return select_object

    if not isinstance(test_select, radb.ast.Cross):

        while (isinstance(test_select.inputs[0], radb.ast.Select)):
            valEcprBinaryOp_list.append(test_select.cond)
            test_select = test_select.inputs[0]

        valEcprBinaryOp_list.append(test_select.cond)
        n = len(valEcprBinaryOp_list)
        res = valEcprBinaryOp_list[0]
        for i in range(1, n):
            res = radb.ast.ValExprBinaryOp(res, radb.ast.sym.AND, valEcprBinaryOp_list[i])

        return radb.ast.Select(cond=res, input=test_select.inputs[0])
    else:
        return radb.ast.Select(cond=valEcprBinaryOp_list[0], input=test_select)


def joint_r(object):
    """joint function"""
    if isinstance(object, radb.ast.RelRef):
        return object
    if isinstance(object.inputs[0], radb.ast.Rename):
        return object

    if isinstance(object.inputs[0], radb.ast.RelRef):
        return object
    else:
        if isinstance(object.inputs[0], radb.ast.Cross):
            return radb.ast.Join(joint_r(object.inputs[0].inputs[0]), object.cond, object.inputs[0].inputs[1])
        if isinstance(object.inputs[1], radb.ast.RelRef):
            return radb.ast.Join(joint_r(object.inputs[0]), object.cond, object.inputs[1])


def rule_break_up_selections(ra):
    """break_up selections function"""
    if isinstance(ra, radb.ast.RelRef):
        return ra
    if str(ra).count('and') == 0:
        return ra
    if isinstance(ra, radb.ast.Select):
        return break_select(ra)
    elif isinstance(ra, radb.ast.Project):
        return radb.ast.Project(attrs=ra.attrs, input=break_select(ra.inputs[0]))
    elif isinstance(ra, radb.ast.Cross):
        if isinstance(ra.inputs[0], radb.ast.Select):
            return radb.ast.Cross(break_select(ra.inputs[0]), ra.inputs[1])
        elif isinstance(ra.inputs[1], radb.ast.Select):
            return radb.ast.Cross(ra.inputs[0], break_select(ra.inputs[1]))


def rule_push_down_selections(ra, dd):
    """push_down selections function """
    if isinstance(ra, radb.ast.RelRef):
        return ra
    if cross_number(ra) == 0:
        return ra
    elif isinstance(ra, radb.ast.Project):
        res = radb.ast.Project(ra.attrs, push_down_rule_selection(ra.inputs[0], dd))
        return res
    elif isinstance(ra, radb.ast.Select):
        return push_down_rule_selection(ra, dd)
    else:
        return ra


def rule_merge_selections(ra):
    """merge selections function"""
    if isinstance(ra, radb.ast.RelRef):
        return ra
    if select_number(ra) <= 1:
        return ra
    if isinstance(ra, radb.ast.Select):
        return merge_select(ra)
    if isinstance(ra, radb.ast.Cross):
        return rule_merge_selections_cross(ra)
    if isinstance(ra, radb.ast.Project):
        if isinstance(ra.inputs[0], radb.ast.Select):
            return radb.ast.Project(attrs=ra.attrs, input=merge_select(ra.inputs[0]))
        elif isinstance(ra.inputs[0], radb.ast.Cross):
            return radb.ast.Project(attrs=ra.attrs, input=rule_merge_selections_cross(ra.inputs[0]))


def rule_introduce_joins(ra):
    """join introduce function"""
    if isinstance(ra, radb.ast.RelRef):
        return ra
    if cross_number(ra) == 0:
        return ra
    if select_number(ra) == 0:
        return ra
    if isinstance(ra, radb.ast.Cross):
        return ra
    if isinstance(ra, radb.ast.Project):
        return radb.ast.Project(attrs=ra.attrs, input=joint_r(ra.inputs[0]))
    else:
        return joint_r(ra)


######### Test #########
# dd = {}
# dd["PART"] = {"P_PARTKEY": "int", "P_NAME": "string", "P_MFGR": "string",
#               "P_BRAND": "string", "P_TYPE": "string", "P_SIZE": "int", "P_CONTAINER": "string",
#               "P_RETAILPRICE": "float", "P_COMMENT": "STRING"}
# dd["CUSTOMER"] = {"C_CUSTKEY": "int", "C_NAME": "string", "C_ADDRESS": "string",
#                   "C_NATIONKEY": "int", "C_PHONE": "string", "C_ACCTBAL": "float",
#                   "C_MKTSEGMENT": "string", "C_COMMENT": "string"}
#
# dd["REGION"] = {"R_REGIONKEY": "int", "R_NAME" : "string", "R_COMMENT": "string"}
# dd["ORDERS"] = {"O_ORDERKEY": "int", "O_CUSTKEY": "int", "O_ORDERSTATUS": "string",
#                 "O_TOTALPRICE": "float", "O_ORDERDATE": "string", "O_ORDERPRIORITY": "string",
#                 "O_CLERK": "string", "O_SHIPPRIORITY": "int", "O_COMMENT": "string"}
# dd["LINEITEM"] = {"L_ORDERKEY": "int", "L_PARTKEY": "int", "L_SUPPKEY": "int",
#                   "L_LINENUMBER": "int", "L_QUANTITY": "int", "L_EXTENDEDPRICE": "float",
#                   "L_DISCOUNT": "float", "L_TAX": "float", "L_RETURNFLAG": "string",
#                   "L_LINESTATUS": "string", "L_SHIPDATE": "string", "L_COMMITDATE": "string",
#                   "L_RECEIPTDATE": "string", "L_SHIPINSTRUCT": "string", "L_SHIPMODE": "string",
#                   "L_COMMENT": "string"}
# dd["NATION"] = {"N_NATIONKEY": "int", "N_NAME": "string", "N_REGIONKEY": "int", "N_COMMENT": "string"}
# dd["SUPPLIER"] = {"S_SUPPKEY": "int", "S_NAME": "string", "S_ADDRESS": "string", "S_NATIONKEY": "int",
#                   "S_PHONE": "string", "S_ACCTBAL": "float", "S_COMMENT": "string"}
#
# dd["PARTSUPP"] = {"PS_PARTKEY": "int", "PS_SUPPKEY": "int", "PS_AVAILQTY": "int",
#                   "PS_SUPPLYCOST": "float", "PS_COMMENT": "string"}
#
# stmt = "\select_{(((CUSTOMER.C_CUSTKEY = ORDERS.O_CUSTKEY) and (ORDERS.O_ORDERKEY = LINEITEM.L_ORDERKEY))" \
#        " and (LINEITEM.L_SHIPMODE = 'AIR')) and (CUSTOMER.C_MKTSEGMENT = 'HOUSEHOLD')} " \
#        "((CUSTOMER \cross ORDERS) \cross LINEITEM);"
#
# ra = radb.parse.one_statement_from_string(stmt)
#
# print('-' * 100)
# b = rule_break_up_selections(ra)
# print(b)
# print('-' * 100)
# p = rule_push_down_selections(b, dd)
# print(p)
# print('-' * 100)
# m = rule_merge_selections(p)
# print(m)
# print('-' * 100)
# L = rule_introduce_joins(m)
# print(L)



dd = {}
dd["Person"] = {"name": "string", "age": "integer", "gender": "string"}
dd["Eats"] = {"name": "string", "pizza": "string"}
dd["Serves"] = {"pizzeria": "string", "pizza": "string", "price": "integer"}
dd["Frequents"] = {"name": "string", "pizzeria": "string"}
#
stmt = "\project_{P.name, S.pizzeria} (\select_{((P.name = E.name) and (E.pizza = S.pizza)) " \
       "and (E.pizza = 'mushroom')} (((\\rename_{P: *} Person) \cross (\\rename_{E: *} Eats)) " \
       "\cross (\\rename_{S: *} Serves)));"

ra = radb.parse.one_statement_from_string(stmt)

print('-' * 100)
b = rule_break_up_selections(ra)
print(b)
print('-' * 100)
p = rule_push_down_selections(b, dd)
print(p)
print('-' * 100)
m = rule_merge_selections(p)
print(m)
print('-' * 100)
L = rule_introduce_joins(m)
print(L)