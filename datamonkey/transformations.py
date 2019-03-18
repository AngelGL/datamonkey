import re
import operator
import pandas

from pandas._libs.tslibs.timestamps import Timestamp

equality_operators = {
    'LE': (operator.le, 'less than or equals'),
    'LT': (operator.lt, 'less than'),
    'EQ': (operator.eq, 'equals'),
    'NE': (operator.ne, "doesn't equal"),
    'GT': (operator.gt, 'greater than'),
    'GE': (operator.ge, 'greater than or equals')
}

math_operators = {
    'ADD': (operator.add, 'less than or equals'),
    'SUBTRACT': (operator.sub, 'less than'),
    'MULTIPLY': (operator.mul, 'equals'),
    'DIVIDE': (operator.truediv, "doesn't equal")
}

date_formats = {
    'YYYY-MM-DD': "%Y-%m-%d",
    'MM-DD-YYYY': "%m-%d-%Y",
    'DD-MM-YYYY': "%d-%m-%Y",
    'MM/DD/YYYY': "%D",
    'MMM-DD-YYYY': "%h-%d-%Y",
    'DD-MMM-YYYY': "%d-%h-%Y",
}


# *** Modifiers **
def modify_do_math(value, params):
    #TODO: there is operator.floor_div, which returns an int, and operator.true_div, which returns a float... we should probably call the appropriate func based on field type

    if params['operator'] == 'DIVIDE' and params['value'] == 0:
        # WHY?!
        return 0, None, None

    operator_func = math_operators[params['operator']][0]
    return operator_func(value, params['value']), None, None


def modify_round_number(value, params):
    return round(value, params['precision']), None, None


def modify_trim_string(value, params):
    if len(value) <= params['value']:
        return '', None, None
    else:
        if params['operator'] == 'LEFT':
            return value[params['value']:], None, None
        elif params['operator'] == 'RIGHT':
            return value[:params['value'] * -1], None, None


def modify_remove_whitespace(value, params):
    if params['operator'] == 'LEFT':
        return value.lstrip(), None, None
    elif params['operator'] == 'RIGHT':
        return value.rstrip(), None, None
    elif params['operator'] == 'BOTH':
        return value.strip(), None, None


def modify_change_case(value, params):
    if params['operator'] == 'UPPER':
        return value.upper(), None, None
    elif params['operator'] == 'LOWER':
        return value.lower(), None, None
    elif params['operator'] == 'CAPITALIZE':
        return value.capitalize(), None, None


def modify_remove_substring(value, params):
    # TODO: add operators for first instance, N instances, all instances
    return value.replace(params['value'], ''), None, None


def modify_append_string(value, params):
    if params['operator'] == 'LEFT':
        return str(params['value']) + value, None, None
    elif params['operator'] == 'RIGHT':
        return value + str(params['value']), None, None


def modify_replace_value(value, params):
    return value.replace(params['old_value'], params['new_value']), None, None


def modify_mask_field(value, params):
    # TODO: implement
    pass


def modify_change_date_format(value, params):
    date_value = value if isinstance(value, Timestamp) else Timestamp(value)
    return date_value.strftime(date_formats[params['operator']]), None, None


def modify_cast_type(value, params):
    if params['value'] == 'STRING':
        return str(value), None, None
    elif params['value'] == 'INT':
        return int(value), None, None
    elif params['value'] == 'FLOAT':
        return float(value), None, None
    elif params['value'] == 'DATE':
        return pandas.to_datetime(value), None, None
    elif params['value'] == 'BOOLEAN':
        return bool(value), None, None


# *** Validators ***

def validate_by_range(value, params):
    if params['min'] > value or value > params['max']:
        message = "%d was not between the specified range of %d to %d" % (value, params['min'], params['max'])
        return value, 'ERROR' if params['stopOnInvalid'] else 'WARN', message
    else:
        return value, None, None


def validate_by_date_range(value, params):
    date_value = value if isinstance(value, Timestamp) else Timestamp(value)

    if Timestamp(params['min']) > date_value or date_value > Timestamp(params['max']):
        message = "%s was not between the specified range of %s to %s" % (str(value), params['min'], params['max'])
        return value, 'ERROR' if params['stopOnInvalid'] else 'WARN', message
    else:
        return value, None, None


def validate_by_list(value, params):
    if params['operator'] == "EXCLUDE":
        if value in params['values']:
            message = "'%s' is in the list of prohibited values (%s)" % (value, ', '.join(params["values"]))
            return value, 'ERROR' if params['stopOnInvalid'] else 'WARN', message
        else:
            return value, None, None

    elif params['operator'] == "INCLUDE":
        if value not in params['values']:
            message = "'%s' is not in the list of accepted values (%s)" % (value, ', '.join(params["values"]))
            return value, 'ERROR' if params['stopOnInvalid'] else 'WARN', message
        else:
            return value, None, None


def validate_by_regex(value, params):
    if not re.match(params['value'], value):
        message = "'%s 'did not match the specified regex (%s)" % (value, params['value'])
        return value, 'ERROR' if params['stopOnInvalid'] else 'WARN', message
    else:
        return value, None, None


def validate_by_length(value, params):
    operator_func = equality_operators[params['operator']][0]
    if not operator_func(len(value), params['value']):
        message = "'%s 'did not match the specified length requirements (%s %d characters)" % (value, equality_operators[params['operator']][1], params['value'])
        return value, 'ERROR' if params['stopOnInvalid'] else 'WARN', message
    else:
        return value, None, None


def validate_by_value(value, params):
    operator_func = equality_operators[params['operator']][0]
    if not operator_func(value, params['value']):
        message = "'%s 'did not match the specified value requirements (%s %d)" % (value, equality_operators[params['operator']][1], params['value'])
        return value, 'ERROR' if params['stopOnInvalid'] else 'WARN', message
    else:
        return value, None, None


def validate_by_date_value(value, params):
    date_value = value if isinstance(value, Timestamp) else Timestamp(value)

    operator_func = equality_operators[params['operator']][0]
    if not operator_func(date_value, Timestamp(params['value'])):
        message = "'%s 'did not match the specified value requirements (%s %s)" % (value, equality_operators[params['operator']][1], params['value'])
        return value, 'ERROR' if params['stopOnInvalid'] else 'WARN', message
    else:
        return value, None, None


def validate_by_substring(value, params):
    if params['operator'] == "INCLUDE":
        if value.find(params['value']) == -1:
            message = "'%s 'did not contain the expected value '%s'" % (value, params['value'])
            return value, 'ERROR' if params['stopOnInvalid'] else 'WARN', message
        else:
            return value, None, None

    if params['operator'] == "EXCLUDE":
        if value.find(params['value']) != -1:
            message = "'%s ' contained the invalid value '%s'" % (value, params['value'])
            return value, 'ERROR' if params['stopOnInvalid'] else 'WARN', message
        else:
            return value, None, None


# *** Filters ***

def filter_by_range(value, params):
    return value, "FILTER" if params['min'] > value or value > params['max'] else None, None


def filter_by_date_range(value, params):
    date_value = value if isinstance(value, Timestamp) else Timestamp(value)
    return value, "FILTER" if Timestamp(params['min']) > date_value or date_value > Timestamp(params['max']) else None, None


def filter_by_list(value, params):
    if params['operator'] == 'INCLUDE':
        return value, "FILTER" if value not in params['values'] else None, None

    if params['operator'] == 'EXCLUDE':
        return value, "FILTER" if value in params['values'] else None, None


def filter_by_regex(value, params):
    return value, "FILTER" if not re.match(params['value'], value) else None, None


def filter_by_length(value, params):
    operator_func = equality_operators[params['operator']][0]
    return value, "FILTER" if not operator_func(len(value), params['value']) else None, None


def filter_by_value(value, params):
    operator_func = equality_operators[params['operator']][0]
    return value, "FILTER" if not operator_func(value, params['value']) else None, None


def filter_by_date_value(value, params):
    date_value = value if isinstance(value, Timestamp) else Timestamp(value)
    operator_func = equality_operators[params['operator']][0]
    return value, "FILTER" if not operator_func(date_value, Timestamp(params['value'])) else None, None


def filter_by_substring(value, params):
    if params['operator'] == "INCLUDE":
        return value, "FILTER" if value.find(params['value']) == -1 else None, None
    if params['operator'] == "EXCLUDE":
        return value, "FILTER" if value.find(params['value']) != -1 else None, None


# *** Function Mapping ***

FUNCTION_MAP = {"MODIFY_DO_MATH": modify_do_math,
                "MODIFY_CHANGE_DATE_FORMAT": modify_change_date_format,
                "MODIFY_ROUND_NUMBER": modify_round_number,
                "MODIFY_TRIM_STRING": modify_trim_string,
                "MODIFY_REMOVE_WHITESPACE": modify_remove_whitespace,
                "MODIFY_CHANGE_CASE": modify_change_case,
                "MODIFY_REMOVE_SUBSTRING": modify_remove_substring,
                "MODIFY_APPEND_STRING": modify_append_string,
                "MODIFY_REPLACE_VALUE": modify_replace_value,
                "MODIFY_MASK_FIELD": modify_mask_field,
                "MODIFY_CAST_TYPE": modify_cast_type,
                "VALIDATE_BY_RANGE": validate_by_range,
                "VALIDATE_BY_VALUE": validate_by_value,
                "VALIDATE_BY_DATE_RANGE": validate_by_date_range,
                "VALIDATE_BY_DATE_VALUE": validate_by_date_value,
                "VALIDATE_BY_LIST": validate_by_list,
                "VALIDATE_BY_REGEX": validate_by_regex,
                "VALIDATE_BY_LENGTH": validate_by_length,
                "VALIDATE_BY_SUBSTRING": validate_by_substring,
                "FILTER_BY_RANGE": filter_by_range,
                "FILTER_BY_VALUE": filter_by_value,
                "FILTER_BY_DATE_RANGE": filter_by_date_range,
                "FILTER_BY_DATE_VALUE": filter_by_date_value,
                "FILTER_BY_LIST": filter_by_list,
                "FILTER_BY_REGEX": filter_by_regex,
                "FILTER_BY_SUBSTRING": filter_by_substring,
                "FILTER_BY_LENGTH": filter_by_length}
