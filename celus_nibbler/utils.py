from datetime import date, timedelta

from jellyfish import porter_stem
from unidecode import unidecode

from celus_nibbler.templates import Content, RelatedTo, Text


def start_month(in_date: date) -> date:
    return in_date.replace(day=1)


def end_month(in_date: date) -> date:
    if in_date.month == 12:
        return date(year=in_date.year + 1, month=1, day=1) - timedelta(days=1)
    else:
        return date(year=in_date.year, month=in_date.month + 1, day=1) - timedelta(days=1)


def colnum_to_colletters(colnum: int) -> str:
    colletters = ""
    while colnum > 0:
        colnum, remainder = divmod(colnum - 1, 26)
        colletters = chr(65 + remainder) + colletters
    return colletters


def format_str(item: str) -> str:
    item = porter_stem(unidecode(item.strip()).lower())
    return item


def content_check(item_to_check: str, control_item: Content) -> bool:

    if isinstance(item_to_check, str):
        item_to_check = format_str(item_to_check)
    elif item_to_check is None:
        pass
    else:
        raise TypeError("item_to_check cannt be other than str or None")

    if control_item is None:
        if item_to_check is None:
            return True
        else:
            return False
    elif control_item.conttype is None:
        raise TypeError('you have to define Content.conttype it cannt be None')

    control_item_content = None
    if isinstance(control_item.content, str):
        control_item_content = format_str(control_item.content)
    elif control_item.content is None:
        pass
    else:
        raise TypeError("control_item.content cannt be other than str or None")

    if control_item.conttype == Text.ISANY:
        return True
    elif control_item.conttype == Text.IS:
        if control_item_content == item_to_check:
            return True
        else:
            return False
    elif control_item.conttype == Text.ISNOT:
        if control_item_content != item_to_check:
            return True
        else:
            return False
    elif control_item.conttype == Text.CONTAINS:
        if control_item_content in item_to_check:
            return True
        else:
            return False
    elif control_item.conttype == Text.STARTSWITH:
        if item_to_check.startswith(control_item_content):
            return True
        else:
            return False
    elif control_item.conttype == Text.ENDSWITH:
        if item_to_check.endswith(control_item_content):
            return True
        else:
            return False
    else:
        raise Exception(
            'The %s is not supperted Content.conttype to check by content_check()',
            control_item.conttype,
        )


def assign_by_relatedto(item, sheet):

    if item is not None:
        if item.relation == RelatedTo.TABLE:
            item_one_for_whole_sheet = sheet.values[item.start_row][item.start_col]
        else:
            item_one_for_whole_sheet = None
        if item.relation == RelatedTo.ROW:
            col_with_items = item.start_col
        else:
            col_with_items = None
        # if item.relation == RelatedTo.COL:
        #     row_with_items = item.start_row
        # else:
        #     row_with_items = None
    else:
        item_one_for_whole_sheet = col_with_items = None  # row_with_items =

    return item_one_for_whole_sheet, col_with_items  # , row_with_items
