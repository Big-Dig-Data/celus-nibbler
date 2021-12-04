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


def content_check(item_to_check: str, control_item_content: Content) -> bool:
    item_to_check = format_str(item_to_check) if item_to_check is not None else None
    control_item = (
        format_str(control_item_content.content)
        if control_item_content.content is not None
        else None
    )
    control_item_type = control_item_content.type
    if control_item_type == Text.ISANY:
        return True
    elif control_item_type == Text.IS:
        if control_item == item_to_check:
            return True
        else:
            return False
    elif control_item_type == Text.ISNOT:
        if control_item != item_to_check:
            return True
        else:
            return False
    elif control_item_type == Text.CONTAINS:
        if control_item in item_to_check:
            return True
        else:
            return False
    elif control_item_type == Text.STARTSWITH:
        if item_to_check.startswith(control_item):
            return True
        else:
            return False
    elif control_item_type == Text.ENDSWITH:
        if item_to_check.endswith(control_item):
            return True
        else:
            return False
    else:
        raise TypeError(
            'The %s is not supperted Content.type to check by content_check()', control_item_type
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
