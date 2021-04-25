import csv

from celus_nibbler.parsers import all_parsers


def findparser(table, platform):
    plat_OK = [parser for parser in all_parsers if platform in parser.platforms]
    if len(plat_OK) < 1:
        print(f'there is no parser which expects your platform {platform}')

    plat_heur_OK = [parser for parser in plat_OK if parser.heuristic_check(parser(table))]
    if len(plat_heur_OK) < 1:
        print(
            f'there is no parser which heuristics matching your format of the table. Platform is {platform}.'
        )

    plat_heur_metrtitle_OK = [
        parser for parser in plat_heur_OK if parser.metric_title_check(parser(table))
    ]
    if len(plat_heur_metrtitle_OK) < 1:
        print('col with metrics not found')
    elif len(plat_heur_metrtitle_OK) > 1:
        print('more than 1 parser found for your format')
    elif len(plat_heur_metrtitle_OK) == 1:
        parser = plat_heur_metrtitle_OK[0]
        return parser


def findparser_and_parse(file, platform):
    with open(file) as f:
        reader = csv.reader(f)
        table = list(reader)
        parser = findparser(table, platform)
        new_metrics = parser.find_new_metrics(parser(table))
        counter_report = parser.parse(parser(table, platform))
        output = (parser, new_metrics, counter_report)
        # print('\r')
        # print(f'     Parser used: {output[0]}')
        # print(f'     new metrics found: {output[1]}')
        # print('\r')
        # print(output[2])
        return output
