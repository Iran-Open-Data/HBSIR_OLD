import hbsir.utils as utils


def test_build_year_interval():
    assert utils.build_year_interval(1396, 1400) == (1396, 1401)


def test_parse_sentence():
    for sentence, parsed_sentence in [
        ["a",               (1, "a")            ],
        ["1*b",             (1, "b")            ],
        ["ab",              (1, "ab")           ],
        ["12*a",            (12, "a")           ],
        ["12a",             (12, "a")           ],
        ["1234variable",    (1234, "variable")  ],
        ["3421*variable",   (3421, "variable")  ],
        ["12.34a",          (12.34, "a")        ],
        ["0.1b",            (0.1, "b")          ],
    ]:
        assert parsed_sentence == utils._parse_sentence(sentence)


def test_parse_expression():
    for expression, parsed_expression in [
        ["a",               [("+", 1, "a")]                                     ],
        ["4a",              [("+", 4, "a")]                                     ],
        ["-24a",            [("-", 24, "a")]                                    ],
        ["+36b",            [("+", 36, "b")]                                    ],
        ["- 65 * var",      [("-", 65, "var")]                                  ],
        ["5 * v a  r",      [("+", 5, "var")]                                   ],
        ["a+b",             [("+", 1, "a"), ("+", 1, "b")]                      ],
        ["1.2a-4.3*b",      [("+", 1.2, "a"), ("-", 4.3, "b")]                  ],
        ["6a+3b",           [("+", 6, "a"), ("+", 3, "b")]                      ],
        ["26a-563* b+ c",   [("+", 26, "a"), ("-", 563, "b"), ("+", 1, "c")]    ],
    ]:
        assert parsed_expression == utils._parse_expression(expression)


def test_build_pandas_expression():
    for expression, parsed_expression in [
        ["a",                       "table['a'].fillna(0)"                                       ],
        ["ab",                      "table['ab'].fillna(0)"                                      ],
        ["a+b",                     "table['a'].fillna(0) + table['b'].fillna(0)"                ],
        ["1 * a",                   "table['a'].fillna(0)"                                       ],
        ["1a",                      "table['a'].fillna(0)"                                       ],
        ["10a",                     "table['a'].fillna(0) * 10"                                  ],
        ["10*a",                    "table['a'].fillna(0) * 10"                                  ],
        ["10 * a",                  "table['a'].fillna(0) * 10"                                  ],
        ["-123 abc",                "- table['abc'].fillna(0) * 123"                             ],
        ["-4321 * cba",             "- table['cba'].fillna(0) * 4321"                            ],
        ["1.2 ab - 3.4*cd",         "table['ab'].fillna(0) * 1.2 - table['cd'].fillna(0) * 3.4"  ],
        ["12 ab - 34cd",            "table['ab'].fillna(0) * 12 - table['cd'].fillna(0) * 34"    ],
        ["12 * a - 15b +c -12  *d", ("table['a'].fillna(0) * 12 - table['b'].fillna(0) * 15 +"
                                     " table['c'].fillna(0) - table['d'].fillna(0) * 12")        ],
        ["Income + Profit",         "table['Income'].fillna(0) + table['Profit'].fillna(0)"      ],
        ["0.001 * Grams + Kilos",   "table['Grams'].fillna(0) * 0.001 + table['Kilos'].fillna(0)"],
    ]:
        assert parsed_expression == utils.build_pandas_expression(expression)
