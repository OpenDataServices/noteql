from IPython.testing.globalipapp import get_ipython
import noteql
import pytest
from sqlalchemy.exc import OperationalError

ip = get_ipython()
ip.user_ns["session2"] = noteql.Session(dburi="sqlite://")
ip.user_ns["session"] = noteql.Session(dburi="sqlite://")

SIMPLE_QUERY = "SELECT * FROM test where length(atitle) > 1"


def create_test_table():
    dfs = ip.run_cell_magic(
        "nql",
        "CREATE test",
        """
        SELECT 'a' atitle, 'b' btitle
        UNION ALL
        select 'aa', 'bb'
        UNION ALL
        select 'aaa', 'bbb'
    """,
    )
    return dfs


def test_create():
    assert create_test_table() == [None]

    dfs = ip.run_cell_magic("nql", "", SIMPLE_QUERY)
    assert dfs[0].to_dict("list") == {"atitle": ["aa", "aaa"], "btitle": ["bb", "bbb"]}


def test_line_magic_select():
    create_test_table()

    df = ip.run_line_magic("nql", SIMPLE_QUERY)
    assert df.to_dict("list") == {"atitle": ["aa", "aaa"], "btitle": ["bb", "bbb"]}


def test_df_assign():
    create_test_table()

    ip.run_cell_magic("nql", "df=DF", SIMPLE_QUERY)

    assert ip.user_ns["df"].to_dict("list") == {
        "atitle": ["aa", "aaa"],
        "btitle": ["bb", "bbb"],
    }


def test_all_assign():
    create_test_table()

    ip.run_cell_magic(
        "nql",
        "col=COL cols=COLS row=ROW rows=ROWS cell=CELL record=RECORD records=RECORDS headings=HEADINGS sql=SQL",
        SIMPLE_QUERY,
    )

    assert ip.user_ns["headings"] == ["atitle", "btitle"]

    assert ip.user_ns["records"] == [
        {"atitle": "aa", "btitle": "bb"},
        {"atitle": "aaa", "btitle": "bbb"},
    ]
    assert ip.user_ns["record"] == {"atitle": "aa", "btitle": "bb"}

    assert ip.user_ns["rows"] == [["aa", "bb"], ["aaa", "bbb"]]
    assert ip.user_ns["row"] == ["aa", "bb"]
    assert ip.user_ns["cols"] == [["aa", "aaa"], ["bb", "bbb"]]
    assert ip.user_ns["col"] == ["aa", "aaa"]

    assert ip.user_ns["cell"] == "aa"

    assert ip.user_ns["sql"] == SIMPLE_QUERY


def test_basic_params():
    create_test_table()

    ip.user_ns["my_variable"] = "aa"
    ip.user_ns["my_variables"] = ["aa", "aaa"]

    dfs = ip.run_cell_magic("nql", "", SIMPLE_QUERY + " and atitle={{my_variable}}")

    assert dfs[0].to_dict("list") == {"atitle": ["aa"], "btitle": ["bb"]}

    df = ip.run_line_magic("nql", SIMPLE_QUERY + " and atitle={{my_variable}}")

    assert df.to_dict("list") == {"atitle": ["aa"], "btitle": ["bb"]}

    dfs = ip.run_cell_magic("nql", "", SIMPLE_QUERY + " and atitle in {{my_variables | inclause}}")

    assert dfs[0].to_dict("list") == {"atitle": ["aa", "aaa"], "btitle": ["bb", "bbb"]}

    df = ip.run_line_magic("nql", SIMPLE_QUERY + " and atitle in {{my_variables | inclause}}")

    assert df.to_dict("list") == {"atitle": ["aa", "aaa"], "btitle": ["bb", "bbb"]}


def test_multi_sql():
    create_test_table()

    ip.run_cell_magic(
        "nql",
        "col=COL cols=COLS row=ROW rows=ROWS cell=CELL record=RECORD records=RECORDS headings=HEADINGS sql=SQL",
        SIMPLE_QUERY
        + "\n%%nql col2=COL cols2=COLS row2=ROW rows2=ROWS cell2=CELL record2=RECORD records2=RECORDS headings2=HEADINGS sql2=SQL\n"
        + SIMPLE_QUERY,
    )

    assert ip.user_ns["headings2"] == ["atitle", "btitle"]

    assert ip.user_ns["records2"] == [
        {"atitle": "aa", "btitle": "bb"},
        {"atitle": "aaa", "btitle": "bbb"},
    ]
    assert ip.user_ns["record2"] == {"atitle": "aa", "btitle": "bb"}

    assert ip.user_ns["rows2"] == [["aa", "bb"], ["aaa", "bbb"]]
    assert ip.user_ns["row2"] == ["aa", "bb"]
    assert ip.user_ns["cols2"] == [["aa", "aaa"], ["bb", "bbb"]]
    assert ip.user_ns["col2"] == ["aa", "aaa"]

    assert ip.user_ns["cell2"] == "aa"

    assert ip.user_ns["sql"] == SIMPLE_QUERY


def test_multisession():
    create_test_table()

    pytest.raises(OperationalError, ip.run_cell_magic, "nql", "SESSION session2", SIMPLE_QUERY)

    dfs = ip.run_cell_magic("nql", "SESSION session", SIMPLE_QUERY)
    assert dfs[0].to_dict("list") == {"atitle": ["aa", "aaa"], "btitle": ["bb", "bbb"]}
