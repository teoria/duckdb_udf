import duckdb
from duckdb.typing import VARCHAR, INTEGER, DOUBLE
from duckdb.typing import DuckDBPyType


def reverse_list_typeless(v):
    return v[::-1]


def add_key_to_struct_typeless(v):
    return {"a": v["a"], "b": len(v["a"])}


def f4_complex_typeless() -> None:
    conn = duckdb.connect(":memory:")
    LIST_TYPE = DuckDBPyType(list[int | str])
    STRUCT_INPUT_TYPE = DuckDBPyType({"a": str})
    STRUCT_OUTPUT_TYPE = DuckDBPyType({"a": str, "b": int})
    conn.create_function("reverse_array", reverse_list_typeless, [LIST_TYPE], LIST_TYPE)
    conn.create_function(
        "add_key_to_struct",
        add_key_to_struct_typeless,
        [STRUCT_INPUT_TYPE],
        STRUCT_OUTPUT_TYPE,
    )
    ret = conn.execute("SELECT reverse_array([1,2,3]) AS result").fetchall()
    print(ret)
    ret = conn.execute("SELECT reverse_array(['a','b','c']) AS result").fetchall()
    print(ret)
    ret = conn.execute("SELECT add_key_to_struct({'a': 'duckdb'}) AS result").fetchall()
    print(ret)



def repeat_str(v: str, n: int) -> str:
    return v * n


def f1_simple() -> None:
    conn = duckdb.connect(":memory:")
    conn.create_function("repeat_str", repeat_str)
    ret = conn.execute("SELECT repeat_str('my_str', 3) AS result").fetchall()
    print(ret)




def repeat_str_typeless(v, n):
    return v * n

 

def f2_explicit() -> None:
    conn = duckdb.connect(":memory:")
    conn.create_function("repeat_str", repeat_str_typeless, [VARCHAR, INTEGER], VARCHAR)
    ret = conn.execute("SELECT repeat_str('my_str', 3) AS result").fetchall()
    print(ret)

def reverse_list(v: list[int | str]) -> list[int | str]:
    return v[::-1]


def add_key_to_struct(v: {"a": str}) -> {"a": str, "b": int}:
    return {"a": v["a"], "b": len(v["a"])}


def f3_complex() -> None:
    conn = duckdb.connect(":memory:")
    conn.create_function("reverse_array", reverse_list)
    conn.create_function("add_key_to_struct", add_key_to_struct)
    ret = conn.execute("SELECT reverse_array([1,2,3]) AS result").fetchall()
    print(ret)
    ret = conn.execute("SELECT reverse_array(['a','b','c']) AS result").fetchall()
    print(ret)
    ret = conn.execute("SELECT add_key_to_struct({'a': 'duckdb'}) AS result").fetchall()
    print(ret)

call_count_vec = 0


def func_with_call_count_vectorized(v: int) -> int:
    print(v)
    global call_count_vec
    call_count_vec += 1
    return [call_count_vec for _ in v]


def v9_vectorization():
    conn = duckdb.connect(":memory:")
    conn.create_function(
        "call_count_helper",
        func_with_call_count_vectorized,
        type="arrow",
        side_effects=True,
    )
    ret = conn.execute(
        "SELECT call_count_helper(i) FROM generate_series(1, 100) s(i)"
    ).fetchall()
    print(ret)  # [(1,), (1,), ..., (1,), (1,)]
    
    
def anomaly(v: int) -> int:
    return v > 50


def f5_anomaly() -> None:
    conn = duckdb.connect(":memory:")
    conn.create_function("anomaly", anomaly)
    ret = conn.execute("SELECT anomaly(100) AS result").fetchall()
    print(ret)
    ret = conn.execute("SELECT anomaly(10) AS result").fetchall()
    print(ret)   
    ret = conn.execute("SELECT 55 as valor, anomaly(valor) AS result").fetchall()
    print(ret)   
    
i 
import duckdb 
if __name__ == "__main__":
    f1_simple()
    # f2_explicit()
    # f3_complex()
    # f4_complex_typeless()
    #f5_anomaly()
    
     
    #v9_vectorization()