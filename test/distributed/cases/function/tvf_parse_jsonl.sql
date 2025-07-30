select * from parse_jsonl_data($$[1, 2, 3]
["foo", "bar", "zoo"]
{"foo": 1, "bar": "zoo"}
$$) t;

select * from parse_jsonl_data($$[1, 2, 3]
[4, 5, 6]
[1.1,2.2,3.3]
$$, 'iii'
) t;

select * from parse_jsonl_data($$[1, 2, 3]
[4, 5, 6]
[1.1,2.2,3.3]
$$, 'fIF'
) t;

select * from parse_jsonl_data($$[1, 2, 3]
[4, 5, 6]
[1.1,2.2, {"foo":2,"bar":"zoo"}]
$$, 'sss'
) t;

select * from parse_jsonl_data($$[1, 2, 3]
[4, 5, 6]
[1.1,2.2, {"foo":2,"bar":"zoo"}]
$$, '{"format":"array", "cols":[{"name":"x", "type":"int32"},{"name":"y","type":"float64"},{"name":"z","type":"string"}]}'
) t;

select * from parse_jsonl_data($${"x":1, "z":"zzz", "y":3.14}
{"x":2, "z":"zzz"}
{"x":3, "zzz":"z"}
{"y":2.7183, "zzz":666}
$$, '{"format":"object", "cols":[{"name":"x", "type":"int32"},{"name":"y","type":"float64"},{"name":"z","type":"string"}]}'
) t;

-- error
select * from parse_jsonl_data($$[1, 2, 3]
[4, 5, 6]
[1.1,2.2, {"foo":2,"bar":"zoo"}]
$$, 'six'
) t;

-- error
select * from parse_jsonl_data($$[1, 2, 3]
[4, 5, 6]
[1.1,2.2, {"foo":2,"bar":"zoo"}]
$$, '{"six'
) t;

select * from parse_jsonl_file('$resources/load_data/jsonline_array.jl') t;
select * from parse_jsonl_file('$resources/load_data/jsonline_array.jl.gz') t;
select * from parse_jsonl_file('$resources/load_data/jsonline_array.jl.bz2') t;
