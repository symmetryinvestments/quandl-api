module kaleidic.api.quandl.tickers;

version(KaleidicApiQuandlTest) {
	import unit_threaded;
}


string[] getTickers(string dataSourceFileName)
@safe
{
	import std.file: readText;

	immutable filename = dataSourceFileName ~ ".json";
	immutable data = readText(filename);
	return getTickersFromString(data, data.length / 500);
}

version(StdDataJson) {
	string[] getTickersFromString(string data, size_t reserve = 0)
		@safe
	{
		import stdx.data.json: parseJSONStream, JSONParserNodeKind, JSONTokenKind;

		string seriesName = null;
		string lastKey = null;
		int depth = 0;

		string[] ret;
		if(reserve != 0)
			ret.reserve(reserve);

		foreach(entry; parseJSONStream(data))
		{
			switch(entry.kind) with (JSONParserNodeKind) {
				case objectStart:
					++depth;
					break;
				case objectEnd:
					seriesName = null;
					--depth;
					break;
				case arrayStart:
					++depth;
					seriesName = lastKey;
					lastKey=null;
					break;
				case arrayEnd:
					--depth;
					seriesName = null;
					break;
				case key:
					if (seriesName is null)
						seriesName = entry.toString;
					lastKey = entry.key;
					break;
				case literal:
					switch(entry.literal.kind) with (JSONTokenKind) {
						case string:
							if (lastKey == "code")
								ret ~= entry.literal.string.stripQuotes;
							break;
						default:
							break;
						}
					lastKey = null;
					break;
				default:
					break;
				}
		}
		return ret;
	}
} else {

	string[] getTickersFromString(string jsonString, size_t reserve = 0)
		@safe
	{
		import std.json: parseJSON, JSONValue;

		string[] helper(in JSONValue json) @safe {

			import std.algorithm: map;
			import std.array: join;
			import std.json: JSONType;

			switch(json.type) {
				default:
					return [];

				case JSONType.object:
					immutable(char)[][] ret;
					foreach(key, value; () @trusted { return json.object; }()) {
						if(key == "code" && value.type == JSONType.string)
							ret ~= value.str;
						if(value.type == JSONType.object)
							ret ~= helper(value);
					}
					return ret;

				case JSONType.array:
					auto arr = () @trusted { return json.array; }();
					return arr.map!helper.join;
				}
		}

		return helper(parseJSON(jsonString));
	}
}

@("getTickersFromString empty string")
@safe unittest {
	getTickersFromString("").shouldBeEmpty;
}

@("getTickersFromString object")
@safe unittest {
	getTickersFromString(`{"key": "value", "code": "thecode"}`).shouldEqual(
		["thecode"]);

	getTickersFromString(`{"key": "thecode", "code": "huh"}`).shouldEqual(
		["huh"]);

	getTickersFromString(`{"key": "thecode", "code": "huh", "foo": "bar"}`).shouldEqual(
		["huh"]);

	getTickersFromString(`{"key": "value", "bar": "thecode"}`).shouldBeEmpty;
}

@("getTickersFromString array of objects")
@safe unittest {
	getTickersFromString(`[]`).shouldBeEmpty;

	getTickersFromString(`[{"key": "thecode", "code": "huh"}]`).shouldEqual(
		["huh"]);

	getTickersFromString(`[{"key": "thecode", "code": "huh"}, {"key": "k", "code": "c"}]`)
		.shouldEqual(["huh", "c"]);
}

@("getTickersFromString array of nested objects")
@safe unittest {
    getTickersFromString(`
[ {"obj1": {"foo": "bar", "code": "code1"}}, {"obj2": {"code": "code2"}}]
`).shouldEqual(
        [
            "code1",
            "code2",
        ]
    );
}

@("getTickersFromString array of arrays")
@safe unittest {
    getTickersFromString(`[[{"code": "1"}, {"code": "2"}], [{"code": "3"}, {"code": "4"}, {"code": "5"}]]
[ {"obj1": {"foo": "bar", "code": "code1"}}, {"obj2": {"code": "code2"}}]
`).shouldEqual(
        [
            "1", "2", "3", "4", "5",
        ]
    );
}


@("getTickersFromString code to object")
@safe unittest {
    getTickersFromString(`{"code": {"foo": "bar"}}`).shouldBeEmpty;
}

string stripQuotes(string s)
@safe pure
{
	if (s[0] == '\"')
		s = s[1..$];
	if (s[$-1] == '\"')
		s = s[0..$-1];
	return s;
}
