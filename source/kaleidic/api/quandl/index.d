module kaleidic.api.quandl.index;

version(KaleidicApiQuandlTest) {
	import unit_threaded;
}

struct TickerMetaEntry
{
	string ticker;
	string quandlTicker;
	string name;
}

TickerMetaEntry[][string] loadTickerMetaEquity(string baseDir, string[] filenames, string[] indexNames)
@safe
{
	import std.file: readText;
	import std.path: buildPath;
	import std.exception: enforce;
	import std.string: splitLines;

	if (indexNames.length==0)
		indexNames=filenames;
	else
		enforce(filenames.length==indexNames.length);

	TickerMetaEntry[][string] ret;
	foreach(i, filename; filenames)
	{
		TickerMetaEntry[] entries;
		auto fileStr = readText(buildPath(baseDir, filename));
		ret[indexNames[i]] = tickerMetaEntries(fileStr.splitLines[1..$]);
	}

	return ret;
}

@("loadTickerMetaEquity")
@safe unittest {

	with(immutable Sandbox()) {

		writeFile("foo.csv",
				  ["ticker,quandlTicker,name",
				   "footicker0,fooquandl0,fooname0",
				   "footicker1,fooquandl1,fooname1"]);
		writeFile("bar.csv",
				  ["ticker,quandlTicker,name",
				   "barticker,barquandl,barname"]);

		const equities = loadTickerMetaEquity(testPath,
											  ["foo.csv", "bar.csv"],
											  ["foo", "bar"]);
		equities.shouldEqual(
			[
				"foo": [
					TickerMetaEntry("footicker0", "fooquandl0", "fooname0"),
					TickerMetaEntry("footicker1", "fooquandl1", "fooname1"),
				],
				"bar": [
					TickerMetaEntry("barticker", "barquandl", "barname")
				],
			]
		);
	}
}

private TickerMetaEntry[] tickerMetaEntries(in string[] lines) @safe pure {
	TickerMetaEntry[] entries;
	foreach(line; lines)
	{
		entries ~= tickerMetaEntry(line);
	}
	return entries;
}

@("tickerMetaEntries")
@safe pure unittest {
	tickerMetaEntries(["foo,quandlfoo,fooname", "bar,quandlbar,barname"]).shouldEqual(
		[
			TickerMetaEntry("foo", "quandlfoo", "fooname"),
			TickerMetaEntry("bar", "quandlbar", "barname"),
		]);
}

private TickerMetaEntry tickerMetaEntry(in string line) @safe pure {
	import std.string: split;

	TickerMetaEntry entry;

	auto splitLine = line.split(',');

	entry.ticker = splitLine[0];
	entry.quandlTicker = splitLine[1];
	entry.name = splitLine[2];

	return entry;
}

@("tickerMetaEntry")
@safe pure unittest {
	tickerMetaEntry("ticker,quandlTicker,name").shouldEqual(
		TickerMetaEntry("ticker", "quandlTicker", "name"));
}

string[] getEquityIndexTickers(TickerMetaEntry[][string] table, string needle) @safe pure
{
	string[] ret;
	foreach(item; table[needle])
		ret ~= item.ticker;
	return ret;
}

@("getEquityIndexTickers")
@safe pure unittest {
	import core.exception: RangeError;
	auto table = [
		"foo": [
			TickerMetaEntry("ticker", "quandlTicker", "name"),
			TickerMetaEntry("leticker", "lequandl", "lename"),
		],
		"bar": [
			TickerMetaEntry("barticker", "barquandl", "barname"),
		],
	];

	getEquityIndexTickers(table, "foo").shouldEqual(["ticker", "leticker"]);
	getEquityIndexTickers(table, "bar").shouldEqual(["barticker"]);
	getEquityIndexTickers(typeof(table).init, "whatever").shouldThrow!RangeError;
}
